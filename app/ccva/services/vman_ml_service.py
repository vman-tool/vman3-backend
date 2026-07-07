"""
VMan ML 1.0 CCVA prediction service.

Wraps the vman_ml package (CCVAPredictor + DataPreprocessor) so it fits
the same call pattern as runCCVA / InterVA5.  Returns a list of dicts
with an 'ID' key (== instance-ID from the ODK form) and cause-of-death
fields that are compatible with getVADataAndMergeWithResults.

vman_ml is installed as a pip package from https://github.com/vman-tool/ccva-ml.
To update the ML code, bump the tag in requirements.txt and rebuild the image.
To update the model, copy the new .pkl into ml_models/ and update model_registry.json.
"""

from __future__ import annotations

import os
import sys
import time
import types
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from app.shared.utils.async_utils import call_update_callback

logger = logging.getLogger(__name__)

# Limit native threading before any sklearn/numpy/shap import.
# SHAP 0.49.x TreeExplainer uses multi-threaded C++ that segfaults on
# Apple Silicon (ARM64) with sklearn 1.7+ in thread contexts (SIGSEGV at 0x580).
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

from vman_ml.processing import DataPreprocessor
from vman_ml.prediction import CCVAPredictor
from vman_ml.instrument_dictionary import detect_instrument_version


def _register_ccva_ml_aliases() -> None:
    """Register ccva_ml.* aliases for model files pickled before the package rename.

    The current ccva_model_combined.pkl was saved when the package was still
    named 'ccva_ml'. joblib/pickle needs the original module paths to unpickle
    the classes. Once the model is retrained with the package named 'vman_ml',
    this function can be removed.
    """
    import importlib
    for sub in ("processing", "prediction", "narrative", "instrument_dictionary",
                "mapcauselist", "label_audit"):
        src = f"vman_ml.{sub}"
        dst = f"ccva_ml.{sub}"
        if dst not in sys.modules:
            try:
                mod = importlib.import_module(src)
                alias = types.ModuleType(dst)
                alias.__dict__.update(mod.__dict__)
                alias.__name__ = dst
                sys.modules[dst] = alias
            except ImportError:
                pass
    if "ccva_ml" not in sys.modules:
        import vman_ml as _vman_ml
        root = types.ModuleType("ccva_ml")
        root.__dict__.update(_vman_ml.__dict__)
        sys.modules["ccva_ml"] = root


_register_ccva_ml_aliases()

# Default model: backend/app/ccva/ml_models/ccva_model_combined.pkl
# Replace this file (and update model_registry.json) after each retrain.
_DEFAULT_MODEL = Path(__file__).parent.parent / "ml_models" / "ccva_model_combined.pkl"


def run_vman_ml(
    odk_raw: pd.DataFrame,
    file_id: str,
    id_col: Optional[str],
    update_callback: Optional[Callable] = None,
    dk_threshold: Optional[float] = None,
    ood_threshold: Optional[float] = None,
    model_path: Optional[str] = None,
    start_time: Optional[datetime] = None,
) -> list[dict]:
    """
    Run VMan ML 1.0 prediction on *odk_raw* and return a list of result dicts
    whose 'ID' field contains the VA instance-ID value from *id_col*.

    Each dict includes:
      ID, CAUSE1, LIK1, CAUSE2, LIK2,
      pred_probability, pred_confidence_lower, pred_confidence_upper,
      pred_entropy, pred_notes, algorithm, task_id
    """
    if start_time is None:
        start_time = datetime.now()
    n_records = len(odk_raw)

    def _elapsed() -> str:
        s = (datetime.now() - start_time).seconds
        return f"{s // 3600}:{(s // 60) % 60:02d}:{s % 60:02d}"

    def _progress(pct: int, msg: str, log: Optional[str] = None) -> None:
        if update_callback:
            payload = {
                "progress": pct,
                "message": msg,
                "status": "running",
                "task_id": file_id,
                "elapsed_time": _elapsed(),
                "error": False,
                "total_records": n_records,
            }
            if log:
                payload["log"] = log
            call_update_callback(update_callback, payload)

    # ── 1. Validate model path ────────────────────────────────────────────────
    resolved_model = Path(model_path) if model_path else _DEFAULT_MODEL
    if not resolved_model.exists():
        raise FileNotFoundError(f"VMan ML model not found: {resolved_model}")

    # ── 2. Preprocess ─────────────────────────────────────────────────────────
    _progress(10, f"VMan ML 1.0: preprocessing {n_records} records...",
              log=f"VMan ML 1.0 | preprocessing {n_records} VA records")
    t_preprocess = time.perf_counter()

    preprocessor = DataPreprocessor(verbose=False)

    def _preprocess_progress(pct: int) -> None:
        # Map 0-100 from change_null_toskipped → overall 10-20%
        _progress(10 + int(pct * 0.10), f"VMan ML 1.0: preprocessing... ({pct}%)")

    df = preprocessor._preprocess_data(odk_raw.copy(), progress_callback=_preprocess_progress)
    t_preprocess = time.perf_counter() - t_preprocess

    # ── 3. Detect instrument version ──────────────────────────────────────────
    t_detect = time.perf_counter()
    detection = detect_instrument_version(df)
    version = detection.get("version", "unknown") if isinstance(detection, dict) else "unknown"
    t_detect = time.perf_counter() - t_detect
    _progress(22, "VMan ML 1.0: detecting instrument version...",
              log=(f"VMan ML 1.0 | preprocess: {t_preprocess:.1f}s | "
                   f"instrument version detected: {version} ({t_detect:.2f}s)"))

    # ── 4. Load model ─────────────────────────────────────────────────────────
    t_load = time.perf_counter()
    _progress(30, "VMan ML 1.0: loading model...",
              log=f"VMan ML 1.0 | loading model from {resolved_model.name}")
    predictor = CCVAPredictor(str(resolved_model), verbose=False)
    t_load = time.perf_counter() - t_load

    # Disable SHAP: shap 0.49.x + sklearn 1.7+ causes SIGSEGV on ARM64 in
    # multi-threaded C++ (thread crash at 0x0580). pred_notes will be empty.
    predictor._get_shap_explainer = lambda: None

    model_name = type(predictor.model).__name__
    n_classes = len(predictor.original_classes)
    n_features = len(predictor.expected_columns)
    _progress(35, "VMan ML 1.0: model ready.",
              log=(f"VMan ML 1.0 | model loaded in {t_load:.1f}s | "
                   f"{model_name} | {n_features} features | {n_classes} cause classes | "
                   f"DK threshold: {predictor.dk_threshold:.0%} | "
                   f"OOD entropy > {predictor.ood_entropy_threshold:.3f}"))

    # ── 5. Apply threshold overrides ─────────────────────────────────────────
    if ood_threshold is not None and 0 < ood_threshold < 1:
        predictor.ood_threshold = ood_threshold
        predictor.ood_entropy_threshold = None
        _progress(36, "VMan ML 1.0: applying threshold overrides.",
                  log=f"VMan ML 1.0 | OOD threshold overridden to {ood_threshold}")

    if dk_threshold is not None and 0 < dk_threshold <= 1:
        predictor.dk_threshold = dk_threshold
        _progress(37, "VMan ML 1.0: applying threshold overrides.",
                  log=f"VMan ML 1.0 | DK threshold overridden to {dk_threshold:.0%}")

    # ── 6. Predict ────────────────────────────────────────────────────────────
    t_predict = time.perf_counter()
    _progress(40, "VMan ML 1.0: running predictions...",
              log=f"VMan ML 1.0 | running predictions on {n_records} records...")
    pred_df = predictor.predict_detailed(df)
    t_predict = time.perf_counter() - t_predict

    # ── 7. Log OOD / DK / classified counts ──────────────────────────────────
    n_ood = int((pred_df["prediction"] == "out_of_distribution").sum())
    n_classified = int((pred_df["prediction"] != "out_of_distribution").sum())
    _progress(80, "VMan ML 1.0: formatting results...",
              log=(f"VMan ML 1.0 | predictions done in {t_predict:.1f}s | "
                   f"classified: {n_classified} | out-of-distribution: {n_ood} | "
                   f"total: {n_records}"))

    # ── 8. Map to InterVA5-compatible result dicts ────────────────────────────
    id_field = id_col or "instanceid"
    if id_field not in odk_raw.columns:
        id_series = pd.Series(odk_raw.index.astype(str), index=odk_raw.index, name=id_field)
    else:
        id_series = odk_raw[id_field].reindex(pred_df.index)

    records = []
    for idx, row in pred_df.iterrows():
        va_id = id_series.get(idx, str(idx))
        pred = row.get("prediction", "Undetermined")
        prob = float(row.get("pred_probability", 0.0)) if pd.notna(row.get("pred_probability")) else 0.0

        records.append({
            "ID":                      va_id,
            "CAUSE1":                  pred,
            "LIK1":                    round(prob * 100, 2),
            "CAUSE2":                  row.get("pred_second_prediction") or "",
            "LIK2":                    None,
            "pred_probability":        prob,
            "pred_confidence_lower":   row.get("pred_confidence_lower"),
            "pred_confidence_upper":   row.get("pred_confidence_upper"),
            "pred_entropy":            row.get("pred_entropy"),
            "pred_notes":              row.get("pred_notes", ""),
            "algorithm":               "VManML10",
            "task_id":                 file_id,
        })

    logger.info(f"VMan ML produced {len(records)} predictions for task {file_id}")
    _progress(90, f"VMan ML 1.0: complete ({len(records)} predictions).",
              log=(f"VMan ML 1.0 | pipeline complete | "
                   f"{len(records)} predictions | elapsed {_elapsed()}"))
    return records
