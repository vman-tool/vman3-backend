"""
Run the VMan ML model against a single VA record, for the coding window.

The CCVA module already runs this model over whole datasets as a Celery job.
This is the same model applied to one record, synchronously, so a coder can
ask "what does the model think?" while coding and get an answer in the same
screen.

What is returned, and what it is not
------------------------------------
`CCVAPredictor.predict_detailed` gives a predicted cause together with a
confidence profile: the probability of the predicted class, a 95% Wilson
interval on it, the margin over the runner-up, the normalised entropy across
all classes, and the runner-up itself. That supports an honest account of *how
sure* the model is and *what it nearly said instead*.

Alongside that, `_top_contributions` reports *which inputs* pushed the model
toward the answer, via XGBoost's own TreeSHAP. The `shap` package cannot do
this here at all - see the note on that function - and the batch path leaves it
disabled, but per-record attribution is cheap and is the difference between
"the model is 92% sure" and "it is 92% sure, mainly because of the injury
question".

Both are a second opinion. Neither replaces the coder's judgement, and the
response says so in the caveat the panel displays.
"""

from typing import Any, Dict, Optional

import pandas as pd
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.ccva.services.vman_ml_service import (
    _DEFAULT_MODEL,
    _display_cause,
    _get_cached_predictor,
)
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException


def _fetch_record(db: StandardDatabase, va_id: str) -> Optional[dict]:
    cursor = db.aql.execute(
        f"FOR doc IN {db_collections.VA_TABLE} FILTER doc.instanceid == @va_id LIMIT 1 RETURN doc",
        bind_vars={"va_id": va_id},
    )
    return next(cursor, None)


def _fetch_dictionary(db: StandardDatabase) -> tuple:
    """Question names (for padding a lone record) and their display labels."""
    rows = list(db.aql.execute(
        f"FOR q IN {db_collections.VA_QUESTIONS} RETURN {{name: q.name, label: q.label}}"
    ))
    names = [r["name"] for r in rows if r.get("name")]
    labels = {r["name"].lower(): r.get("label") for r in rows if r.get("name") and r.get("label")}
    return names, labels


def _confidence_band(probability: float) -> str:
    """Plain-language band for the predicted class probability."""
    if probability >= 0.75:
        return "High"
    if probability >= 0.50:
        return "Moderate"
    if probability >= 0.30:
        return "Low"
    return "Very low"


def _build_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    """Turn the raw confidence numbers into something a coder can read.

    Each statement is derived from a figure the model actually produced; none
    of it is inferred about which symptoms mattered, because the model as
    configured here cannot report that.
    """
    probability = float(row.get("pred_probability") or 0.0)
    margin = float(row.get("pred_margin") or 0.0)
    entropy = float(row.get("pred_entropy") or 0.0)
    lower = float(row.get("pred_confidence_lower") or 0.0)
    upper = float(row.get("pred_confidence_upper") or 0.0)
    runner_up = _display_cause(row.get("pred_second_prediction"))

    points = [
        f"The model assigns {probability:.0%} probability to this cause "
        f"(95% confidence interval {lower:.0%}–{upper:.0%})."
    ]

    if runner_up:
        if margin < 0.10:
            points.append(
                f"It is only {margin:.0%} ahead of the next most likely cause, "
                f"{runner_up}. Treat the two as close alternatives."
            )
        else:
            points.append(
                f"It leads the next most likely cause, {runner_up}, by {margin:.0%}."
            )

    if entropy >= 0.70:
        points.append(
            "Probability is spread widely across many causes, so the model is "
            "not settled on any one of them."
        )
    elif entropy <= 0.30:
        points.append("Probability is concentrated on few causes rather than spread thinly.")

    return {
        "confidence_band": _confidence_band(probability),
        "points": points,
        # Stated explicitly so the UI never implies more than the model gave.
        "caveat": (
            "A second opinion from a statistical model, not a substitute for "
            "coding. Contributions show which inputs moved the model, not "
            "clinical causation."
        ),
    }


def _top_contributions(
    predictor,
    x_scaled,
    class_index: int,
    labels_by_name: Dict[str, str],
    top_n: int = 6,
) -> list:
    """Which inputs pushed the model toward this cause, and how hard.

    Uses XGBoost's own TreeSHAP (`pred_contribs=True`) rather than the `shap`
    package. That is not a stylistic preference: shap 0.49's XGBTreeModelLoader
    does `float(learner_model_param["base_score"])`, and a 23-class XGBoost 3.x
    model stores base_score as a 23-element vector, so TreeExplainer raises
    ValueError before it computes anything - for one record exactly as for
    forty thousand. XGBoost computes the same exact TreeSHAP values internally
    with no such problem.

    Returns [] on any failure: an explanation is a bonus, and losing it must
    never cost the prediction itself.
    """
    try:
        import numpy as np
        import xgboost as xgb

        booster = predictor.model.get_booster()
        contribs = booster.predict(xgb.DMatrix(x_scaled), pred_contribs=True)

        # (rows, classes, features+bias) for multi-class; (rows, features+bias)
        # when the model is binary.
        row = contribs[0][class_index] if contribs.ndim == 3 else contribs[0]

        feature_names = list(predictor.scaler_feature_columns)
        values = row[: len(feature_names)]

        ranked = np.argsort(-np.abs(values))[:top_n]

        out = []
        for index in ranked:
            weight = float(values[index])
            if weight == 0:
                continue
            raw_name = feature_names[index]
            # Encoded columns carry suffixes (id10120_unit, id10077_a); the
            # dictionary is keyed by the bare question name.
            base = raw_name.split("_")[0].lower()
            out.append({
                "variable": raw_name,
                "label": labels_by_name.get(base) or labels_by_name.get(raw_name.lower()) or raw_name,
                "weight": round(weight, 4),
                "direction": "towards" if weight > 0 else "away from",
            })
        return out
    except Exception as exc:  # never let explanation failure lose the answer
        print(f"ML explanation unavailable: {exc}")
        return []


def _predict(record: dict, question_names: list, labels_by_name: Dict[str, str]) -> Dict[str, Any]:
    """Run the model over a one-row frame. Executed off the event loop."""
    if not _DEFAULT_MODEL.exists():
        raise BadRequestException(
            "No ML model is installed. Upload one under Settings > Configurations."
        )

    predictor = _get_cached_predictor(_DEFAULT_MODEL)

    from vman_ml.processing import DataPreprocessor

    # Submissions are stored sparsely - unanswered questions are dropped
    # rather than stored empty, so one record carries ~41 columns where the
    # instrument has several hundred. A batch run never notices, because
    # pandas unions the columns across records and the gaps become NaN. On its
    # own a record would instead arrive with those columns *absent*, which is a
    # different input. Padding against the stored dictionary reproduces the
    # batch shape, so a single record is scored the same way as it would be in
    # a full run.
    padded = {name: record.get(name, float("nan")) for name in question_names}
    padded.update(record)

    frame = pd.DataFrame([padded])
    cleaned = DataPreprocessor(verbose=False)._preprocess_data(frame.copy())

    detailed = predictor.predict_detailed(cleaned)
    if detailed is None or not len(detailed):
        raise BadRequestException("The model returned no prediction for this record.")

    row = detailed.iloc[0].to_dict()

    # Feature attribution for this one record. Affordable here in a way it is
    # not for a whole dataset, and the reason the panel can say *why* rather
    # than only *how confident*.
    try:
        import numpy as np
        x_scaled, _ = predictor._prepare_features(cleaned)
        class_index = int(np.argmax(predictor.model.predict_proba(x_scaled)[0]))
        row["_contributions"] = _top_contributions(
            predictor, x_scaled, class_index, labels_by_name
        )
    except Exception as exc:
        print(f"ML explanation unavailable: {exc}")
        row["_contributions"] = []

    return row


async def analyse_va_with_ml(va_id: str, db: StandardDatabase) -> ResponseMainModel:
    """Predict the cause of death for one VA record."""
    if not (va_id or "").strip():
        raise BadRequestException("A VA record id is required.")

    record = await run_in_threadpool(_fetch_record, db, va_id)
    if not record:
        raise BadRequestException(f"No VA record was found for {va_id}.")

    # Arango bookkeeping keys are not model features.
    record = {k: v for k, v in record.items() if not k.startswith("_")}

    question_names, labels_by_name = await run_in_threadpool(_fetch_dictionary, db)
    row = await run_in_threadpool(_predict, record, question_names, labels_by_name)

    cause = _display_cause(row.get("prediction"))
    return ResponseMainModel(
        data={
            "va_id": va_id,
            "cause": cause,
            "raw_label": row.get("prediction"),
            "probability": row.get("pred_probability"),
            "confidence_lower": row.get("pred_confidence_lower"),
            "confidence_upper": row.get("pred_confidence_upper"),
            "margin": row.get("pred_margin"),
            "entropy": row.get("pred_entropy"),
            "second_cause": _display_cause(row.get("pred_second_prediction")),
            "summary": _build_summary(row),
            "contributions": row.get("_contributions") or [],
            "model": _DEFAULT_MODEL.name,
        },
        message=f"Probable cause of death: {cause}",
    )
