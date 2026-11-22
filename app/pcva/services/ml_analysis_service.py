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

from app.ccva.services.vman_ml_service import (
    _DEFAULT_MODEL,
    _display_cause,
    _get_cached_predictor,
    predictor_thresholds,
)

# Matches the CCVA "Run CCVA" form's own default (run-ccva.component.ts) so a
# single-record analysis here and a bulk run use the same missing-data
# cutoff. Previously this path applied no threshold at all, silently
# inheriting whatever state the shared cached predictor happened to be in -
# including, briefly, a concurrent bulk run's overridden thresholds.
_DK_THRESHOLD = 0.60
from app.shared.configs.constants import db_collections
from app.shared.middlewares.exceptions import BadRequestException


def _fetch_record(db: StandardDatabase, va_id: str) -> Optional[dict]:
    cursor = db.aql.execute(
        f"FOR doc IN {db_collections.VA_TABLE} FILTER doc.instanceid == @va_id LIMIT 1 RETURN doc",
        bind_vars={"va_id": va_id},
    )
    return next(cursor, None)


def _fetch_dictionary(db: StandardDatabase) -> tuple:
    """Question names (for padding a lone record) and their display labels.

    `q.label` is whatever language that question happened to be authored or
    imported in - for most WHO VA questions that's English, but some groups
    (vital registration/civil registry fields, and the generic ODK meta
    fields like starttime/endtime) have it stored in the deployment's other
    configured language (Khmer, in this deployment) instead. `q.labels` is
    the actual per-language map every question has (confirmed: every
    document checked had a `labels.English` entry, including the ones whose
    bare `label` was Khmer) - reading that instead is what makes this panel
    actually honour the admin panel's "default language: English" setting
    rather than showing whichever language the source form happened to use
    for a given question.
    """
    rows = list(db.aql.execute(
        f"FOR q IN {db_collections.VA_QUESTIONS} RETURN {{name: q.name, label: q.labels.English, fallback: q.label}}"
    ))
    names = [r["name"] for r in rows if r.get("name")]
    labels = {
        r["name"].lower(): (r.get("label") or r.get("fallback"))
        for r in rows if r.get("name") and (r.get("label") or r.get("fallback"))
    }
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


def _row_get_ci(row: pd.Series, name: str):
    """Case-insensitive lookup into a single cleaned_df row."""
    if name in row.index:
        return row[name]
    lname = name.lower()
    for key in row.index:
        if key.lower() == lname:
            return row[key]
    return None


def _top_contributions(
    predictor,
    x_scaled,
    class_index: int,
    labels_by_name: Dict[str, str],
    cleaned_row: pd.Series,
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

    Two quality filters this used to skip, both already solved for the batch
    path's own notes (CCVAPredictor._generate_notes) but never applied here
    - found while checking a real record (uuid:008d9a0c-...) whose top-ranked
    "contributor" was `narr_emb_287`, a narrative-embedding vector dimension
    no coder can act on, and whose third-ranked one (id10080, "What was
    her/his role in the road traffic accident?") had no answer at all for
    this record (not asked, since id10079 was "no"):

    - Narrative embedding dimensions (`narr_emb_*`) are collapsed into one
      aggregate "Free-text description" entry carrying the record's actual
      narrative text, instead of surfacing uninterpretable dimension names
      individually - same threshold CCVAPredictor._NARRATIVE_NOTE_THRESHOLD
      uses for the batch path's notes, so a record with no meaningful
      narrative signal doesn't get a noise entry.
    - A structured variable whose own answer is missing/not-applicable for
      this record (CCVAPredictor._is_informative) is skipped in favour of
      the next-ranked one, same as CCVAPredictor._SUPPRESSED_FEATURES'
      administrative columns (id10010 and friends) - a variable the coder
      cannot even see an answer for is not an explanation they can act on,
      whatever weight XGBoost's attribution assigns it.

    Each returned entry also carries `response`: the record's actual answer
    for that variable, so a coder can judge the explanation themselves
    instead of taking "pushed towards X" on faith.

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
        is_narrative = np.array([n.startswith("narr_emb_") for n in feature_names])

        narrative_entry = None
        total_abs = float(np.sum(np.abs(values)))
        if is_narrative.any() and total_abs > 1e-6:
            narr_values = values[is_narrative]
            narr_frac = float(np.sum(np.abs(narr_values))) / total_abs
            if narr_frac >= predictor._NARRATIVE_NOTE_THRESHOLD:
                narr_signed = float(np.sum(narr_values))
                text = predictor._narrative_text(cleaned_row)
                narrative_entry = {
                    "variable": "narrative_text",
                    "label": "Free-text description",
                    "response": text or "(no narrative text recorded)",
                    "weight": round(narr_signed, 4),
                    "direction": "towards" if narr_signed > 0 else "away from",
                }

        structured_idx = np.where(~is_narrative)[0]
        ranked = structured_idx[np.argsort(-np.abs(values[structured_idx]))]

        out = []
        for index in ranked:
            if len(out) >= top_n:
                break
            weight = float(values[index])
            if weight == 0:
                continue
            raw_name = feature_names[index]
            if raw_name.lower() in predictor._SUPPRESSED_FEATURES:
                continue

            response = _row_get_ci(cleaned_row, raw_name)
            if isinstance(response, float) and pd.isna(response):
                response = None
            if not predictor._is_informative(response):
                continue

            # Encoded columns sometimes carry suffixes (id10120_unit,
            # id10077_a) that are themselves real, separately-dictionaried
            # sub-questions - e.g. id10120_unit is "For how long was (s)he
            # ill before death?" (the units of a duration), a different and
            # more specific question than its bare parent id10120 ("Calculated
            # number of Days with illness"). Try the exact suffixed name
            # first so a sub-question gets its own label rather than silently
            # borrowing its parent's - which previously showed "Calculated
            # number of Days with illness" for a record whose actual answer
            # was "months" (the unit, not a day count), a mismatch a coder
            # has no way to catch without already knowing the raw column
            # names. Only fall back to the bare base name for columns that
            # don't have their own dictionary entry at all.
            base = raw_name.split("_")[0].lower()
            out.append({
                "variable": raw_name,
                "label": labels_by_name.get(raw_name.lower()) or labels_by_name.get(base) or raw_name,
                "response": str(response),
                "weight": round(weight, 4),
                "direction": "towards" if weight > 0 else "away from",
            })

        if narrative_entry:
            out.append(narrative_entry)
            out.sort(key=lambda c: -abs(c["weight"]))
            out = out[:top_n]

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

    with predictor_thresholds(predictor, dk_threshold=_DK_THRESHOLD):
        detailed = predictor.predict_detailed(cleaned)
    if detailed is None or not len(detailed):
        raise BadRequestException("The model returned no prediction for this record.")

    row = detailed.iloc[0].to_dict()

    # Feature attribution for this one record. Affordable here in a way it is
    # not for a whole dataset, and the reason the panel can say *why* rather
    # than only *how confident*.
    #
    # class_index must come from the plausibility-constrained distribution,
    # not raw predict_proba - otherwise a class the guard has already zeroed
    # out for this record (e.g. Neonatal sepsis for a confirmed adult) could
    # still be the one "explained", which would be actively wrong rather than
    # just uninformative. It also won't always match the record's *displayed*
    # cause: predict_detailed can additionally override to "Undetermined" via
    # the OOD/entropy check, which _apply_plausibility_constraints doesn't
    # touch - in that case the contributions below explain the model's
    # strongest concrete-class signal, not literally "why Undetermined" (that
    # would mean explaining high entropy across every class at once, a
    # different and much harder question). row["_contributions_for"] records
    # which class that actually is, so the caller can tell the two apart
    # instead of silently implying the contributions justify the shown cause.
    try:
        import numpy as np
        x_scaled, _ = predictor._prepare_features(cleaned)
        raw_probs = predictor.model.predict_proba(x_scaled)
        adjusted_probs = predictor._apply_plausibility_constraints(raw_probs, cleaned)
        class_index = int(np.argmax(adjusted_probs[0]))
        row["_contributions"] = _top_contributions(
            predictor, x_scaled, class_index, labels_by_name, cleaned.iloc[0]
        )
        explained_label = predictor.label_encoder.inverse_transform(
            [predictor.model.classes_[class_index]]
        )[0]
        row["_contributions_for"] = _display_cause(explained_label)
    except Exception as exc:
        print(f"ML explanation unavailable: {exc}")
        row["_contributions"] = []
        row["_contributions_for"] = None

    return row


def analyse_va_with_ml_sync(va_id: str, db: StandardDatabase, progress=None) -> Dict[str, Any]:
    """Predict the cause of death for one VA record. Blocking.

    Runs in a Celery worker, not in the API process. Inference takes 20-30
    seconds on a developer machine and around five times that on a modest
    server - beyond gunicorn's --timeout 120, which killed the request outright
    with no traceback. The worker also pre-warms the predictor at process init,
    so there it is already in memory.

    :param progress: optional callback(pct, message) for the caller to report
        against; `predict_detailed` drives it through the real stages rather
        than a made-up clock.
    """
    if not (va_id or "").strip():
        raise BadRequestException("A VA record id is required.")

    def _report(pct, message):
        if progress:
            progress(pct, message)

    _report(5, "Loading the VA record...")
    record = _fetch_record(db, va_id)
    if not record:
        raise BadRequestException(f"No VA record was found for {va_id}.")

    # Arango bookkeeping keys are not model features.
    record = {k: v for k, v in record.items() if not k.startswith("_")}

    _report(10, "Preparing the record...")
    question_names, labels_by_name = _fetch_dictionary(db)

    _report(20, "Running the model...")
    row = _predict(record, question_names, labels_by_name)

    _report(95, "Summarising...")
    cause = _display_cause(row.get("prediction"))
    contributions_for = row.get("_contributions_for")
    return {
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
        # Which cause the contributions above actually explain - usually the
        # same as `cause`, but not when the record got flagged "Undetermined"
        # (contributions still explain the model's underlying best concrete
        # guess, not the OOD override itself, which has no per-feature
        # explanation of its own). The frontend uses this to caveat the panel
        # instead of implying these factors justify the displayed cause when
        # they explain a different one.
        "contributions_for": contributions_for,
        "contributions_mismatch": bool(contributions_for) and contributions_for != cause,
        "model": _DEFAULT_MODEL.name,
    }
