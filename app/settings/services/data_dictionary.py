import io
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from arango.database import StandardDatabase
from starlette.concurrency import run_in_threadpool

from app.shared.configs.constants import db_collections

VERSION_COLLECTION_MAP = {
    "2022": db_collections.DATA_DICT_2022,
    "2016": db_collections.DATA_DICT_2016,
}

METADATA_KEY = "__metadata"


def _get_collection_name(version: str) -> str:
    col = VERSION_COLLECTION_MAP.get(version)
    if not col:
        raise ValueError(f"Invalid version '{version}'. Must be '2022' or '2016'.")
    return col


def parse_who_questionnaire(file_contents: bytes) -> Dict[str, Any]:
    sheets = pd.read_excel(
        io.BytesIO(file_contents), sheet_name=None, engine="openpyxl"
    )

    sheet_map = {k.lower(): k for k in sheets.keys()}

    if "survey" not in sheet_map:
        raise ValueError("Missing 'survey' sheet in the uploaded questionnaire.")
    if "choices" not in sheet_map:
        raise ValueError("Missing 'choices' sheet in the uploaded questionnaire.")

    df_survey = sheets[sheet_map["survey"]]
    df_choices = sheets[sheet_map["choices"]]

    survey_cols = {c.lower(): c for c in df_survey.columns}
    if "name" not in survey_cols:
        raise ValueError("Missing 'name' column in survey sheet.")
    if "type" not in survey_cols:
        raise ValueError("Missing 'type' column in survey sheet.")

    name_col = survey_cols["name"]
    type_col = survey_cols["type"]
    order_col = survey_cols.get("order")

    survey_label_cols = [c for c in df_survey.columns if c.startswith("label::")]
    languages = [c.replace("label::", "") for c in survey_label_cols]

    choices_label_cols = [c for c in df_choices.columns if c.startswith("label::")]
    for c in choices_label_cols:
        lang = c.replace("label::", "")
        if lang not in languages:
            languages.append(lang)

    choices_by_list: Dict[str, List[Dict[str, Any]]] = {}
    if "list_name" in {c.lower() for c in df_choices.columns}:
        list_name_col = next(
            c for c in df_choices.columns if c.lower() == "list_name"
        )
        choices_name_col = next(
            (c for c in df_choices.columns if c.lower() == "name"), None
        )

        for _, row in df_choices.iterrows():
            ln = str(row[list_name_col]).strip()
            if not ln or ln == "nan":
                continue

            choice: Dict[str, Any] = {
                "name": str(row[choices_name_col]).strip() if choices_name_col else "",
                "labels": {},
            }
            for lc in choices_label_cols:
                val = row[lc]
                choice["labels"][lc.replace("label::", "")] = (
                    "" if pd.isna(val) else str(val)
                )

            choices_by_list.setdefault(ln, []).append(choice)

    questions: List[Dict[str, Any]] = []
    skip_types = {"begin group", "end group", "begin_group", "end_group"}
    total_select = 0
    row_order = 0

    for _, row in df_survey.iterrows():
        raw_type = str(row[type_col]).strip()
        if not raw_type or raw_type == "nan":
            continue
        if raw_type.lower() in skip_types:
            continue

        q_name = str(row[name_col]).strip()
        if not q_name or q_name == "nan":
            continue

        row_order += 1
        parts = raw_type.split()
        q_type = parts[0]
        list_name = parts[1] if len(parts) >= 2 else None

        order_val = row_order
        if order_col is not None:
            raw_order = row[order_col]
            if not pd.isna(raw_order):
                try:
                    order_val = int(raw_order)
                except (ValueError, TypeError):
                    pass

        labels: Dict[str, str] = {}
        for lc in survey_label_cols:
            val = row[lc]
            labels[lc.replace("label::", "")] = "" if pd.isna(val) else str(val)

        choices: List[Dict[str, Any]] = []
        if list_name and raw_type.lower().startswith("select_"):
            choices = choices_by_list.get(list_name, [])
            total_select += 1

        question: Dict[str, Any] = {
            "name": q_name,
            "type": q_type,
            "order": order_val,
            "labels": labels,
            "choices": choices,
        }
        if list_name:
            question["list_name"] = list_name

        questions.append(question)

    return {
        "languages": languages,
        "questions": questions,
        "total_questions": len(questions),
        "total_select_questions": total_select,
    }


def _merge_labels(existing: Dict[str, str], new: Dict[str, str]) -> Dict[str, str]:
    merged = dict(existing)
    for lang, val in new.items():
        if lang not in merged or not merged[lang]:
            merged[lang] = val
    return merged


def _merge_choices(
    existing: List[Dict[str, Any]], new: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    existing_map = {c["name"]: c for c in existing}
    for nc in new:
        if nc["name"] in existing_map:
            existing_map[nc["name"]]["labels"] = _merge_labels(
                existing_map[nc["name"]].get("labels", {}), nc.get("labels", {})
            )
        else:
            existing_map[nc["name"]] = nc
    return list(existing_map.values())


async def upload_and_merge(
    file_contents: bytes,
    version: str,
    questionnaire_name: Optional[str],
    db: StandardDatabase,
) -> Dict[str, Any]:
    collection_name = _get_collection_name(version)
    parsed = parse_who_questionnaire(file_contents)
    new_languages = parsed["languages"]
    new_questions = parsed["questions"]

    if not db.has_collection(collection_name):
        db.create_collection(collection_name)

    collection = db.collection(collection_name)

    # --- metadata ---
    try:
        metadata = collection.get(METADATA_KEY)
    except Exception:
        metadata = None

    if metadata is None:
        metadata = {
            "_key": METADATA_KEY,
            "languages": [],
            "questionnaires": [],
        }

    for lang in new_languages:
        if lang not in metadata["languages"]:
            metadata["languages"].append(lang)

    q_info = {
        "name": questionnaire_name or f"Questionnaire ({version})",
        "uploaded_at": datetime.now().isoformat(),
        "languages": new_languages,
    }
    metadata["questionnaires"].append(q_info)

    def _do_merge():
        # upsert metadata
        if collection.has(METADATA_KEY):
            collection.update(metadata)
        else:
            collection.insert(metadata)

        # get current max order
        cursor = db.aql.execute(
            f"FOR doc IN @@col FILTER doc._key != @meta SORT doc.order DESC LIMIT 1 RETURN doc.order",
            bind_vars={"@col": collection_name, "meta": METADATA_KEY},
        )
        max_order_list = [doc for doc in cursor]
        max_order = max_order_list[0] if max_order_list else 0
        if max_order is None:
            max_order = 0

        inserted = 0
        updated = 0

        for q in new_questions:
            doc_key = q["name"]
            existing = None
            try:
                existing = collection.get(doc_key)
            except Exception:
                pass

            if existing:
                existing["labels"] = _merge_labels(
                    existing.get("labels", {}), q.get("labels", {})
                )
                if q.get("choices"):
                    existing["choices"] = _merge_choices(
                        existing.get("choices", []), q["choices"]
                    )
                collection.update(existing)
                updated += 1
            else:
                max_order += 1
                doc = {
                    "_key": doc_key,
                    "name": q["name"],
                    "type": q["type"],
                    "order": q.get("order", max_order),
                    "labels": q.get("labels", {}),
                    "choices": q.get("choices", []),
                }
                if q.get("list_name"):
                    doc["list_name"] = q["list_name"]
                collection.insert(doc)
                inserted += 1

        return {"inserted": inserted, "updated": updated}

    result = await run_in_threadpool(_do_merge)

    return {
        "languages": metadata["languages"],
        "questionnaires": metadata["questionnaires"],
        "inserted": result["inserted"],
        "updated": result["updated"],
        "total_processed": len(new_questions),
    }


async def get_questions(version: str, db: StandardDatabase) -> Dict[str, Any]:
    collection_name = _get_collection_name(version)

    if not db.has_collection(collection_name):
        return {
            "languages": [],
            "questions": [],
            "questionnaires": [],
            "total_questions": 0,
            "total_select_questions": 0,
        }

    def _do_fetch():
        collection = db.collection(collection_name)

        metadata = None
        try:
            metadata = collection.get(METADATA_KEY)
        except Exception:
            pass

        languages = metadata.get("languages", []) if metadata else []
        questionnaires = metadata.get("questionnaires", []) if metadata else []

        cursor = db.aql.execute(
            "FOR doc IN @@col FILTER doc._key != @meta SORT doc.order ASC RETURN doc",
            bind_vars={"@col": collection_name, "meta": METADATA_KEY},
        )
        questions = []
        total_select = 0
        for doc in cursor:
            doc.pop("_id", None)
            doc.pop("_rev", None)
            doc.pop("_key", None)
            if doc.get("type", "").startswith("select_"):
                total_select += 1
            questions.append(doc)

        return {
            "languages": languages,
            "questions": questions,
            "questionnaires": questionnaires,
            "total_questions": len(questions),
            "total_select_questions": total_select,
        }

    return await run_in_threadpool(_do_fetch)


async def save_all_questions(
    version: str, questions: List[Dict[str, Any]], db: StandardDatabase
) -> Dict[str, Any]:
    collection_name = _get_collection_name(version)
    collection = db.collection(collection_name)

    def _do_save():
        saved = 0
        for q in questions:
            doc_key = q["name"]
            doc = {
                "_key": doc_key,
                "name": q["name"],
                "type": q["type"],
                "order": q.get("order", 0),
                "labels": q.get("labels", {}),
                "choices": q.get("choices", []),
            }
            if q.get("list_name"):
                doc["list_name"] = q["list_name"]

            if collection.has(doc_key):
                collection.update(doc)
            else:
                collection.insert(doc)
            saved += 1

        # update metadata languages based on all saved labels
        all_langs = set()
        for q in questions:
            all_langs.update(q.get("labels", {}).keys())
            for ch in q.get("choices", []):
                all_langs.update(ch.get("labels", {}).keys())

        if all_langs:
            try:
                metadata = collection.get(METADATA_KEY)
            except Exception:
                metadata = {"_key": METADATA_KEY, "languages": [], "questionnaires": []}

            for lang in all_langs:
                if lang not in metadata["languages"]:
                    metadata["languages"].append(lang)

            if collection.has(METADATA_KEY):
                collection.update(metadata)
            else:
                collection.insert(metadata)

        return saved

    saved = await run_in_threadpool(_do_save)
    return {"saved": saved}


async def add_question(
    version: str, question_data: Dict[str, Any], db: StandardDatabase
) -> Dict[str, Any]:
    collection_name = _get_collection_name(version)
    collection = db.collection(collection_name)

    def _do_add():
        doc_key = question_data["name"]
        if collection.has(doc_key):
            raise ValueError(f"Question '{doc_key}' already exists.")

        cursor = db.aql.execute(
            f"FOR doc IN @@col FILTER doc._key != @meta SORT doc.order DESC LIMIT 1 RETURN doc.order",
            bind_vars={"@col": collection_name, "meta": METADATA_KEY},
        )
        max_order_list = [doc for doc in cursor]
        max_order = max_order_list[0] if max_order_list else 0
        if max_order is None:
            max_order = 0

        doc = {
            "_key": doc_key,
            "name": question_data["name"],
            "type": question_data["type"],
            "order": max_order + 1,
            "labels": question_data.get("labels", {}),
            "choices": question_data.get("choices", []),
        }
        if question_data.get("list_name"):
            doc["list_name"] = question_data["list_name"]

        collection.insert(doc)

        # update metadata languages
        new_langs = list(question_data.get("labels", {}).keys())
        if new_langs:
            try:
                metadata = collection.get(METADATA_KEY)
            except Exception:
                metadata = {"_key": METADATA_KEY, "languages": [], "questionnaires": []}

            changed = False
            for lang in new_langs:
                if lang not in metadata["languages"]:
                    metadata["languages"].append(lang)
                    changed = True
            if changed:
                if collection.has(METADATA_KEY):
                    collection.update(metadata)
                else:
                    collection.insert(metadata)

        doc.pop("_key", None)
        return doc

    return await run_in_threadpool(_do_add)


async def update_question(
    version: str,
    question_name: str,
    updates: Dict[str, Any],
    db: StandardDatabase,
) -> Dict[str, Any]:
    collection_name = _get_collection_name(version)
    collection = db.collection(collection_name)

    def _do_update():
        existing = collection.get(question_name)
        if not existing:
            raise ValueError(f"Question '{question_name}' not found.")

        if "name" in updates and updates["name"] != question_name:
            new_name = updates["name"]
            if collection.has(new_name):
                raise ValueError(f"Question '{new_name}' already exists.")
            existing["name"] = new_name

        if "type" in updates:
            existing["type"] = updates["type"]
        if "labels" in updates:
            existing["labels"] = updates["labels"]
        if "choices" in updates:
            existing["choices"] = updates["choices"]
        if "list_name" in updates:
            existing["list_name"] = updates["list_name"]

        collection.update(existing)

        if "name" in updates and updates["name"] != question_name:
            new_doc = dict(existing)
            new_doc["_key"] = updates["name"]
            new_doc["name"] = updates["name"]
            new_doc.pop("_id", None)
            new_doc.pop("_rev", None)
            collection.delete(question_name)
            collection.insert(new_doc)
            return new_doc

        existing.pop("_id", None)
        existing.pop("_rev", None)
        existing.pop("_key", None)
        return existing

    return await run_in_threadpool(_do_update)
