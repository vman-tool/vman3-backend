"""
Seed the WHO target cause list on a fresh installation.

PCVA coding assigns a cause from this list, and the vman_ml package predicts
against the same one. It lives across three collections, each level pointing at
the one above:

    icd10_category_type   Broad group    Group I: Communicable, ...
      icd10_category      Major cause    Infectious and parasitic diseases, ...
        icd10             Specific cause VAs-01.01 Sepsis, ...

Until now nothing created it. A deployment that started from an empty database
- which `docker compose down -v` guarantees - had no causes to code against,
and the only way to get them was to copy the collections from another instance.

The uuids in the resource file are part of the data, not incidental. A coded VA
stores an icd10 uuid in frameA, and the two lower levels reference the level
above by uuid, so regenerating them would orphan existing codings and break
comparison between deployments. They are inserted exactly as shipped.
"""

import json
from pathlib import Path

from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections

# Inside the app package, like the WHO xForm: the Dockerfile only copies `app`,
# and resolving from __file__ keeps it independent of the working directory.
TARGET_CAUSE_LIST = (
    Path(__file__).resolve().parents[2] / "resources" / "who_target_cause_list.json"
)

# Ordered parent-first, so a level never references one that does not exist yet.
_LEVELS = (
    ("icd10_category_type", db_collections.ICD10_CATEGORY_TYPE),
    ("icd10_category", db_collections.ICD10_CATEGORY),
    ("icd10", db_collections.ICD10),
)


def _seed_sync(db: StandardDatabase) -> dict:
    if not TARGET_CAUSE_LIST.exists():
        print(f"Target cause list not found at {TARGET_CAUSE_LIST}; skipping seed")
        return {}

    payload = json.loads(TARGET_CAUSE_LIST.read_text(encoding="utf-8"))
    created = {}

    for key, collection_name in _LEVELS:
        rows = payload.get(key) or []
        if not rows:
            continue

        if not db.has_collection(collection_name):
            db.create_collection(collection_name)
        collection = db.collection(collection_name)

        # Only ever seed an empty level. An established deployment may have
        # edited or extended its list, and a startup task must not touch it.
        if collection.count():
            continue

        collection.insert_many(rows, overwrite=False)
        created[collection_name] = len(rows)

    return created


async def seed_target_cause_list_if_empty(db: StandardDatabase) -> dict:
    """Create the target cause list when a level is empty. Never overwrites.

    Returns the number of documents created per collection; an empty dict when
    everything was already present, which is the normal case after first boot.
    """
    from fastapi.concurrency import run_in_threadpool

    try:
        created = await run_in_threadpool(_seed_sync, db)
        if created:
            summary = ", ".join(f"{n} {name}" for name, n in created.items())
            print(f"Seeded WHO target cause list: {summary}")
        return created
    except Exception as exc:  # seeding must never block application startup
        print(f"Target cause list seeding skipped: {exc}")
        return {}
