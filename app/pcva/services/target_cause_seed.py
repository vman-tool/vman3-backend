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
from datetime import datetime
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

    # Each level is seeded independently. They used to share one try block, so
    # a failure on any level abandoned the ones after it and reported a single
    # unattributed line - which is how a deployment ended up with the two upper
    # levels present and the specific causes missing, with nothing in the log
    # saying which level had failed or why.
    for key, collection_name in _LEVELS:
        rows = payload.get(key) or []
        if not rows:
            print(f"Target cause list: '{key}' is empty in the resource file")
            continue

        try:
            if not db.has_collection(collection_name):
                try:
                    db.create_collection(collection_name)
                except Exception:
                    # The other worker created it between the check and here.
                    # Harmless, and not worth a stack trace on every boot.
                    pass
            collection = db.collection(collection_name)

            # Only ever seed an empty level. An established deployment may have
            # edited or extended its list, and a startup task must not touch
            # it. A level that is empty is filled even when the others are
            # already populated, so a partially seeded install repairs itself
            # on the next restart.
            existing = collection.count()
            if existing:
                print(f"Target cause list: {collection_name} already has {existing} entries, left alone")
                continue

            # Keyed by the entry's own uuid, and duplicates ignored rather than
            # inserted. gunicorn runs two workers and each runs this startup
            # task, so "count it, then fill it" is a race both can win: without
            # a stable key a fresh deployment would end up with the list twice
            # over. With one, the second writer's rows collide and are dropped.
            # Stamp the bookkeeping fields the API expects. ICD10ResponseClass
            # declares `created_at: str` and `updated_at: Optional[str]` with no
            # default, so in pydantic v2 both must be *present* - a document
            # without them makes /pcva/get-icd10 fail validation and return 500,
            # which is indistinguishable in the UI from an empty cause list.
            # The resource file deliberately carries no timestamps: they belong
            # to the deployment that seeded the rows, not to the list itself.
            stamped_at = datetime.utcnow().isoformat()
            keyed = [
                dict(
                    row,
                    _key=row["uuid"],
                    created_at=row.get("created_at") or stamped_at,
                    updated_at=row.get("updated_at") or stamped_at,
                    created_by=row.get("created_by"),
                    updated_by=row.get("updated_by"),
                    deleted_by=row.get("deleted_by"),
                    deleted_at=row.get("deleted_at"),
                )
                for row in rows if row.get("uuid")
            ]
            if len(keyed) != len(rows):
                print(f"Target cause list: {len(rows) - len(keyed)} entries in '{key}' have no uuid and were skipped")

            collection.insert_many(keyed, overwrite=True, overwrite_mode="ignore")

            stored = collection.count()
            created[collection_name] = stored
            if stored != len(keyed):
                print(
                    f"Target cause list: {collection_name} expected {len(keyed)} "
                    f"entries but holds {stored} after seeding"
                )
        except Exception as exc:
            # Report the level that failed and carry on to the next one, rather
            # than abandoning the rest of the list.
            print(f"Target cause list: FAILED to seed {collection_name}: {type(exc).__name__}: {exc}")

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
