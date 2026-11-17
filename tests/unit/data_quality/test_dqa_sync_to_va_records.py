import numpy as np
import pandas as pd

from app.data_quality.services.general_dqa import sync_dqa_scores_to_va_records
from app.shared.configs.constants import db_collections
from tests.support.fakes import FakeDB


async def test_denormalizes_each_record_s_own_score_onto_its_form_submissions_doc():
    # df's own "_key" column IS the matching form_submissions._key directly
    # (this reads the whole VA_TABLE, unlike CCVA's run-scoped subset) - no
    # join/business-key matching needed.
    df = pd.DataFrame({"_key": ["va-1", "va-2"]})
    rrs = pd.Series([80.0, 40.0], index=df.index)
    ics = pd.Series([95.0, np.nan], index=df.index)
    ici = pd.Series([100.0, 60.0], index=df.index)
    aid = pd.Series([45.0, 20.0], index=df.index)

    fake_db = FakeDB()

    count = await sync_dqa_scores_to_va_records(fake_db, df, rrs, ics, aid, ici)

    assert count == 2
    collection = fake_db.collection(db_collections.VA_TABLE)
    assert collection.updated == [
        {"_key": "va-1", "dqa_rrs": 80.0, "dqa_ics": 95.0, "dqa_ici": 100.0, "dqa_aid": 45.0},
        {"_key": "va-2", "dqa_rrs": 40.0, "dqa_ics": None, "dqa_ici": 60.0, "dqa_aid": 20.0},
    ]


async def test_does_nothing_for_an_empty_dataframe():
    df = pd.DataFrame({"_key": pd.Series([], dtype=object)})
    empty = pd.Series([], dtype=float)
    fake_db = FakeDB()

    count = await sync_dqa_scores_to_va_records(fake_db, df, empty, empty, empty, empty)

    assert count == 0
    assert fake_db.collection(db_collections.VA_TABLE).updated == []
