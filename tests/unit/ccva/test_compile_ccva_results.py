from datetime import datetime
from unittest.mock import patch

from pandas import DataFrame, Series

from app.ccva.services.ccva_services import compile_ccva_results, compile_ml_csmf_results
from tests.support.fakes import FakeDB


class TestCompileCcvaResultsMalariaHiv:
    # csmf() itself is unrelated to this change (which only threads
    # malaria_status/hiv_status through to the saved document) and expects a
    # real InterVA5 VA5 result shape (15/17 specific columns) that isn't
    # worth reconstructing here - mocked out so these tests exercise only
    # compile_ccva_results' own dict-building and DB-insert logic.
    def _patched_csmf(self):
        return patch(
            "app.ccva.services.ccva_services.csmf",
            return_value=Series(dtype=float),
        )

    def test_saves_the_malaria_and_hiv_settings_the_run_was_started_with(self):
        fake_db = FakeDB()

        with self._patched_csmf():
            result = compile_ccva_results(
                iv5out=object(),
                task_id="t1",
                start_time=datetime.now(),
                rangeDates={"start": None, "end": None},
                error_logs={"task_id": "t1", "error_logs": []},
                db=fake_db,
                malaria_status="h",
                hiv_status="l",
            )

        assert result["malaria_status"] == "h"
        assert result["hiv_status"] == "l"
        saved = fake_db.collection("ccva_graph_results").inserted[0]
        assert saved["malaria_status"] == "h"
        assert saved["hiv_status"] == "l"

    def test_defaults_to_none_when_not_provided(self):
        fake_db = FakeDB()

        with self._patched_csmf():
            result = compile_ccva_results(
                iv5out=object(),
                task_id="t1",
                start_time=datetime.now(),
                rangeDates={"start": None, "end": None},
                error_logs={"task_id": "t1", "error_logs": []},
                db=fake_db,
            )

        assert result["malaria_status"] is None
        assert result["hiv_status"] is None


class TestCompileMlCsmfResultsMalariaHiv:
    def test_saves_the_malaria_and_hiv_settings_the_run_was_started_with(self):
        fake_db = FakeDB()

        result = compile_ml_csmf_results(
            results=[{"CAUSE1": "Stroke", "gender": "male", "age_group": "adult"}],
            task_id="t1",
            total_records=1,
            start_time=datetime.now(),
            user_id="admin",
            date_col=None,
            odk_raw=DataFrame(),
            db=fake_db,
            malaria_status="v",
            hiv_status="h",
        )

        assert result["malaria_status"] == "v"
        assert result["hiv_status"] == "h"
        saved = fake_db.collection("ccva_graph_results").inserted[0]
        assert saved["malaria_status"] == "v"
        assert saved["hiv_status"] == "h"
