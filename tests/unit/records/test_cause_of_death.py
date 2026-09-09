from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.records.services.cause_of_death import get_va_cause_of_death
from tests.support.fakes import FakeCursor, FakeDB


def _pcva_settings(concordance_level=2):
    return SimpleNamespace(concordanceLevel=concordance_level)


@pytest.mark.asyncio
async def test_ccva_returns_none_when_no_default_run_is_set():
    # Real AQL: the top-level `FILTER default_run != null` guard produces zero
    # result rows (not a row of nulls) when no default run is set.
    db = FakeDB(lambda query, bind_vars: FakeCursor([]))

    with patch("app.records.services.cause_of_death.fetch_pcva_settings", new=AsyncMock(return_value=_pcva_settings())):
        result = await get_va_cause_of_death("va-1", include_ccva=True, include_pcva=False, db=db)

    assert result.data["ccva"] is None


@pytest.mark.asyncio
async def test_ccva_returns_none_when_default_run_has_no_result_for_this_record():
    def responder(query, bind_vars):
        if "ccva_graph_results" in query:
            return FakeCursor([])
        return FakeCursor([])

    db = FakeDB(responder)

    result = await get_va_cause_of_death("va-1", include_ccva=True, include_pcva=False, db=db)

    assert result.data["ccva"] is None


@pytest.mark.asyncio
async def test_ccva_returns_cause_and_probability_from_the_default_run():
    def responder(query, bind_vars):
        return FakeCursor([{
            "algorithm": "VManML10",
            "cause1": "Sepsis",
            "probability": 78.5,
        }])

    db = FakeDB(responder)

    result = await get_va_cause_of_death("va-1", include_ccva=True, include_pcva=False, db=db)

    assert result.data["ccva"] == {
        "algorithm": "VManML10",
        "cause1": "Sepsis",
        "probability": 78.5,
    }


@pytest.mark.asyncio
async def test_ccva_not_fetched_when_include_ccva_is_false():
    db = FakeDB(lambda query, bind_vars: FakeCursor([{"algorithm": "InterVA5", "cause1": "x", "probability": None}]))

    result = await get_va_cause_of_death("va-1", include_ccva=False, include_pcva=False, db=db)

    assert result.data["ccva"] is None
    assert db.aql.queries == []


@pytest.mark.asyncio
async def test_ccva_scoped_to_a_task_id_ignores_which_run_is_marked_default():
    # CCVA > Display Data always knows exactly which run it's showing - the
    # popup should reflect that run's own cause, not whichever run (if any)
    # happens to be marked default, which may well be a different one.
    def responder(query, bind_vars):
        assert bind_vars == {"va_id": "va-1", "task_id": "run-42"}
        assert "g.task_id == @task_id" in query
        assert "r.task_id == @task_id" in query
        assert "isDefault" not in query
        return FakeCursor([{"algorithm": "InterVA5", "cause1": "Malaria", "probability": 91.0}])

    db = FakeDB(responder)

    result = await get_va_cause_of_death("va-1", include_ccva=True, include_pcva=False, db=db, task_id="run-42")

    assert result.data["ccva"] == {"algorithm": "InterVA5", "cause1": "Malaria", "probability": 91.0}


@pytest.mark.asyncio
async def test_ccva_scoped_to_a_task_id_returns_none_when_that_run_has_no_result_for_this_record():
    db = FakeDB(lambda query, bind_vars: FakeCursor([]))

    result = await get_va_cause_of_death("va-1", include_ccva=True, include_pcva=False, db=db, task_id="run-42")

    assert result.data["ccva"] is None


@pytest.mark.asyncio
async def test_ccva_without_a_task_id_still_falls_back_to_the_default_run():
    def responder(query, bind_vars):
        assert bind_vars == {"va_id": "va-1"}
        assert "g.isDefault == true" in query
        return FakeCursor([{"algorithm": "VManML10", "cause1": "Sepsis", "probability": 78.5}])

    db = FakeDB(responder)

    result = await get_va_cause_of_death("va-1", include_ccva=True, include_pcva=False, db=db, task_id=None)

    assert result.data["ccva"]["algorithm"] == "VManML10"


@pytest.mark.asyncio
async def test_pcva_returns_none_when_no_coder_has_coded_this_va():
    db = FakeDB(lambda query, bind_vars: FakeCursor([]))

    with patch("app.records.services.cause_of_death.fetch_pcva_settings", new=AsyncMock(return_value=_pcva_settings())):
        result = await get_va_cause_of_death("va-1", include_ccva=False, include_pcva=True, db=db)

    assert result.data["pcva"] is None


@pytest.mark.asyncio
async def test_pcva_reaches_concordance_when_enough_coders_agree():
    coders = [
        {"coder": "Dr A", "coded_at": "2026-01-01", "underlying_cause": "A00 - Cholera"},
        {"coder": "Dr B", "coded_at": "2026-01-02", "underlying_cause": "A00 - Cholera"},
        {"coder": "Dr C", "coded_at": "2026-01-03", "underlying_cause": "B01 - Varicella"},
    ]
    db = FakeDB(lambda query, bind_vars: FakeCursor(coders))

    with patch("app.records.services.cause_of_death.fetch_pcva_settings", new=AsyncMock(return_value=_pcva_settings(concordance_level=2))):
        result = await get_va_cause_of_death("va-1", include_ccva=False, include_pcva=True, db=db)

    pcva = result.data["pcva"]
    assert pcva["coders"] == coders
    assert pcva["concordance"] == {
        "reached": True,
        "underlying_cause": "A00 - Cholera",
        "agreeing_coders": 2,
        "total_coders": 3,
        "concordance_level": 2,
    }


@pytest.mark.asyncio
async def test_pcva_not_concordant_when_agreement_is_below_the_configured_level():
    coders = [
        {"coder": "Dr A", "coded_at": "2026-01-01", "underlying_cause": "A00 - Cholera"},
        {"coder": "Dr B", "coded_at": "2026-01-02", "underlying_cause": "B01 - Varicella"},
    ]
    db = FakeDB(lambda query, bind_vars: FakeCursor(coders))

    with patch("app.records.services.cause_of_death.fetch_pcva_settings", new=AsyncMock(return_value=_pcva_settings(concordance_level=2))):
        result = await get_va_cause_of_death("va-1", include_ccva=False, include_pcva=True, db=db)

    pcva = result.data["pcva"]
    assert pcva["concordance"]["reached"] is False
    assert pcva["concordance"]["underlying_cause"] is None


@pytest.mark.asyncio
async def test_pcva_not_fetched_when_include_pcva_is_false():
    db = FakeDB(lambda query, bind_vars: FakeCursor([{"coder": "Dr A", "coded_at": "x", "underlying_cause": "y"}]))

    result = await get_va_cause_of_death("va-1", include_ccva=False, include_pcva=False, db=db)

    assert result.data["pcva"] is None
    assert db.aql.queries == []
