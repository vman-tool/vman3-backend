from types import SimpleNamespace

import pytest
from pandas import DataFrame, Series

from app.ccva.utilits.interva.exceptions import ArgumentException
from app.ccva.utilits.interva.utils import csmf


def test_csmf_raises_when_the_run_produced_no_results_at_all():
    iva5 = SimpleNamespace(results={})

    with pytest.raises(ArgumentException):
        csmf(iva5)


def test_csmf_rejects_an_invalid_age_group():
    iva5 = SimpleNamespace(results={"VA5": DataFrame()})

    with pytest.raises(ArgumentException):
        csmf(iva5, age="infant")


def test_csmf_rejects_an_invalid_sex():
    iva5 = SimpleNamespace(results={"VA5": DataFrame()})

    with pytest.raises(ArgumentException):
        csmf(iva5, sex="unknown")


def test_csmf_returns_an_empty_series_for_a_demographic_subgroup_with_zero_rows():
    # Regression: a demographic slice (e.g. no neonates in this batch) can
    # legitimately have zero rows even when the run overall produced valid
    # results. This used to raise ArgumentException, which crashed the whole
    # CCVA run after classification had already succeeded, with no history
    # record ever written despite the analysis itself having worked.
    iva5 = SimpleNamespace(results={"VA5": DataFrame()})

    result = csmf(iva5)

    assert isinstance(result, Series)
    assert result.empty
    # Every caller treats the return value as a Series unconditionally
    # (.index.tolist(), .tolist()) - None would just move the crash up one
    # level instead of avoiding it.
    assert result.dtype == float
