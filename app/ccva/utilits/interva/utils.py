# -*- coding: utf-8 -*-

"""
interva.utils
-------------------

This module provides utility functions for manipulating and summarizing
output from InterVA5 (i.e., the out attribute).
"""

from __future__ import annotations
from typing import Union, TYPE_CHECKING
from pandas import DataFrame, Index, Series, isna
from numpy import append, argsort, delete, nanmax, where, zeros
from decimal import Decimal
from math import isclose

from interva.exceptions import ArgumentException
if TYPE_CHECKING:
    import interva.interva5


def _get_dem_groups(va_series: Series, detailed=False) -> dict:
    """Retrieve age and sex from the VA record."""

    if not isinstance(va_series, Series):
        raise ArgumentException(
            "The parameter va_series must be a pandas.Series")

    va_sex = _get_sex_group(va_series)
    if detailed:
        va_age = _get_age_group_all(va_series)
    else:
        va_age = _get_age_group(va_series)

    return {"ID": va_series["ID"], "age": va_age, "sex": va_sex}


def _get_sex_group(va_series: Series) -> str:
    """Retrieve sex (male/female/unknown) from the VA record."""

    if not isinstance(va_series, Series):
        raise ArgumentException(
            "The argument for va_series must be a pandas.Series")

    yes = [1, "y", "Y", "yes", "Yes", "YES"]
    no = [0, "n", "N", ".", "-", "no", "No", "NO"]
    va_sex = "unknown"
    if va_series["i019a"] in yes and (va_series["i019b"] in no or
                                      isna(va_series["i019b"])):
        va_sex = "male"
    elif va_series["i019b"] in yes and (va_series["i019a"] in no or
                                        isna(va_series["i019a"])):
        va_sex = "female"
    return va_sex


def _get_age_group(va_series: Series) -> str:
    """Retrieve age group from the VA record."""

    if not isinstance(va_series, Series):
        raise ArgumentException(
            "The argument for va_series must be a pandas.Series")

    yes = [1, "y", "Y", "yes", "Yes", "YES"]
    va_age = "unknown"
    age_indicators = va_series.filter(regex="i022[a-g]")
    age_groups = age_indicators[age_indicators.isin(yes)].index.to_list()
    if len(age_groups) != 1:
        age_group = "unknown"
    else:
        age_group = age_groups[0]

    if age_group in ["i022a", "i022b", "i022c"]:
        va_age = "adult"
    elif age_group in ["i022d", "i022e", "i022f"]:
        va_age = "child"
    elif age_group == "i022g":
        va_age = "neonate"

    return va_age


def _get_age_group_all(va_series: Series) -> str:
    """Retrieve detailed age group from the VA record."""

    if not isinstance(va_series, Series):
        raise ArgumentException(
            "The argument for va_series must be a pandas.Series")

    yes = [1, "y", "Y", "yes", "Yes", "YES"]
    va_age = "unknown"
    age_indicators = va_series.filter(regex="i022[a-g]")
    age_groups = age_indicators[age_indicators.isin(yes)].index.to_list()
    if len(age_groups) != 1:
        age_group = "unknown"
    else:
        age_group = age_groups[0]

    if age_group == "i022a":
        va_age = "age 65+"
    elif age_group == "i022b":
        va_age = "age 50-64"
    elif age_group == "i022c":
        va_age = "age 15-49"
    elif age_group == "i022d":
        va_age = "age 5-14"
    elif age_group == "i022e":
        va_age = "age 1-4"
    elif age_group == "i022f":
        va_age = "age 1-11m"
    elif age_group == "i022g":
        va_age = "age 0-27d"

    return va_age


def csmf(iva5: interva.interva5.InterVA5,
         top: int = 10,
         interva_rule: bool = False,
         top_aggregate: Union[bool, int] = None,
         age: Union[None, str] = None,
         sex: Union[None, str] = None) -> Series:
    """Return top causes in cause-specific mortality fraction (CSMF).

    :param iva5: instance of InterVA5 with results
    :type iva5: interva.interva5.InterVA5
    :param top: number of top causes in the CSMF to be determined.
    :type top: int
    :param interva_rule: If True, only the top 3 causes reported
    by InterVA are used in the calculation of the CSMF; the rest
    of the propensity is assigned to "Undetermined".  Otherwise,
    the InterVA threshold/cutoff is not employed and there will be no
    "Undetermined" cause category.
    :type interva_rule: bool
    :param top_aggregate: Integer indicating how many causes from the top need
    to go into the summary.  The rest of the propensities are assigned into
    the category "Undetermined".  This parameter only takes effect if
    interva_rule == False
    :param age: Name of age group to obtain group-specific results: "adult",
    "child", or "neonate".  If None, then all groups are combined.
    :type age: str
    :param sex: Either "female" or "male" to obtain sex-specific results.  If
    None, then these groups are combined.
    :type sex: str
    :type top_aggregate: Union[int, None]

    :return: the top causes in CSMF with their values.
    :rtype: pandas.series
    """

    # if not isinstance(iva5, interva.interva5.InterVA5):
    #     raise ArgumentException(
    #         "The argument for iva5 must be an instance of the InterVA5 class")
    if len(iva5.results) == 0:
        raise ArgumentException("No results (need to use run() method).")
    if age is not None:
        age_groups = ["adult", "child", "neonate"]
        if not isinstance(age, str) or age.lower() not in age_groups:
            raise ArgumentException(
                "The age parameter must be " + ", ".join(age_groups))
    if sex is not None:
        sex_groups = ["female", "male"]
        if not isinstance(sex, str) or sex.lower() not in sex_groups:
            raise ArgumentException(
                "The sex parameter must be " + ", ".join(sex_groups))

    va5_results = iva5.results["VA5"]
    if age is not None or sex is not None:
        va5_results = _get_cod_with_dem(iva5)
        if age is not None:
            age_index = va5_results["age"] == age.lower()
            va5_results = va5_results[age_index]
        if sex is not None:
            sex_index = va5_results["sex"] == sex.lower()
            va5_results = va5_results[sex_index]

    if va5_results.shape[0] == 0:
        raise ArgumentException("No VA results found.")

    if va5_results.shape[1] != 15 and va5_results.shape[1] != 17:
        raise ArgumentException(
            "Unexpected va5 format (need 15 columns).  The expected format is "
            "InterVA5.results['VA5']")

    if interva_rule:
        dist_cod = _csmf_with_interva_rule(va5_results)
    else:
        dist_cod = _csmf_without_interva_rule(va5_results, top_aggregate)

    if dist_cod is None:
        return None

    dist_cod.sort_values(ascending=False, inplace=True)

    # show causes with top non-zero values
    show_top = 0
    while dist_cod.iloc[show_top] > 0 and show_top < top:
        show_top = show_top + 1
    if show_top == top:
        a = dist_cod.iloc[show_top]
        b = dist_cod.iloc[show_top - 1]
        while show_top < len(dist_cod) and (abs(a - b) < (a + b) * 1e-5):
            show_top = show_top + 1
            a = dist_cod.iloc[show_top]
            b = dist_cod.iloc[show_top - 1]
    top_csmf = dist_cod.head(show_top)

    return top_csmf


def _csmf_without_interva_rule(
        va5: DataFrame,
        top_aggregate: Union[bool, int] = None) -> Union[Series, None]:
    """Return top causes in cause-specific mortality fraction (CSMF) without
    applying the InterVA rule for only considering causes with propensities
    above a threshold.

    :param va5: The out["VA5"] attribute from InterVA5
    :type va5: pandas.DataFrame
    :param top_aggregate: Integer indicating how many causes from the top need
    to go into the summary.  The rest of the propensities are assigned into
    the category "Undetermined".
    :type top_aggregate: Union[int, None]

    :return: cause-specific mortality fractions (CSMF) with causes as the
    index.
    :rtype: pandas.Series
    """

    va = va5.copy()

    # for future compatibility with non-standard input
    cause_names = cause_index = []
    for i in va.index:
        if va.loc[i, "WHOLEPROB"] is not None:
            cause_names = va.loc[i, "WHOLEPROB"].index
            cause_index = [x for x in range(len(cause_names))]
            break
    include_prob_ac = False
    # fix for removing the first 3 preg related death in standard input
    if ("Not pregnant or recently delivered" in cause_names[0] and
            "Pregnancy ended within 6 weeks of death" in cause_names[1] and
            "Pregnant at death" in cause_names[2] and
            "Culture" in cause_names[64] and
            "Emergency" in cause_names[65] and
            "Health" in cause_names[66] and
            "Inevitable" in cause_names[67] and
            "Knowledge" in cause_names[68] and
            "Resources" in cause_names[69]):
        del cause_index[64:70]
        del cause_index[0:3]
        cause_names = cause_names.delete([0, 1, 2, 64, 65, 66, 67, 68, 69])
        include_prob_ac = True

    # Check if there is a valid va object
    if va.shape[0] < 1:
        print("No va5 object found")
        return None

    # Initialize the population distribution
    dist = None
    for i in va.index:
        if va.loc[i, "WHOLEPROB"] is not None:
            dist = zeros(len(va.loc[i, "WHOLEPROB"]))
            break
    if top_aggregate is None:
        top_aggregate = len(cause_index)
    undetermined = 0

    if dist is None:
        print("No va probability found in input")
        return None

    # Pick not simply the top # causes,
    # but the top # causes reported by InterVA5
    for i in va.index:
        whole_prob = va.loc[i, "WHOLEPROB"]
        if whole_prob is None:
            undetermined = undetermined + 1
            continue
        this_dist = whole_prob.copy()
        this_dist = this_dist.to_numpy()
        if include_prob_ac:
            this_dist[0:3] = 0
            this_dist[64:70] = 0
        if sum(this_dist) == 0:
            undetermined = undetermined + 1
            continue
        cutoff = this_dist[argsort(-this_dist)[top_aggregate - 1]]
        undetermined = undetermined + sum(this_dist[this_dist < cutoff])
        this_dist[this_dist < cutoff] = 0
        if whole_prob is not None:
            dist = dist + this_dist
    if undetermined > 0:
        dist_cod = append(dist[cause_index], undetermined)
        dist_cod = dist_cod / sum(dist_cod)
        cause_names = append(cause_names, "Undetermined")
        dist_cod = Series(dist_cod, index=cause_names)
    else:
        dist_cod = dist[cause_index] / sum(dist[cause_index])
        dist_cod = Series(dist_cod, index=cause_names)

    return dist_cod


def _csmf_with_interva_rule(va5: DataFrame) -> Series:
    """Return top causes in cause-specific mortality fraction (CSMF) with
    applying the InterVA rule for only considering causes with propensities
    above a threshold.

    :param va5: The out["VA5"] attribute from InterVA5
    :type va5: pandas.DataFrame

    :return: cause-specific mortality fractions (CSMF) with causes as the
    index.
    :rtype: pandas.Series
    """

    va = va5.copy()

    # for future compatibility with non-standard input
    cause_names = cause_index = []
    for i in va.index:
        if va.loc[i, "WHOLEPROB"] is not None:
            cause_names = va.loc[i, "WHOLEPROB"].index
            cause_index = [x for x in range(len(cause_names))]
            break
    include_prob_ac = False

    # fix for removing the first 3 preg related death in standard input
    if ("Not pregnant or recently delivered" in cause_names[0] and
            "Pregnancy ended within 6 weeks of death" in cause_names[1] and
            "Pregnant at death" in cause_names[2] and
            "Culture" in cause_names[64] and
            "Emergency" in cause_names[65] and
            "Health" in cause_names[66] and
            "Inevitable" in cause_names[67] and
            "Knowledge" in cause_names[68] and
            "Resources" in cause_names[69]):
        del cause_index[64:70]
        del cause_index[0:3]
        cause_names = cause_names.delete([0, 1, 2, 64, 65, 66, 67, 68, 69])
        include_prob_ac = True

    # Check if there is a valid va object
    if va.shape[0] < 1:
        print("No va5 object found")
        return None
    # Initialize the population distribution
    dist = None
    for i in va.index:
        if va.loc[i, "WHOLEPROB"] is not None:
            dist = zeros(len(va.loc[i, "WHOLEPROB"]))
            # TODO: this has been changed (to fix bug) in interva5.py
            break
    undetermined = 0

    # Pick not simply the top # causes,
    # but the top # causes reported by InterVA5
    for i in va.index:
        whole_prob = va.loc[i, "WHOLEPROB"]
        if whole_prob is None:
            continue
        this_dist = whole_prob.copy()
        this_dist = this_dist.to_numpy()
        if include_prob_ac:
            this_dist[0:3] = 0
            this_dist[64:70] = 0
        if max(this_dist) < 0.4:
            if isclose(sum(this_dist), 0):
                this_undetermined = 1
            else:
                this_undetermined = sum(this_dist)
            undetermined = undetermined + this_undetermined
        else:
            cutoff_3 = Decimal(this_dist[argsort(-this_dist)][2])
            cutoff_2 = Decimal(this_dist[argsort(-this_dist)][1])
            cutoff_1 = Decimal(this_dist[argsort(-this_dist)][0])
            cutoff_1_halved = cutoff_1 / Decimal('2')
            cutoff_pt1 = cutoff_3.max(cutoff_1_halved)
            cutoff_pt2 = cutoff_2.max(cutoff_1_halved)
            cutoff = cutoff_pt1.min(cutoff_pt2)
            adj_cutoff = cutoff - Decimal(1e-15)

            undetermined = undetermined + sum(
                this_dist[where(this_dist < adj_cutoff)[0]])
            this_dist[where(this_dist < adj_cutoff)[0]] = 0

            temp_len = len(this_dist[where(this_dist > 0)[0]])
            close_indices = []
            for j in range(temp_len):
                val = Decimal(
                    this_dist[where(this_dist > 0)[0]][j]) - cutoff
                if abs(val) < 4e-29:
                    close_indices.append(where(this_dist > 0)[0][j])

            close_indices.sort(reverse=True)
            for k in close_indices:
                undetermined = undetermined + this_dist[k]
                this_dist[k] = 0

            if whole_prob is not None:
                if i == 0:
                    dist = this_dist
                else:
                    dist = dist + this_dist
    dist = Series(dist)
    # Normalize the probability for CODs
    if undetermined > 0:
        dist_cod = dist.iloc[cause_index].copy()
        dist_cod.loc[cause_index[len(cause_index) - 1] + 1] = undetermined
        dist_cod = dist_cod / dist_cod.sum()
        dist_cod.index = cause_names.append(Index(["Undetermined"]))
    else:
        dist_cod = dist.iloc[cause_index].copy()
        dist_cod = dist_cod / dist_cod.sum()
        dist_cod.index = cause_names
    if (isna(dist_cod).sum() == len(dist_cod)).all():
        dist_cod[isna(dist_cod)] = 0

    return dist_cod


def _get_cod_with_dem(iva5: interva.interva5.InterVA5) -> DataFrame:
    """Return VA results with demographics (age/sex) attached.

    :param iva5: instance of InterVA5 with results
    :type iva5: interva.interva5.InterVA5

    :return: VA5 results with age and sex indicators included as columns,
    and ID set as the index.
    :rtype: pandas.DataFrame
    """

    # if not isinstance(iva5, interva.interva5.InterVA5):
    #     raise ArgumentException(
    #         "The argument for va5 must be an instance of the InterVA5 class")
    if len(iva5.results) == 0:
        raise ArgumentException("No results (need to use run() method).")

    va5_results = iva5.results["VA5"].copy()
    va5_results = va5_results.set_index("ID")

    all_results = va5_results.merge(iva5.dem_group, on="ID")
    all_results = all_results.reset_index("ID")
    return all_results


def get_indiv_cod(iva5: interva.interva5.InterVA5,
                  top: int = 0,
                  interva_rule: bool = True,
                  include_propensities: bool = False) -> DataFrame:
    """Get individual causes of death distribution.

    :param iva5: instance of InterVA5 with results
    :type iva5: interva.interva5.InterVA5
    :param top: number of top causes to be determined. If top is 0 or None,
    all propensities be returned (unordered).
    :type top: integer or None
    :param interva_rule: Use the InterVA threshold for assigning undetermined.
    :type: bool
    :param include_propensities: a logical value indicating whether the
    propensities of top causes should be included. If top is 0 or None,
    this boolean is automatically set to True.
    :return: the individual cause of death distribution.
    :rtype: pandas DataFrame
    """

    if interva_rule:
        cod = iva5.get_indiv_prob(top=top,
                                  include_propensities=include_propensities)
        return cod
    else:
        VA5 = iva5.results["VA5"].copy()
        num_indiv = VA5.shape[0]
        cod_list = [[] for _ in range(num_indiv)]
        column_names = []
        if top == 0 or top is None:
            column_names = VA5.loc[0, "WHOLEPROB"].iloc[3:64].index
        else:
            for i in range(top):
                name = "CAUSE" + str(i+1)
                column_names.append(name)
                if include_propensities:
                    prob = "PROPENSITY" + str(i+1) + ""
                    column_names.append(prob)

        for indiv in range(num_indiv):
            wholeprob = VA5.loc[indiv, "WHOLEPROB"]
            prob_B = wholeprob.iloc[3:64].copy()

            if top == 0 or top is None:
                cod_list[indiv] = prob_B
            if top > 0:
                prob_temp = prob_B.to_numpy()
                prob_temp_names = prob_B.index
                for cause_num in range(top):
                    if cause_num == 0:
                        max_loc = where(prob_temp == nanmax(prob_temp))[0][0]
                        cause = cod_list[indiv] = [prob_temp_names[max_loc]]
                        if include_propensities:
                            if cause == " ":
                                cod_list[indiv].append(" ")
                            else:
                                cod_list[indiv].append(nanmax(prob_temp))
                                prob_temp = delete(prob_temp, max_loc)
                    if cause_num > 0:
                        max_loc = where(prob_temp == nanmax(prob_temp))[0][0]
                        cause = prob_temp_names[max_loc]
                        cod_list[indiv].append(cause)
                        if include_propensities:
                            if cause == " ":
                                cod_list[indiv].append(" ")
                            else:
                                cod_list[indiv].append(nanmax(prob_temp))
                        prob_temp = delete(prob_temp, max_loc)
        cod_df = DataFrame(cod_list, columns=column_names)
        cod_df.insert(loc=0, column="ID", value=iva5.results["ID"])
        return cod_df
