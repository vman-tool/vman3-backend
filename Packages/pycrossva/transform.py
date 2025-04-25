#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Defines main CrossVA function, `transform` which maps raw VA data into data
for use with a VA algorithm in OpenVA.
"""
import sys
import os

import pandas as pd
import numpy as np

from pycrossva.configuration import Configuration, CrossVA
from pycrossva.utils import flexible_read

SUPPORTED_INPUTS = ["2016WHOv151", "2016WHOv141", "2012WHO", "PHMRCShort"]
SUPPORTED_OUTPUTS = ["InterVA5", "InterVA4", "InSilicoVA", "InSilicoVA_2012"]


def transform(mapping, raw_data, raw_data_id=None, lower=False,
              verbose=2, preserve_na=True,
              result_values={"Present": "y", "Absent": "n", "NA": "."}):
    """transforms raw VA data (`raw_data`) into data suitable for use with a VA
    algorithm, according to the specified transformations given in `mapping`.

    Args:
        mapping (string, tuple or Pandas DataFrame): Should be either a tuple
            in form (input, output), a path to csv containing a configuration
            data file, or a Pandas DataFrame containing configuration data
        raw_data (string or Pandas DataFrame): raw verbal autopsy data to
            process
        raw_data_id (string): column name with record ID
        lower (bool): whether the column names in the data should be
            case-sensitive.
        verbose (int): integer from 0 to 5, controlling how much status detail
            is printed to console. Silent if 0. Defaults to 2, which will print
            only errors and warnings.
        preserve_na (bool): whether to preserve NAs in data, or to count them
            as FALSE. Overridden with True for InSilicoVA, False for InterVA4
            when mapping is given as a tuple. Defaults to TRUE, which allows
            NA values to perpetuate through the data.
        result_values (dict): available as a simple customization option if
            user would like values indicating presence, absence, and NAs to
            be mapped to certain values.

    Returns:
        Pandas DataFrame: the raw data transformed according to specifications
        given in mapping data. Default values are y where symptom is present,
        n where symptom is absent, and if . are preserved, they are represented
        in the data as NaNs. If NAs are not preserved, they are considered to be
        false / absent / 0.

    Examples:
        You can specify the mapping as ('input', 'output') and the path to csv
        as a string:

        >>> transform(("2016WHOv151", "InterVA4"), "resources/sample_data/2016WHO_mock_data_1.csv").loc[range(5),["ACUTE","CHRONIC","TUBER"]]
           ACUTE  CHRONIC  TUBER
        0      y        n      .
        1      y        n      .
        2      n        y      .
        3      n        y      .
        4      y        n      .

        You can also give the data and mapping as Pandas DataFrames:

        >>> my_special_data = pd.read_csv("resources/sample_data/2016WHO_mock_data_1.csv")
        >>> my_special_mapping = pd.read_csv("resources/mapping_configuration_files/2016WHOv151_to_InterVA4.csv")
        >>> transform(my_special_mapping, my_special_data).loc[range(5),["ACUTE","CHRONIC","TUBER"]]
           ACUTE  CHRONIC  TUBER
        0      y        n      .
        1      y        n      .
        2      n        y      .
        3      n        y      .
        4      y        n      .

        Note that by default, `preserve_na` is `True` and NA values will be
        left in. If `preserve_na` is `False`, or if the algorithm does not
        preserve NAs, then NA values will be filled in as 0's, as they are in
        the first InterVA4 example above.

        The user can also pass in a different mapping dictionary for
        result_values to change the values from their defaults of
        0 (False / Absent), 1 (True / Present), and NaN
        (No data / missing), if they need their results in a different format.

        >>> transform(("2016WHOv151", "InterVA4"), "resources/sample_data/2016WHO_mock_data_1.csv", result_values={"Absent":"A","Present":"P","NA":"Missing"}).loc[range(5),["ACUTE","CHRONIC","TUBER"]]
          ACUTE CHRONIC    TUBER
        0     P       A  Missing
        1     P       A  Missing
        2     A       P  Missing
        3     A       P  Missing
        4     P       A  Missing

        The mapping-data relationship is designed to be as flexible as possible,
        while still emphasizing tracebility and alerting the user to data
        integrity issues.

        Not every source column in the mapping needs to be
        represented in the data. If source columns are missing in the source data,
        then those columns will be created and filled with NA values.

        >>> transform(("2016WHOv151", "InterVA4"), "resources/sample_data/2016WHO_mock_data_2.csv").loc[range(5),["ACUTE","FEMALE","MARRIED"]]
        Validating Mapping-Data Relationship . . .
        <BLANKLINE>
         WARNINGS
        [?]          3 (1.3%) expected source column IDs listed in mapping file ('-ageInDaysNeonate', '-Id10019', and '-Id10059') were not found in the input data columns. Their values will be NA.
        [?]          '-Id10019' is missing, which affects the creation of  column(s) 'FEMALE', and 'MALE'
        [?]          '-Id10059' is missing, which affects the creation of  column(s) 'MARRIED'
        [?]          '-ageInDaysNeonate' is missing, which affects the creation of  column(s) 'DIED_D1', 'DIED_D23', 'DIED_D36', 'DIED_W1', and 'NEONATE'
           ACUTE  FEMALE  MARRIED
        0      y       .        .
        1      y       .        .
        2      y       .        .
        3      y       .        .
        4      y       .        .

        `transform` will also accept mapping configurations with missing values,
        with new columns that are specified but missing source columns.
        These new columns will be created so that the final result has the correct
        expeted columns for the algorithm, but filled with NA values to indicate
        the lack of information. If `preserve_na` is set to `False`, then the NA
        values will also be `False`.

        This situation is common between certain questionnaire sources and algorithms.
        For example, in the mapping between the PHMRC Short questionnaire
        to InterVA5 mapping, there are 107 InterVA5
        variables that are listed in the mapping configuration to be created,
        but have no corresponding question in PHMRC short.

        For example, variables i004a and i004b have no specifications in the
        mapping below. They are still listed under "New Column Name" so
        CrossVA knows that they should be created in the final result, but
        because they have no logic defined, they will be left as their default
        value of NA.

        >>> phrmc_to_interva5 = pd.read_csv('resources/mapping_configuration_files/PHMRCShort_to_InterVA5.csv')
        >>> phrmc_to_interva5.iloc[:5,[0,2,4,-1]]
          New Column Name Source Column ID Relationship Meta: Notes
        0           i004a              NaN          NaN   Not asked
        1           i004b              NaN          NaN   Not asked
        2           i019a          gen_5_2           eq         NaN
        3           i019b          gen_5_2           eq         NaN
        4           i022a         gen_5_4h           ge         NaN

        The `transform` function will warn the user of this behavior.

        >>> transform(phrmc_to_interva5, "resources/sample_data/PHMRC_mock_data_1.csv").iloc[:5,:5]
        Validating Mapping Configuration . . .
        <BLANKLINE>
         WARNINGS
        [?] 	 124 new column(s) listed but not defined in Mapping Configuration detected. These ('i004a', 'i004b', 'i059o', 'i082o', 'i087o', 'i091o', 'i092o', 'i093o', 'i094o', 'i095o', etc) will be treated as NA.
        Validating Mapping-Data Relationship . . .
        <BLANKLINE>
         WARNINGS
        [?] 	 9 (5.7%) expected source column IDs listed in mapping file ('child_6_2', 'child_4_4', 'child_4_20', 'child_4_7a', 'child_4_40', 'child_4_28', 'child_4_30', 'child_1_5a', and 'child_5_1') were not found in the input data columns. Their values will be NA.
        [?] 	 'child_1_5a' is missing, which affects the creation of  column(s) 'i358a'
        [?] 	 'child_4_20' is missing, which affects the creation of  column(s) 'i171o'
        [?] 	 'child_4_28' is missing, which affects the creation of  column(s) 'i208o'
        [?] 	 'child_4_30' is missing, which affects the creation of  column(s) 'i233o'
        [?] 	 'child_4_4' is missing, which affects the creation of  column(s) 'i150a'
        [?] 	 'child_4_40' is missing, which affects the creation of  column(s) 'i200o'
        [?] 	 'child_4_7a' is missing, which affects the creation of  column(s) 'i183o'
        [?] 	 'child_5_1' is missing, which affects the creation of  column(s) 'i418o'
        [?] 	 'child_6_2' is missing, which affects the creation of  column(s) 'i130o'
           ID  i004a  i004b  i019a  i019b
        0   1      .      .      y      n
        1   2      .      .      n      n
        2   3      .      .      n      n
        3   4      .      .      y      n
        4   5      .      .      n      n

        However, the mapping-data relationship must be valid. For example, if
        the source column IDs are not unique for the input data - that is,
        if multiple columns in the input data contain the same source ID - then
        validation will fail.

        For example, `bad_data` contains columns named `A-Id10004` and `B-Id10004`,
        but the 2016 WHO mapping is looking for just `-Id10004` as a source ID.
        CrossVA cannot tell which column should be used, so validation fails.

        >>> bad_data = pd.read_csv("resources/sample_data/2016WHO_bad_data_1.csv")
        >>> transform(("2016WHOv151", "InterVA4"), bad_data)
        Validating Mapping-Data Relationship . . .
        <BLANKLINE>
         ERRORS
        [!]      1 source column IDs ('-Id10004') were found multiple times in the input data. Each source column ID should only occur once as part of an input data column name. It should be a unique identifier at the end of an input data column name. Source column IDs are case sensitive. Please revise your mapping configuration or your input data so that this condition is satisfied.

    """
    mapping_data = pd.DataFrame()

    # read in mapping data
    if isinstance(mapping, tuple):  # if mapping is in (input, output) format
        internal_path = os.path.join(os.path.split(
            __file__)[0], "resources/mapping_configuration_files/")
        if len(mapping) == 2:
            if mapping[0] in SUPPORTED_INPUTS:
                if mapping[1] in SUPPORTED_OUTPUTS:
                    # preserve_na = mapping[1] == "InSilicoVA"  # overides given
                    # if mapping[1] == "InterVA4":
                    #     # treat as Insillico w/o NA
                    #     mapping = (mapping[0], "InSilicoVA")
                    preserve_na = mapping[1] in ("InSilicoVA",
                                                 "InSilicoVA_2012")
                    expected_filename = (f"{internal_path}"
                                         f"{mapping[0]}_to_"
                                         f"{mapping[1]}.csv")
                    if os.path.isfile(expected_filename):
                        mapping_data = pd.read_csv((f"{internal_path}"
                                                    f"{mapping[0]}_to_"
                                                    f"{mapping[1]}.csv"))
                    else:
                        raise ValueError((f"No mapping supporting {mapping[0]} "
                                          f"to {mapping[1]} currently exists."))

                else:
                    raise ValueError(("Output not supported. Expected one of "
                                      f"{supported_outputs}, but received "
                                      f"'{mapping[1]}'"))
            else:
                raise ValueError(("Input not supported. Expected one of "
                                  f"{SUPPORTED_INPUTS}, but received "
                                  f"'{mapping[0]}'"))
        else:
            raise ValueError(("If mapping is tuple, input should be of length "
                              "two in the form (input type, output type)"))

    else:
        mapping_data = flexible_read(mapping)

    if mapping_data.empty:  # this shouldn't happen; if it does, raise
        raise ValueError(("No valid mapping data provided to transform. Should be"
                          " either a tuple in form (input, output), a path to csv"
                          " or a Pandas DataFrame."))

    # init configuration obj from given mapping data
    config = Configuration(config_data=mapping_data,
                           verbose=verbose,
                           process_strings=False)
    if lower:
        config.config_data["Source Column ID"] = config.config_data[
            "Source Column ID"].str.lower()
        config.source_columns = config.source_columns.str.lower()

    # if the configuration isn't valid, or if the data isn't valid for the
    # config file, then raise error
    if not config.validate(verbose=verbose):
        # Raise Error
        raise ValueError(("Configuration from mapping file must be valid "
                          "before transform."))

    # TODO adds args to init based on data type?
    input_data = flexible_read(raw_data).copy()
    if lower:
        input_data.columns = input_data.columns.str.lower()
    cross_va = CrossVA(input_data, config)
    if not cross_va.validate(verbose=verbose):
        return
#        raise ValueError(("Cannot transform if provided raw data is not valid "
#                          "for configuration file."))
    final_data = cross_va.process()
    # if result values have been changed, then map as directed, otherwise
    # leave alone - the default values are what we actually have, so we don't
    # need to do any mapping if they have not specified an alternative.

    defaults = {"Present": 1, "Absent": 0, "NA": np.nan}
    if result_values != defaults:
        actual_mapping = {value: result_values[key] for key,
                          value in defaults.items()}
        final_data = final_data.replace(actual_mapping)
    if raw_data_id is not None:
        try:
            if lower:
                final_data.insert(loc=0,
                                  column="ID",
                                  value=input_data[raw_data_id.lower()])
            else:
                final_data.insert(loc=0,
                                  column="ID",
                                  value=input_data[raw_data_id])
        except KeyError:
            raise ValueError((f"Could not find column named {raw_data_id} "
                              "in raw_data."))
    else:
        final_data.reset_index(inplace=True)
        final_data.rename(columns={"index": "ID"}, inplace=True)
        final_data["ID"] = final_data["ID"] + 1


    if preserve_na:
        return final_data
    return final_data.fillna(0)


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
