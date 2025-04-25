# -*- coding: utf-8 -*-

"""
vacheck.datacheck5

Tool for running data checks used by InterVA5.
"""

from .exceptions import VAInputException, VAIDException
from pandas import read_csv, Series, DataFrame
from pkgutil import get_data
from io import BytesIO
from numpy import nan, ndarray, where, isnan
import numpy


def datacheck5(va_input: Series,
               va_id: str,
               probbase: ndarray,
               insilico_check=False) -> dict:
    """
    Runs verbal autopsy data consistency check from InterVA5 algorithm.
    :param va_input: original data for one observation with values
    0 (absence), 1 (presence), and numpy.nan (missing).
    :type va_input: pandas.Series
    :param va_id: ID for this observation
    :type va_id: string
    :param probbase: SCI from InterVA5
    :type probbase: numpy.ndarray
    :param insilico_check: Indicator to use InSilicoVA rule which sets all
    symptoms that should not be asked to a value of missing. In contrast,
    the default rule sets these symptoms to missing only when they take the
    substantive value.
    :type insilico_check: boolean
    :return: cleaned input with log messages from first and second passes.
    :rtype: dictionary with keys output, first_pass (a list), and
    second_pass (a list).
    """

    if not isinstance(va_input, Series):
        raise VAInputException(
            "`va_input` must be a pandas.Series, not {}".format(
                va_input.__class__.__name__
            ))
    if not all(va_input[1:].dropna().isin([0, 1])):
        raise VAInputException(
            "`va_input` must have values 0, 1, and nan for symptoms."
        )
    if len(va_input) != 354:
        raise VAInputException(
            "`va_input` must have 354 elements"
        )
    if str(va_id) == "":
        raise VAIDException(
            "`va_id` cannot be an empty string"
        )
    tmp_input = va_input.copy()
    tmp_input["ID"] = 0
    input_current = tmp_input.to_numpy()
    number_symptoms = input_current.shape[0]
    index_current = str(va_id)
    first_pass = []
    second_pass = []

    for k in range(2):
        for j in range(1, number_symptoms):
            # print(f"Current iteration: {j}\n")
            subst_val = int(probbase[j, 5] == "Y")
            dont_asks = where(probbase[j, 7:15] != ".")[0]
            if len(dont_asks) > 0:
                for q in dont_asks:
                    dont_ask_q = probbase[j, q + 7].item()
                    input_index = where(probbase[:, 0] == dont_ask_q[0:5])[0]
                    input_dont_ask = input_current[input_index].item()
                    dont_ask_val = int(dont_ask_q[5:6] == "Y")

                    if (not isnan(input_current[j]) and
                            not isnan(input_dont_ask)):
                        if (
                                (input_current[j] == subst_val or
                                 insilico_check) and
                                # the following
                                # subst_val == 1 and
                                input_dont_ask == dont_ask_val):

                            input_current[j] = nan

                            dont_ask_q_who = probbase[input_index, 3]
                            dont_ask_sdesc = probbase[input_index, 2]
                            msg = (f"{index_current}   {probbase[j, 4]} "
                                   f"({probbase[j, 3]}) " 
                                   "value inconsistent with "
                                   f"{dont_ask_q_who} ({dont_ask_sdesc}) "
                                   "- cleared in working information")

                            if k == 0:
                                first_pass.append(msg)
                            else:
                                second_pass.append(msg)

            # ask if
            if probbase[j, 15] != "." and not isnan(input_current[j]):
                ask_if_indic = probbase[j, 15][0:5]
                ask_if_row = probbase[:, 0] == ask_if_indic
                input_ask_if = input_current[ask_if_row]
                ask_if_val_str = probbase[j, 15][5:6]
                ask_if_val = int(
                    ask_if_val_str.replace("Y", "1").replace("N", "0"))

                if input_current[j] == subst_val:
                    change_ask_if = (
                            input_ask_if != ask_if_val and
                            subst_val != input_ask_if)

                    if change_ask_if:
                        input_current[ask_if_row] = ask_if_val
                        msg = (f"{index_current}   {probbase[j, 3]} "
                               f"({probbase[j, 2]})" 
                               "  not flagged in category "
                               f"{probbase[ask_if_row][0, 3]} "
                               f"({probbase[ask_if_row][0, 2]}) "
                               "- updated in working information")

                        if k == 0:
                            first_pass.append(msg)
                        else:
                            second_pass.append(msg)

            # neonates only
            if probbase[j, 16] != "." and not isnan(input_current[j]):
                nn_only = probbase[j, 16][0:5]
                input_nn_only = input_current[probbase[:, 0] == nn_only].item()
                if isnan(input_nn_only):
                    input_nn_only = 0

                # the following alternate syncs the log output with the
                # InterVA5 software
                # if input_current[j] == 1 and input_nn_only != 1:
                if input_current[j] == subst_val and input_nn_only != 1:
                    input_current[j] = nan

                    msg = (f"{index_current}   {probbase[j, 3]} "
                           f"({probbase[j, 2]}) only required for neonates"
                           " - cleared in working information")
                    if k == 0:
                        first_pass.append(msg)
                    else:
                        second_pass.append(msg)
    input_final = Series(input_current,
                         index=va_input.index)
    input_final["ID"] = va_id

    output = {"output": input_final,
              "first_pass": first_pass,
              "second_pass": second_pass}
    return output


def get_example_input() -> DataFrame:
    """
    Get an example input.

    :return: 200 records of sample input.
    :rtype: pandas.DataFrame
    """

    example_input_bytes = get_data(__name__, "data/example_input.csv")
    example_input = read_csv(BytesIO(example_input_bytes))
    return example_input


def get_probbase(drop_prior: bool = True,
                 replace_nan: bool = True,
                 replace_qdesc: bool = True) -> numpy.ndarray:
    """
    Get the probbase (the source of the data consistency checks).

    :param drop_prior: Indicator for retaining row with
    unconditional prior
    :type drop_prior: bool
    :param replace_nan: Indicator for replacing NaN values with "."
    :type replace_nan: bool
    :param replace_qdesc: Indicator for replacing values in column 'qdesc'
    with "" (empty strings)
    :type replace_qdesc: bool
    :return: symptom-cause-information matrix for InterVA5
    :rtype: numpy.array
    """

    probbase_bytes = get_data(__name__, "data/probbaseV5.csv")
    probbase = read_csv(BytesIO(probbase_bytes))
    # note: drop first row so it matches the input
    if drop_prior:
        probbase.drop(index=0, inplace=True)
    if replace_nan:
        probbase.fillna(".", inplace=True)
    if replace_qdesc:
        probbase["qdesc"] = ""
    probbase_array = probbase.to_numpy(dtype=str)
    return probbase_array
