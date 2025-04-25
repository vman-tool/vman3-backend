#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module containing Validation class, and Vcheck class and its subclasses
"""
from abc import ABCMeta, abstractmethod

import pandas as pd
import numpy as np
import re

from pycrossva.utils import report_list


class VCheck(metaclass=ABCMeta):
    """Abstract class fior a single validation check"""

    def __init__(self, message):
        """Inits VCheck class

        Args:
            message (str): the message associated with the validation check,
            which should describe why the check has passed or failed.

        Examples:
            >>> VCheck("Test Message")
            Traceback (most recent call last):
            TypeError: Can't instantiate abstract class VCheck with abstract
            methods bullet, level, tier, title
        """
        self.message = message

    def expand(self):
        """Expands VCheck information as a Pandas Series

        Args:
            None

        Returns:
            Pandas Series: representing VCheck attributes as a Pandas Series

        Examples:
            >>> Err("Error Message").expand()
            Tier               Error
            Bullet               [!]
            Level                  1
            Title             ERRORS
            Message    Error Message
            dtype: object
        """
        return pd.Series([self.tier(), self.bullet(), self.level(),
                          self.title(), self.message],
                         ["Tier", "Bullet", "Level", "Title", "Message"])

    @property
    @abstractmethod
    def tier(self):
        """abstract property, must be overriden.
        Should be str, representing name of VCheck tier"""
        return

    @property
    @abstractmethod
    def bullet(self):
        """abstract property, must be overriden.
        Should be a str, representing a bullet point"""
        return

    @property
    @abstractmethod
    def level(self):
        """abstract property, must be overriden.
        Should be int ,representing VCheck tier"""
        return

    @property
    @abstractmethod
    def title(self):
        """abstract property, must be overriden.
        Should be str, representing title of VCheck type"""
        return


class Err(VCheck):
    """VCheck subclass representing a serious problem in data validation
    that prevents validation.

    Examples:
        >>> Err("This is a data validation error").expand()
        Tier                                 Error
        Bullet                                 [!]
        Level                                    1
        Title                               ERRORS
        Message    This is a data validation error
        dtype: object
    """

    def tier(self):
        return "Error"

    def bullet(self):
        return "[!]"

    def level(self):
        return 1

    def title(self):
        return "ERRORS"


class Warn(VCheck):
    """VCheck subclass representing a problem in data validation that
    can be fixed in place, but would otherwise prevent validation.

    Examples:
        >>> Warn("This is a data validation warning").expand()
        Tier                                 Warning
        Bullet                                   [?]
        Level                                      2
        Title                               WARNINGS
        Message    This is a data validation warning
        dtype: object
    """

    def tier(self):
        return "Warning"

    def bullet(self):
        return "[?]"

    def level(self):
        return 2

    def title(self):
        return "WARNINGS"


class Suggest(VCheck):
    """VCheck subclass representing a minor problem with data that
    does not prevent data validation.

    Examples:
        >>> Suggest("This is a data validation suggestion").expand()
        Tier                                 Suggestion
        Bullet                                      [i]
        Level                                         3
        Title                               SUGGESTIONS
        Message    This is a data validation suggestion
        dtype: object
    """

    def tier(self):
        return "Suggestion"

    def bullet(self):
        return "[i]"

    def level(self):
        return 3

    def title(self):
        return "SUGGESTIONS"


class Passing(VCheck):
    """VCheck subclass representing a passed check in data validation, where
    there is no problem.

    Examples:
        >>> Passing("This is a passing data validation check").expand()
        Tier                                       Passing
        Bullet                                         [X]
        Level                                            4
        Title                                CHECKS PASSED
        Message    This is a passing data validation check
        dtype: object
    """

    def tier(self):
        return "Passing"

    def bullet(self):
        return "[X]"

    def level(self):
        return 4

    def title(self):
        return "CHECKS PASSED"


class Validation():
    """Validation object represents an organized dataframe of validation checks

    Attributes:
        vchecks (Pandas DataFrame): a dataframe containing the expanded form
            of the VCheck instances that have been added.
    """

    def __init__(self, name=""):
        """inits Validation class"""
        self.vchecks = pd.DataFrame()
        self.name = name

    def _add_condition(self, flagged_series, pass_check, fail_check):
        """Internal method that adds a fail_check to the self.vchecks attribute
        if any in flagged_series are True, else adds pass_check to the
        self.vchecks attribute. Both fail_check and pass_check are appended to
        self.vchecks through their .expand() method, which returns their
        information as a Pandas Series.

        Args:
            flagged_series (Pandas Series): a boolean Pandas Series, where True
                represents a failed condition that has been flagged.
            pass_check (VCheck): the VCheck to report if the check passes
            fail_check (VCheck): the VCheck to report if the check fails

        Returns:
            None

        Examples:
            >>> v = Validation()
            >>> v._add_condition(pd.Series([False, False, False]),  Passing("Passed test"), Err("Failed test"))
            >>> v._add_condition(pd.Series([False, False, True]),  Passing("Passed test"), Err("Failed test"))
            >>> v.vchecks
                 Tier  Bullet  Level          Title      Message
            0 Passing     [X]      4  CHECKS PASSED  Passed test
            1   Error     [!]      1         ERRORS  Failed test
        """
        if flagged_series.sum() > 0:
            # self.vchecks = self.vchecks.append(fail_check.expand(),
            #                                    ignore_index=True)
            fail_check_expanded = pd.DataFrame(fail_check.expand()).T
            self.vchecks = pd.concat([self.vchecks, fail_check_expanded],
                                     ignore_index=True)
        else:
            # self.vchecks = self.vchecks.append(pass_check.expand(),
            #                                    ignore_index=True)
            pass_check_expanded = pd.DataFrame(pass_check.expand()).T
            self.vchecks = pd.concat([self.vchecks, pass_check_expanded],
                                     ignore_index=True)

    def no_duplicates(self, my_series):
        """ adds a validation check as `Err` if any items in my_series are
        duplicates. Intended to alert users of issues where there are duplicate
        columns before an exception is raised.

            my_series (Pandas Series): series where there should not be dupes

            Returns:
                None
        """
        comparison = my_series.duplicated()
        passing_msg = ("Source column IDs do not match more than one column in"
                       " input data.")
        fail_msg = (f"{comparison.sum()} source column IDs {report_list(my_series[comparison])}"
                    " were found multiple times in the input data. Each source"
                    " column ID should only occur once as part of an input data"
                    " column name. It should be a unique identifier at"
                    " the end of an input data column name. Source column IDs"
                    " are case sensitive. Please revise your mapping configuration"
                    " or your input data so that this condition is satisfied.")
        self._add_condition(my_series.duplicated(), Passing(passing_msg),
                            Err(fail_msg))

    def affected_by_absence(self, missing_grped):
        """ adds a validation check as `Warn` describing the items in missing_grped,
        which detail the impact that missing columns have on newly created
        mappings.
            missing_grped (Pandas Series): series where the index is the name
                of the missing source column, and the values are a list of
                affected values.

            Returns:
                None
        """

        for source, affected_list in missing_grped.items():
            msg = (f"'{source}' is missing, which affects the creation of "
                   f" column(s) {report_list(affected_list, paren=False)}")
            # self.vchecks = self.vchecks.append(Warn(msg).expand(),
            #                                    ignore_index=True)
            warn_expanded = pd.DataFrame(Warn(msg).expand()).T
            self.vchecks = pd.concat([self.vchecks, warn_expanded],
                                     ignore_index=True)

    def must_contain(self, given, required, passing_msg="", fail=Err):
        """adds a validation check where `given` must contain every item in
        `required` at least once to pass, and `fail_check` is `fail`,
        (fails validation).

        Args:
            given (Pandas Series): the items representing input given
            required (Pandas Series): the items required to be in `given`
            passing_msg (str): Message to return if all items in `expected` are
                listed in `given`. Defaults to "".
            fail (VCheck): the outcome if the check fails. Default is Err.
            impact (Pandas Series): a corresponding series to `required` that
                represents the affected information when

        Returns:
            None

        Examples:
            >>> v = Validation()
            >>> v.must_contain(pd.Series(["a","b","c"], name="example input"),  pd.Series(["a","b"],  name="example requirement(s)"),  "all included")
            >>> v.must_contain(pd.Series(["a","b","c"], name="example input"),  pd.Series(["a","b","d"],  name="example requirement(s)"))
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]          all included
            <BLANKLINE>
             ERRORS
             [!]          1 (33.3%) example requirement(s) ('d') were not found in example input. Their values will be NA.
            """
        # Comparison is true (fails) when an item in required isn't in given
        comparison = ~required.isin(given)
        percentage = '{0:.1%}'.format(comparison.sum() / comparison.size)
        fail_msg = " ".join([str(comparison.sum()),
                             '(' + percentage + ')',
                             str(required.name),
                             report_list(required[comparison]),
                             "were not found in", str(given.name) + ".",
                             "Their values will be NA."])
        self._add_condition(comparison, Passing(passing_msg),
                            fail(fail_msg))

    def no_extraneous(self, given, relevant, value_type):
        """adds a validation check where all values in `given` should also be
        in `relevant` to pass. `fail_check` is `Warn`

        Args:
            given (Pandas Series): the items representing input given
            relevant (Pandas Series): all items in `given` that will be used
            value_type (str): string describing the kind of noun that is
                listed in `given`

        Returns:
            None

        Examples:
            >>> v = Validation()
            >>> v.no_extraneous(pd.Series(["a","b"], name="example input"),  pd.Series(["a","b","c"],  name="relevant value(s)"), "example")
            >>> v.no_extraneous(pd.Series(["a","b","c"], name="example input"),  pd.Series(["a","d"],  name="relevant value(s)"), "example")
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
            CHECKS PASSED
            [X]      No extraneous example found in example input.
            <BLANKLINE>
            ERRORS
            [!]      2 extraneous example(s) found in example input
            ('b', and 'c') Extraneous example(s) will be ommitted.
            """
        # comparison is true (fails) when an  item in `given` isn't in
        # `relevant`
        comparison = ~given.isin(relevant)
        fail_msg = " ".join([str(comparison.sum()),
                             "extraneous", value_type + "(s)",
                             "found in", str(given.name),
                             report_list(given[comparison], limit=5),
                             "Extraneous", value_type + "(s)",
                             "will be ommitted."])
        passing_msg = " ".join(["No extraneous", value_type, "found in",
                                str(given.name) + "."])
        self._add_condition(comparison, Passing(passing_msg),
                            Err(fail_msg))

    def all_valid(self, given, valid, definition):
        """adds a validation check where all values in `given` must be in `valid`
        to pass. `fail_check` is `Err` (fails validation).

        Args:
            given (Pandas Series): the items representing input given
            valid (Pandas Series): list of all possible valid items accepted in
                `given`
            definition (str): string describing what makes an item in `given`
                be in `valid`

        Returns:
            None

        Examples:
            >>> v = Validation()
            >>> v.all_valid(pd.Series(["a","b"], name="example input"),  pd.Series(["a","b","c"],  name="valid value(s)"), "pre-defined")
            >>> v.all_valid(pd.Series(["a","b","c"], name="example input"),  pd.Series(["a","d"],  name="valid value(s)"), "'a' or 'd'")
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]          All values in example input are valid.
            <BLANKLINE>
             ERRORS
            [!]          2 values in example input were invalid ('b', and 'c').
            These must be 'a' or 'd' to be valid.
                    """
        # comparison is true (fails) when an item in `given` isn't in `valid`
        comparison = ~given.isin(valid)
        passing_msg = " ".join(["All values in", str(given.name),
                                "are valid."])
        fail_msg = " ".join([str((comparison).sum()), "values in",
                             str(given.name),
                             "were invalid", report_list(
                                 given[(comparison)]) + ".",
                             "These must be", definition, "to be valid."])
        self._add_condition(comparison, Passing(passing_msg),
                            Err(fail_msg))

    def flag_elements(self, flag_where, flag_elements, criteria):
        """Adds a validation check seeing if any values in flag_where are true,
        and then reports on the corresponding items in flag_elements.

        Args:
            flag_where (Pandas Series): a boolean Pandas Series where True
                represents a failed check
            flag_elements (Pandas Series): a boolean Pandas Series listing
                elements that are affected by True values in `flag_where`
            criteria (String): a brief description of what elements are
                being flagged and reported on

        Returns:
            None

        Examples:
            >>> v = Validation("element test")
            >>> v.flag_elements(pd.Series([False, False]),  pd.Series(["A", "B"]), "red flag(s)")
            >>> v.flag_elements(pd.Series([False, True]),  pd.Series(["A", "B"]), "blue flag(s)")
            >>> v.report(verbose=4)
            Validating element test . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]          No red flag(s) in element test detected.
            <BLANKLINE>
             WARNINGS
            [?]          1 blue flag(s) in element test detected. These ('B') will be treated as NA.
        """
        passing_msg = f"No {criteria} in {self.name} detected."
        fail_msg = " ".join([str(flag_where.sum()), criteria, "in",
                             self.name, "detected.",
                             "These", report_list(flag_elements[flag_where]),
                             "will be treated as NA."])

        self._add_condition(flag_where, Passing(passing_msg),
                            Warn(fail_msg))

    def flag_rows(self, flag_where, flag_criteria, flag_action="",
                  flag_tier=Warn):
        """Adds a validation check seeing if any values in flag_where are true,
        where fail_check is of type flag_tier. Note that rows are reported
        counting from 0.

        Args:
            flag_where (Pandas Series): a boolean Pandas Series where True
                represents a failed check.
            flag_criteria (str): a noun clause describing the criteria for an
                item to be flagged in `flag_where`
            flag_action (str): string describing the action to be taken if
                an item is flagged. Defaults to "".
            flag_tier (VCheck): should be either Suggest, Warn, or Err, is
                the seriousness of the failed check.

        Returns:
            None

        Examples:
            >>> v = Validation()
            >>> v.flag_rows(pd.Series([False, False]),  flag_criteria="true values")
            >>> v.flag_rows(pd.Series([False, True]),  flag_criteria="true values")
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
            CHECKS PASSED
            [X]      No true values detected.
            <BLANKLINE>
            WARNINGS
            [?]      1 true values detected in row(s) #1.
        """
        passing_msg = " ".join(["No", flag_criteria, "detected."])
        fail_msg = " ".join([str(flag_where.sum()), flag_criteria,
                             "detected in row(s)", report_row(
                                 flag_where) + ".",
                             flag_action])

        self._add_condition(flag_where, Passing(passing_msg),
                            flag_tier(fail_msg))

    def _check_df(self, df, condition, flag_criteria, flag_action="",
                  flag_tier=Warn):
        """Adds a validation check flagging the rows in every column of `df`
        where applying the function condition changes the value of the element.
        Passes flag_criteria, flag_action and flag_tier on to flag_rows().

        Args:
            df (Pandas DataFrame): a Pandas DataFrame where each column should
                be checked
            condition (function): a function that corrects possible errors in
                each column, but does not change elements which are already
                fine.
            flag_criteria (str): a noun clause describing the criteria for an
                item to be changed by `condition`
            flag_action (str): string describing the action to be taken if
                an item is flagged. Defaults to "".
            flag_tier (VCheck): should be either Suggest, Warn, or Err, is
                the seriousness of the failed check.
        Returns:
            Pandas DataFrame: the Pandas DataFrame where condition() has been
                applied to every column.

        Examples:
            >>> v = Validation()
            >>> test_df = pd.DataFrame({"A":["a","B","c"], "B":["D","e","F"]})
            >>> v._check_df(test_df, str.lower, flag_criteria="lowercase char")
               A  B
            0  a  d
            1  b  e
            2  c  f

            >>> v._check_df(test_df, str.upper, flag_criteria="uppercase char")
               A  B
            0  A  D
            1  B  E
            2  C  F

            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             WARNINGS
            [?]      1 lowercase char column A detected in row(s) #1.
            [?]      2 lowercase char column B detected in row(s) #0, and #2.
            [?]      2 uppercase char column A detected in row(s) #0, and #2.
            [?]      1 uppercase char column B detected in row(s) #1.

        """
        df = df.copy()
        for name, aseries in df.items():
            applied = aseries.apply(condition)
            self.flag_rows(applied.ne(aseries),
                           flag_criteria + " column " + str(name),
                           flag_action,
                           flag_tier)
            df.loc[:, name] = applied
        return df

    def check_na(self, df):
        """Adds a validation check flagging the rows in every column of `df`
        that are `None`

        Args:
            df (Pandas DataFrame): a Pandas DataFrame with columns that should
                have no NA values

        Returns:
            None

        Examples:
            >>> v = Validation()
            >>> test_df = pd.DataFrame({"A":["a","B","c"], "B":["D","e",None]})
            >>> v.check_na(test_df)
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]          No NA's in column A detected.
            <BLANKLINE>
             WARNINGS
            [?]          1 NA's in column B detected in row(s) #2.
        """
        self._check_df(df,
                       condition=lambda x: "" if x is None else x,
                       flag_criteria="NA's in",
                       flag_tier=Warn
                       )

    def fix_whitespace(self, df):
        """Adds a validation check flagging the rows in every column of `df`
        that contain whitespace

        Args:
            df (Pandas DataFrame): a Pandas DataFrame with columns that should
                have no whitespace

        Returns:
            Pandas DataFrame: `df` where whitespace is replaced with an
                underscore

        Examples:
            >>> v = Validation()
            >>> test_df = pd.DataFrame({"A":["a"," B ","Test Data"],  "B":["D"," e","F "]})
            >>> v.fix_whitespace(test_df)
                       A  B
            0          a  D
            1          B  e
            2  Test_Data  F
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]          No whitespace in column B detected.
            <BLANKLINE>
             WARNINGS
            [?]          1 leading/trailing spaces column A detected in row(s) #1.  Leading/trailing spaces will be removed.
            [?]          2 leading/trailing spaces column B detected in row(s)  #1, and #2. Leading/trailing spaces will be removed.
            [?]          1 whitespace in column A detected in row(s) #2.  Whitespace will be converted to '_'
        """
        stripped_df = self._check_df(df.fillna("").astype(str),
                                     str.strip,
                                     flag_criteria="leading/trailing "
                                     "spaces",
                                     flag_action="Leading/trailing spaces "
                                     "will be removed.")
        # pass stripped_df to check_df with a regex expression to replace
        # remaining whitespace with underscores, except for the " to "
        # construction
        return self._check_df(stripped_df.fillna("").astype(str),
                              lambda x: re.sub(r"(?<!to)\s(?!to)", "_", x),
                              flag_criteria="whitespace in",
                              flag_action="Whitespace will be converted to '_'"
                              )

    def fix_alnum(self, df):
        """Adds a validation check flagging the rows in every column of `df`
        that contain non-alphanumeric characters. Regex removes all characters
        that are not alpha-numeric, but leaves periods that are part of a
        number.

        Args:
            df (Pandas DataFrame): a Pandas DataFrame with columns that should
                have only alphanumeric characters

        Returns:
            Pandas DataFrame: `df` where alphanumeric characters are removed

        Examples:
            >>> v = Validation()
            >>> test_df = pd.DataFrame({"A":["a","3.0","c"],  "B":["??.test","test<>!",";test_data"]})
            >>> v.fix_alnum(test_df)
               A          B
            0  a       test
            1  3.0     test
            2  c  test_data
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]      No non-alphanumeric value(s) in column A detected.
            <BLANKLINE>
             WARNINGS
            [?]      3 non-alphanumeric value(s) in column B detected in row(s)
            #0, #1, and #2. This text should be alphanumeric. Non-alphanumeric
            characters will be removed.

        """
        return self._check_df(df,
                              lambda x: re.sub(r"[^a-zA-Z0-9_ -\.]|\.(?!\d)|\!",
                                               r"", str(x)),
                              flag_criteria="non-alphanumeric value(s) in",
                              flag_action="This text should be alphanumeric. "
                              "Non-alphanumeric characters will be removed."
                              )

    def fix_lowcase(self, df):
        """Adds a validation check flagging the rows in every column of `df`
        that contain lowercase characters.

        Args:
            df (Pandas DataFrame): a Pandas DataFrame with columns that should
                have only uppercase characters

        Returns:
            Pandas DataFrame: `df` where all characters are uppercase

        Examples:
            >>> v = Validation()
            >>> test_df = pd.DataFrame({"A":["a","B","c"], "B":["D","e","F"]})
            >>> v.fix_lowcase(test_df)
               A  B
            0  A  D
            1  B  E
            2  C  F
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             WARNINGS
            [?]      2 lower case value(s) in  column A detected in row(s) #0,
            and #2. Convention to have this text be uppercase. Lower case text
            will be made uppercase.
            [?]      1 lower case value(s) in  column B detected in row(s) #1.
            Convention to have this text be uppercase. Lower case text will be
            made uppercase.

        """
        return self._check_df(df.astype(str),
                              lambda x: x.upper(),
                              flag_criteria="lower case value(s) in ",
                              flag_action="Convention to have this text be "
                              "uppercase. Lower case text will be made "
                              "uppercase.")

    def fix_upcase(self, df):
        """Adds a validation check flagging the rows in every column of `df`
        that contain uppercase characters

        Args:
            df (Pandas DataFrame): a Pandas DataFrame with columns that should
                have only lowercase characters

        Returns:
            Pandas DataFrame: `df` where all characters are lowercase

        Examples:
            >>> v = Validation()
            >>> test_df = pd.DataFrame({"A":["a","B","c"], "B":["D","e","F"]})
            >>> v.fix_upcase(test_df)
               A  B
            0  a  d
            1  b  e
            2  c  f
            >>> v.report(verbose=4)
            Validating  . . .
            <BLANKLINE>
             WARNINGS
            [?]      1 upper case value(s) in column A detected in row(s) #1.
            Convention is to have this text be lowercase. Upper case text will
            be made lowercase.
            [?]      2 upper case value(s) in column B detected in row(s) #0,
            and #2. Convention is to have this text be lowercase. Upper case
            text will be made lowercase.
        """
        return self._check_df(df.astype(str),
                              lambda x: x.lower(),
                              flag_criteria="upper case value(s) in",
                              flag_action="Convention is to have this text be "
                              "lowercase. Upper case text will be made"
                              " lowercase.")

    def is_valid(self):
        """Checks to see if instance is valid.

        Args:
            None

        Returns:
            bool: True if is valid (has no errors in vchecks) and False if
                instance has errors or where vchecks is empty.

        Examples:
            >>> Validation().is_valid()
            False
            >>> v = Validation()
            >>> v.must_contain(pd.Series(["A", "B"]), pd.Series(["B"]))
            >>> v.is_valid()
            True
            >>> v.must_contain(pd.Series(["A", "B"]), pd.Series(["C"]))
            >>> v.is_valid()
            False
        """
        if self.vchecks.empty:
            return False
        return (self.vchecks["Tier"] == "Error").sum() == 0

    def report(self, verbose=2):
        """Prints the checks in the vchecks attribute

        Args:
            verbose (int): Parameter controlling how much to print by filtering
                for the level in each vchecks row to be less than or equal to
                verbose. Defaults to 2 (print only converted `Warn` and `Err`
                checks)

        Returns:
            None

        Examples:
            >>> v = Validation("Testing Tests")
            >>> v._add_condition(pd.Series([False, False, False]),  Passing("Passed test"), Err("Failed test"))
            >>> v._add_condition(pd.Series([False, False, False]),  Passing("Passed test 2"), Err("Failed test"))
            >>> v._add_condition(pd.Series([False, False, True]),  Passing("Passed test"), Err("Error test"))
            >>> v._add_condition(pd.Series([False, False, True]),  Passing("Passed test"), Warn("Warn test"))
            >>> v._add_condition(pd.Series([False, False, True]),  Passing(""), Suggest("Suggest test"))
            >>> v.report(verbose=1)
            Validating Testing Tests . . .
            <BLANKLINE>
             ERRORS
            [!]      Error test
            >>> v.report(verbose=4)
            Validating Testing Tests . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]      Passed test
            [X]      Passed test 2
            <BLANKLINE>
             ERRORS
            [!]      Error test
            <BLANKLINE>
             SUGGESTIONS
            [i]      Suggest test
            <BLANKLINE>
             WARNINGS
            [?]      Warn test
        """
        if self.vchecks.empty:
            print("No validation checks made.")
            return
        within_verbose = self.vchecks[self.vchecks["Level"] <= verbose]
        if not within_verbose.empty:
            print(f"Validating {self.name} . . .")
        final_reports = within_verbose.groupby("Title")
        for title in final_reports.groups:
            print("\n", title)
            for i, single_report in final_reports.get_group(title).iterrows():
                print(single_report["Bullet"], "\t",
                      single_report["Message"])


def report_row(flag_where):
    """A helper method to return an english explanation of what rows have been
    flagged with a failed validation check.

    Args:
        flag_where (Pandas Series): boolean Pandas Series representing failed
            validation checks.

    Returns:
        str: a string reporting the index of the flagged rows

    Examples:
        >>> report_row(pd.Series([True, True, False, True, False]))
        '#0, #1, and #3'

    """
    flagged = flag_where[flag_where.fillna(False)]
    # if flagged.index.is_numeric():
    if pd.api.types.is_numeric_dtype(flagged.index):
        unformatted_report = report_list(flagged.index.tolist(), paren=False)
        return re.sub(r"'(\d+)'", r"#\1", unformatted_report)
    return report_list(flagged.index.tolist(), paren=False)


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
