#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Defines MapCondition class and its subclasses, each represent a single condition
that uses a relationship to transform raw data into a boolean column while
preserving the NA values.
"""
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd

from pycrossva.utils import english_relationship


class MapCondition(ABC):
    """ Abstract class representing a single mapped condition in the mapping
    data, which gives instructions to transform the raw input data into the
    form needed for a VA instrument. The main configuration class is composed
    of these.

    Attributes:
        name (str): the name of the new column to be created
        relationship (str): the relationship of the input data to the condition
            Should be one of "ge" (greater than or equal to), "gt"
            (greater than), "le" (less than or equal to), "lt" (less than),
            "eq" (equal to), "ne" (not equal to),
            "contains" (if string contains) or "between"
            (between the two numbers, inclusive).
        preq_column (str or None): name of the pre-requisite column if it exists,
            or `None` if no pre-requisite
        source (str): the name of the column to be checked

    """

    def __init__(self, condition_row):
        """Inits SingleMapCondition instance with a row of a valid mapping file
         (or pd.Series or dictionary containing the correct fields)
         to be created.

        Args:
            condition_row (Pandas Series or dict): that contains five key-value
                pairs. Required keys are "New Column Name", "Relationship",
                "Condition", "Prerequisite", and "Source Column ID". This
                should come from a valid mapping file with a Configuration
                object.

        Returns:
            None

        """
        # TODO init checks
        self.condition = condition_row["Condition"]
        self.name = condition_row["New Column Name"]
        self.relationship = condition_row["Relationship"]
        self.preq_column = condition_row["Prerequisite"]
        self.source_name = condition_row["Source Column ID"]

    def factory(relationship, condition=""):
        """static class factory method, which determines which subclass to
        return

        Args:
            relationship (str): a relationship in (gt, ge, lt, le, ne, eq,
                contains, between) that represents a comparison to be
                made to the raw data
            condition (str or int): the condition being matched. if
                relationship is ambiguous, then this determins if condition
                is numerical or string. Defaults to empty string.

        Returns:
            MapCondition: returns specific subclass that corresponds to the
            correct relationship

        Examples:
            >>> MapCondition.factory("ge") #doctest: +ELLIPSIS
            <class '...NumMapCondition'>

            >>> MapCondition.factory("eq", 0) #doctest: +ELLIPSIS
            <class '...NumMapCondition'>

            >>> MapCondition.factory("eq") #doctest: +ELLIPSIS
            <class '...StrMapCondition'>

            >>> MapCondition.factory("contains") #doctest: +ELLIPSIS
            <class '...ContainsCondition'>

            >>> MapCondition.factory("between") #doctest: +ELLIPSIS
            <class '...BetweenCondition'>

            >>> MapCondition.factory("eqq") #doctest: +ELLIPSIS
            Traceback (most recent call last):
            AssertionError: No defined Condition class for eqq type

        """
        if relationship in ["gt", "ge", "lt", "le"]:
            return NumMapCondition
        if relationship in ["ne", "eq"]:
            if str(condition).isdigit():
                return NumMapCondition
            return StrMapCondition
        if relationship == "contains":
            return ContainsCondition
        if relationship == "between":
            return BetweenCondition
        assert 0, "No defined Condition class for " + relationship + " type"

    def check(self, prepared_data):
        """Checks the condition against dataframe. Do not check NAs, just add
        them back afterward.

        Args:
            prepared_data (Pandas DataFrame): a dataframe containing a created
                column with the name specified in self.source_dtype

        Returns:
            Array: returns a boolean array where the condition is met (as float)

        Examples:
            >>> test_df = pd.DataFrame({"source_test_str": ["test condition", "test condition 2", np.nan], "source_test_num": [4, 5, np.nan]})
            >>> StrMapCondition({"Condition" : "test condition", "New Column Name" : "test new column name", "Relationship" : "eq", "Prerequisite" : None, "Source Column ID" : "source_test"}).check(test_df)
            array([ 1., 0., nan])

            >>> NumMapCondition({"Condition" : 4.5, "New Column Name" : "test new column name", "Relationship" : "ge", "Prerequisite" : None, "Source Column ID" : "source_test"}).check(test_df)
            array([ 0., 1., nan])
        """
        eval_col = prepared_data[self.source_dtype]

        return np.where(eval_col.notnull(),
                        self._run_check(eval_col),
                        np.nan)

    def _run_check(self, eval_col):
        """internal method to check the condition on a given column with no NAs

        Args:
            eval (Pandas Series): a Pandas Series containing data to evaluate

        Returns:
            Pandas Series: returns a bolean series where the condition is met

        Examples:
            >>> test = pd.Series(["A","B","C"])
            >>> StrMapCondition({"Condition" : "B", "New Column Name" : "test new column name", "Relationship" : "eq", "Prerequisite" : None, "Source Column ID" : "source_test"})._run_check(test)
            0    False
            1     True
            2    False
            dtype: bool

            >>> test = pd.Series([1, 2, 3])
            >>> NumMapCondition({"Condition" : 3,  "New Column Name" : "test new column name",  "Relationship" : "ge",  "Prerequisite" : None,  "Source Column ID" : "source_test"})._run_check(test)
            0    False
            1    False
            2     True
            dtype: bool
        """
        return getattr(eval_col, self.relationship)(self.condition)

    def check_prereq(self, transformed_data):
        """checks for pre-req column status; if there is no pre-req, returns
        true, else looks up values of pre-req column from transformed_data

        Args:
            transformed_data (Pandas DataFrame): the new dataframe
                being created, which contains any pre-req columns

        Returns:
            boolean or boolean pd.series: representing whether pre-req is
                satisfied

        Examples:
            >>> test_df = pd.DataFrame({"preq_one": np.repeat(True,5),  "preq_two": np.repeat(False, 5)})

            If there is no pre-req, simply returns True (1) Pandas can
            interpret this in boolean indexing.

            >>> NumMapCondition({"Condition" : 4.5,  "New Column Name" : "test new column name",  "Relationship" : "ge",  "Prerequisite" : None,  "Source Column ID" : "source_test"}  ).check_prereq(test_df)
            1

            If there is a pre-req, then returns the value of transformed_data
            with that column.

            >>> NumMapCondition({"Condition" : 4.5,  "New Column Name" : "test new column name",  "Relationship" : "ge",  "Prerequisite" : "preq_one",  "Source Column ID" : "source_test"}  ).check_prereq(test_df)
            0    True
            1    True
            2    True
            3    True
            4    True
            Name: preq_one, dtype: bool

            >>> NumMapCondition({"Condition" : 4.5,  "New Column Name" : "test new column name",  "Relationship" : "ge",  "Prerequisite" : "preq_two",  "Source Column ID" : "source_test"}  ).check_prereq(test_df)
            0    False
            1    False
            2    False
            3    False
            4    False
            Name: preq_two, dtype: bool
        """
        if pd.isnull(self.preq_column) or self.preq_column == "":
            return 1
        return transformed_data[self.preq_column]

    @property
    @abstractmethod
    def possible_values():
        """ abstract method stub
        generate a non-exhaustive list possible values implied by condition """
        return

    def prepare_data(self, raw_data):
        """prepares raw_data by ensuring dtypes are correct for each comparison

        Args:
            raw_data (dataframe): a data frame containing raw data, including
                the column given in self.source_name.

        Returns:
            Pandas Series: the column in `raw_data` named in self.source_name,  with the attribute self.prep_func applied to it.

        """
        return np.where(raw_data[self.source_name].notnull(),
                        self.prep_func(raw_data[self.source_name]),
                        np.nan)

    def describe(self):
        """ just a wrapper for the __str__ function """
        return self.__str__()

    def __repr__(self):
        """console representation for class

        Examples:
        >>> NumMapCondition({"Condition" : 4,  "New Column Name" : "test new name",  "Relationship" : "ge",  "Prerequisite" : "preq_two",  "Source Column ID" : "source_test"})
        <NumMapCondition:	 test new name = [column source_test].ge(4.0)>
"""
        return " ".join(["<" + self.__class__.__name__ + ":\t",
                         self.name, "=",
                         "[column " + self.source_name + "]." +
                         self.relationship + "(" + str(self.condition) + ")>"
                         ])

    def __str__(self):
        """str representation

        Examples:
            >>> print(NumMapCondition({"Condition" : 4,  "New Column Name" : "test new column name",  "Relationship" : "ge",  "Prerequisite" : "preq_two",  "Source Column ID" : "source_test"}))
            NumMapCondition: New column test new column name is true where   input data column source_test is greater than or equal to 4.0 and   the new column preq_two is true.

        """
        report_as_list = [self.__class__.__name__ + ":",
                          "New column", self.name,
                          "is true where input data column",
                          self.source_name,
                          english_relationship(self.relationship),
                          str(self.condition)]
        report_as = " ".join(report_as_list)

        #if not self.preq_column is "":
        if self.preq_column != "":
            return (report_as + " and the new column " +
                    str(self.preq_column) + " is true.")
        return report_as


class StrMapCondition(MapCondition):
    """class representing a str condition, inherits from MapCondition

    Attributes:
        source_dtype (str): instance attribute, a copy of the instance
            attribute self.source_name with "_str" appended,
            to represent the expected dtype
        prep_func (function): class attribute, a function to apply before
            making a string-based comparison. It preserves null values but
            changes all else to str.
    """

    def __init__(self, condition_row):
        """Inits StrMapCondition

        Args:
            condition_row (Pandas Series or dict): see MapCondition's __init__

        Examples:
            >>> StrMapCondition({"Condition" : "test cond",  "New Column Name" : "test new name",  "Relationship" : "eq",  "Prerequisite" : None,  "Source Column ID" : "source_test"}  )
            <StrMapCondition:	 test new name = [column source_test].eq(test cond)>
        """
        super().__init__(condition_row)
        self.source_dtype = self.source_name + "_str"
        self.prep_func = lambda x: x.astype(str)

    def possible_values(self):
        """generate a non-exhaustive list possible values implied by condition

        Args:   None

        Returns:
            list: list containing 4 possible values (empty string, NA, None,
                and the self.condition attribute) that might be expected by
                this condition

        Examples:
            >>> StrMapCondition({"Condition" : "test condition",  "New Column Name" : "test new column name",  "Relationship" : "eq",  "Prerequisite" : None,  "Source Column ID" : "source_test"}  ).possible_values()
            ['', nan, None, 'test condition', 'yes', 'no', 'dk', 'ref']

        """
        return ["", np.nan, None, self.condition, "yes", "no", "dk", "ref"]


class NumMapCondition(MapCondition):
    """class representing a numerical condition, inherits from MapCondition

    Attributes:
        source_dtype (str): a copy of the instance attribute self.source_name
            with "_num" appended, to represent the expected dtype
        prep_func (function): class attr, a function to apply before making a
            numerical-based comparison. pd.to_numeric() coerces non-number data
            to NaN.
    """

    def __init__(self, condition_row, cast_cond=True):
        """Inits NumMapCondition

        Args:
            condition_row (Pandas Series or dict): see MapCondition's __init__
            cast_cond (bool): Whether to force self.condition attribute to be a
                float. Defaults to True.

        Examples:
            >>> NumMapCondition({"Condition" : 3,  "New Column Name" : "test new name",  "Relationship" : "ge",  "Prerequisite" : None,  "Source Column ID" : "source_test"})
            <NumMapCondition:	 test new name = [column source_test].ge(3.0)>

        """
        super().__init__(condition_row)
        if cast_cond:
            self.condition = float(condition_row["Condition"])
        self.source_dtype = self.source_name + "_num"
        self.prep_func = lambda x: pd.to_numeric(x, errors="coerce")

    def possible_values(self):
        """generate a non-exhaustive list of possible values implied by
        condition

        Args:   None

        Returns:
            list: list containing range of possible values. If a greater than
                relationship, the list will include ints from self.condition
                + 1 to self.condition*2. If a less than relationship, it will
                include values from 0 to self.condition. If the condition
                includes "equal to", then self.condition will also be included.

        Examples:
            >>> NumMapCondition({"Condition" : 3,  "New Column Name" : "test new name",  "Relationship" : "ge",  "Prerequisite" : None,  "Source Column ID" : "source_test"}).possible_values()
            [4.0, 5.0, 3.0]
            >>> NumMapCondition({"Condition" : 3,  "New Column Name" : "test new name",  "Relationship" : "lt",  "Prerequisite" : None,  "Source Column ID" : "source_test"}).possible_values()
            [0.0, 1.0, 2.0]

        """
        if self.relationship[0] == "g":
            possible = np.arange(self.condition + 1, self.condition * 2)
        else:
            possible = np.arange(0, self.condition)
        if self.relationship[1] == "e":
            possible = np.append(possible, self.condition)
        return possible.tolist()


class ContainsCondition(StrMapCondition):
    """ Subclass of StrMapCondition that overrides ._run_check() method for the
    `contains` relationship
    """

    def _run_check(self, eval_col):
        """overides _run_check condition of abstract MapCondition.
        checks condition against input data to see if input data contains
        the substring in the self.condition attribute.


        Args:
            input_data (Pandas DataFrame): a prepared dataframe from a CrossVA
                instance containing a column named self.source_dtype

        Returns:
            boolean Pandas Series: a Pandas Series that is true where the
                column named in self.source_dtype contains the substring in
                self.condition

        Examples:
            >>> test_df = pd.DataFrame({"source_test_1_str": ["test condition",  "test condition 2"],  "source_test_2_str": ["test test",  "test condition"]})
            >>> ContainsCondition({"Condition" : "test condition",  "New Column Name" : "test new column name",  "Relationship" : "contains",  "Prerequisite" : None,  "Source Column ID" : "source_test_1"}).check(test_df)
             array([1., 1.])

            >>> ContainsCondition({"Condition" : "test condition",  "New Column Name" : "test new column name",  "Relationship" : "contains",  "Prerequisite" : None,  "Source Column ID" : "source_test_2"}).check(test_df)
            array([0., 1.])
        """
        return eval_col.fillna("").str.contains(self.condition)


class BetweenCondition(NumMapCondition):
    """Subclass of NumMapCondition that overrides __init__ and .check() methods
    for the `between` relationship

    Attributes:
        low (float): a float representing the lowest acceptable value (incl)
        high (float): a float representing the highest acceptable value (incl)
    """

    def __init__(self, condition_row):
        """Inits between condition. Same as NumMapCondition init, but do not
        cast condition_row["condition"] to a float, instead split it at " to "
        and convert the first and second elements to floats.

        Args:
            condition_row (Pandas Series or dict): see MapCondition's __init__

        Examples:
            >>> BetweenCondition({"Condition" : "3 to 5",  "New Column Name" : "test new column name",  "Relationship" : "between",  "Prerequisite" : None,  "Source Column ID" : "source_test_1"})
            <BetweenCondition:	 test new column name = [column
            source_test_1].between(3 to 5)>
        """
        super().__init__(condition_row, False)
        self.low, self.high = [float(e) for e in self.condition.split(" to ")]

    def _run_check(self, eval_col):
        """checks condition against input data to see if condition is true.

        Args:
            input_data (Pandas DataFrame): a prepared dataframe from a CrossVA
                instance containing a column named self.source_dtype

        Returns:
            array: true (1) where the column named in
                self.source_dtype has a number between self.low and self.high,
                inclusive

        Examples:
            >>> test_df = pd.DataFrame({"source_test_1_num": [1,2,3],   "source_test_2_num": [4,5,6]})
            >>> BetweenCondition({"Condition" : "3 to 5",  "New Column Name" : "test new column name",  "Relationship" : "between",  "Prerequisite" : None,  "Source Column ID" : "source_test_1"}).check(test_df)
            array([0., 0., 1.])

            >>> BetweenCondition({"Condition" : "3 to 5",  "New Column Name" : "test new column name",  "Relationship" : "between",  "Prerequisite" : None,  "Source Column ID" : "source_test_2"}).check(test_df)
            array([1., 1., 0.])

        """
        return eval_col.between(self.low, self.high)

    def possible_values(self):
        """generate a non-exhaustive list of possible values implied by the
        condition

        Args:   None

        Returns:
            list: a list of integers between self.low - 1 and self.high + 2

        Examples:
            >>> BetweenCondition({"Condition" : "3 to 5",  "New Column Name" : "test new column name",  "Relationship" : "between",  "Prerequisite" : None,  "Source Column ID" : "source_test_2"}  ).possible_values()
            [2.0, 3.0, 4.0, 5.0, 6.0]

        """
        return np.arange(self.low - 1, self.high + 2).tolist()


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
