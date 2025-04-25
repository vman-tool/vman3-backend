"""
Structure for Configuration class
"""

# !/usr/bin/env python3
#  -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

from pycrossva.mappings import MapCondition
from pycrossva.utils import report_list
from pycrossva.validation import Validation, Err, Warn


class Configuration():
    """ Configuration class details the relationship between a set of input
    data and output data. It is composed of MapConditions that
    transform an input data source (2012 WHO, 2016 WHO 141, 2016 WHO 151,
    PHMRC SHORT) into a different data form (PHMRC SHORT, InSilicoVA,
    InterVA4, InterVA5, or Tarrif2) for verbal autopsy.

    Attributes:
        given_columns (Pandas Series): columns of mapping dataframe.
        required_columns (Pandas Series): required columns in mapping data.
        main_columns (list): the four main columns required in config_data.
        valid_relationships (Pandas Series): contains list of valid
            relationships to use in comparisons. Relationships should be an
            attr of Pandas Series object, or be defined as a subclass of
            MapCondition.
        config_data (Pandas DataFrame): dataframe containing mapping
            relationships written out.
        given_prereq (Pandas Series): lists pre-requisites referenced in config
            data.
        new_columns (Pandas Series): lists the new columns to be created with
            config data.
        source_columns (Pandas Series): lists the source columns required in
            the raw input data.
        verbose (int): controls default verbosity of printing to console.
        process_strings (boolean): whether or not to remove whitespace and
            non-alphanumeric characters from strings in condition field and in
            raw_data during mapping.
        validation (Validation): a validation object containing the validation
            checks made

    """
    required_columns = pd.Series(['New Column Name',
                                  'New Column Documentation',
                                  'Source Column ID',
                                  'Source Column Documentation',
                                  'Relationship',
                                  'Condition', 'Prerequisite',
                                  ],
                                 name="expected columns")

    main_columns = ["New Column Name", "Source Column ID",
                    "Relationship", "Condition"]

    valid_relationships = pd.Series(["gt", "ge", "lt", "le", "between", "eq",
                                     "ne", "contains"],
                                    name="valid relationships")

    def __init__(self, config_data, verbose=1, process_strings=True):
        """Inits configuration class from a Pandas DataFrame containing
            mapping data.

        Args:
            config_data (Pandas DataFrame): dataframe representing the mapping
                data relationships, in specified format.
            verbose (int): should be in range 1-5,
                controls print output, where 0=silent. Defaults to 1.
            process_strings (boolean): Whether or not to normalize string
                conditions and data. Defaults to True.

        Returns:
            Nothing

        Examples:
            >>> MAP_PATH = "resources/mapping_configuration_files/"
            >>> EX_MAP_1 = pd.read_csv(MAP_PATH + "example_config_1.csv")
            >>> Configuration(EX_MAP_1)
            Unvalidated Configuration instance with 16 mapping conditions

        """
        # TODO check for df & # of rows & these 3 columns
        # throw cant instantiate error?

        self.config_data = config_data
        self.given_columns = pd.Series(config_data.columns,
                                       name="the given columns")
        self.process_strings = process_strings

        def plain_info(aseries, new_name):
            """ returns Pandas Series without NAs or duplicates renamed to
            `new_name` """
            return aseries.drop_duplicates().dropna().rename(new_name)

        self.given_relationships = plain_info(config_data["Relationship"],
                                              "Relationship column")
        self.given_prereq = plain_info(config_data["Prerequisite"],
                                       "Prerequisite column")
        self.new_columns = plain_info(config_data["New Column Name"],
                                      "New Column Name column")
        self.source_columns = plain_info(config_data["Source Column ID"],
                                         "Source Column ID column")
        self.verbose = verbose
        self.validation = Validation("Mapping Configuration")

    def __str__(self):
        """string representation of class"""
        return self.__repr__()

    def __repr__(self):
        """string representation of class"""
        val_status = ("Validated" if self.validation.is_valid() else
                      "Unvalidated")
        return " ".join([val_status, self.__class__.__name__, "instance with",
                         str(len(self.config_data)), "mapping conditions"])

    def list_conditions(self):
        """Lists the final mapping conditions contained in Configuration object

        Returns:
            list: list of MapConditions, where each MapConditions is
            created from a row of processed mapping data.

        Examples:
            >>> MAP_PATH = "resources/mapping_configuration_files/"
            >>> EX_MAP_1 = pd.read_csv(MAP_PATH + "example_config_1.csv")
            >>> c = Configuration(EX_MAP_1)
            >>> c.list_conditions()[:5]
            [<StrMapCondition:     AB_POSIT = [column Id10403].eq(yes)>,
             <StrMapCondition:     AB_SIZE = [column Id10362].eq(yes)>,
             <NumMapCondition:     AC_BRL = [column Id10169].lt(14.0)>,
             <NumMapCondition:     AC_CONV = [column Id10221].lt(10.0)>,
             <NumMapCondition:     AC_COUGH = [column Id10154].lt(21.0)>]

        """
        self.config_data["Standalone"] = self.config_data["Prerequisite"].isnull()
        # Sort first so that columns w/o prereqs are processed first
        return [MapCondition.factory(row["Relationship"], row["Condition"])(row)
                for i, row in self.config_data.sort_values("Standalone",
                                                           ascending=False).iterrows()]

    def validate(self, verbose=None):
        """Prepares and validates the Configuration object's mapping conditions.
        Validation fails if there are any inoperable errors. Problems that can
        be fixed in place are processed and flagged as warnings.

        Args:
            verbose (int): controls print output, should be in range 0-5,
                each higher level includes the messages of each level below it.
                Where verbose = 0, nothing will be printed to console.
                Where verbose = 1, print only errors to console,
                where verbose = 2, also print warnings,
                where verbose = 3, also print suggestions and status checks,
                where verbose = 4, also print passing validation checks,
                where verbose = 5, also print description of configuration
                conditions.
                Defaults to None; if none, replace with self.verbose attribute
        Returns:
            Boolean: boolean representing whether there are any errors that
                prevent validation

        Examples:
            >>> MAP_PATH = "resources/mapping_configuration_files/"
            >>> EX_MAP_2 = pd.read_csv(MAP_PATH + "example_config_2.csv")
            >>> c = Configuration(EX_MAP_2)
            >>> c.validate(verbose=4)
            Validating Mapping Configuration . . .
            <BLANKLINE>
             CHECKS PASSED
            [X]          All expected columns ('New Column Name', 'New Column Documentation', 'Source Column ID', 'Source Column Documentation', 'Relationship', 'Condition', and 'Prerequisite') accounted for in configuration file.
            [X]          No leading/trailing spaces column New Column Name detected.
            [X]          No leading/trailing spaces column Relationship detected.
            [X]          No leading/trailing spaces column Prerequisite detected.
            [X]          No leading/trailing spaces column Condition detected.
            [X]          No whitespace in column Condition detected.
            [X]          No upper case value(s) in column Relationship detected.
            [X]          No upper case value(s) in column Condition detected.
            [X]          No non-alphanumeric value(s) in column Source Column ID detected.
            [X]          No non-alphanumeric value(s) in column Relationship detected.
            [X]          No non-alphanumeric value(s) in column Condition detected.
            [X]          No new column(s) listed but not defined in Mapping Configuration detected.
            [X]          No NA's in column New Column Name detected.
            [X]          No NA's in column Source Column ID detected.
            <BLANKLINE>
             ERRORS
            [!]          3 values in Relationship column were invalid ('eqqqq', 'another fake', and 'gee'). These must be a valid method of pd.Series, e.g. ('gt', 'ge', 'lt', 'le', 'between', 'eq', 'ne', and 'contains') to be valid.
            [!]          2 row(s) containing a numerical relationship with non-number condition detected in row(s) #8, and #9.
            [!]          2 values in Prerequisite column were invalid ('ABDOMM', and 'Placeholder here'). These must be defined in the 'new column name' column of the config file to be valid.
            <BLANKLINE>
             WARNINGS
            [?]          2 whitespace in column New Column Name detected in row(s) #6, and #8. Whitespace will be converted to '_'
            [?]          1 whitespace in column Relationship detected in row(s) #4. Whitespace will be converted to '_'
            [?]          1 whitespace in column Prerequisite detected in row(s) #9. Whitespace will be converted to '_'
            [?]          1 non-alphanumeric value(s) in column New Column Name detected in row(s) #6. This text should be alphanumeric. Non-alphanumeric characters will be removed.
            [?]          2 duplicate row(s) detected in row(s) #1, and #14. Duplicates will be dropped.
            [?]          1 NA's in column Relationship detected in row(s) #3.
            [?]          1 NA's in column Condition detected in row(s) #6.
            False
        """
        if verbose is None:
            verbose = self.verbose

        # Check that all expected columns accounted for
        col_passing_msg = " ".join(["All expected columns",
                                    report_list(self.required_columns),
                                    "accounted for in configuration file."])
        self.validation.must_contain(self.given_columns,
                                     self.required_columns,
                                     passing_msg=col_passing_msg)

        # reindex - any missing columns become filled with NA
        self.config_data = self.config_data.reindex(
            columns=self.required_columns)

        # Drop any rows that are entirely blank without warnings
        self.config_data = self.config_data.dropna(how="all")

        # Processing strings
        # columns that should contain no whitespace
        ws_col = ["New Column Name", "Relationship", "Prerequisite"]
        lowercase_col = ["Relationship"]  # columns that should be lowercase
        if self.process_strings:
            ws_col.append("Condition")
            lowercase_col.append("Condition")
        self.config_data.fillna("na", inplace=True)  # fill NAs for str ops

        # Remove whitespace
        self.config_data.loc[:, ws_col] = self.validation.fix_whitespace(
            self.config_data.loc[:, ws_col])

        # Check for uppercase characters
        self.config_data.loc[:, lowercase_col] = self.validation.fix_upcase(
            self.config_data.loc[:, lowercase_col])

        # Check that main columns contain only alphanumeric values
        self.config_data.loc[:, self.main_columns] = self.validation.fix_alnum(
            self.config_data.loc[:, self.main_columns])

        # Check for missing values
        self.config_data = self.config_data.replace("na", np.nan)

        # Check for duplicate rows & drop them
        self.validation.flag_rows(self.config_data.duplicated(),
                                  flag_criteria="duplicate row(s)",
                                  flag_action="Duplicates will be dropped.")
        self.config_data = self.config_data.drop_duplicates()

        # Check and note if there are missing sources/conditions/rel
        # ie if we expect any of these sources to be absent
        defined_no_source = (np.all(self.config_data[["Source Column ID",
                                                      "Relationship", "Condition"]].isnull(), axis=1)
                             & self.config_data["New Column Name"].notnull())

        self.validation.flag_elements(
            defined_no_source,
            self.config_data["New Column Name"],
            criteria="new column(s) listed but not defined")

        self.config_data = self.config_data.loc[~defined_no_source, :]

        # Check & drop rows that contain any NAs in main columns
        self.validation.check_na(self.config_data[self.main_columns])
        self.config_data = self.config_data.loc[np.all(
            self.config_data[self.main_columns].notnull(), axis=1), :]

        # check all relationships in relationship column are valid
        self.validation.all_valid(self.given_relationships,
                                  self.valid_relationships,
                                  "a valid method of pd.Series, e.g. " +
                                  report_list(self.valid_relationships))

        # check for non-number conditions with numerical relationships
        invalid_num = (self.config_data["Relationship"].isin(
            ["gt", "ge", "le", "lt"]) &
            (pd.to_numeric(self.config_data["Condition"],
                           errors="coerce").isnull()))
        self.validation.flag_rows(invalid_num,
                                  flag_criteria="row(s) containing a numerical"
                                  + " relationship with non-number condition",
                                  flag_tier=Err)

        # check all prerequisite columns are also defined in configuration
        self.validation.all_valid(self.given_prereq, self.new_columns,
                                  "defined in the 'new column name' column " +
                                  "of the config file")

        self.validation.report(verbose=verbose)  # report
        if verbose == 5:
            self.describe()

        # return true only if there are zero errors
        return self.validation.is_valid()

    def describe(self):
        """Prints the mapping relationships in the Configuration object to
        console.

        Args:
            None

        Returns:
            None

        Examples:
            >>> MAP_PATH = "resources/mapping_configuration_files/"
            >>> EX_MAP_1 = pd.read_csv(MAP_PATH + "example_config_1.csv")
            >>> Configuration(EX_MAP_1).describe()
            MAPPING STATS
            <BLANKLINE>
             -   16 new columns produced ('AB_POSIT', 'AB_SIZE', 'AC_BRL', 'AC_CONV', 'AC_COUGH', etc)
             -   12 source columns required ('Id10403', 'Id10362', 'Id10169', 'Id10221', 'Id10154', etc)
             -   7 relationships invoked ('eq', 'lt', 'between', 'ge', 'contains', etc)
             -   13 conditions listed ('yes', '14', '10', '21', '15 to 49', etc)
             -   1 prerequisites checked ('FEMALE')

        """
        print("MAPPING STATS\n")
        spacer = " - \t"

        unique_checks = [("New Column Name", "new columns produced"),
                         ("Source Column ID", "source columns required"),
                         ("Relationship", "relationships invoked"),
                         ("Condition", "conditions listed"),
                         ("Prerequisite", "prerequisites checked")]

        for col_name, context in unique_checks:
            print(spacer, self.config_data[col_name].nunique(), context,
                  report_list(self.config_data[col_name].dropna().unique(),
                              limit=5))


class CrossVA():
    """Class representing raw VA data, and how to map it to an algorithm

    Attributes:
        mapping (type): a validated Configuration object that details how to
            transform the type of data in `raw_data` to the desired output.
        data (Pandas DataFrame): a Pandas DataFrame containing the raw VA data
        prepared_data (Pandas DataFrame): a Pandas DataFrame containing a
            prepared form of the VA data to use with the Configuration object.
        validation (Validation): Validation object containing the validation
            checks that have been made on the raw data and between the raw data
            and mapping Configuration.
        verbose (int): Controls verbosity of printing to console, 0-5 where 0
            is silent.
    """

    def __init__(self, raw_data, mapping_config, na_values=["dk", "ref", ""],
                 verbose=2):
        """Inits CrossVA class

        Args:
            raw_data (Pandas DataFrame): a Pandas DataFrame containing the
                raw data
            mapping_config (Configuration): a validated Configuration object
                that details how to transform the type of data in `raw_data`
                to the desired output.
            na_values (list): List of values to consider NA.
            verbose (int): Controls verbosity of printing to console. Defaults
                to 2.
        Returns:
            type: Description of returned object.

        Examples
            >>> MAP_PATH = "resources/mapping_configuration_files/"
            >>> EX_MAP_1 = pd.read_csv(MAP_PATH + "example_config_1.csv")
            >>> EX_DATA_1 = pd.read_csv("resources/sample_data/mock_data_2016WHO151.csv")
            >>> CrossVA(EX_DATA_1, Configuration(EX_MAP_1))
            <CrossVA with (4, 12) raw data and Validated Configuration instance
            with 16 mapping conditions>

        """
        # if mapping_config isn't valid, attempt to validate before aborting
        if not mapping_config.validation.is_valid():
            if not mapping_config.validate():
                pass
                # raise error - mapping_config should be a valid

        # Crop input data to only the columns which have a matching ID
        # at the end of their name using regex, and any additional columns
        # passed as an argument, and then rename dataframe to just the column
        # IDs expected from the mapping
        self.mapping = mapping_config
        new_columns = raw_data.columns.str.extract(
            "(" + "$|".join(self.mapping.source_columns.tolist()) + "$)",
            expand=False)
        cropped_data = raw_data.loc[:, new_columns.notnull()].copy()
        cropped_data = cropped_data.replace(na_values, np.nan)
        self.data = cropped_data.rename(columns=pd.Series(new_columns,
                                                          raw_data.columns))
        self.prepared_data = pd.DataFrame()
        self.verbose = verbose
        self.validation = Validation("Mapping-Data Relationship")

    def __str__(self):
        """ str representation method """
        return ""

    def __repr__(self):
        return ("<" + self.__class__.__name__ + " with " + str(self.data.shape)
                + " raw data and " + str(self.mapping) + ">")

    def process(self):
        """Applies the given configuration object's mappings to the given raw
        data.

        Args:   None

        Returns:
            Pandas DataFrame: a dataframe where the transformations specified
            have been applied to the raw data, resulting

        """
        if not self.validation.is_valid():
            if not self.validate():
                raise ValueError(("Can't process without valid"
                                  " CrossVA instance"))

        # Create empty dataframe with the list of columns given in mapping
        # If the new columns listed in the mapping have no definition (ie source,
        # relationship, and condition) then they will keep their default value
        # as NA.
        transformed_data = pd.DataFrame(index=np.arange(len(self.data)),
                                        columns=self.mapping.new_columns,
                                        dtype=float)
        transformed_data.columns.name = ""

        for condition in self.mapping.list_conditions():
            # this is the big transformation
            # create column_values (boolean pd.series) representing if
            # condition and preq is met in raw data for each row, preserving NA
            pre_req = condition.check_prereq(transformed_data)
            initial_condition = condition.check(self.prepared_data)

            # using 0 for false and 1 for true
            new_val = np.where((initial_condition == 0) | (pre_req == 0),
                               0,
                               pre_req * initial_condition)

            # update new column where new column is currently NA and new values
            # are True or False. If new column is False, overwrite if new value
            # is true. If new column is already True, do not overwrite.
            # This preserves NAs logically, creates implicit ANY relationship
            # between different conditions for the same new column
            transformed_data[condition.name] = np.sign(
                transformed_data[condition.name].add(new_val,
                                                     fill_value=0))

        return transformed_data

    def validate(self, verbose=None):
        """Validates that RawVAData's raw input data and its mapping
        configuration object are compatible and prepares input data for use.

        Args:
            verbose (int): int from 0 to 5, representing verbosity of printing
                to console. Defaults to None; if None, replaced with
                self.verbose attribute.

        Returns:
            boolean: True if valid, False if not.

        Examples:
            >>> MAP_PATH = "resources/mapping_configuration_files/"
            >>> EX_MAP_1 = pd.read_csv(MAP_PATH + "example_config_1.csv")
            >>> EX_DATA_1 = pd.read_csv("resources/sample_data/mock_data_2016WHO151.csv")
            >>> CrossVA(EX_DATA_1, Configuration(EX_MAP_1)).validate(verbose=0)
            True

        """
        if verbose is None:
            verbose = self.verbose

        if self.mapping.process_strings:
            # strip whitespace and replace non-trailing/leading with underscore
            # for str operation convenience
            self.data.fillna("NA", inplace=True)
            self.data = self.validation.fix_whitespace(self.data)
            # make all characters lowercase
            self.data = self.validation.fix_upcase(self.data)
            # strip for alphanumeric characters
            self.data = self.validation.fix_alnum(self.data)
            self.data = self.data.replace("NA", np.nan)

        # check all expected columns from config Source Column ID are present
        col_msg = "All expected columns from mapping file are present in data"
        self.validation.must_contain(self.data.columns.rename(
            "the input data columns"),
            self.mapping.source_columns.rename(
            "expected source column IDs listed in mapping file"),
            passing_msg=col_msg,
            fail=Warn)

        # warn about associated missing values
        flagged_missing = ~(self.mapping.config_data["Source Column ID"].isin(
            self.data.columns))

        if flagged_missing.sum() > 0:
            affected = self.mapping.config_data[flagged_missing].groupby(
                "Source Column ID")["New Column Name"].apply(list)
            self.validation.affected_by_absence(affected)

        self.validation.no_duplicates(self.data.columns)

        # TODO Check to see if relationship and conditions correspond to
        # logical values in each column
        if self.validation.is_valid():
            # add missing columns as NA
            self.data = self.data.reindex(columns=self.mapping.source_columns)
            # for mapping_condition in self.mapping.list_conditions():
            #     self.prepared_data[mapping_condition.source_dtype] = \
            #         mapping_condition.prepare_data(self.data)
            # get rid of: "PerformanceWarning: DataFrame is highly fragmented."
            dict_of_cols = {}
            for mapping_condition in self.mapping.list_conditions():
                dict_of_cols[mapping_condition.source_dtype] = \
                    mapping_condition.prepare_data(self.data)
            self.prepared_data = pd.concat([self.prepared_data,
                                            pd.DataFrame(
                                                dict_of_cols)],
                                           axis=1)

        self.validation.report(verbose)
        return self.validation.is_valid()


if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.NORMALIZE_WHITESPACE)
