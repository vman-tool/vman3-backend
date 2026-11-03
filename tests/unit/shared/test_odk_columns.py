import math

import pandas as pd

from app.shared.utils.odk_columns import (
    clean_odk_column_name,
    clean_odk_columns,
    synthesize_coordinates_from_latlon,
)


class TestCleanOdkColumnName:
    def test_strips_a_single_group_prefix(self):
        assert clean_odk_column_name("orgunit-region", sep="-") == "region"

    def test_keeps_a_bare_column_unchanged(self):
        assert clean_odk_column_name("instanceid", sep="-") == "instanceid"

    def test_folds_a_short_trailing_segment_back_into_its_parent(self):
        assert clean_odk_column_name("vital_reg_certifConst-id10013-a", sep="-") == "id10013-a"


class TestSynthesizeCoordinatesFromLatlon:
    def test_builds_lng_lat_alt_array_from_separate_columns(self):
        # Regression test for the real bug: a CSV upload with a geopoint
        # question exported as <field>-Latitude/-Longitude/-Altitude (ODK
        # Central's own CSV export convention) collapses to bare
        # "latitude"/"longitude"/"altitude" columns after clean_odk_columns,
        # which the Data Map can't read - it only looks at a single
        # "coordinates" [lng, lat, alt] array field (fetch_va_map_records
        # requires exactly 3 elements), the same shape ODK's API sync path
        # already produces for a geopoint question.
        df = pd.DataFrame({
            "latitude": [8.67485031],
            "longitude": [38.07539992],
            "altitude": [2100.5],
        })

        result = synthesize_coordinates_from_latlon(df)

        assert "coordinates" in result.columns
        coord = result["coordinates"].iloc[0]
        # [lng, lat, alt] order - GeoJSON Point order, not [lat, lng].
        assert coord == [38.07539992, 8.67485031, 2100.5]
        assert len(coord) == 3

    def test_defaults_altitude_to_zero_when_the_column_is_absent(self):
        df = pd.DataFrame({
            "latitude": [8.5],
            "longitude": [38.0],
        })

        result = synthesize_coordinates_from_latlon(df)

        assert result["coordinates"].iloc[0] == [38.0, 8.5, 0]

    def test_defaults_altitude_to_zero_when_a_rows_value_is_not_numeric(self):
        df = pd.DataFrame({
            "latitude": [8.5],
            "longitude": [38.0],
            "altitude": ["n/a"],
        })

        result = synthesize_coordinates_from_latlon(df)

        assert result["coordinates"].iloc[0] == [38.0, 8.5, 0]

    def test_missing_latitude_or_longitude_becomes_none_not_a_crash(self):
        df = pd.DataFrame({
            "latitude": [8.5, None, 8.7],
            "longitude": [38.0, 38.1, None],
        })

        result = synthesize_coordinates_from_latlon(df)

        assert result["coordinates"].iloc[0] == [38.0, 8.5, 0]
        assert result["coordinates"].iloc[1] is None
        assert result["coordinates"].iloc[2] is None

    def test_non_numeric_values_are_treated_as_missing(self):
        df = pd.DataFrame({
            "latitude": ["not-a-number"],
            "longitude": ["38.0"],
        })

        result = synthesize_coordinates_from_latlon(df)

        assert result["coordinates"].iloc[0] is None

    def test_leaves_the_dataframe_unchanged_when_either_column_is_absent(self):
        df = pd.DataFrame({"latitude": [8.5]})

        result = synthesize_coordinates_from_latlon(df)

        assert "coordinates" not in result.columns

    def test_already_present_coordinates_column_is_left_alone_when_no_latlon(self):
        # API-synced data already has "coordinates" and no bare latitude/
        # longitude columns - this function must be a no-op for it.
        df = pd.DataFrame({"coordinates": [[38.0, 8.5, 0]]})

        result = synthesize_coordinates_from_latlon(df)

        assert result["coordinates"].iloc[0] == [38.0, 8.5, 0]


class TestCleanOdkColumns:
    def test_cleans_every_column_in_a_list(self):
        assert clean_odk_columns(["orgunit-region", "meta-instanceID"], sep="-") == [
            "region", "instanceID",
        ]
