from typing import Iterable, List

import pandas as pd

MIN_SEGMENT_LEN = 3


def clean_odk_column_name(col: str, sep: str = '-') -> str:
    """Strip ODK group prefixes from a submission column name.

    ODK joins a submission's nested group path onto each field name with
    `sep` ('-' in Central's CSV exports, '/' after pd.json_normalize on the
    API's JSON response) - e.g. "orgunit-region" -> "region". Splitting on
    `sep` and keeping the last piece recovers the bare field/question id.

    Some forms suffix a repeat/loop index or sub-part onto the id itself
    (e.g. "vital_reg_certifConst-id10013-a"), which would otherwise collapse
    to a useless one- or two-letter name ("a"). Any trailing segment shorter
    than MIN_SEGMENT_LEN is folded back into the segment before it instead
    of being kept on its own, so that case resolves to "id10013-a".
    """
    parts = col.split(sep)
    while len(parts) > 1 and len(parts[-1]) < MIN_SEGMENT_LEN:
        parts[-2] = parts[-2] + sep + parts[-1]
        parts.pop()
    return parts[-1]


def clean_odk_columns(columns: Iterable[str], sep: str = '-') -> List[str]:
    return [clean_odk_column_name(col, sep=sep) for col in columns]


def synthesize_coordinates_from_latlon(df: pd.DataFrame) -> pd.DataFrame:
    """Rebuilds the single `coordinates` [lng, lat, alt] array column the
    Data Map reads (both fetch_va_map_records, which requires exactly 3
    elements, and compute_and_store_dqa_map_points, which only needs the
    first 2), for CSV-uploaded records that don't have one.

    ODK Central's CSV export represents a geopoint question as separate
    <field>-Latitude/-Longitude/-Altitude(/-Accuracy) columns, which collapse
    to bare "latitude"/"longitude"/"altitude" after clean_odk_column_name's
    group-prefix stripping (keeps only the last '-'-separated segment). The
    API sync path doesn't have this problem: ODK's own GeoJSON geopoint
    encoding already nests the [lng, lat, alt] array under a "coordinates"
    key one level below the question's own name, and that same stripping
    logic (run with sep='/' there) preserves "coordinates" as-is. A CSV
    upload never has that nested structure, so without rebuilding it here,
    CSV-uploaded records have no "coordinates" field at all and the Data Map
    silently skips every one of them - the whole point of this function.

    Order is [lng, lat, alt], matching compute_and_store_dqa_map_points' own
    `lng, lat = coord[0], coord[1]` unpacking (GeoJSON Point order, not the
    [lat, lng] order latitude/longitude naming might suggest) and
    fetch_va_map_records' `LENGTH(doc.coordinates) == 3` filter. Altitude
    defaults to 0 when the CSV has no altitude column, or a row's value
    isn't numeric - a geopoint with unknown altitude, not a missing one.
    """
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        return df

    lat = pd.to_numeric(df['latitude'], errors='coerce')
    lng = pd.to_numeric(df['longitude'], errors='coerce')
    alt = pd.to_numeric(df['altitude'], errors='coerce') if 'altitude' in df.columns else pd.Series(0, index=df.index)
    alt = alt.fillna(0)
    df['coordinates'] = [
        None if pd.isna(lat_v) or pd.isna(lng_v) else [lng_v, lat_v, alt_v]
        for lat_v, lng_v, alt_v in zip(lat, lng, alt)
    ]
    return df
