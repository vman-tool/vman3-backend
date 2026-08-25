from typing import Iterable, List

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
