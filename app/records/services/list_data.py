from datetime import date
from typing import Dict, List, Optional, Tuple

from arango import ArangoError
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.pcva.services.target_cause_categories import get_cause_category_lookup
from app.records.responses.data import map_to_data_response
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import build_location_limit_filter, build_locations_query_filter
from app.shared.middlewares.exceptions import BadRequestException
from app.utilits.logger import app_logger


# The 4 CCVA columns denormalized onto form_submissions by
# _sync_ccva_default_to_va_records (ccva_data_services.py) - sortable by
# any of them. All but Algorithm are also filterable - Algorithm only ever
# has 1-2 distinct values in play at a time (whichever engine(s) produced
# the currently default run), so a filter for it adds little over just
# looking at the column.
_VA_RECORDS_CCVA_SORT_FIELDS = {
    'ccvaCauseBroad': 'ccva_cause1_broad',
    'ccvaCauseMajor': 'ccva_cause1_major',
    'ccvaCauseSpecific': 'ccva_cause1',
    'ccvaAlgorithm': 'ccva_algorithm',
}
_VA_RECORDS_CCVA_FILTERABLE_FIELDS = {
    'ccvaCauseBroad': 'ccva_cause1_broad',
    'ccvaCauseMajor': 'ccva_cause1_major',
    'ccvaCauseSpecific': 'ccva_cause1',
}
# Gender and Age Group aren't CCVA-denormalized - Gender is a plain
# form_submissions field (deployment-configurable name), Age Group is
# computed the same Neonate/Child/Adult way as the Instrument Type column
# (map_to_data_response) rather than being a stored field at all. Both
# still belong in Filter By/Group By, so _group_expression() resolves all
# 5 dimensions through one place.
_VA_RECORDS_FILTERABLE_DIMENSIONS = set(_VA_RECORDS_CCVA_FILTERABLE_FIELDS) | {'gender', 'age_group'}


def _group_expression(config, dimension: str) -> Optional[str]:
    """The raw AQL expression (no null-coalescing) for one Filter By/Group
    By dimension, or None if unrecognized."""
    if dimension in _VA_RECORDS_CCVA_FILTERABLE_FIELDS:
        return f"doc.{_VA_RECORDS_CCVA_FILTERABLE_FIELDS[dimension]}"
    if dimension == 'gender':
        return f"doc.{config.field_mapping.deceased_gender}"
    if dimension == 'age_group':
        is_neonate = config.field_mapping.is_neonate
        is_child = config.field_mapping.is_child
        is_adult = config.field_mapping.is_adult
        return (
            f'TO_STRING(doc.{is_neonate}) == "1" ? "Neonate" : '
            f'TO_STRING(doc.{is_child}) == "1" ? "Child" : '
            f'TO_STRING(doc.{is_adult}) == "1" ? "Adult" : "Unclassified"'
        )
    return None


async def get_va_records_ccva_filter_options(db: StandardDatabase) -> ResponseMainModel:
    """Filter By dropdown options for all 5 VA Records dimensions.
    Broad/Major Category come from the full configured target cause list
    (Settings > PCVA Configuration), not just whatever the currently
    default CCVA run happens to use, so the dropdown's meaning doesn't
    shift every time the default run changes. Specific Cause and Gender
    have no fixed universe to enumerate that way, so they're the distinct
    values actually present on VA records right now instead. Age Group is
    the fixed Neonate/Child/Adult/Unclassified set _group_expression()
    always produces - nothing to query."""
    config = await fetch_odk_config(db)
    _, category_names = await get_cause_category_lookup(db)
    majors = sorted({name for name, _ in category_names if name})
    broads = sorted({broad for _, broad in category_names if broad})

    collection = db.collection(db_collections.VA_TABLE)

    def execute_distinct(field: str):
        query = f"""
            FOR doc IN {collection.name}
                FILTER doc.{field} != null AND doc.{field} != ""
                COLLECT value = doc.{field}
                RETURN value
        """
        cursor = db.aql.execute(query)
        return sorted(document for document in cursor)

    specific = await run_in_threadpool(lambda: execute_distinct("ccva_cause1"))
    genders = await run_in_threadpool(lambda: execute_distinct(config.field_mapping.deceased_gender))

    return ResponseMainModel(
        data={
            "broad": broads,
            "major": majors,
            "specific": specific,
            "gender": genders,
            "age_group": ["Neonate", "Child", "Adult", "Unclassified"],
        },
        message="VA Records filter options fetched successfully",
    )


async def _build_va_records_filters(
    current_user: dict,
    start_date: Optional[date],
    end_date: Optional[date],
    locations: Optional[str],
    date_type: Optional[str],
    search_by: Optional[str],
    search_value: Optional[str],
    filter_by: Optional[str],
    filter_value: Optional[str],
    db: StandardDatabase,
) -> Tuple[List[str], Dict, object]:
    """The location-limit/date/location/search/CCVA-filter AQL clauses
    shared by every form_submissions read (the individual list and the
    Group By summary) - kept in one place so the two stay in sync, same
    reasoning as CCVA's own _build_ccva_result_filters. Also returns the
    resolved field-mapping config, since fetch_va_records needs it again
    for map_to_data_response and refetching it there would be redundant.
    """
    config = await fetch_odk_config(db)
    region_field = config.field_mapping.location_level1
    district_field = config.field_mapping.location_level2
    # Two distinct, independently-configurable fields (Settings >
    # Configuration > Field Mapping) - va_id is whatever this deployment
    # has mapped as the VA ID, instance_id is the record's own uuid:...
    # identifier. Both are offered as separate search-by options rather
    # than one silently standing in for the other.
    va_id_field = config.field_mapping.va_id
    instance_id_field = config.field_mapping.instance_id or 'instanceid'
    interviewer_field = config.field_mapping.interviewer_name

    death_date = config.field_mapping.death_date
    submitted_date = config.field_mapping.submitted_date
    interview_date = config.field_mapping.interview_date or 'id10012'

    if date_type == 'submission_date':
        today_field = submitted_date
    elif date_type == 'death_date':
        today_field = death_date
    else:
        today_field = interview_date

    bind_vars: Dict = {}
    filters: List[str] = []

    location_limit_filter = build_location_limit_filter(current_user, bind_vars)
    if location_limit_filter:
        filters.append(location_limit_filter)

    if start_date:
        filters.append(f"doc.{today_field} >= @start_date")
        bind_vars["start_date"] = str(start_date)

    if end_date:
        filters.append(f"doc.{today_field} <= @end_date")
        bind_vars["end_date"] = str(end_date)

    locations_filter = build_locations_query_filter(locations, bind_vars)
    if locations_filter:
        filters.append(locations_filter)

    if search_by and search_value:
        search_field_map = {
            'vaId': va_id_field,
            'instanceId': instance_id_field,
            'region': region_field,
            'district': district_field,
            'location_level1': region_field,
            'location_level2': district_field,
            'interviewer_name': interviewer_field,
            'interviewerName': interviewer_field,
            'interviewDay': today_field,
        }
        field_name = search_field_map.get(search_by)
        if field_name:
            filters.append(f"LIKE(LOWER(TO_STRING(doc.{field_name})), @search_value_pattern, true)")
            bind_vars["search_value_pattern"] = f"%{search_value.lower()}%"

    if filter_by in _VA_RECORDS_FILTERABLE_DIMENSIONS and filter_value:
        filters.append(f"({_group_expression(config, filter_by)}) == @ccva_filter_value")
        bind_vars["ccva_filter_value"] = filter_value

    return filters, bind_vars, config


async def fetch_va_records(current_user:dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[str] = None, date_type:Optional[str]=None, search_by: Optional[str] = None, search_value: Optional[str] = None, sort_by: Optional[str] = None, sort_dir: str = "asc", filter_by: Optional[str] = None, filter_value: Optional[str] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        filters, bind_vars, config = await _build_va_records_filters(
            current_user, start_date, end_date, locations, date_type, search_by, search_value, filter_by, filter_value, db
        )
        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        if sort_by in _VA_RECORDS_CCVA_SORT_FIELDS:
            direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
            query += f"SORT doc.{_VA_RECORDS_CCVA_SORT_FIELDS[sort_by]} {direction} "

        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += "RETURN doc"
        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [map_to_data_response(config, document) for document in cursor]

        data = await run_in_threadpool(execute_query)

        # Fetch total count of documents matching the same filters
        count_query = f"RETURN LENGTH(FOR doc IN {collection.name} "
        if filters:
            count_query += "FILTER " + " AND ".join(filters) + " "
        count_query += "RETURN 1)"

        def execute_count_query():
            # Avoid passing pagination bind vars (offset/size) to the count query
            count_bind_vars = {k: v for k, v in bind_vars.items() if k not in ("offset", "size")}
            total_records_cursor = db.aql.execute(count_query, bind_vars=count_bind_vars)
            return total_records_cursor.next()

        total_records = await run_in_threadpool(execute_count_query)

        return ResponseMainModel(
            data=data,
            message="Records fetched successfully",
            total=total_records
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))


async def fetch_va_records_grouped(
    current_user: dict,
    group_by: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[str] = None,
    date_type: Optional[str] = None,
    search_by: Optional[str] = None,
    search_value: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """Counts of (already filtered) VA records grouped by one of the same
    5 fields Filter By offers (Broad/Major/Specific Cause, Gender, Age
    Group) - backs the Group By control's summary table. Small result set
    (a handful of group values) - no pagination, same as CCVA's own
    grouped results (fetch_ccva_grouped_results in ccva_data_services.py),
    which this mirrors. A record with no value for the active dimension
    (e.g. no CCVA result under the default run, or gender never recorded)
    collects into an explicit "Unclassified" bucket rather than being
    silently dropped.
    """
    if group_by not in _VA_RECORDS_FILTERABLE_DIMENSIONS:
        raise BadRequestException(f"Unsupported group_by value: {group_by}", group_by)
    try:
        filters, bind_vars, config = await _build_va_records_filters(
            current_user, start_date, end_date, locations, date_type, search_by, search_value, filter_by, filter_value, db
        )
        collection = db.collection(db_collections.VA_TABLE)
        query = f"FOR doc IN {collection.name} "
        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        group_expr = _group_expression(config, group_by)
        query += f"""
        COLLECT grouped_value = ({group_expr}) != null ? ({group_expr}) : "Unclassified" WITH COUNT INTO count
        RETURN {{"group": grouped_value, "count": count}}
        """

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)
        total = sum(row["count"] for row in data)

        return ResponseMainModel(
            data=data,
            message="VA records grouped results fetched successfully",
            total=total,
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch grouped VA records", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch grouped VA records: {str(e)}", str(e))


# Individual VA map points are capped well below the general-purpose
# /records/maps endpoint's effectively-uncapped default (1_000_000) - a
# Leaflet-clustered view of this table's own (already filtered) records
# stays responsive at this size, same reasoning as CCVA's own
# _CCVA_MAP_POINTS_LIMIT.
_VA_RECORDS_MAP_POINTS_LIMIT = 5000


async def fetch_va_records_map_points(
    current_user: dict,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[str] = None,
    date_type: Optional[str] = None,
    search_by: Optional[str] = None,
    search_value: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """GPS points for the VA Records table's own Map view - shares every
    filter (date range/location/search/Filter By-Value) with the
    individual list and Group By summary via _build_va_records_filters,
    unlike the general-purpose /records/maps endpoint (fetch_va_map_records,
    which backs the standalone Data Map page and has no search_by/filter_by
    support at all). Mirrors CCVA's own dedicated fetch_ccva_map_points
    rather than complicating that already-shared endpoint for a second,
    narrower caller. Capped at _VA_RECORDS_MAP_POINTS_LIMIT (+1, fetched to
    detect truncation the same way CCVA's does) rather than left uncapped.
    """
    try:
        filters, bind_vars, config = await _build_va_records_filters(
            current_user, start_date, end_date, locations, date_type, search_by, search_value, filter_by, filter_value, db
        )
        region_field = config.field_mapping.location_level1
        district_field = config.field_mapping.location_level2
        instance_id_field = config.field_mapping.instance_id or 'instanceid'
        # location_level2 (district) isn't always mapped - some deployments
        # only configure admin level 1. Interpolating a blank field name as
        # `doc.` is invalid AQL and fails this whole query.
        district_expr = f"doc.{district_field}" if district_field else "null"
        gender_expr = _group_expression(config, 'gender')
        age_group_expr = _group_expression(config, 'age_group')

        collection = db.collection(db_collections.VA_TABLE)
        all_filters = filters + ["doc.coordinates != null", "LENGTH(doc.coordinates) == 3"]
        bind_vars["limit"] = _VA_RECORDS_MAP_POINTS_LIMIT + 1

        query = f"""
            FOR doc IN {collection.name}
                FILTER {" AND ".join(all_filters)}
                LIMIT @limit
                RETURN {{
                    vaId: doc.{instance_id_field},
                    lat: doc.coordinates[1],
                    lng: doc.coordinates[0],
                    region: doc.{region_field},
                    district: {district_expr},
                    gender: {gender_expr},
                    ageGroup: ({age_group_expr}),
                    ccvaCauseBroad: doc.ccva_cause1_broad,
                    ccvaCauseMajor: doc.ccva_cause1_major,
                    ccvaCauseSpecific: doc.ccva_cause1,
                    ccvaAlgorithm: doc.ccva_algorithm
                }}
        """

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        rows = await run_in_threadpool(execute_query)
        # Fetched one extra row (bind_vars["limit"]) purely to detect
        # truncation - the frontend treats a result exactly at
        # _VA_RECORDS_MAP_POINTS_LIMIT as "there may be more" and shows a
        # note, so no separate response field is needed for this.
        data = rows[:_VA_RECORDS_MAP_POINTS_LIMIT]

        return ResponseMainModel(
            data=data,
            message="VA records map points fetched successfully",
            total=len(data),
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch VA records map points", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch VA records map points: {str(e)}", str(e))






async def fetch_va_records_json(current_user:dict,paging: bool = True,data_source:Optional[str]=None, task_id:Optional[str]=None, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[str] = None,date_type:Optional[str]=None, db: StandardDatabase = None, top:Optional[int]=None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db)
        region_field = config.field_mapping.location_level1
        # locationKey, locationLimitValues = get_location_limit_values(current_user)
        if date_type is not None:
            if date_type == 'submission_date':
                today_field = 'submissiondate'
            elif date_type == 'death_date':
                today_field = 'id10023'
            elif date_type == 'interview_date':
                today_field = 'id10012'
            else:
                today_field = config.field_mapping.interview_date or 'id10012'
        else:
            today_field = config.field_mapping.interview_date or 'id10012'
        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []
        ## filter by location limits
        # if locationLimitValues and locationKey:
        #     filters.append(f"doc.{locationKey} IN @locationValues")
        #     bind_vars["locationValues"] = locationLimitValues
        # ##
        if start_date:
            filters.append(f"doc.{today_field} >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append(f"doc.{today_field} <= @end_date")
            bind_vars["end_date"] = str(end_date)

        locations_filter = build_locations_query_filter(locations, bind_vars)
        if locations_filter:
            filters.append(locations_filter)
        # filter by data source and task id, THIS ID CASE WHEN WE IMPORT DATA FROM CSV, AND WE WANT THEM ONLY
        if data_source is  None : 
            filters.append('doc.vman_data_source !="data_source"')
        if data_source :
            filters.append(f'doc.vman_data_source =="{data_source}"')
        
        # if task_id:
        #     filters.append(f'doc.trackid =="{task_id}"')
            

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })
        elif top and top > 0:
            query += "LIMIT @size "
            bind_vars["size"] = top

        query += "RETURN doc"
        def execute_json_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_json_query)

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        
        def execute_json_count_query():
            total_records_cursor = db.aql.execute(count_query)
            return total_records_cursor.next()

        total_records = await run_in_threadpool(execute_json_count_query)

        return ResponseMainModel(
            data=data,
            message="Records fetched successfully",
            total=total_records
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
    
async def fetch_va_records_count(current_user:dict, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[str] = None,date_type:Optional[str]=None, db: StandardDatabase = None, top:Optional[int]=None, data_source:Optional[str]=None) -> int:
    try:
        config = await fetch_odk_config(db)
        region_field = config.field_mapping.location_level1
        
        if date_type is not None:
            if date_type == 'submission_date':
                today_field = 'submissiondate'
            elif date_type == 'death_date':
                today_field = 'id10023'
            elif date_type == 'interview_date':
                today_field = 'id10012'
            else:
                today_field = config.field_mapping.interview_date or 'id10012'
        else:
            today_field = config.field_mapping.interview_date or 'id10012'

        collection = db.collection(db_collections.VA_TABLE)

        # Build count query with same filters
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []
        
        if start_date:
            filters.append(f"doc.{today_field} >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append(f"doc.{today_field} <= @end_date")
            bind_vars["end_date"] = str(end_date)

        locations_filter = build_locations_query_filter(locations, bind_vars)
        if locations_filter:
            filters.append(locations_filter)

        if data_source is None:
            filters.append('doc.vman_data_source !="data_source"')
        if data_source:
            filters.append(f'doc.vman_data_source =="{data_source}"')
            
        if filters:
            query += "FILTER " + " AND ".join(filters) + " "
            
        if top and top > 0:
            query += "LIMIT @size "
            bind_vars["size"] = top
            
        query += "RETURN 1"
        
        count_query = f"RETURN LENGTH({query})"
        
        def execute_count():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            # Length of RETURN 1 is the count
            count = 0
            for _ in cursor:
                count += 1
            return count

        return await run_in_threadpool(execute_count)
        
    except Exception as e:
        print(f"Error in fetch_va_records_count: {e}")
        return 0
