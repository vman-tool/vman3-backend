from datetime import date
from typing import Dict, List, Optional, Tuple

from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

from app.pcva.services.target_cause_categories import classify_cause, get_cause_category_lookup
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException
from app.utilits.db_logger import db_logger, log_to_db
from app.shared.utils.cache import ttl_cache, invalidate_cache_pattern
from fastapi_cache.decorator import cache
# #@log_to_db(context="fetch_ccva_records", log_args=True)
async def fetch_ccva_records(paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        region_field = config.field_mapping.location_level1

        today_field = config.field_mapping.interview_date or 'id10012'
        collection = db.collection(db_collections.CCVA_RESULTS)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []

        if start_date:
            filters.append(f"doc.{today_field} >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append(f"doc.{today_field} <= @end_date")
            bind_vars["end_date"] = str(end_date)

        if locations:
            filters.append(f"doc.{region_field} IN @locations")
            bind_vars["locations"] = locations

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += "RETURN doc"
        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        
        def execute_count_query():
            total_records_cursor = db.aql.execute(count_query)
            return total_records_cursor.next()

        total_records = await run_in_threadpool(execute_count_query)

        return ResponseMainModel(
            data=data,
            message="CCVA fetched successfully",
            total=total_records
        )
    except ArangoError as e:
        
    #     await db_logger.log(
    #         message="Failed to fetched records " + str(e),
    #         level=db_logger.LogLevel.ERROR,
    #         context="fetch_ccva_records",
    #         data={}
         
    # )
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        await db_logger.log(
            message="Failed to fetched records " + str(e),
            level=db_logger.LogLevel.ERROR,
            context="fetch_ccva_records",
            data={}
         
    )
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))

# Whitelisted sort columns for fetch_ccva_individual_results - never
# interpolate the caller's own sort_by string into AQL directly.
_CCVA_INDIVIDUAL_SORT_FIELDS = {
    "va_id": "doc.ID",
    "region": "doc.locationLevel1",
    "district": "doc.locationLevel2",
    "ward": "doc.locationLevel3",
    "gender": "doc.gender",
    "age_group": "doc.age_group",
    "cause1": "doc.CAUSE1",
    "cause2": "doc.CAUSE2",
}

# Broad/Major Category aren't stored fields (they're derived - see
# target_cause_categories.py) - sorting by them translates each row's CAUSE1
# into its category via AQL's TRANSLATE(), fed a cause->category map built
# from this run's distinct causes (see _cause_category_maps below, also used
# to turn a Broad/Major filter into a `CAUSE1 IN [...]` filter).
_CCVA_CATEGORY_SORT_FIELDS = {"broad", "major"}

# "Group by" fields the individual-results table's grouped/chart views
# expose - plain stored fields grouped directly; Broad/Major Category
# grouped via the same CAUSE1->category TRANSLATE() map used for sorting.
_CCVA_GROUPABLE_FIELDS = {
    "region": "doc.locationLevel1",
    "district": "doc.locationLevel2",
    "ward": "doc.locationLevel3",
    "gender": "doc.gender",
    "age_group": "doc.age_group",
}
_CCVA_CATEGORY_GROUP_FIELDS = {"broad", "major"}

# Individual VA map points are capped well below `/records/maps`'s
# effectively-uncapped default (10_000_000) - a Leaflet-clustered view of a
# CCVA run stays responsive at this size, and a truncation note is more
# useful to a user than a sluggish map of tens of thousands of markers.
_CCVA_MAP_POINTS_LIMIT = 5000


def _clean_ccva_text(value) -> Optional[str]:
    """InterVA5's own CSV output uses a literal " " (single space) for an
    absent CAUSE2/CAUSE3 rather than leaving the field blank or null."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_ccva_probability(value) -> Optional[float]:
    """LIK1/LIK2 come as a numeric string ("83") from InterVA5's CSV output,
    a real float from the VManML10 branch, or a blank " " when absent -
    normalized to a single float-or-None shape either way."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


# "Filter by" fields the individual-results table exposes - Gender and Age
# Group are plain stored fields; Broad/Major Category are computed (see
# target_cause_categories.py), so filtering by them resolves to a set of
# matching CAUSE1 values first (see _cause1_values_for_category below).
_CCVA_FILTERABLE_FIELDS = {"gender", "age_group", "broad", "major"}


async def _distinct_cause1_values(task_id: str, db: StandardDatabase) -> List[str]:
    collection = db.collection(db_collections.CCVA_RESULTS)
    query = f"""
        FOR doc IN {collection.name}
            FILTER doc.task_id == @task_id
            COLLECT cause = doc.CAUSE1
            RETURN cause
    """

    def execute():
        cursor = db.aql.execute(query, bind_vars={"task_id": task_id})
        return [document for document in cursor]

    return await run_in_threadpool(execute)


async def _cause_category_maps(task_id: str, db: StandardDatabase) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Raw CAUSE1 value -> (major category, broad group), for every distinct
    CAUSE1 value present in this run. Shared basis for turning a Broad/Major
    Category filter into a `CAUSE1 IN [...]` filter, and for sorting by
    Broad/Major Category (via AQL's TRANSLATE(), fed one of these maps)."""
    distinct_causes = await _distinct_cause1_values(task_id, db)
    lookup = await get_cause_category_lookup(db)
    cause_to_major: Dict[str, str] = {}
    cause_to_broad: Dict[str, str] = {}
    for cause in distinct_causes:
        cleaned = _clean_ccva_text(cause)
        if not cleaned:
            continue
        major, broad = classify_cause(cleaned, lookup)
        if major:
            cause_to_major[cause] = major
        if broad:
            cause_to_broad[cause] = broad
    return cause_to_major, cause_to_broad


async def _cause1_values_for_category(
    task_id: str, filter_by: str, filter_value: str, db: StandardDatabase
) -> List[str]:
    """Every distinct CAUSE1 value (present in this run) that classifies
    into the given broad group or major category - used to turn a
    Broad/Major Category filter into a plain `CAUSE1 IN [...]` AQL filter,
    since the category itself isn't a stored field."""
    cause_to_major, cause_to_broad = await _cause_category_maps(task_id, db)
    target_map = cause_to_broad if filter_by == "broad" else cause_to_major
    return [cause for cause, category in target_map.items() if category == filter_value]


async def get_ccva_filter_options(task_id: str, db: StandardDatabase) -> ResponseMainModel:
    """Distinct values for each "filter by" field, restricted to what
    actually occurs in this run (per the individual-results table's Filter
    Value dropdown, which is auto-populated rather than a fixed list)."""
    try:
        collection = db.collection(db_collections.CCVA_RESULTS)

        def execute_distinct(field: str):
            query = f"""
                FOR doc IN {collection.name}
                    FILTER doc.task_id == @task_id AND doc.{field} != null AND doc.{field} != ""
                    COLLECT value = doc.{field}
                    RETURN value
            """
            cursor = db.aql.execute(query, bind_vars={"task_id": task_id})
            return [document for document in cursor]

        genders = await run_in_threadpool(lambda: execute_distinct("gender"))
        age_groups = await run_in_threadpool(lambda: execute_distinct("age_group"))

        distinct_causes = await _distinct_cause1_values(task_id, db)
        lookup = await get_cause_category_lookup(db)
        broads, majors = set(), set()
        for cause in distinct_causes:
            cleaned = _clean_ccva_text(cause)
            if not cleaned:
                continue
            major, broad = classify_cause(cleaned, lookup)
            if broad:
                broads.add(broad)
            if major:
                majors.add(major)

        return ResponseMainModel(
            data={
                "gender": sorted(genders),
                "age_group": sorted(age_groups),
                "broad": sorted(broads),
                "major": sorted(majors),
            },
            message="CCVA filter options fetched successfully",
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch CCVA filter options", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch CCVA filter options: {str(e)}", str(e))


async def _build_ccva_result_filters(
    task_id: str,
    search_va_id: Optional[str],
    filter_by: Optional[str],
    filter_value: Optional[str],
    db: StandardDatabase,
) -> Tuple[List[str], Dict]:
    """The task_id + optional VA-ID-search + optional Filter By/Value AQL
    clauses shared by every `ccva_results` read (individual rows, grouped
    counts, map points) - kept in one place so the three stay in sync."""
    bind_vars: Dict = {"task_id": task_id}
    filters = ["doc.task_id == @task_id"]

    if search_va_id:
        filters.append("CONTAINS(LOWER(TO_STRING(doc.ID)), @search_va_id)")
        bind_vars["search_va_id"] = search_va_id.strip().lower()

    if filter_by in _CCVA_FILTERABLE_FIELDS and filter_value:
        if filter_by in ("gender", "age_group"):
            filters.append(f"doc.{filter_by} == @filter_value")
            bind_vars["filter_value"] = filter_value
        else:
            matching_causes = await _cause1_values_for_category(task_id, filter_by, filter_value, db)
            filters.append("doc.CAUSE1 IN @matching_causes")
            bind_vars["matching_causes"] = matching_causes

    return filters, bind_vars


async def fetch_ccva_individual_results(
    task_id: str,
    page_number: int = 1,
    limit: int = 10,
    search_va_id: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: str = "asc",
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """The individual VA-level classifications for one CCVA run (the
    "Display Data" table) - paginated, optionally filtered by VA id and/or
    one of _CCVA_FILTERABLE_FIELDS, sortable by any of
    _CCVA_INDIVIDUAL_SORT_FIELDS.
    """
    try:
        collection = db.collection(db_collections.CCVA_RESULTS)
        filters, bind_vars = await _build_ccva_result_filters(task_id, search_va_id, filter_by, filter_value, db)

        base_query = f"FOR doc IN {collection.name} FILTER " + " AND ".join(filters) + " "

        count_query = base_query + "COLLECT WITH COUNT INTO total RETURN total"

        def execute_count():
            cursor = db.aql.execute(count_query, bind_vars=dict(bind_vars))
            return cursor.next()

        total = await run_in_threadpool(execute_count)

        sort_field = _CCVA_INDIVIDUAL_SORT_FIELDS.get(sort_by, "doc.ID")
        direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"

        if sort_by in _CCVA_CATEGORY_SORT_FIELDS:
            cause_to_major, cause_to_broad = await _cause_category_maps(task_id, db)
            bind_vars["cause1_category_map"] = cause_to_broad if sort_by == "broad" else cause_to_major
            sort_field = 'TRANSLATE(doc.CAUSE1, @cause1_category_map, "")'

        query = base_query + f"SORT {sort_field} {direction} "
        if page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars["offset"] = (page_number - 1) * limit
            bind_vars["size"] = limit

        query += """
        RETURN {
            va_id: doc.ID,
            locationLevel1: doc.locationLevel1,
            locationLevel2: doc.locationLevel2,
            locationLevel3: doc.locationLevel3,
            gender: doc.gender,
            age_group: doc.age_group,
            cause1: doc.CAUSE1,
            cause1_probability: doc.LIK1,
            cause2: doc.CAUSE2,
            cause2_probability: doc.LIK2
        }
        """

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)

        category_lookup = await get_cause_category_lookup(db)

        for row in data:
            row["cause1"] = _clean_ccva_text(row.get("cause1"))
            row["cause2"] = _clean_ccva_text(row.get("cause2"))
            row["cause1_probability"] = _clean_ccva_probability(row.get("cause1_probability"))
            row["cause2_probability"] = _clean_ccva_probability(row.get("cause2_probability"))
            # Broad group (Communicable/Non-Communicable/Injuries) and major
            # cause category, based on Cause 1 (the primary predicted
            # cause) - see target_cause_categories.py for how a CCVA cause
            # name is matched against the WHO target cause list configured
            # under Settings > PCVA Configuration.
            row["cause1_major"], row["cause1_broad"] = classify_cause(row["cause1"], category_lookup)

        return ResponseMainModel(
            data=data,
            message="CCVA individual results fetched successfully",
            total=total,
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch CCVA individual results", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch CCVA individual results: {str(e)}", str(e))


async def fetch_ccva_grouped_results(
    task_id: str,
    group_by: str,
    search_va_id: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """Counts of (already filtered) CCVA individual results grouped by one
    of _CCVA_GROUPABLE_FIELDS or _CCVA_CATEGORY_GROUP_FIELDS - backs the
    Display Data table's Group By + Table/Pie/Bar views. Small result set
    (a handful of group values) - no pagination, frontend sorts/paginates
    the returned array itself if needed.
    """
    if group_by not in _CCVA_GROUPABLE_FIELDS and group_by not in _CCVA_CATEGORY_GROUP_FIELDS:
        raise BadRequestException(f"Unsupported group_by value: {group_by}", group_by)
    try:
        collection = db.collection(db_collections.CCVA_RESULTS)
        filters, bind_vars = await _build_ccva_result_filters(task_id, search_va_id, filter_by, filter_value, db)
        base_query = f"FOR doc IN {collection.name} FILTER " + " AND ".join(filters) + " "

        if group_by in _CCVA_CATEGORY_GROUP_FIELDS:
            cause_to_major, cause_to_broad = await _cause_category_maps(task_id, db)
            bind_vars["cause1_category_map"] = cause_to_broad if group_by == "broad" else cause_to_major
            group_expression = 'TRANSLATE(doc.CAUSE1, @cause1_category_map, "Unclassified")'
        else:
            group_expression = _CCVA_GROUPABLE_FIELDS[group_by]

        query = base_query + f"""
        COLLECT grouped_value = {group_expression} WITH COUNT INTO count
        RETURN {{"group": grouped_value, "count": count}}
        """

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)
        total = sum(row["count"] for row in data)

        return ResponseMainModel(
            data=data,
            message="CCVA grouped results fetched successfully",
            total=total,
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch CCVA grouped results", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch CCVA grouped results: {str(e)}", str(e))


async def fetch_ccva_map_points(
    task_id: str,
    search_va_id: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """Individual VA points (already filtered, same shape as
    fetch_ccva_individual_results's filters) with GPS coordinates, for the
    Display Data table's Map view. Joins back to the raw VA submission
    (`db_collections.VA_TABLE`) the same way `getVADataAndMergeWithResults`
    in ccva_services.py already does: `ccva_results.ID ==
    form_submissions.<instance_id field>`, where the instance-id field name
    is resolved per-deployment from ODK config rather than hardcoded.
    Capped at _CCVA_MAP_POINTS_LIMIT with `truncated` reported in the
    response so a run with far more matching records than that doesn't
    silently drop points with no indication.
    """
    try:
        config = await fetch_odk_config(db, True)
        instance_id_field = config.field_mapping.instance_id or "instanceid"

        collection = db.collection(db_collections.CCVA_RESULTS)
        filters, bind_vars = await _build_ccva_result_filters(task_id, search_va_id, filter_by, filter_value, db)
        bind_vars["limit"] = _CCVA_MAP_POINTS_LIMIT + 1

        query = f"""
        FOR doc IN {collection.name}
            FILTER {" AND ".join(filters)}
            LET submission = FIRST(
                FOR s IN {db_collections.VA_TABLE}
                    FILTER s.{instance_id_field} == doc.ID
                    LIMIT 1
                    RETURN s
            )
            FILTER submission != null AND submission.coordinates != null AND LENGTH(submission.coordinates) == 3
            LIMIT @limit
            RETURN {{
                va_id: doc.ID,
                lat: submission.coordinates[1],
                lng: submission.coordinates[0],
                gender: doc.gender,
                age_group: doc.age_group,
                cause1: doc.CAUSE1,
                locationLevel1: doc.locationLevel1
            }}
        """

        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        rows = await run_in_threadpool(execute_query)
        # Fetched one extra row (bind_vars["limit"]) purely to detect
        # truncation - the frontend treats a result exactly at
        # _CCVA_MAP_POINTS_LIMIT as "there may be more" and shows a note,
        # so no separate response field is needed for this.
        data = rows[:_CCVA_MAP_POINTS_LIMIT]

        category_lookup = await get_cause_category_lookup(db)
        for row in data:
            cause1 = _clean_ccva_text(row.get("cause1"))
            row["cause1"] = cause1
            _, row["cause1_broad"] = classify_cause(cause1, category_lookup)

        return ResponseMainModel(
            data=data,
            message="CCVA map points fetched successfully",
            total=len(data),
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch CCVA map points", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch CCVA map points: {str(e)}", str(e))


# @ttl_cache(ttl=300, key_prefix="ccva_processed_graphs")
# @cache(expire=6000, namespace="ccva_processed_graphs")
async def fetch_processed_ccva_graphs(
    ccva_id: Optional[str] = None,
    is_default: Optional[bool] = None,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 30,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[str] = None,
    date_type:Optional[str]=None,
    db: StandardDatabase = None
) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []

        # If fetching by CCVA ID
        if ccva_id:
            filters.append("doc._key == @ccva_id")
            bind_vars["ccva_id"] = ccva_id
        
        # If fetching by isDefault
        if is_default is not None or ccva_id is None:
            filters.append("doc.isDefault == @is_default")
            bind_vars["is_default"] = True
        
        # Filtering by start and end dates
        if start_date:
            filters.append("doc.range.start >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append("doc.range.end <= @end_date")
            bind_vars["end_date"] = str(end_date)

        # Apply filters if any
        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        # Sorting by created date
        query += "SORT doc.created_at DESC "

        # Apply pagination if needed
        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += """
        RETURN doc
        """

        def execute_processed_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_processed_query)

        if not data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No records found")

        # Return response
        return ResponseMainModel(
            data=data,
            message="Processed CCVA fetched successfully",
            total=len(data)
        )
    
    except ArangoError as e:
        raise BadRequestException("Failed to fetch records", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch records: {str(e)}", str(e))   
    
    
async def fetch_processed_individual_ccva_graphs(
    ccva_id: Optional[str] = None, 
    is_default: Optional[bool] = None, 
    paging: bool = True, 
    page_number: int = 1, 
    limit: int = 30, 
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None, 
    locations: Optional[List[str]] = None,
    date_type:Optional[str]=None,
    db: StandardDatabase = None
) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.CCVA_RESULTS)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []

        # If fetching by CCVA ID
        if ccva_id:
            filters.append("doc._key == @ccva_id")
            bind_vars["ccva_id"] = ccva_id
        
        # If fetching by isDefault
        if is_default is not None or ccva_id is None:
            filters.append("doc.isDefault == @is_default")
            bind_vars["is_default"] = True
        
        # Filtering by start and end dates
        if start_date:
            filters.append("doc.range.start >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append("doc.range.end <= @end_date")
            bind_vars["end_date"] = str(end_date)

        # Apply filters if any
        if filters:
            query += "FILTER " + " AND ".join(filters) + " "

        # Sorting by created date
        query += "SORT doc.created_at DESC "

        # Apply pagination if needed
        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += """
        RETURN doc
        """

        def execute_individual_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_individual_query)

        if not data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No records found")

        # Return response
        return ResponseMainModel(
            data=data,
            message="Processed CCVA fetched successfully",
            total=len(data)
        )
    
    except ArangoError as e:
        raise BadRequestException("Failed to fetch records", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch records: {str(e)}", str(e))   
  
       
        
async def fetch_all_processed_ccva_graphs(paging: bool = True, page_number: int = 1, limit: int = 30, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:

        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []

        if start_date:
            filters.append("doc.range.start >= @start_date")
            bind_vars["start_date"] = str(start_date)
        
        if end_date:
            filters.append("doc.range.end <= @end_date")
            bind_vars["end_date"] = str(end_date)

    

        if filters:
            query += "FILTER " + " AND ".join(filters) + " "
        query += "SORT doc.created_at DESC "

        if paging and page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars.update({
                "offset": (page_number - 1) * limit,
                "size": limit
            })

        query += """
        LET user = (
            FOR u IN @@users_collection
            FILTER u._key == doc.user_id OR u.id == doc.user_id OR u.uid == doc.user_id
            LIMIT 1
            RETURN u
        )[0]
        RETURN {
            "id": doc._key,
            "task_id": doc.task_id,
            "created_at": doc.created_at,
            "total_records": doc.total_records,
            "elapsed_time": doc.elapsed_time,
            "start": doc.range.start,
            "end": doc.range.end,
            "isDefault": doc.isDefault,
            "algorithm": doc.algorithm != null ? doc.algorithm : "InterVA5",
            "malaria_status": doc.malaria_status,
            "hiv_status": doc.hiv_status,
            "run_by_name": user ? user.name : "Unknown"
        }
        """
        bind_vars["@users_collection"] = db_collections.USERS

        def execute_all_processed_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_all_processed_query)


        # Fetch total count of documents
        

        return ResponseMainModel(
            data=data,
            message="Processed CCVA fetched successfully",
            total=None
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
        
        
async def update_ccva_entry(ccva_id: str, update_data: dict, db: StandardDatabase) -> ResponseMainModel:
    try:
        # Define the collection
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)

        # Fetch the existing document
        query = f"FOR doc IN {collection.name} FILTER doc._key == @ccva_id RETURN doc"
        
        def execute_fetch_entry():
            cursor = db.aql.execute(query, bind_vars={"ccva_id": ccva_id})
            return next(cursor, None)

        ccva_doc = await run_in_threadpool(execute_fetch_entry)

        if not ccva_doc:
            raise BadRequestException(f"CCVA entry with id {ccva_id} not found")

        # Perform the update
        query = f"UPDATE {{ _key: @ccva_id }} WITH @update_data IN {collection.name} RETURN NEW"
        bind_vars = {
            "ccva_id": ccva_id,
            "update_data": update_data
        }
        
        def execute_update_entry():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return next(cursor, None)

        updated_doc = await run_in_threadpool(execute_update_entry)
        await invalidate_cache_pattern("ccva_*")
        return ResponseMainModel(
            data=updated_doc,
            message=f"CCVA entry with id {ccva_id} updated successfully",
            total=None
        )
    except ArangoError as e:
        raise BadRequestException("Failed to update record", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to update record: {str(e)}", str(e))        
async def set_ccva_as_default(ccva_id: str, db: StandardDatabase) -> ResponseMainModel:
    try:
        # Define the collection
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)

        # Step 1: Find the currently default CCVA and unset it
        query_unset_default = f"""
        FOR doc IN {collection.name} 
        FILTER doc.isDefault == true 
        UPDATE doc WITH {{ isDefault: false }} IN {collection.name}
        RETURN NEW
        """
        
        def execute_toggle_default():
            db.aql.execute(query_unset_default)  # Unset the current default entry

            # Step 2: Set the new CCVA as default
            query_set_default = f"""
            FOR doc IN {collection.name} 
            FILTER doc._key == @ccva_id 
            UPDATE doc WITH {{ isDefault: true }} IN {collection.name}
            RETURN NEW
            """
            bind_vars = {"ccva_id": ccva_id}
            cursor = db.aql.execute(query_set_default, bind_vars=bind_vars)
            return next(cursor, None)

        updated_doc = await run_in_threadpool(execute_toggle_default)
        if not updated_doc:
            raise BadRequestException(f"CCVA entry with id {ccva_id} not found")

        await invalidate_cache_pattern("ccva_*")
        # Step 3: Return the updated document as a response
        return ResponseMainModel(
            data=updated_doc,
            message=f"CCVA entry with id {ccva_id} set as default successfully",
            total=None
        )

    except ArangoError as e:
        raise BadRequestException("Failed to set CCVA as default", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to set CCVA as default: {str(e)}", str(e))

async def clear_ccva_default(ccva_id: str, db: StandardDatabase) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)

        query_clear_default = f"""
        FOR doc IN {collection.name}
        FILTER doc._key == @ccva_id AND doc.isDefault == true
        UPDATE doc WITH {{ isDefault: false }} IN {collection.name}
        RETURN NEW
        """
        bind_vars = {"ccva_id": ccva_id}

        def execute_clear_default():
            cursor = db.aql.execute(query_clear_default, bind_vars=bind_vars)
            return next(cursor, None)

        updated_doc = await run_in_threadpool(execute_clear_default)
        if not updated_doc:
            raise BadRequestException(f"CCVA entry with id {ccva_id} not found or not currently default")

        await invalidate_cache_pattern("ccva_*")
        return ResponseMainModel(
            data=updated_doc,
            message=f"CCVA entry with id {ccva_id} default cleared successfully",
            total=None
        )

    except ArangoError as e:
        raise BadRequestException("Failed to clear CCVA default", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to clear CCVA default: {str(e)}", str(e))

async def delete_ccva_entry(ccva_id: str, db: StandardDatabase) -> ResponseMainModel:
    try:
        # Define the collection
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)

        # Check if the document exists
        query = f"FOR doc IN {collection.name} FILTER doc._key == @ccva_id RETURN doc"
        
        def execute_delete_entry():
            cursor = db.aql.execute(query, bind_vars={"ccva_id": ccva_id})
            ccva_doc = next(cursor, None)
            if not ccva_doc:
                raise BadRequestException(f"CCVA entry with id {ccva_id} not found")
            
            # Delete the document
            query_delete = f"REMOVE {{ _key: @ccva_id }} IN {collection.name}"
            bind_vars = {"ccva_id": ccva_id}
            db.aql.execute(query_delete, bind_vars=bind_vars)

            task_id = ccva_doc.get("task_id")
            if task_id:
                try:
                    db.collection(db_collections.CCVA_RESULTS).delete_match({
                        "task_id": task_id
                    })
                except Exception:
                    pass

        await run_in_threadpool(execute_delete_entry)
        
        # await invalidate_cache_pattern("ccva_*")

        return ResponseMainModel(
            data=None,
            message=f"CCVA entry with id {ccva_id} deleted successfully",
            total=None
        )
    except ArangoError as e:
        raise BadRequestException("Failed to delete record", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to delete record: {str(e)}", str(e))