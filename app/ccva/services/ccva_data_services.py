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
    "cause_specific": "doc.CAUSE1",
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
            row["cause1_major"], row["cause1_broad"] = classify_cause(cause1, category_lookup)

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
async def _sync_ccva_default_to_va_records(
    task_id: Optional[str], algorithm: Optional[str], db: StandardDatabase
) -> None:
    """Denormalizes the default run's Broad/Major/Specific Cause + Algorithm
    onto each matching `form_submissions` doc (VA Records table columns).

    The VA Records table paginates/sorts/filters server-side like any other
    column - doing that against a *joined* value (one `FIRST(FOR r IN
    ccva_results ...)` per row) would force AQL to evaluate the join for
    every row before it can sort/filter/paginate, i.e. a full join over the
    whole table on every single page load. Writing the result onto the
    document itself instead moves that cost to here - once, only when the
    default run changes - and turns every later read into an ordinary
    indexed column lookup, the same as any other VA Records field.

    Always resets every VA record first (so clearing the default, or a
    record no longer matching the new default, correctly lands on "-" /
    null rather than keeping a stale value), then - when `task_id` is given
    - re-populates only the records with a match in that run.
    """
    va_collection = db.collection(db_collections.VA_TABLE)

    reset_query = f"""
        FOR doc IN {va_collection.name}
            FILTER doc.ccva_cause1 != null OR doc.ccva_cause1_broad != null
                OR doc.ccva_cause1_major != null OR doc.ccva_algorithm != null
            UPDATE doc WITH {{
                ccva_cause1: null, ccva_cause1_broad: null,
                ccva_cause1_major: null, ccva_algorithm: null
            }} IN {va_collection.name}
    """

    def execute_reset():
        db.aql.execute(reset_query)

    await run_in_threadpool(execute_reset)

    if not task_id:
        return

    config = await fetch_odk_config(db, True)
    instance_id_field = config.field_mapping.instance_id or "instanceid"
    cause_to_major, cause_to_broad = await _cause_category_maps(task_id, db)

    sync_query = f"""
        FOR ccva IN {db_collections.CCVA_RESULTS}
            FILTER ccva.task_id == @task_id AND ccva.CAUSE1 != null AND ccva.CAUSE1 != ""
            LET submission = FIRST(
                FOR s IN {va_collection.name}
                    FILTER s.{instance_id_field} == ccva.ID OR s.{instance_id_field} == ccva.uid
                    LIMIT 1
                    RETURN s
            )
            FILTER submission != null
            UPDATE submission WITH {{
                ccva_cause1: ccva.CAUSE1,
                ccva_cause1_broad: TRANSLATE(ccva.CAUSE1, @broad_map, "Unclassified"),
                ccva_cause1_major: TRANSLATE(ccva.CAUSE1, @major_map, "Unclassified"),
                ccva_algorithm: @algorithm
            }} IN {va_collection.name}
    """
    bind_vars = {
        "task_id": task_id,
        "broad_map": cause_to_broad,
        "major_map": cause_to_major,
        "algorithm": algorithm or "InterVA5",
    }

    def execute_sync():
        db.aql.execute(sync_query, bind_vars=bind_vars)

    await run_in_threadpool(execute_sync)


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

        await _sync_ccva_default_to_va_records(updated_doc.get("task_id"), updated_doc.get("algorithm"), db)

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

        await _sync_ccva_default_to_va_records(None, None, db)

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


# ── Compare CoD: per-record Cause-of-Death comparison between two runs ──────
#
# Unlike fetch_ccva_individual_results (one run, one CAUSE1/CAUSE2 pair),
# this inner-joins two runs' results by VA id (doc.ID) so each returned row
# carries both algorithms' Cause 1/2 + probability side by side. Only VA ids
# present in BOTH runs are returned - records found in only one run are
# silently excluded from the table itself, but counted (see
# get_ccva_compare_filter_options' matched/task totals) so that's visible
# rather than a silent discrepancy.
#
# Broad/Major Category are deliberately NOT included here (unlike the
# single-run table): both are derived from Cause 1 (see
# target_cause_categories.py), and Cause 1 can legitimately differ between
# the two algorithms being compared - a single Broad/Major column would be
# ambiguous about whose Cause 1 it's describing.

_CCVA_COMPARE_SORT_FIELDS = {
    "va_id":        "doc1.ID",
    "region":       "doc1.locationLevel1",
    "district":     "doc1.locationLevel2",
    "ward":         "doc1.locationLevel3",
    "gender":       "doc1.gender",
    "age_group":    "doc1.age_group",
    "cause1_algo1": "doc1.CAUSE1",
    "cause1_algo2": "doc2.CAUSE1",
}

# "Cause Specific" needs a third dimension (which of the two algorithms the
# chosen cause belongs to) that single-run filtering never had to deal with -
# filter_algo picks doc1 ("1") or doc2 ("2") as the side filter_value applies to.
_CCVA_COMPARE_FILTERABLE_FIELDS = {"gender", "age_group", "cause_specific", "agreement"}

# ArangoDB 3.12.2's "join-index-nodes" optimizer rule turns this module's
# self-joins (FOR doc1 ... FOR doc2 ... FILTER doc1.ID == doc2.ID, both over
# the same compound task_id+ID index) into a TwoIndicesMergeJoin executor.
# Combined with an aggregating executor on top (COLLECT WITH COUNT, or
# COLLECT value = ...) that executor SIGSEGVs the whole server (confirmed via
# docker logs: crash inside arangodb::aql::JoinExecutor /
# TwoIndicesMergeJoin::reset, immediately followed by an auto-restart) -
# reproduced live against this run's real task ids. Disabling the rule for
# every query below that pairs the self-join with an aggregate falls back to
# a plain indexed nested loop (still using the same index per outer row, just
# not the specialized merge-join operator) and ran in ~25ms against the
# in-repro 4k-record collections. Remove this once ArangoDB ships a fix.
_CCVA_COMPARE_JOIN_CRASH_WORKAROUND = {"optimizer_rules": ["-join-index-nodes"]}


async def _build_ccva_compare_filters(
    task_id_1: str,
    task_id_2: str,
    search_va_id: Optional[str],
    filter_by: Optional[str],
    filter_value: Optional[str],
    filter_algo: Optional[str],
    db: StandardDatabase,
) -> Tuple[List[str], Dict]:
    bind_vars: Dict = {"task_id_1": task_id_1, "task_id_2": task_id_2}
    filters = ["doc1.task_id == @task_id_1", "doc2.task_id == @task_id_2", "doc1.ID == doc2.ID"]

    if search_va_id:
        filters.append("CONTAINS(LOWER(TO_STRING(doc1.ID)), @search_va_id)")
        bind_vars["search_va_id"] = search_va_id.strip().lower()

    if filter_by in ("gender", "age_group") and filter_value:
        filters.append(f"doc1.{filter_by} == @filter_value")
        bind_vars["filter_value"] = filter_value
    elif filter_by == "cause_specific" and filter_value and filter_algo in ("1", "2"):
        doc_ref = "doc1" if filter_algo == "1" else "doc2"
        filters.append(f"{doc_ref}.CAUSE1 == @filter_value")
        bind_vars["filter_value"] = filter_value
    elif filter_by == "agreement" and filter_value in ("agreement", "disagreement"):
        # Mirrors the cause1_agrees flag computed below on the cleaned
        # values: non-null/non-blank and equal on both sides counts as
        # agreement, everything else (including either side blank) as
        # disagreement.
        agree_condition = (
            'doc1.CAUSE1 != null AND TRIM(TO_STRING(doc1.CAUSE1)) != "" AND doc1.CAUSE1 == doc2.CAUSE1'
        )
        filters.append(agree_condition if filter_value == "agreement" else f"NOT ({agree_condition})")

    return filters, bind_vars


async def fetch_ccva_compare_results(
    task_id_1: str,
    task_id_2: str,
    page_number: int = 1,
    limit: int = 10,
    search_va_id: Optional[str] = None,
    filter_by: Optional[str] = None,
    filter_value: Optional[str] = None,
    filter_algo: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: str = "asc",
    db: StandardDatabase = None,
) -> ResponseMainModel:
    """The "Compare CoD" table - one row per VA id present in both runs,
    with each algorithm's Cause 1/2 + probability side by side (see module
    note above for why Broad/Major Category aren't included)."""
    try:
        collection = db.collection(db_collections.CCVA_RESULTS)
        filters, bind_vars = await _build_ccva_compare_filters(
            task_id_1, task_id_2, search_va_id, filter_by, filter_value, filter_algo, db,
        )

        base_query = (
            f"FOR doc1 IN {collection.name} "
            f"FOR doc2 IN {collection.name} "
            "FILTER " + " AND ".join(filters) + " "
        )

        count_query = base_query + "COLLECT WITH COUNT INTO total RETURN total"

        def execute_count():
            cursor = db.aql.execute(
                count_query, bind_vars=dict(bind_vars), **_CCVA_COMPARE_JOIN_CRASH_WORKAROUND
            )
            return cursor.next()

        total = await run_in_threadpool(execute_count)

        sort_field = _CCVA_COMPARE_SORT_FIELDS.get(sort_by, "doc1.ID")
        direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"

        query = base_query + f"SORT {sort_field} {direction} "
        if page_number and limit:
            query += "LIMIT @offset, @size "
            bind_vars["offset"] = (page_number - 1) * limit
            bind_vars["size"] = limit

        query += """
        RETURN {
            va_id: doc1.ID,
            locationLevel1: doc1.locationLevel1,
            locationLevel2: doc1.locationLevel2,
            locationLevel3: doc1.locationLevel3,
            gender: doc1.gender,
            age_group: doc1.age_group,
            cause1_algo1: doc1.CAUSE1,
            cause1_probability_algo1: doc1.LIK1,
            cause2_algo1: doc1.CAUSE2,
            cause2_probability_algo1: doc1.LIK2,
            cause1_algo2: doc2.CAUSE1,
            cause1_probability_algo2: doc2.LIK1,
            cause2_algo2: doc2.CAUSE2,
            cause2_probability_algo2: doc2.LIK2
        }
        """

        def execute_query():
            cursor = db.aql.execute(
                query, bind_vars=bind_vars, **_CCVA_COMPARE_JOIN_CRASH_WORKAROUND
            )
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)

        for row in data:
            row["cause1_algo1"] = _clean_ccva_text(row.get("cause1_algo1"))
            row["cause2_algo1"] = _clean_ccva_text(row.get("cause2_algo1"))
            row["cause1_algo2"] = _clean_ccva_text(row.get("cause1_algo2"))
            row["cause2_algo2"] = _clean_ccva_text(row.get("cause2_algo2"))
            row["cause1_probability_algo1"] = _clean_ccva_probability(row.get("cause1_probability_algo1"))
            row["cause2_probability_algo1"] = _clean_ccva_probability(row.get("cause2_probability_algo1"))
            row["cause1_probability_algo2"] = _clean_ccva_probability(row.get("cause1_probability_algo2"))
            row["cause2_probability_algo2"] = _clean_ccva_probability(row.get("cause2_probability_algo2"))
            # Quick-scan agreement flag - the whole point of this table is
            # spotting where the two algorithms differ, not just listing both.
            row["cause1_agrees"] = (
                row["cause1_algo1"] is not None and row["cause1_algo1"] == row["cause1_algo2"]
            )

        return ResponseMainModel(
            data=data,
            message="CCVA comparison results fetched successfully",
            total=total,
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch CCVA comparison results", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch CCVA comparison results: {str(e)}", str(e))


async def get_ccva_compare_filter_options(
    task_id_1: str, task_id_2: str, db: StandardDatabase
) -> ResponseMainModel:
    """Filter Value choices for the Compare CoD table, plus match-rate
    transparency numbers (how many of each run's records actually lined up
    by VA id) shown above the table rather than left as a silent gap."""
    try:
        collection = db.collection(db_collections.CCVA_RESULTS)

        def execute_distinct_joined(field: str):
            query = f"""
                FOR doc1 IN {collection.name}
                    FILTER doc1.task_id == @task_id_1 AND doc1.{field} != null AND doc1.{field} != ""
                    FOR doc2 IN {collection.name}
                        FILTER doc2.task_id == @task_id_2 AND doc2.ID == doc1.ID
                        COLLECT value = doc1.{field}
                        RETURN value
            """
            cursor = db.aql.execute(
                query,
                bind_vars={"task_id_1": task_id_1, "task_id_2": task_id_2},
                **_CCVA_COMPARE_JOIN_CRASH_WORKAROUND,
            )
            return [d for d in cursor]

        def execute_matched_total():
            query = f"""
                FOR doc1 IN {collection.name}
                    FILTER doc1.task_id == @task_id_1
                    FOR doc2 IN {collection.name}
                        FILTER doc2.task_id == @task_id_2 AND doc2.ID == doc1.ID
                        COLLECT WITH COUNT INTO total
                        RETURN total
            """
            cursor = db.aql.execute(
                query,
                bind_vars={"task_id_1": task_id_1, "task_id_2": task_id_2},
                **_CCVA_COMPARE_JOIN_CRASH_WORKAROUND,
            )
            return cursor.next()

        def execute_task_total(task_id: str):
            query = f"""
                FOR doc IN {collection.name}
                    FILTER doc.task_id == @task_id
                    COLLECT WITH COUNT INTO total
                    RETURN total
            """
            cursor = db.aql.execute(query, bind_vars={"task_id": task_id})
            return cursor.next()

        genders    = await run_in_threadpool(lambda: execute_distinct_joined("gender"))
        age_groups = await run_in_threadpool(lambda: execute_distinct_joined("age_group"))
        cause1_algo1 = await _distinct_cause1_values(task_id_1, db)
        cause1_algo2 = await _distinct_cause1_values(task_id_2, db)
        matched    = await run_in_threadpool(execute_matched_total)
        task1_total = await run_in_threadpool(lambda: execute_task_total(task_id_1))
        task2_total = await run_in_threadpool(lambda: execute_task_total(task_id_2))

        return ResponseMainModel(
            data={
                "gender":       sorted(genders),
                "age_group":    sorted(age_groups),
                "cause1_algo1": sorted(c for c in cause1_algo1 if _clean_ccva_text(c)),
                "cause1_algo2": sorted(c for c in cause1_algo2 if _clean_ccva_text(c)),
                "matched_total": matched,
                "task1_total":   task1_total,
                "task2_total":   task2_total,
            },
            message="CCVA comparison filter options fetched successfully",
        )
    except ArangoError as e:
        raise BadRequestException("Failed to fetch CCVA comparison filter options", str(e))
    except Exception as e:
        raise BadRequestException(f"Failed to fetch CCVA comparison filter options: {str(e)}", str(e))