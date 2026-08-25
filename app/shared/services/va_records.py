import re
from typing import Dict, List

from arango.database import StandardDatabase
from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool

from app.pcva.utilities.va_records_utils import format_va_record
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import Pager, ResponseMainModel
from app.shared.configs.security import build_location_limit_filter
from app.shared.utils.database_utilities import add_query_filters


async def shared_fetch_va_records(paging: bool = True,  page_number: int = 1, limit: int = 10, include_assignment: bool = False, filters: Dict = {}, format_records:bool = True, db: StandardDatabase = None) -> ResponseMainModel:
    va_table_collection = db.collection(db_collections.VA_TABLE)
    assignment_collection_exists = db.has_collection(db_collections.ASSIGNED_VA)
    bind_vars = {}

    config = await fetch_odk_config(db)
    


    if not include_assignment or not assignment_collection_exists:
        records_name = 'doc'
    else:
        records_name = 'v'
    
    filters_list, vars = add_query_filters(filters, {}, records_name)

    bind_vars.update(vars)

    filters_string: str = ""

    if filters_list:
        filters_string += " FILTER " + " AND ".join(filters_list)

    total_count_query = f"""
        RETURN LENGTH(
            FOR {records_name} IN {db_collections.VA_TABLE}
            {filters_string}
            RETURN 1
        )
    """

    def execute_total_count():
        total_count_cursor = db.aql.execute(query = total_count_query, bind_vars = bind_vars)
        return total_count_cursor.next()

    total_count = await run_in_threadpool(execute_total_count) 

    if total_count is None or total_count == 0:
        return ResponseMainModel(
            data=[],
            total_count=0,
            page_number=page_number,
            limit=limit
        )
    
    try:
        if not include_assignment or not assignment_collection_exists:
            query = f"FOR {records_name} IN {va_table_collection.name} "

            if filters_string:
                query += f"{filters_string}  "
                
            if paging and page_number and limit:
            
                query += f"LIMIT @offset, @size RETURN {records_name}"
            
                bind_vars.update({
                    "offset": (page_number - 1) * limit,
                    "size": limit
                })
            else:
                query += f"RETURN {records_name}"

            def execute_records_query():
                cursor = db.aql.execute(query, bind_vars=bind_vars)
                return [document for document in cursor]

            cursor_data = await run_in_threadpool(execute_records_query)

            if format_records:
                data = [format_va_record(document, config) for document in cursor_data]
            else:
                data = cursor_data
            
                
            
            return ResponseMainModel(data=data, message="Records fetched successfully!", total=total_count)
        else:
            query = f"""
                // Step 1: Get paginated va records
                LET paginatedVa = (
                    FOR {records_name} IN {db_collections.VA_TABLE}
                        
                """
            
            if filters_string:
                query += f"{filters_string}  "
            
            
            if paging and page_number and limit:
        
                query += f"""LIMIT @offset, @size RETURN {records_name})"""
            
            else:
                query += f"RETURN {records_name})"
                
            query += f"""
                // Step 2: Get the va IDs for the paginated results
                LET vaIds = (
                    FOR v IN paginatedVa
                        RETURN v.__id
                )

                // Step 3: Get assignments for the va IDs
                LET assignments = (
                    FOR a IN {db_collections.ASSIGNED_VA}
                        FILTER a.vaId IN vaIds AND a.is_deleted == false
                        RETURN {{
                            vaId: a.vaId,
                            coder: a.coder
                        }}
                )

                LET formattedAssignments = (
                    FOR a in assignments
                        FOR user in {db_collections.USERS}
                        FILTER user.uuid == a.coder
                        RETURN {{
                            vaId: a.vaId,
                            coder: {{
                                uuid: user.uuid,
                                name: user.name
                            }}
                        }}
                )

                // Step 4: Combine the paginated va records with their assignments
                LET vaWithAssignments = (
                    FOR v IN paginatedVa
                        LET vAssignments = (
                            FOR a IN formattedAssignments
                                FILTER a.vaId == v.__id
                                RETURN a
                        )
                        RETURN MERGE(v, {{ assignments: vAssignments }})
                )

                FOR va IN vaWithAssignments
                    RETURN va
            """

            if paging and page_number and limit:
                bind_vars.update({
                    "offset": (page_number - 1) * limit,
                    "size": limit
                })
            
            def execute_assignments_query():
                cursor = db.aql.execute(query, bind_vars=bind_vars)
                return [document for document in cursor]

            cursor_data = await run_in_threadpool(execute_assignments_query)
            
            if format_records:
                data = [format_va_record(document, config) for document in cursor_data]
            else:
                data = cursor_data

            return ResponseMainModel(data=data, message="Records fetched successfully!", total = total_count, pager=Pager(page=page_number, limit=limit))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")
    

# Bookkeeping fields the insert path stamps onto every VA record document
# (or ArangoDB itself adds) - never a real submission column, so they'd
# only clutter the Field Mapping picker.
_INTERNAL_VA_FIELDS = {
    '_key', '_id', '_rev',
    'vman_data_source', 'vman_data_name', '__id', 'version_number', 'trackid',
}


async def get_distinct_va_field_names(db: StandardDatabase = None) -> List[str]:
    """All distinct top-level field names actually present on VA records.

    The question dictionary (form_questions, backing Field Mapping's search)
    only ever contains XLSForm survey rows - ODK org-unit/geo metadata like
    "region"/"woreda" are never survey questions, so no sync or xForm upload
    can put them there, no matter how fresh. This reads the real data
    instead, so those fields can still be mapped.
    """
    query = f"""
        FOR doc IN {db_collections.VA_TABLE}
            FOR k IN ATTRIBUTES(doc)
                COLLECT field = k
                RETURN field
    """

    def execute_field_names_query():
        cursor = db.aql.execute(query)
        return [document for document in cursor]

    fields = await run_in_threadpool(execute_field_names_query)
    return sorted(f for f in fields if f not in _INTERNAL_VA_FIELDS)


async def get_field_value_from_va_records(
    field: str,
    db: StandardDatabase = None,
    parent_field: str = None,
    parent_value: str = None,
    current_user: dict = None,
    scope_to_user: bool = True,
):
    """Distinct values of `field`, optionally scoped to records where
    `parent_field` equals `parent_value` - e.g. the districts that actually
    occur within one specific region, for building a real drill-down
    location tree instead of a flat, unscoped value list.

    Also scoped to `current_user`'s own access_limit when they have one and
    `scope_to_user` is true (the default) - without this, a location-
    restricted user could browse (and filter dashboard content by) areas
    outside their own permission, even though the underlying data queries
    were already correctly restricted.

    `scope_to_user=False` is for admin-configuration screens - e.g.
    re-labelling raw ODK values into friendlier display names - where the
    caller needs the full universe of values that exist in the system to
    manage, regardless of their own data-access boundary. Restricting that
    picker to the admin's own locations doesn't protect anything (they are
    not being shown other people's data, just field values to rename) and
    only hides values they need to fix.
    """
    try:
        for name in (field, parent_field):
            if name is not None and not re.fullmatch(r'[A-Za-z0-9_]+', name):
                raise HTTPException(status_code=400, detail="Invalid field name.")

        bind_vars = {}
        filters = [f"doc.{field} != null"]
        if parent_field and parent_value is not None:
            filters.append(f"doc.{parent_field} == @parent_value")
            bind_vars["parent_value"] = parent_value

        if current_user and scope_to_user:
            location_limit_filter = build_location_limit_filter(current_user, bind_vars)
            if location_limit_filter:
                filters.append(location_limit_filter)

        query = f"""
                FOR doc IN {db_collections.VA_TABLE}
                FILTER {' AND '.join(filters)}
                COLLECT unique = doc.{field}
                RETURN unique
        """

        def execute_field_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_field_query)

        return ResponseMainModel(data=data, message="Unique data fetched successfully!", total = len(data))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")
 