from typing import Dict

from arango.database import StandardDatabase
from fastapi import HTTPException

from fastapi import HTTPException

from app.pcva.utilities.va_records_utils import format_va_record
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import Pager, ResponseMainModel
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

    print(total_count_query)

    total_count_cursor = db.aql.execute(query = total_count_query, bind_vars = bind_vars)
    total_count = total_count_cursor.next() 

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

            cursor = db.aql.execute(query, bind_vars=bind_vars)

            if format_records:
                data = [format_va_record(document, config) for document in cursor]
            else:
                data = [document for document in cursor]
            
                
            
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
                query += f"RETURN {records_name}"
                
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
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            
            if format_records:
                data = [format_va_record(document, config) for document in cursor]
            else:
                data = [document for document in cursor]

            return ResponseMainModel(data=data, message="Records fetched successfully!", total = total_count, pager=Pager(page=page_number, limit=limit))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {e}")
 