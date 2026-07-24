from datetime import date, datetime
from typing import List, Optional

from arango import ArangoError
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException


async def fetch_error_list(
    db: StandardDatabase,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 10,
    error_type: Optional[str] = None,
    group: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    locations: Optional[List[str]] = None
) -> ResponseMainModel:
    try:
        print(error_type)
        # config = await fetch_odk_config(db)
        # date_field = config.field_mapping.date
        # region_field = config.field_mapping.location_level1

        collection = db.collection(db_collections.CCVA_ERRORS)
        # query = f"FOR doc IN {collection.name} "
        bind_vars = {}

        
        query = f"""
        LET groupedErrorsCounts = (
            FOR e IN {db_collections.CCVA_ERRORS}
           // {"FILTER e.error_type == @error_type" if error_type else ""}
            COLLECT errorType = e.error_type INTO groupedDocs
            LET uniqueUuids = UNIQUE(groupedDocs[*].e.uuid)
            RETURN {{
            error_type: errorType,
            total: LENGTH(uniqueUuids)
            }}
        )

        LET individualErrorsResults = (
            FOR e IN {db_collections.CCVA_ERRORS}
            {"FILTER e.error_type == @error_type" if error_type else ""}
            COLLECT uuid = e.uuid, group = e.group INTO groupedErrors
            LET error_types = UNIQUE(groupedErrors[*].e.error_type)
            LET error_messages = UNIQUE(groupedErrors[*].e.error_message)
            SORT uuid ASC
            LIMIT @offset, @size
            RETURN {{
                uuid: uuid,
                error_types: error_types,
                group: group,
                error_messages: error_messages
            }}
        )

        RETURN {{
            groupedErrorsCounts: groupedErrorsCounts,
            individualErrorsResults: individualErrorsResults
        }}
        """


     
        bind_vars.update({
            "offset": 0,
            "size": 1000
        })

        if error_type:
            bind_vars["error_type"] = error_type
        
        def execute_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_query)

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        
        def execute_count_query():
            total_records_cursor = db.aql.execute(count_query, cache=True)
            return total_records_cursor.next()

        total_records = await run_in_threadpool(execute_count_query)

        return ResponseMainModel(
            data=data[0] if data else {},
            message="Records fetched successfully",
            total=total_records
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
    
async def fetch_error_details(db: StandardDatabase, error_id: str) -> ResponseMainModel:
    try:
        # Original query — unchanged so it stays reliable
        query = f"""
            FOR e IN {db_collections.CCVA_ERRORS}
                FILTER e.uuid == @error_id
                COLLECT uuid = e.uuid, errorType = e.error_type, group = e.group INTO groupedErrors

            LET formData = (
                FOR fs IN {db_collections.VA_TABLE}
                    FILTER fs.__id == CONCAT("uuid:", uuid)
                    RETURN UNSET(fs, ["_id", "_rev", "__id", "_key", "instanceid", "submissiondate"])
            )

            RETURN {{
                error: {{
                    uuid: uuid,
                    error_types: errorType,
                    group: group,
                    error_messages: UNIQUE(groupedErrors[*].e.error_message)
                }},
                form_data: formData[0]
            }}
        """

        def execute_details_query():
            cursor = db.aql.execute(query, bind_vars={"error_id": error_id}, cache=True)
            return cursor.next()

        result = await run_in_threadpool(execute_details_query)

        if not result:
            raise BadRequestException("Error not found", f"No error found with id {error_id}")

        # Fetch audit trail separately so a missing/empty collection never breaks the main load
        audit_trail = []
        try:
            audit_query = f"""
                FOR c IN {db_collections.CCVA_ERRORS_CORRECTIONS}
                    FILTER c.form_id == @error_id
                    SORT c.changed_at DESC
                    RETURN KEEP(c, ["changed_at", "changed_by", "changes"])
            """

            def execute_audit_query():
                cursor = db.aql.execute(audit_query, bind_vars={"error_id": error_id})
                return list(cursor)

            audit_trail = await run_in_threadpool(execute_audit_query)
        except Exception as audit_err:
            print(f"Audit trail fetch failed (non-fatal): {audit_err}")

        result["audit_trail"] = audit_trail

        return ResponseMainModel(
            data=result,
            message="Error details fetched successfully",
            total=1
        )
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch error details: {str(e)}", str(e))

async def fetch_form_data(db: StandardDatabase, form_id: str) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.FORM_SUBMISSIONS)
        query = f"""
        FOR fs IN {collection.name}
        FILTER fs.__id == @form_id
        RETURN fs
        """
        
        def execute_form_data_query():
            cursor = db.aql.execute(query, bind_vars={"form_id": form_id}, cache=True)
            return cursor.next()

        result = await run_in_threadpool(execute_form_data_query)

        if not result:
            raise BadRequestException("Form data not found", f"No form data found with id {form_id}")

        return ResponseMainModel(
            data=result,
            message="Form data fetched successfully",
            total=1
        )
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch form data: {str(e)}", str(e))

async def update_form_data(db: StandardDatabase, form_id: str, updated_data: dict) -> ResponseMainModel:
    try:
        collection = db.collection(db_collections.FORM_SUBMISSIONS)
        query = f"""
        FOR fs IN {collection.name}
        FILTER fs.__id == @form_id
        UPDATE fs WITH @updated_data IN {collection.name}
        RETURN NEW
        """
        
        def execute_update_form_query():
            cursor = db.aql.execute(query, bind_vars={"form_id": form_id, "updated_data": updated_data}, cache=True)
            return cursor.next()

        result = await run_in_threadpool(execute_update_form_query)

        if not result:
            raise BadRequestException("Form data not found", f"No form data found with id {form_id}")

        return ResponseMainModel(
            data=result,
            message="Form data updated successfully",
            total=1
        )
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to update form data: {str(e)}", str(e))


async def save_corrections_data(
    db: StandardDatabase, uuid: str, updated_data: dict, changed_by: dict
) -> ResponseMainModel:
    """Update the VA record in VA_TABLE and log a structured audit entry."""
    try:
        now = datetime.utcnow().isoformat()

        # Fetch current record to compute field-level diff
        fetch_query = f"""
        FOR fs IN {db_collections.VA_TABLE}
            FILTER fs.__id == CONCAT("uuid:", @uuid)
            RETURN fs
        """

        def execute_fetch():
            cursor = db.aql.execute(fetch_query, bind_vars={"uuid": uuid})
            return cursor.next()

        current = await run_in_threadpool(execute_fetch)
        if not current:
            raise BadRequestException("Form data not found", f"No VA record found with uuid {uuid}")

        changes = [
            {"field": field, "old_value": current.get(field), "new_value": new_val}
            for field, new_val in updated_data.items()
            if current.get(field) != new_val
        ]

        update_query = f"""
        FOR fs IN {db_collections.VA_TABLE}
            FILTER fs.__id == CONCAT("uuid:", @uuid)
            UPDATE fs WITH @updated_data IN {db_collections.VA_TABLE}
            RETURN NEW
        """

        def execute_update():
            cursor = db.aql.execute(
                update_query,
                bind_vars={"uuid": uuid, "updated_data": updated_data},
            )
            return cursor.next()

        result = await run_in_threadpool(execute_update)

        log_entry = {
            "form_id": uuid,
            "changed_by": changed_by,
            "changed_at": now,
            "changes": changes,
        }
        log_query = f"INSERT @entry INTO {db_collections.CCVA_ERRORS_CORRECTIONS}"

        def execute_log():
            db.aql.execute(log_query, bind_vars={"entry": log_entry})

        await run_in_threadpool(execute_log)

        return ResponseMainModel(
            data=result,
            message="Form data updated successfully",
            total=1
        )
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to update form data: {str(e)}", str(e))