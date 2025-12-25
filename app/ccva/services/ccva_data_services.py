from datetime import date
from typing import List, Optional

from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException
from app.utilits.db_logger import db_logger, log_to_db

# #@log_to_db(context="fetch_ccva_records", log_args=True)
async def fetch_ccva_records(paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db, True)
        region_field = config.field_mapping.location_level1

        today_field = config.field_mapping.date
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
        # print(query)
        # print(query)
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
        print(e)
        await db_logger.log(
            message="Failed to fetched records " + str(e),
            level=db_logger.LogLevel.ERROR,
            context="fetch_ccva_records",
            data={}
         
    )
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
    
    
    
async def fetch_processed_ccva_graphs(
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
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
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
        print(e)
        raise BadRequestException("Failed to fetch records", str(e))
    except Exception as e:
        print(e)
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
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
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
        print(e)
        raise BadRequestException("Failed to fetch records", str(e))
    except Exception as e:
        print(e)
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
        RETURN {
            
            "id": doc._key,
            "task_id": doc.task_id,
            "created_at": doc.created_at,
            "total_records": doc.total_records,
            "elapsed_time": doc.elapsed_time,
            "start": doc.range.start,
            "end": doc.range.end,
            "isDefault": doc.isDefault
        }
        """

        def execute_all_processed_query():
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
            return [document for document in cursor]

        data = await run_in_threadpool(execute_all_processed_query)


        # Fetch total count of documents
        

        return ResponseMainModel(
            data=data,
            message="Processed CCVA fetched successfully",
            total=None
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to fetched records",str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to fetch records: {str(e)}",str(e))
        
        
async def update_ccva_entry(ccva_id: str, update_data: dict, db: StandardDatabase) -> ResponseMainModel:
    try:
        # Define the collection
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)

        # Fetch the existing document
        query = f"FOR doc IN {collection.name} FILTER doc._key == @ccva_id RETURN doc"
        
        def execute_fetch_entry():
            cursor = db.aql.execute(query, bind_vars={"ccva_id": ccva_id}, cache=True)
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
            cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
            return next(cursor, None)

        updated_doc = await run_in_threadpool(execute_update_entry)
        return ResponseMainModel(
            data=updated_doc,
            message=f"CCVA entry with id {ccva_id} updated successfully",
            total=None
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to update record", str(e))
    except Exception as e:
        print(e)
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
            db.aql.execute(query_unset_default, cache=True)  # Unset the current default entry

            # Step 2: Set the new CCVA as default
            query_set_default = f"""
            FOR doc IN {collection.name} 
            FILTER doc._key == @ccva_id 
            UPDATE doc WITH {{ isDefault: true }} IN {collection.name}
            RETURN NEW
            """
            bind_vars = {"ccva_id": ccva_id}
            cursor = db.aql.execute(query_set_default, bind_vars=bind_vars, cache=True)
            return next(cursor, None)

        updated_doc = await run_in_threadpool(execute_toggle_default)
        if not updated_doc:
            raise BadRequestException(f"CCVA entry with id {ccva_id} not found")

        # Step 3: Return the updated document as a response
        return ResponseMainModel(
            data=updated_doc,
            message=f"CCVA entry with id {ccva_id} set as default successfully",
            total=None
        )

    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to set CCVA as default", str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to set CCVA as default: {str(e)}", str(e))
          
async def delete_ccva_entry(ccva_id: str, db: StandardDatabase) -> ResponseMainModel:
    try:
        # Define the collection
        collection = db.collection(db_collections.CCVA_GRAPH_RESULTS)

        # Check if the document exists
        query = f"FOR doc IN {collection.name} FILTER doc._key == @ccva_id RETURN doc"
        
        def execute_delete_entry():
            cursor = db.aql.execute(query, bind_vars={"ccva_id": ccva_id}, cache=True)
            ccva_doc = next(cursor, None)

            if not ccva_doc:
                raise BadRequestException(f"CCVA entry with id {ccva_id} not found")
        
            # Delete the document
            query_delete = f"REMOVE {{ _key: @ccva_id }} IN {collection.name}"
            bind_vars = {"ccva_id": ccva_id}
            db.aql.execute(query_delete, bind_vars=bind_vars, cache=True)
            db.collection(db_collections.CCVA_RESULTS).delete_match({
                "task_id": ccva_doc.get("task_id")
            })

        await run_in_threadpool(execute_delete_entry)

        return ResponseMainModel(
            data=None,
            message=f"CCVA entry with id {ccva_id} deleted successfully",
            total=None
        )
    except ArangoError as e:
        print(e)
        raise BadRequestException("Failed to delete record", str(e))
    except Exception as e:
        print(e)
        raise BadRequestException(f"Failed to delete record: {str(e)}", str(e))