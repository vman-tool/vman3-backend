from datetime import date
from typing import List, Optional

from arango import ArangoError
from arango.database import StandardDatabase

from app.records.responses.data import map_to_data_response
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values
from app.shared.middlewares.exceptions import BadRequestException


async def fetch_va_records(current_user:dict,paging: bool = True, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None,  date_type:Optional[str]=None,  db: StandardDatabase = None) -> ResponseMainModel:
    try:
        config = await fetch_odk_config(db)
        region_field = config.field_mapping.location_level1
        locationKey, locationLimitValues = get_location_limit_values(current_user)


        death_date = config.field_mapping.death_date 
        submitted_date = config.field_mapping.submitted_date 
        interview_date = config.field_mapping.interview_date 
            
        if date_type is not None:
            if date_type == 'submission_date':
                today_field = submitted_date
            elif date_type == 'death_date':
                today_field = death_date
            elif date_type == 'interview_date':
                today_field = interview_date
            else:
                today_field = config.field_mapping.date 
        else:
            today_field = config.field_mapping.date 

        collection = db.collection(db_collections.VA_TABLE)  # Use the actual collection name here
        query = f"FOR doc IN {collection.name} "
        bind_vars = {}
        filters = []
       ## filter by location limits
        if locationLimitValues and locationKey:
            filters.append(f"doc.{locationKey} IN @locationValues")
            bind_vars["locationValues"] = locationLimitValues
        ##
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
        cursor = db.aql.execute(query, bind_vars=bind_vars,cache=True)
        data = [map_to_data_response(config,document) for document in cursor]

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        total_records_cursor = db.aql.execute(count_query,cache=True)
        total_records = total_records_cursor.next()

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
    
    






async def fetch_va_records_json(current_user:dict,paging: bool = True,data_source:Optional[str]=None, task_id:Optional[str]=None, page_number: int = 1, limit: int = 10, start_date: Optional[date] = None, end_date: Optional[date] = None, locations: Optional[List[str]] = None,date_type:Optional[str]=None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        print(data_source, task_id)
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
                today_field = config.field_mapping.date 
        else:
            today_field = config.field_mapping.date 
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

        if locations:
            filters.append(f"doc.{region_field} IN @locations")
            bind_vars["locations"] = locations
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

        query += "RETURN doc"
        print(query)
        cursor = db.aql.execute(query, bind_vars=bind_vars,cache=True)
        data = [document for document in cursor]

        # Fetch total count of documents
        count_query = f"RETURN LENGTH({collection.name})"
        total_records_cursor = db.aql.execute(count_query,cache=True)
        total_records = total_records_cursor.next()

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
    
    
