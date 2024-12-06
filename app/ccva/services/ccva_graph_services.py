from datetime import date
from typing import List, Optional

from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException, status

from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.configs.security import get_location_limit_values
from app.shared.middlewares.exceptions import BadRequestException


async def fetch_db_processed_ccva_graphs(
    current_user: dict,
    ccva_id: Optional[str] = None, 
    is_default: Optional[bool] = None, 
    paging: bool = True, 
    page_number: int = 1, 
    limit: int = 30, 
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None, 
    locations: Optional[List[str]] = None,
    date_type: Optional[str] = None,

    db: StandardDatabase = None
) -> ResponseMainModel:
    try:
        # print(start_date, end_date, locations, date_type,'dated')
        # date_type= submission_date death_date interview_date

        locationKey, locationLimitValues = get_location_limit_values(current_user)
        if locationKey == 'id10005r':
            locationKey = 'locationLevel1'
        elif locationKey == 'id10005d':
            locationKey = 'locationLevel2'

        
    
        config = await fetch_odk_config(db)
        if date_type is not None:
            if date_type == 'submission_date':
                today_field = 'submitted_date'
            elif date_type == 'death_date':
                today_field = 'death_date'
            elif date_type == 'interview_date':
                today_field = 'interview_date'
            else:
                today_field = 'date' or config.field_mapping.date 
        else:
            today_field = 'date' or config.field_mapping.date 
            
        collection_name = db_collections.CCVA_RESULTS  # Use the actual collection name here
        if ccva_id is not None:
            cursor_cr = db.collection(db_collections().CCVA_GRAPH_RESULTS).find({
            "_key": ccva_id
            })
        else:
            cursor_cr = db.collection(db_collections().CCVA_GRAPH_RESULTS).find({
            "isDefault": True
            })

        defaultsCr = [{key: document.get(key, None) for key in ['task_id', 'isDefault','created_at','total_records','elapsed_time','range']} for document in cursor_cr]
        if not defaultsCr:
            raise BadRequestException("No default CCVA found")
        ccva_task_id = defaultsCr[0].get('task_id')
        created_at= defaultsCr[0].get('created_at')
        # total_records= defaultsCr[0].get('total_records')
        elapsed_time= defaultsCr[0].get('elapsed_time')
        range= defaultsCr[0].get('range')
        # to lower case for locations
        locations = [location.lower() for location in locations] if locations else None
        locationLimitValues = [location.lower() for location in locationLimitValues] if locationLimitValues else None
        
        

        query = f"""
        LET totalRecords = LENGTH(
            FOR cc IN {collection_name}
                FILTER cc.task_id == @taskId AND cc.CAUSE1 != "" AND cc.ID != null
                {f'AND cc.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            RETURN 1
        )
              LET totalchildRecords = LENGTH(
            FOR cc IN {collection_name}
                 FILTER cc.task_id == @taskId AND cc.CAUSE1 != "" AND (cc.ischild == "1" OR cc.age_group == "child") AND cc.ID != null
               
                {f'AND cc.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            RETURN 1
        )
        
              LET totalNeonateRecords = LENGTH(
            FOR cc IN {collection_name}
                 FILTER cc.task_id == @taskId AND cc.CAUSE1 != "" AND (cc.isneonatal == "1" OR cc.age_group == "neonate") AND cc.ID != null
               
                {f'AND cc.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            RETURN 1
        )
                  LET totalAdultRecords = LENGTH(
            FOR cc IN {collection_name}
                 FILTER cc.task_id == @taskId AND cc.CAUSE1 != "" AND (cc.isadult == "1" OR cc.age_group == "adult") AND cc.ID != null
               
                {f'AND cc.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            RETURN 1
        )
        
               LET totalmaleRecords = LENGTH(
            FOR cc IN {collection_name}
                 FILTER cc.task_id == @taskId AND cc.CAUSE1 != ""  AND cc.gender == "male" AND cc.ID != null
               
                {f'AND cc.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            RETURN 1
        )
        
                 LET totalfemaleRecords = LENGTH(
            FOR cc IN {collection_name}
                 FILTER cc.task_id == @taskId AND cc.CAUSE1 != ""  AND cc.gender == "female" AND cc.ID != null
               
                {f'AND cc.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cc.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            RETURN 1
        )
    

        LET allCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.ID != null
                {f'AND cr.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
            LET percent = count / totalRecords
            SORT percent DESC
            RETURN {{ cause, count, percent }}
        )

        LET maleCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.gender == "male" AND cr.ID != null
                {f'AND cr.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
            LET percent = count / totalmaleRecords
            SORT percent DESC
            RETURN {{ cause, count, percent }}
        )

        LET femaleCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.gender == "female" AND cr.ID != null
                {f'AND cr.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
            LET percent = count / totalfemaleRecords
            SORT percent DESC
            RETURN {{ cause, count, percent }}
        )

        LET neonateCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND (cr.isneonatal == "1" OR cr.age_group == "neonate") AND cr.ID != null
                {f'AND cr.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
            LET percent = count / totalNeonateRecords
            SORT percent DESC
            RETURN {{ cause, count, percent }}
        )

        LET childCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND (cr.ischild == "1" OR cr.age_group == "child") AND cr.ID != null
                {f'AND cr.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
            LET percent = count / totalchildRecords
            SORT percent DESC
            RETURN {{ cause, count, percent }}
        )

        LET adultCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND (cr.isadult == "1" OR cr.age_group == "adult") AND cr.ID != null
                {f'AND cr.locationLevel1 IN {locations}' if locations else ''}
                {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                {f'AND DATE_TIMESTAMP(cr.{today_field}) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
            COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
            LET percent = count / totalAdultRecords
            SORT percent DESC
            RETURN {{ cause, count, percent }}
        )

        RETURN {{
         "graphs":{{   "all": {{
                "index": allCauses[*].cause,
                "counts": allCauses[*].count,
                "values": allCauses[*].percent
            }},
            "male": {{
                "index": maleCauses[*].cause,
                "counts": maleCauses[*].count,
                "values": maleCauses[*].percent
            }},
            "female": {{
                "index": femaleCauses[*].cause,
                "counts": femaleCauses[*].count,
                "values": femaleCauses[*].percent
            }},
              "adult": {{
                "index": adultCauses[*].cause,
                "counts": adultCauses[*].count,
                "values": adultCauses[*].percent
            }},
               "child": {{
                "index": childCauses[*].cause,
                "counts": childCauses[*].count,
                "values": childCauses[*].percent
            }},
            "neonate": {{
                "index": neonateCauses[*].cause,
                "counts": neonateCauses[*].count,
                "values": neonateCauses[*].percent
            }},
         
          
            }},
            "total_records": totalRecords,
            "created_at": "{created_at}",
            "elapsed_time": "{elapsed_time}",
            "range": {range},
            "task_id": "{ccva_task_id}"
        }}
        """

        bind_vars = {
            "taskId": ccva_task_id
        }


        cursor = db.aql.execute(query, bind_vars=bind_vars, cache=True)
        data = [document for document in cursor]


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