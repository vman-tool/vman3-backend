from datetime import date
from typing import List, Optional

from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException, status

from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
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
        print(start_date, end_date, locations, date_type,'dated')
        # date_type= submission_date death_date interview_date
        locationKey=current_user['access_limit']['field'] or None ## locationLevel1
        if locationKey == 'id10005r':
            locationKey = 'locationLevel1'
        elif locationKey == 'id10005d':
            locationKey = 'locationLevel2'

        locationLimitValues = [item['value'] for item in current_user['access_limit']['limit_by']] if current_user['access_limit']['limit_by'] else None
    
    
        # config = await fetch_odk_config(db)
        # region_field = config.field_mapping.location_level1
        # district_field = config.field_mapping.location_level2
        # is_adult_field = config.field_mapping.is_adult
        # is_child_field = config.field_mapping.is_child
        # is_neonte_field = config.field_mapping.is_neonate
        # 
        
        # if date_type is not None:
        #     if date_type == 'submission_date':
        #         today_field = 'submissiondate'
        #     elif date_type == 'death_date':
        #         today_field = 'Id10022'
        #     elif date_type == 'interview_date':
        #         today_field = 'Id10012'
        #     else:
        #         today_field = config.field_mapping.date 
        # else:
        #     today_field = config.field_mapping.date 
            
        # if start_date:
        #     filters.append(f"doc.{today_field} >= @start_date")
        #     bind_vars["start_date"] = str(start_date)

        # if end_date:
        #     filters.append(f"doc.{today_field} <= @end_date")
        #     bind_vars["end_date"] = str(end_date)
            
            
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
        
        print(ccva_task_id,defaultsCr)
        query = f"""
             LET allTotalCount = LENGTH(
                    FOR cc IN {collection_name}
                        FILTER cc.task_id == @taskId AND cc.CAUSE1 != "" AND cc.ID != null
                         {f'AND cc.locationLevel1 == "{locations[0].lower()}"'  if locations else  ''}
        
                        {f'AND cc.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                        {f'AND DATE_TIMESTAMP(cc.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                       
                        RETURN cc
                )
        LET allCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.ID != null 
                {f"AND cr.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                 {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                     {f'AND DATE_TIMESTAMP(cr.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} {f'AND DATE_TIMESTAMP(cr.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                
                COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
                // LET totalCount = LENGTH(
                //     FOR cr2 IN {collection_name}
                //         FILTER cr2.task_id == @taskId AND cr2.CAUSE1 != "" AND cr2.ID != null
                //          {f"AND cr2.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                        
                       
                //         RETURN cr2
                // )
                LET percent = (count / allTotalCount)
                SORT percent DESC
                RETURN {{ cause, count, percent }}
        )

        LET maleCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.gender == "male" AND cr.ID != null
                  {f"AND cr.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                   {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                       {f'AND DATE_TIMESTAMP(cr.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                       {f'AND DATE_TIMESTAMP(cr.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
                LET totalCount = LENGTH(
                    FOR cr2 IN {collection_name}
                        FILTER cr2.task_id == @taskId AND cr2.CAUSE1 != "" AND cr2.gender == "male" AND cr2.ID != null
                           {f"AND cr2.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                            {f'AND cr2.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                           {f'AND DATE_TIMESTAMP(cr2.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} 
                           {f'AND DATE_TIMESTAMP(cr2.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                        RETURN cr2
                )
                LET percent = (count / totalCount)
                SORT percent DESC
                RETURN {{ cause, count, percent }}
        )

        LET femaleCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.gender == "female" AND cr.ID != null
                  {f"AND cr.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                   {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                       {f'AND DATE_TIMESTAMP(cr.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} 
                       {f'AND DATE_TIMESTAMP(cr.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
                LET totalCount = LENGTH(
                    FOR cr2 IN {collection_name}
                        FILTER cr2.task_id == @taskId AND cr2.CAUSE1 != "" AND cr2.gender == "female" AND cr2.ID != null
                        {f"AND cr2.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                         {f'AND cr2.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                          {f'AND DATE_TIMESTAMP(cr2.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                          {f'AND DATE_TIMESTAMP(cr2.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                        RETURN cr2
                )
                LET percent = (count / totalCount)
                SORT percent DESC
                RETURN {{ cause, count, percent }}
        )

        LET neonateCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.age_group == "neonate" AND cr.ID != null
                {f"AND cr.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                 {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                     {f'AND DATE_TIMESTAMP(cr.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} 
                     {f'AND DATE_TIMESTAMP(cr.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
                LET totalCount = LENGTH(
                    FOR cr2 IN {collection_name}
                        FILTER cr2.task_id == @taskId AND cr2.CAUSE1 != "" AND cr2.age_group == "neonate" AND cr2.ID != null
                        {f"AND cr2.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                         {f'AND cr2.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                            {f'AND DATE_TIMESTAMP(cr2.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} 
                            {f'AND DATE_TIMESTAMP(cr2.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                        RETURN cr2
                )
                LET percent = (count / totalCount)
                SORT percent DESC
                RETURN {{ cause, count, percent }}
        )

        LET childCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.age_group == "child" AND cr.ID != null
                {f"AND cr.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                 {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                     {f'AND DATE_TIMESTAMP(cr.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} 
                     {f'AND DATE_TIMESTAMP(cr.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
                LET totalCount = LENGTH(
                    FOR cr2 IN {collection_name}
                        FILTER cr2.task_id == @taskId AND cr2.CAUSE1 != "" AND cr2.age_group == "child" AND cr2.ID != null
                        {f"AND cr2.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                           {f'AND cr2.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                           {f'AND DATE_TIMESTAMP(cr2.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''} 
                           {f'AND DATE_TIMESTAMP(cr2.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                        RETURN cr2
                )
                LET percent = (count / totalCount)
                SORT percent DESC
                RETURN {{ cause, count, percent }}
        )

        LET adultCauses = (
            FOR cr IN {collection_name}
                FILTER cr.task_id == @taskId AND cr.CAUSE1 != "" AND cr.age_group == "adult" AND cr.ID != null
                 {f"AND cr.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                    {f'AND cr.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                        {f'AND DATE_TIMESTAMP(cr.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                        {f'AND DATE_TIMESTAMP(cr.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                COLLECT cause = cr.CAUSE1 WITH COUNT INTO count
                LET totalCount = LENGTH(
                    FOR cr2 IN {collection_name}
                        FILTER cr2.task_id == @taskId AND cr2.CAUSE1 != "" AND cr2.age_group == "adult" AND cr2.ID != null
                         {f"AND cr2.locationLevel1 == '{locations[0].lower()}'"  if locations else  ""}
                            {f'AND cr2.{locationKey} IN {locationLimitValues}' if locationKey and locationLimitValues else ''}
                           {f'AND DATE_TIMESTAMP(cr2.date) >= DATE_TIMESTAMP("{start_date}")' if start_date else ''}
                           {f'AND DATE_TIMESTAMP(cr2.date) <= DATE_TIMESTAMP("{end_date}")' if end_date else ''}
                           
                        RETURN cr2
                )
                LET percent = (count / totalCount)
                SORT percent DESC
                RETURN {{ cause, count, percent }}
        )

        RETURN {{
            "all": {{
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
            "neonate": {{
                "index": neonateCauses[*].cause,
                "counts": neonateCauses[*].count,
                "values": neonateCauses[*].percent
            }},
            "child": {{
                "index": childCauses[*].cause,
                "counts": childCauses[*].count,
                "values": childCauses[*].percent
            }},
            "adult": {{
                "index": adultCauses[*].cause,
                "counts": adultCauses[*].count,
                "values": adultCauses[*].percent
            }},
                "created_at": "{created_at}",
            "total_records": allTotalCount,
            "elapsed_time": "{elapsed_time}",
            "range": {range},
            "task_id": "{ccva_task_id}"
            }}
        
        """
        print(query, 'ccva_task_id',ccva_task_id)
        bind_vars = {
            "taskId": ccva_task_id,
            # "locationLevel1": locations[0].lower() if locations else None  # Assuming one location for simplicity
        }

        # # Filtering logic for ccva_id, is_default, date range
        # filters = []

        # if ccva_id:
        #     filters.append("doc._key == @ccva_id")
        #     bind_vars["ccva_id"] = ccva_id

        # if is_default is not None or ccva_id is None:
        #     filters.append("doc.isDefault == @is_default")
        #     bind_vars["is_default"] = is_default if is_default is not None else True

        # if start_date:
        #     filters.append("doc.range.start >= @start_date")
        #     bind_vars["start_date"] = str(start_date)

        # if end_date:
        #     filters.append("doc.range.end <= @end_date")
        #     bind_vars["end_date"] = str(end_date)

        # # Apply the filters to the query
        # if filters:
        #     query = f"FOR doc IN {collection_name} FILTER " + " AND ".join(filters) + " " + query
        print(query)
        cursor = db.aql.execute(query, bind_vars=bind_vars,cache=True)
        data = [document for document in cursor]
        # print(data)

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