
import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta
from typing import Dict, Optional

import numpy as np
import pandas as pd
from arango.database import StandardDatabase
from interva.utils import csmf
from pycrossva.transform import transform

from app.ccva.models.ccva_models import InterVA5Progress
from app.ccva.utilits.interva.interva5 import InterVA5
from app.records.services.list_data import fetch_va_records_json
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.arangodb import null_convert_data
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


# The websocket_broadcast function for broadcasting progress updates
async def websocket_broadcast(task_id: str, progress_data: dict):
    from app.main import (
        websocket__manager,  # Ensure this points to your actual WebSocket manager instance
    )
    await websocket__manager.broadcast(task_id, json.dumps(progress_data))
async def get_record_to_run_ccva(db: StandardDatabase, task_id: str, task_results: Dict,start_date: Optional[date] = None, end_date: Optional[date] = None,):
    try:
        records= await fetch_va_records_json(paging=False, start_date=start_date, end_date=end_date,  db=db)
        if records.data == []:
            raise Exception("No records found")
        
        return records
    except Exception as e:
        print(e)
        pass
        

        
# The main run_ccva function that integrates everything
async def run_ccva(db: StandardDatabase, records:ResponseMainModel, task_id: str, task_results: Dict,start_date: Optional[date] = None, end_date: Optional[date] = None, malaria_status:Optional[str]=None, hiv_status:Optional[str]=None, ccva_algorithm:Optional[str]=None):
    try:
                # Define the async callback to send progress updates
        async def update_callback(progress):
            await websocket_broadcast(task_id, progress)
                # Initial update for task start
        start_time = datetime.now()

        # initial_message = {"progress": 1, "message": "Collecting data.", "status":'init',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}","task_id": task_id, "error": False}
        # await update_callback(initial_message)
        await websocket_broadcast(task_id=task_id, progress_data= InterVA5Progress(
            progress=1,
            total_records= len(records.data),
            message="Collecting data.",
            status="running",
            elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
            task_id=task_id,
            error=False
        ).model_dump_json())


        # Convert records to DataFrame directly
        database_dataframe = pd.DataFrame.from_records(records.data)

        

        

        

       
        # Fetch the  configuration
        config = await fetch_odk_config(db)
        id_col = config.field_mapping.instance_id
        date_col = config.field_mapping.date
        # Run the CCVA process in a thread pool, with real-time updates
        await update_callback(InterVA5Progress(
        progress=4,
        message="Running InterVA5 analysis...",
        status="running",
        total_records=len(database_dataframe),
        elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
        task_id=task_id,
        error=False
    ).model_dump_json())
        
        await asyncio.to_thread(
            runCCVA, odk_raw=database_dataframe, file_id=task_id, update_callback=update_callback,db= db, id_col=id_col,date_col=date_col,start_time=start_time, algorithm= ccva_algorithm,   malaria= malaria_status, hiv= hiv_status,
            
        )
        

    except Exception as e:
        print(e)
        error_message = {"progress": 0, "message": str(e), "status":'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": task_id, "error": True}
        await update_callback(error_message)
        task_results[task_id] = error_message
        
        
def runCCVA(odk_raw:pd.DataFrame, id_col: str = None,date_col:str =None,start_time:timedelta=None, instrument: str = '2016WHOv151', algorithm: str = 'InterVA5',
            top=10, undetermined: bool = True, malaria: str = "h", hiv: str = "h",
            file_id: str = "unnamed_file", update_callback=None, db: StandardDatabase=None):
    
    try:
        # Transform the input data
        if id_col:
            input_data = transform((instrument, algorithm), odk_raw, raw_data_id=id_col, lower=True)
        else:
            input_data = transform((instrument, algorithm), odk_raw, lower=True)
        
        # Define the output folder
        output_folder = "./ccva_files/"
        # output_folder = f"../ccva_files/{file_id}/"
        
        # Create an InterVA5 instance with the async callback
        iv5out = InterVA5(input_data,task_id=file_id, hiv=hiv, malaria=malaria, write=True, directory=output_folder, filename=file_id,start_time=start_time, update_callback=update_callback, return_checked_data=True)

        asyncio.run(update_callback(InterVA5Progress(
        progress=7,
        message="Running InterVA5 analysis...",
        status="running",
        total_records=len(input_data),
        elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
        task_id=file_id,
        error=False
    ).model_dump_json()))
        
        # Run the InterVA5 analysis, with progress updates via the async callback
        iv5out.run()
        records =  iv5out.get_indiv_prob(
            top=10
        )
       
        rcd = records.to_dict(orient='records')

        # Iterate over each dictionary and add the 'task_id' field
        for record in rcd:
            record["task_id"] = file_id
        # Insert the records into the database
      
        
       # get the ccva form data (individual ones, eg, locations, gender, age_group) from the database and merge with the results
        results_to_insert = asyncio.run(getVADataAndMergeWithResults(db, null_convert_data(rcd)))
        db.collection(db_collections.CCVA_RESULTS).insert_many(results_to_insert, overwrite=True, overwrite_mode="update")
        print("InterVA5 analysis completed.")

        # Remove the temporary CSV file
        # os.remove(f"{file_id}-dt.csv")
        total_records = len(records)
        rangeDates={"start": odk_raw[date_col].max(), "end":odk_raw[date_col].min()}
        ## get ccva error logs to be added to the ccva_results
        error_logs = process_ccva_errorlogs(output_folder + file_id + "_")
        print(error_logs)
        
        ccva_results= compile_ccva_results(iv5out, error_logs=error_logs, top=top, undetermined=undetermined, task_id=file_id,start_time= start_time,total_records=total_records,  rangeDates =rangeDates, db=db)
        error_log_path = f"{output_folder}{file_id}_errorlogV5.txt"
        log_path = f"{output_folder}{file_id}.csv"
        
        if os.path.exists(error_log_path):
            os.remove(error_log_path)
        if os.path.exists(log_path):
            os.remove(log_path)
        return ccva_results

    except Exception as e:
        print(f"Error during CCVA analysis: {e}")
        asyncio.run(update_callback({"progress": 0, "message": f"Error during CCVA analysis: {e}", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True}))
        

        

# Function to compile the results from InterVA5
def compile_ccva_results(iv5out, top=10, undetermined=True,start_time:timedelta=None,
                         task_id:str=None,
                         total_records:int=0, rangeDates: Dict={},
                         error_logs: Optional[any]=None,
                         db: StandardDatabase=None):
    # Compile results for all groups
    all_results = {
        "index": csmf(iv5out, top=top, age=None, sex=None).index.tolist(),
        "values": csmf(iv5out, top=top, age=None, sex=None).tolist()
    }
    if not undetermined:
        top += 1
        index = np.array(csmf(iv5out, top=top, age=None, sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age=None, sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        if len(idx) != 0:
            index = np.delete(index, idx)
            values = np.delete(values, idx)
            all_results = {"index": index.tolist(), "values": values.tolist()}

    # Compile results for male group
    male_results = {
        "index": csmf(iv5out, top=top, age=None, sex='male').index.tolist(),
        "values": csmf(iv5out, top=top, age=None, sex='male').tolist()
    }
    if not undetermined:
        top += 1
        index = np.array(csmf(iv5out, top=top, age=None, sex='male').index.tolist())
        values = np.array(csmf(iv5out, top=top, age=None, sex='male').tolist())
        idx = np.argwhere(index == "Undetermined")
        if len(idx) != 0:
            index = np.delete(index, idx)
            values = np.delete(values, idx)
            male_results = {"index": index.tolist(), "values": values.tolist()}

    # Compile results for female group
    female_results = {
        "index": csmf(iv5out, top=top, age=None, sex='female').index.tolist(),
        "values": csmf(iv5out, top=top, age=None, sex='female').tolist()
    }
    if not undetermined:
        top += 1
        index = np.array(csmf(iv5out, top=top, age=None, sex='female').index.tolist())
        values = np.array(csmf(iv5out, top=top, age=None, sex='female').tolist())
        idx = np.argwhere(index == "Undetermined")
        if len(idx) != 0:
            index = np.delete(index, idx)
            values = np.delete(values, idx)
            female_results = {"index": index.tolist(), "values": values.tolist()}

    # Compile results for adult group
    adult_results = {
        "index": csmf(iv5out, top=top, age='adult', sex=None).index.tolist(),
        "values": csmf(iv5out, top=top, age='adult', sex=None).tolist()
    }
    if not undetermined:
        top += 1
        index = np.array(csmf(iv5out, top=top, age='adult', sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age='adult', sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        if len(idx) != 0:
            index = np.delete(index, idx)
            values = np.delete(values, idx)
            adult_results = {"index": index.tolist(), "values": values.tolist()}

    # Compile results for child group
    child_results = {
        "index": csmf(iv5out, top=top, age='child', sex=None).index.tolist(),
        "values": csmf(iv5out, top=top, age='child', sex=None).tolist()
    }
    if not undetermined:
        top += 1
        index = np.array(csmf(iv5out, top=top, age='child', sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age='child', sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        if len(idx) != 0:
            index = np.delete(index, idx)
            values = np.delete(values, idx)
            child_results = {"index": index.tolist(), "values": values.tolist()}

    # Compile results for neonate group
    neonate_results = {
        "index": csmf(iv5out, top=top, age='neonate', sex=None).index.tolist(),
        "values": csmf(iv5out, top=top, age='neonate', sex=None).tolist()
    }
    if not undetermined:
        top += 1
        index = np.array(csmf(iv5out, top=top, age='neonate', sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age='neonate', sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        if len(idx) != 0:
            index = np.delete(index, idx)
            values = np.delete(values, idx)
            neonate_results = {"index": index.tolist(), "values": values.tolist()}

    # Create the final merged DataFrame
    merged_df = pd.concat([csmf(iv5out, top=top, age=None, sex=None), 
                           csmf(iv5out, top=top, age=None, sex='male'),
                           csmf(iv5out, top=top, age=None, sex='female'),
                           csmf(iv5out, top=top, age='adult', sex=None),
                           csmf(iv5out, top=top, age='child', sex=None),
                           csmf(iv5out, top=top, age='neonate', sex=None)], axis=1)
    
    merged_df.columns = ['all', 'male', 'female', 'adult', 'child', 'neonate']
    merged_df.fillna(0, inplace=True)

    merged_arr = []
    for col in merged_df.columns:
        merged_arr.append(merged_df[col].tolist())

    # merged_results = {
    #     "index": merged_df.index.tolist(),
    #     "values": merged_arr
    # }

    # Combine all results into a single dictionary
    elapsed_time = datetime.now() - start_time
    ccva_results = {
        "task_id": task_id,
        "created_at": datetime.now().isoformat(),
        "total_records": total_records,
        "elapsed_time":   f"{elapsed_time.seconds // 3600}:{(elapsed_time.seconds // 60) % 60}:{elapsed_time.seconds % 60}",

        "range":rangeDates,
        "all": all_results,
        "male": male_results,
        "female": female_results,
        "adult": adult_results,
        "child": child_results,
        "neonate": neonate_results,
        "error_logs": error_logs,
        # "merged": merged_results
    }

    db.collection(db_collections.CCVA_GRAPH_RESULTS).insert(ccva_results)
    asyncio.run(  websocket_broadcast(task_id,{"progress": 100, "message": "Finish CCVA analysis...", "status": 'completed', "data": ccva_results ,"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": task_id, "error": False}))

    return ccva_results

def process_ccva_errorlogs(output_folder: str):
    # Path to the error log file
    log_file_path = output_folder + 'errorlogV5.txt'
    log_entries = []

    # Regular expression to capture error logs
    error_pattern = r'uuid:([\w-]+)\s(Error in (indicators|sex indicator|age indicator)):\s(.+)'
    discrepancy_pattern = r'uuid:([\w-]+)\s+(.+)'
    
    current_group = None  # Track the current group for categorization

    # Read the log file
    with open(log_file_path, 'r') as file:
        logs = file.readlines()

    # Process each log entry
    for log in logs:
        log = log.strip()  # Clean up leading/trailing spaces

        # Detect the group headers
        if "The following records are incomplete and excluded from further processing:" in log:
            current_group = "incomplete_records"
            continue  # Skip to the next log line

        if "The following data discrepancies were identified and handled:" in log:
            current_group = "data_discrepancies"
            continue  # Skip to the next log line

        # Match the log entry with the appropriate pattern based on current group
        if current_group == "incomplete_records":
            match = re.search(error_pattern, log)
        elif current_group == "data_discrepancies":
            match = re.search(discrepancy_pattern, log)
        else:
            continue

        # If a match is found, capture relevant information
        if match:
            uuid = match.group(1)
            if current_group == "incomplete_records":
                error_type = match.group(2)
                error_message = match.group(4)
            else:  # data discrepancies
                error_type = "data discrepancy"
                error_message = match.group(2)

            # Create a log entry for insertion into ArangoDB
            log_entry = {
                "uuid": uuid,
                "error_type": error_type,
                "error_message": error_message,
                "group": current_group  # Add the group/category for identification
            }

            # Append log entry to the list
            log_entries.append(log_entry)



    # Return the processed log entries
    print(log_entries)
    return log_entries

async def fetch_ccva_results_and_errors(db: StandardDatabase, task_id: str):
    try:
        # AQL query to fetch individual results and error logs
        query = f"""
        LET graph_results = (
            FOR g IN ccva_graph_results
            FILTER g.task_id == "{task_id}"
            RETURN g.error_logs
        )

        LET indiv_results = (
            FOR d IN ccva_results
            FILTER d.task_id == "{task_id}"
            RETURN {{
                ID: d.ID,
                CAUSE1: d.CAUSE1,
                CAUSE2: d.CAUSE2
            }}
        )

        RETURN {{
            results: indiv_results,
            error_logs: LENGTH(graph_results) > 0 ? graph_results[0] : null
        }}
        """

        # Execute the AQL query
        cursor = db.aql.execute(query)

        # Retrieve the result (first result since RETURN only outputs one document)
        result = cursor.next()

        return result

    except Exception as e:
        print(f"Error fetching CCVA results and error logs: {e}")
        return None
    
    
async def getVADataAndMergeWithResults(db: StandardDatabase, results: list):
    ###
    
    ###
    from app.settings.services.odk_configs import fetch_odk_config


    config = await fetch_odk_config(db)
    is_adult=config.field_mapping.is_adult
    is_child=config.field_mapping.is_child
    is_neonate=config.field_mapping.is_neonate
    deceased_gender=config.field_mapping.deceased_gender
    location_level1=config.field_mapping.location_level1
    location_level2=config.field_mapping.location_level2
    date=config.field_mapping.date
    instance_id=config.field_mapping.instance_id or 'instanceid'

    for  result in results:
        data_uid = result.get('ID', None)
        if data_uid is None:
            continue
        
        cursor=db.collection(db_collections.VA_TABLE).find({instance_id: data_uid})
        ind_results = [{key: document.get(key, None) for key in ['id10019', is_adult, is_child,is_neonate,deceased_gender,location_level1,location_level2,date]} for document in cursor]
        # Rename fields in the results and determine the age group
        renamed_results = [
                {
                'gender': doc.pop('id10019', None).lower() if doc.get('id10019') else None,
                'date': doc.pop('today', None).lower() if doc.get('today') else None,
                'ageGroup': 'adult' if doc.pop(is_adult, None) else 'child' if doc.pop(is_child, None) else 'neonate' if doc.pop(is_neonate, None) else None,
                'locationLevel1': doc.pop(location_level1, None).lower() if doc.get(location_level1) else None,
                'locationLevel2': doc.pop(location_level2, None).lower() if doc.get(location_level2) else None
                }
                for doc in ind_results
            ]

        # Check if there are any results to merge
        if renamed_results:
            # Merge the results with the original dictionary
            result.update(renamed_results[0] , )

    return results
