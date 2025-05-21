
import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta
from typing import Dict, Optional

import numpy as np
import pandas as pd
from arango.database import StandardDatabase
from app.ccva.utilits.interva.utils import csmf
from app.ccva.utilits.pycrossva.transform import transform

from app.ccva.models.ccva_models import InterVA5Progress
from app.ccva.utilits.interva.interva5 import InterVA5
from app.records.services.list_data import fetch_va_records_json
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.arangodb import null_convert_data, remove_null_values
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


# The websocket_broadcast function for broadcasting progress updates
async def websocket_broadcast(task_id: str, progress_data: dict):
    from app.main import (
        websocket__manager,  # Ensure this points to your actual WebSocket manager instance
    )
    await websocket__manager.broadcast(task_id, json.dumps(progress_data))

async def get_record_to_run_ccva(current_user:dict,db: StandardDatabase,data_source:Optional[str], task_id: Optional[str], task_results: Dict,start_date: Optional[date] = None, end_date: Optional[date] = None,date_type:Optional[str]=None,):
    try:
        records= await fetch_va_records_json(current_user=current_user,paging=False,data_source=data_source,task_id=task_id,  start_date=start_date, end_date=end_date,  db=db,date_type=date_type)
        if records.data == []:
            raise Exception("No records found")
        
        return records
    except Exception as e:
        print(e)
        # logger.error(f"Error fetching ODK data: {e}")
        # print(e)
        raise Exception(status_code=500, detail=str(e))
        

        
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
        database_dataframe = pd.DataFrame.from_records( remove_null_values(records.data))

        

        

        

       
        # Fetch the  configuration
        config = await fetch_odk_config(db, True)
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
        print('pass here 0', file_id,instrument,id_col)
        # Transform the input data
        if id_col:
            input_data = transform((instrument, algorithm), odk_raw, raw_data_id=id_col, lower=True)
        else:
            input_data = transform((instrument, algorithm), odk_raw, lower=True)
        print('pass here')
        # Define the output folder

        output_folder = os.path.dirname(os.path.abspath(__file__))
        # create subdirectory for the task
        output_folder = os.path.join(output_folder, "ccva_files")
        os.makedirs(output_folder, exist_ok=True)
        print(f'Output directory ready: {output_folder}')
        # output_folder = f"../ccva_files/{file_id}/"
        print('pass here 2')
        # Create an InterVA5 instance with the async callback
        iv5out = InterVA5(input_data,task_id=file_id, hiv=hiv, malaria=malaria, write=True,
                          directory=output_folder,
                          filename=file_id,start_time=start_time, update_callback=update_callback, return_checked_data=True)

        asyncio.run(update_callback(InterVA5Progress(
        progress=7,
        message="Running InterVA5 analysis...",
        status="running",
        total_records=len(input_data),
        elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
        task_id=file_id,
        error=False
    ).model_dump_json()))
        output_folder=output_folder+'/'
        
        # Run the InterVA5 analysis, with progress updates via the async callback
        iv5out.run()
        print('after run')
        records =  iv5out.get_indiv_prob(
            top=10,
            include_propensities=False
        )
        print('after get_indiv_prob')
       ## TODOS: find the corect way to load data from records (fuction)
        rcd = records.to_dict(orient='records')
        # pd.DataFrame(rcd).to_csv(f"{output_folder}{file_id}_ccva_results-test.csv")
        # get from csv(official)
        print('rcd total')
        print(len(rcd))
        print(f"{output_folder}{file_id}.csv")
        try:
            csv_path = f"{output_folder}{file_id}.csv"
            if os.path.exists(csv_path):
                rcd = pd.read_csv(csv_path).to_dict(orient='records')
                print("CSV file read successfully.")
                print(len(rcd))
            else:
                if len(rcd) <= 0:
                    rcd = []
             
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            rcd = []
        print(len(rcd))
        # print(rcd)
        if rcd == [] or rcd is None:
            ensure_task(update_callback({"progress": 0, "message": "No records found", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True}))
            raise Exception("No records found")
            return
        # Iterate over each dictionary and add the 'task_id' field
        for record in rcd:
            record["task_id"] = file_id
        # Insert the records into the database
      
        # print(rcd)
       # get the ccva form data (individual ones, eg, locations, gender, age_group) from the database and merge with the results
        results_to_insert = asyncio.run(getVADataAndMergeWithResults(db, null_convert_data(rcd)))
        if results_to_insert is None:
            print("No records found")
            ensure_task(update_callback({"progress": 0, "message": "No records found", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True}))
            return
        print("InterVA5 analysis completed.",len(results_to_insert))
        db.collection(db_collections.CCVA_RESULTS).insert_many(results_to_insert, overwrite=True, overwrite_mode="update")
        print("CCVA results inserted into the database.")


        total_records = len(records)
        rangeDates={"start": odk_raw[date_col].max(), "end":odk_raw[date_col].min()}
        ## get ccva error logs to be added to the ccva_results
        error_logs = process_ccva_errorlogs(output_folder + file_id + "_", task_id=file_id)
        print("Processing error logs...")

        ccva_results= compile_ccva_results(iv5out,
                                           data_processed_with_results=len(results_to_insert),
                                           error_logs=error_logs,
                                           top=top,
                                           undetermined=undetermined,
                                           task_id=file_id,
                                           start_time= start_time,
                                           total_records=total_records,  
                                           rangeDates =rangeDates, 
                                           db=db)
        print("CCVA run is completed.")
        error_log_path = f"{output_folder}{file_id}_errorlogV5.txt"
        log_path = f"{output_folder}{file_id}.csv"

        
        if os.path.exists(error_log_path):
            os.remove(error_log_path)
        if os.path.exists(log_path):
            os.remove(log_path)
        return ccva_results

    except Exception as e:
        print(f"Error during CCVA analysis: {e}")
        ensure_task(update_callback({"progress": 0, "message": f"Error during CCVA analysis: {e}", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True}))
        raise e

        

# Function to compile the results from InterVA5
def compile_ccva_results(iv5out, top=10, undetermined=True,start_time:timedelta=None,
                         task_id:str=None,
                         data_processed_with_results:int=0,
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
        "data_processed_with_results": data_processed_with_results,
        "elapsed_time":   f"{elapsed_time.seconds // 3600}:{(elapsed_time.seconds // 60) % 60}:{elapsed_time.seconds % 60}",

        "range":rangeDates,
        "all": all_results,
        "male": male_results,
        "female": female_results,
        "adult": adult_results,
        "child": child_results,
        "neonate": neonate_results,
        # "error_logs": error_logs,
        # "merged": merged_results
    }

    db.collection(db_collections.CCVA_GRAPH_RESULTS).insert(ccva_results)
    db.collection(db_collections.CCVA_ERRORS).insert(error_logs, overwrite=True, overwrite_mode="update")
    
    ensure_task(  websocket_broadcast(task_id,{"progress": 100, "message": "Finish CCVA analysis...", "status": 'completed', "data": ccva_results ,"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": task_id, "error": False}))

    return ccva_results

def process_ccva_errorlogs(output_folder: str,task_id:str=None):
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
                "task_id": task_id,  # Use the UUID as the task_id for easy reference
                "error_type": error_type,
                "error_message": error_message,
                "group": current_group  # Add the group/category for identification
            }

            # Append log entry to the list
            log_entries.append(log_entry)



    # Return the processed log entries

    return log_entries

async def fetch_ccva_results_and_errors(db: StandardDatabase, task_id: str):
    try:
        # AQL query to fetch individual results and error logs
        query = f"""
        LET graph_results = (
            FOR g IN ccva_errors
            FILTER g.task_id == "{task_id}"
            RETURN g
        )

        LET indiv_results = (
            FOR d IN ccva_results
            FILTER d.task_id == "{task_id}"
            RETURN d
        )

        RETURN {{
            results: indiv_results,
            error_logs: LENGTH(graph_results) > 0 ? graph_results : null
        }}
        """

        # Execute the AQL query
        cursor = db.aql.execute(query,cache=True)

        # Retrieve the result (first result since RETURN only outputs one document)
        result = cursor.next()

        return result

    except Exception as e:
        print(f"Error fetching CCVA results and error logs: {e}")
        return None
    
async def getVADataAndMergeWithResults(db: StandardDatabase, results: list):
    # save results to csv
    df = pd.DataFrame(results)
    df.to_csv('results.csv')
    from app.settings.services.odk_configs import fetch_odk_config

    # Fetch configurations asynchronously
    config = await fetch_odk_config(db, True)

    # Extract field mappings from the configuration
    is_adult = config.field_mapping.is_adult
    is_child = config.field_mapping.is_child
    is_neonate = config.field_mapping.is_neonate
    deceased_gender = config.field_mapping.deceased_gender
    location_level1 = config.field_mapping.location_level1
    location_level2 = config.field_mapping.location_level2
    death_date = config.field_mapping.death_date or 'id10023'
    submitted_date = config.field_mapping.submitted_date or 'today' or 'submissiondate'
    interview_date = config.field_mapping.interview_date or 'id10012'
    date = config.field_mapping.date
    instance_id = config.field_mapping.instance_id or 'instanceid'
    # results = [{key: result[key] for key in ['ID', 'CAUSE1', 'task_id']} for result in results]
    # Extract all data UIDs for a batch query
    data_uids = [result.get('ID') for result in results if result.get('ID') is not None]

    # Return early if there are no valid data_uids
    if not data_uids:
        return results

    # Define a batch AQL query for all data_uids at once
    collection = db.collection(db_collections.VA_TABLE)
    data_uids_str = ', '.join(f'"{uid}"' for uid in data_uids)
    query = f"""
FOR doc IN {collection.name}
    FILTER doc.{instance_id} IN [{data_uids_str}]
    LET age_group = 
        (doc.age_group=="neonate" || TO_NUMBER(doc.{is_neonate}) == 1 || ((TO_NUMBER(doc.isneonatal1) == 1 || TO_NUMBER(doc.isneonatal2) == 1))) ? "neonate" :
        (doc.age_group=="child" || TO_NUMBER(doc.{is_child}) == 1 || ((TO_NUMBER(doc.ischild1) == 1 || TO_NUMBER(doc.ischild2) == 1))) ? "child" :
        (doc.age_group=="adult" || TO_NUMBER(doc.{is_adult}) == 1 || ((TO_NUMBER(doc.isadult1) == 1 || TO_NUMBER(doc.isadult2) == 1))) ? "adult" :
        "Unknown"
    RETURN {{
        uid: doc.{instance_id},
        gender: LOWER(doc.{deceased_gender}),
        date: LOWER(doc.{date}),
        age_group: age_group,
        locationLevel1: LOWER(doc.{location_level1}),
        locationLevel2: LOWER(doc.{location_level2}),
        death_date: doc.{death_date},
        submitted_date: doc.{submitted_date},
        interview_date: doc.{interview_date},
        reasoans:doc.id10476,
        form_age_group:doc.age_group,
        isneonatal:doc.isneonatal,
        ischild:doc.ischild, 
        isadult:doc.isadult

    }}
    """
    # print(query)
    # Execute the query with caching
    cursor = db.aql.execute(query, cache=True)

    # Convert the cursor to a dictionary keyed by UID for easy lookup
    va_data_map = {doc['uid']: doc for doc in cursor}
    
    # print(va_data_map)

    # Merge the fetched data with the original results
    updated_results = []
    for result in results:
        data_uid = result.get('ID')
        if data_uid in va_data_map:
            # Create a new dictionary by merging the original and the update
            merged_result = {**result, **va_data_map[data_uid]}
            updated_results.append(merged_result)
        else:
            updated_results.append(result)  # If no update, keep the original


    return updated_results


def ensure_task(task):
    try:
        # Check if an event loop is running
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # If no running loop, create a new one and run the task
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(task)
        loop.close()
    else:
        # If a running loop is available, create a task in the existing loop
        loop.create_task(websocket_broadcast(task))