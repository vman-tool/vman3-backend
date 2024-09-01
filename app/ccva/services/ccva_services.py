
import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd
from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool
from interva.utils import csmf
from pycrossva.transform import transform

from app.ccva.utilits.interva.interva5 import InterVA5
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.constants import db_collections
from app.shared.services.va_records import shared_fetch_va_records


# The websocket_broadcast function for broadcasting progress updates
async def websocket_broadcast(task_id: str, progress_data: dict):
    from app.main import (
        websocket__manager,  # Ensure this points to your actual WebSocket manager instance
    )
    await websocket__manager.broadcast(task_id, json.dumps(progress_data))

        
# The main run_ccva function that integrates everything
async def run_ccva(db: StandardDatabase, task_id: str, task_results: Dict):
    try:
        # Fetch records from the database asynchronously
        records = await shared_fetch_va_records(paging=False, include_assignment=False, format_records=False, db=db)
        database_dataframe = pd.read_json(json.dumps(records.data))
        database_dataframe.to_csv("data.csv")
        
        # Define the async callback to send progress updates
        async def update_callback(progress):
            await websocket_broadcast(task_id, progress)
        
        # Initial update for task start
        initial_message = {"progress": 0, "message": "Starting CCVA analysis...", "status":'init', "task_id": task_id, "error": False}
        await update_callback(initial_message)
        task_results[task_id] = initial_message
        # Fetch the  configuration
        config = await fetch_odk_config(db)
        id_col = config.field_mapping.instance_id
        # Run the CCVA process in a thread pool, with real-time updates
        results_data = await run_in_threadpool(
            runCCVA, odk_raw=database_dataframe, file_id=task_id, update_callback=update_callback,db= db, id_col=id_col
        )
        
        # Final update after completion
        final_message = {"progress": 100, "message": "Finish CCVA analysis...", "status":'completed', "task_id": task_id, "error": False}
        results_data.update(final_message)
        task_results[task_id] = results_data
        
        # await update_callback({"progress": 100, "message": "Finish CCVA analysis...", "status": 'completed', "data": task_results[task_id], "task_id": task_id, "error": False})

    except Exception as e:
        error_message = {"progress": 0, "message": str(e), "status":'error', "task_id": task_id, "error": True}
        await update_callback(error_message)
        task_results[task_id] = error_message
        
        
def runCCVA(odk_raw, id_col: str = None, instrument: str = '2016WHOv151', algorithm: str = 'InterVA5',
            top=10, undetermined: bool = True, malaria: str = "h", hiv: str = "h",
            file_id: str = "unnamed_file", update_callback=None, db: StandardDatabase=None):
    
    try:
        start_time = datetime.now()
        # Transform the input data
        if id_col:
            input_data = transform((instrument, algorithm), odk_raw, raw_data_id=id_col, lower=True)
        else:
            input_data = transform((instrument, algorithm), odk_raw, lower=True)
        
        # Define the output folder
        output_folder = "../ccva_files/"
        
        # Create an InterVA5 instance with the async callback
        iv5out = InterVA5(input_data, hiv=hiv, malaria=malaria, write=True, directory=output_folder, filename=file_id, update_callback=update_callback, return_checked_data=True)
        print("Running InterVA5 analysis...")
        
        # Run the InterVA5 analysis, with progress updates via the async callback
        iv5out.run()
        elapsed_time = datetime.now() - start_time
        print(elapsed_time)
        # print(iv5out.checked_data)
        
        # Assuming the output is written to a CSV
        iv5out.write_indiv_prob(filename=f"{file_id}-dt")
        
        # Read the CSV file into a list of dictionaries
        df = pd.read_csv(f"{file_id}-dt.csv")
        records = df.to_dict(orient='records')

        # db.collection(db_collections.INTERVA5).insert_many(records, overwrite=True)

       
        # get date of first and last record

        rangeDates={"start": df[id_col].max(), "end": df[id_col].min()}
        os.remove(f"{file_id}-dt.csv")
        total_records = len(records)
        print( total_records)
        ccva_results= compile_ccva_results(iv5out, top=top, undetermined=undetermined, task_id=file_id,elapsed_time=elapsed_time,total_records=total_records,  rangeDates =rangeDates, db=db)
        return ccva_results

    except Exception as e:
        asyncio.run(update_callback({"progress": 0, "message": f"Error during CCVA analysis: {e}", "status": 'error', "task_id": file_id, "error": True}))
        
        print(f"Error during CCVA analysis: {e}")

# Function to compile the results from InterVA5
def compile_ccva_results(iv5out, top=10, undetermined=True,elapsed_time:timedelta=None,
                         task_id:str=None,
                         total_records:int=0, rangeDates: Dict={}, db: StandardDatabase=None):
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

    merged_results = {
        "index": merged_df.index.tolist(),
        "values": merged_arr
    }

    # Combine all results into a single dictionary
    ccva_results = {
        "created_at": datetime.now().isoformat(),
        "total_records": total_records,
        "Elapsed":   f"{elapsed_time.seconds // 3600}:{(elapsed_time.seconds // 60) % 60}:{elapsed_time.seconds % 60}",

        "range":rangeDates,
        "all": all_results,
        "male": male_results,
        "female": female_results,
        "adult": adult_results,
        "child": child_results,
        "neonate": neonate_results,
        "merged": merged_results
    }
    # print(ccva_results)

    db.collection(db_collections.CCVA_RESULTS).insert(ccva_results)
    asyncio.run(  websocket_broadcast(task_id,{"progress": 100, "message": "Finish CCVA analysis...", "status": 'completed', "data": ccva_results , "task_id": task_id, "error": False}))

    return ccva_results