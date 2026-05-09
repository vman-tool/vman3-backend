
import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta
from typing import Dict, Optional

import numpy as np
import pandas as pd
from arango.database import StandardDatabase
from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
from app.ccva.utilits.interva.utils import csmf
from app.ccva.utilits.pycrossva.transform import transform

from app.ccva.models.ccva_models import InterVA5Progress
from app.ccva.utilits.interva.interva5 import InterVA5
from app.ccva.utilits.interva.interva6 import InterVA2022
from app.records.services.list_data import fetch_va_records_json
from app.settings.services.odk_configs import fetch_odk_config
from app.shared.configs.arangodb import null_convert_data, remove_null_values
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.services.task_progress_service import TaskProgressService
from app.shared.utils.async_utils import call_update_callback
from app.utilits.logger import app_logger



# The websocket_broadcast function for broadcasting progress updates
async def websocket_broadcast(task_id: str, progress_data: dict):
    from app.main import websocket__manager
    
    # Ensure progress_data is a dict (handles Pydantic models)
    if hasattr(progress_data, 'model_dump'):
        progress_data = progress_data.model_dump()
    elif hasattr(progress_data, 'dict'):
        progress_data = progress_data.dict()
        
    await websocket__manager.broadcast(task_id, json.dumps(progress_data, default=str))

async def get_record_to_run_ccva(current_user:dict,db: StandardDatabase,data_source:Optional[str], task_id: Optional[str], task_results: Dict,start_date: Optional[date] = None, end_date: Optional[date] = None,date_type:Optional[str]=None, top:Optional[int]=None):
    try:
        records= await fetch_va_records_json(current_user=current_user,paging=False,data_source=data_source,task_id=task_id,  start_date=start_date, end_date=end_date,  db=db,date_type=date_type,top=top)
        if records.data == []:
            raise Exception("No records found")
        
        return records
    except Exception as e:
        app_logger.error(f"Error fetching ODK data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
async def get_ccva_record_count(current_user:dict, db: StandardDatabase, start_date: Optional[date] = None, end_date: Optional[date] = None, date_type:Optional[str]=None, top:Optional[int]=None):
    try:
        from app.records.services.list_data import fetch_va_records_count
        count = await fetch_va_records_count(current_user=current_user, start_date=start_date, end_date=end_date, db=db, date_type=date_type, top=top)
        return count
    except Exception as e:
        app_logger.error(f"Error fetching record count: {e}")
        return 0

        
# The main run_ccva function that integrates everything
async def run_ccva(db: StandardDatabase, records:ResponseMainModel, task_id: str, task_results: Dict,start_date: Optional[date] = None, end_date: Optional[date] = None, malaria_status:Optional[str]=None, hiv_status:Optional[str]=None, ccva_algorithm:Optional[str]=None, user_id: str = "unknown", covid_status:Optional[str]=None):
    try:
                # Define the async callback to send progress updates
                # Define the async callback to send progress updates
        # Capture the main loop to ensure thread-safe callbacks
        try:
            main_loop = asyncio.get_running_loop()
        except RuntimeError:
            # Should not happen in run_ccva as it's an async endpoint
            app_logger.error("No running loop found in run_ccva!")
            return

        async def _persist_and_broadcast(progress):
            """Internal function to save to DB and broadcast"""
            # 1. Broadcast via WebSocket
            await websocket_broadcast(task_id, progress)
            
            # 2. Save to Database
            progress_dict = progress.model_dump() if hasattr(progress, 'model_dump') else progress
            if isinstance(progress_dict, str):
                 try:
                     progress_dict = json.loads(progress_dict)
                 except:
                     pass
            
            # Ensure status exists for UI updates
            if isinstance(progress_dict, dict):
                if 'status' not in progress_dict:
                    progress_dict['status'] = 'running'
                
                # Separation of concerns: Keep 'message' stable for UI status, move details to 'log'
                # If InterVA5 sends a "log" field, it's likely a detail. 
                # If 'message' looks like an error/warning (which InterVA5 does), revert 'message' to generic status.
                if 'log' in progress_dict and progress_dict['log']:
                    # Check if message is essentially the log (InterVA5 behavior)
                    msg = progress_dict.get('message', '')
                    if "Error" in msg or "WARNING" in msg or "Not Specified" in msg:
                        progress_dict['message'] = "Running InterVA5 analysis..." 
                
                progress_dict['user_id'] = user_id
                
            # Use TaskProgressService (async wrapper)
            await TaskProgressService.save_progress(db, task_id, progress_dict)

        async def update_callback(progress):
            """
            Thread-safe callback wrapper.
            If called from a thread (different loop), schedules _persist_and_broadcast on the main loop.
            """
            try:
                curr_loop = asyncio.get_running_loop()
            except RuntimeError:
                curr_loop = None

            if curr_loop == main_loop:
                 await _persist_and_broadcast(progress)
            else:
                 # We are in a thread/different context, update on main loop
                 future = asyncio.run_coroutine_threadsafe(_persist_and_broadcast(progress), main_loop)
                 return future.result()

        # Initial update for task start
        start_time = datetime.now()

        initial_progress = InterVA5Progress(
            progress=1,
            total_records= len(records.data),
            message="Collecting data.",
            status="running",
            elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
            task_id=task_id,
            error=False
        )
        
        await update_callback(initial_progress.model_dump_json())


        # Convert records to DataFrame directly - Run in thread to prevent blocking
        database_dataframe = await asyncio.to_thread(lambda: pd.DataFrame.from_records(records.data))

        

        

        

       
        # Fetch the  configuration
        config = await fetch_odk_config(db, True)
        id_col = config.field_mapping.instance_id
        date_col = config.field_mapping.date
        # Run the CCVA process in a thread pool, with real-time updates
        algo_label = ccva_algorithm or "InterVA5"
        await update_callback(InterVA5Progress(
        progress=4,
        message=f"Running {algo_label} analysis...",
        status="running",
        total_records=len(database_dataframe),
        elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
        task_id=task_id,
        error=False
    ).model_dump_json())
        if(user_id is None):
        # throw error
            raise ValueError("User ID is required,")


        await asyncio.to_thread(
            runCCVA, odk_raw=database_dataframe, file_id=task_id, update_callback=update_callback,db= db, id_col=id_col,date_col=date_col,start_time=start_time, algorithm= ccva_algorithm,   malaria= malaria_status, hiv= hiv_status, user_id=user_id, covid=covid_status,

        )
        

    except Exception as e:
        print(e)
        error_message = {"progress": 0, "message": str(e), "status":'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": task_id, "error": True}
        call_update_callback(update_callback, error_message)
        task_results[task_id] = error_message
        
        


def runCCVA(odk_raw:pd.DataFrame, id_col: str = None,date_col:str =None,start_time:timedelta=None, instrument: str = '2016WHOv151', algorithm: str = 'InterVA5',
            top=10, undetermined: bool = True, malaria: str = "h", hiv: str = "h", covid: str = "v",
            file_id: str = "unnamed_file", update_callback=None, db: StandardDatabase=None, user_id: str = None):

    try:
        # Define the output folder
        output_folder = "ccva_files/"
        os.makedirs(output_folder, exist_ok=True)

        algo = algorithm or "InterVA5"
        algo_label = algo

        if algo == "InterVA6":
            # ── InterVA6 path ──────────────────────────────────────────────
            call_update_callback(update_callback, InterVA5Progress(
                progress=7,
                message="Running InterVA6 (InterVA2022) analysis...",
                status="running",
                total_records=len(odk_raw),
                elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
                task_id=file_id,
                error=False
            ).model_dump_json())

            # Transform raw data to InterVA5 format first, then remap columns
            # to InterVA2022 format (270 shared indicators, fill 72 V6-only with 0)
            if id_col:
                input_data = transform((instrument, 'InterVA5'), odk_raw, raw_data_id=id_col, lower=True)
            else:
                input_data = transform((instrument, 'InterVA5'), odk_raw, lower=True)

            input_data = _remap_interva5_to_interva2022(input_data)

            interva6 = InterVA2022()
            results = interva6.analyze(
                input_data=input_data,
                hiv=hiv or "h",
                malaria=malaria or "h",
                covid=covid or "v",
                write=True,
                directory=output_folder,
                filename=file_id,
                output="classic",
                update_callback=update_callback,
                task_id=file_id,
            )

            # Read results from the CSV that InterVA6 wrote
            csv_path = os.path.join(output_folder, f"{file_id}.csv")
            if os.path.exists(csv_path):
                rcd = pd.read_csv(csv_path).to_dict(orient='records')
            else:
                # Fallback: build rcd from in-memory results
                rcd = _adapt_interva6_results(results)

            # Build adapter object so compile_ccva_results / csmf() works unchanged
            iv_adapter = _build_interva6_adapter(results, input_data)

            # Error log file name differs for InterVA6
            error_log_filename = "errorlog2022.txt"

        else:
            # ── InterVA5 path (existing, unchanged) ───────────────────────
            # Transform the input data
            if id_col:
                input_data = transform((instrument, algorithm), odk_raw, raw_data_id=id_col, lower=True)
            else:
                input_data = transform((instrument, algorithm), odk_raw, lower=True)

            # Create an InterVA5 instance with the async callback
            iv5out = InterVA5(input_data,task_id=file_id, hiv=hiv, malaria=malaria, write=True, directory=output_folder, filename=file_id,start_time=start_time, update_callback=update_callback, return_checked_data=True)

            call_update_callback(update_callback, InterVA5Progress(
                progress=7,
                message="Running InterVA5 analysis...",
                status="running",
                total_records=len(input_data),
                elapsed_time=f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}",
                task_id=file_id,
                error=False
            ).model_dump_json())

            # Run the InterVA5 analysis, with progress updates via the async callback
            iv5out.run()
            records = iv5out.get_indiv_prob(
                top=10,
                include_propensities=False
            )
            rcd = records.to_dict(orient='records')
            # get from csv(official)
            try:
                csv_path = f"{output_folder}{file_id}.csv"
                if os.path.exists(csv_path):
                    rcd = pd.read_csv(csv_path).to_dict(orient='records')
            except Exception as e:
                app_logger.error(f"Error reading CCVA binary CSV output: {e}")
                rcd = []

            iv_adapter = iv5out
            error_log_filename = f"{file_id}_errorlogV5.txt"

        # ── Common post-processing (both algorithms) ──────────────────
        if rcd == [] or rcd is None:
            call_update_callback(update_callback, {"progress": 0, "message": "No records found", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True})
            raise Exception("No records found")

        # Iterate over each dictionary and add the 'task_id' field
        for record in rcd:
            record["task_id"] = file_id

        # get the ccva form data and merge with the results
        results_to_insert = asyncio.run(getVADataAndMergeWithResults(db, null_convert_data(rcd)))
        if results_to_insert is None:
            call_update_callback(update_callback, {"progress": 0, "message": "No records found", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True})
            return
        db.collection(db_collections.CCVA_RESULTS).insert_many(results_to_insert, overwrite=True, overwrite_mode="update")

        total_records = len(rcd)

        # Normalize date column to avoid str/float comparison errors when deriving ranges
        if date_col and date_col in odk_raw.columns:
            date_series = pd.to_datetime(odk_raw[date_col], errors="coerce")
            valid_dates = date_series.dropna()
            if not valid_dates.empty:
                latest_date = valid_dates.max().to_pydatetime().isoformat()
                earliest_date = valid_dates.min().to_pydatetime().isoformat()
            else:
                latest_date = earliest_date = None
        else:
            latest_date = earliest_date = None
        rangeDates = {"start": latest_date, "end": earliest_date}

        ## get ccva error logs
        error_log_path = os.path.join(output_folder, error_log_filename)
        if algo == "InterVA6":
            error_logs = process_ccva_errorlogs_v6(error_log_path, task_id=file_id)
        else:
            error_logs = process_ccva_errorlogs(output_folder + file_id + "_", task_id=file_id)

        ccva_results = compile_ccva_results(iv_adapter,
                                           data_processed_with_results=len(results_to_insert),
                                           error_logs=error_logs,
                                           top=top,
                                           undetermined=undetermined,
                                           task_id=file_id,
                                           start_time=start_time,
                                           total_records=total_records,
                                           rangeDates=rangeDates,
                                           db=db,
                                           user_id=user_id)

        # Cleanup temp files
        if os.path.exists(error_log_path):
            os.remove(error_log_path)
        csv_cleanup = os.path.join(output_folder, f"{file_id}.csv")
        if os.path.exists(csv_cleanup):
            os.remove(csv_cleanup)
        return ccva_results

    except Exception as e:
        app_logger.error(f"Error during CCVA analysis: {e}")
        call_update_callback(update_callback, {"progress": 0, "message": f"Error during CCVA analysis: {e}", "status": 'error',"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": file_id, "error": True})
        raise e


def _remap_interva5_to_interva2022(input_data: pd.DataFrame) -> pd.DataFrame:
    """
    Remap pycrossva InterVA5 output (354 columns) to InterVA2022 input format (343 columns).
    270 indicators are shared; 72 V6-only indicators are filled with 0; 83 V5-only are dropped.
    """
    probbase_path = os.path.join(
        os.path.dirname(__file__), "..", "utilits", "interva", "data", "probbase2022.csv"
    )
    probbase_path = os.path.normpath(probbase_path)
    pb = pd.read_csv(probbase_path, nrows=0)

    # First column of probbase rows = indicator names. Read them from the full file.
    pb_full = pd.read_csv(probbase_path, usecols=[0])
    v6_indicators = pb_full.iloc[:, 0].dropna().tolist()
    # First row is the prior row (blank indicator), skip it
    v6_indicators = [ind for ind in v6_indicators if ind and str(ind).strip()]

    # The ID column is always first in both formats
    id_col_name = input_data.columns[0]
    v5_cols = set(input_data.columns)

    cols = {id_col_name: input_data[id_col_name].values}
    for ind in v6_indicators:
        if ind in v5_cols:
            cols[ind] = input_data[ind].values
        else:
            cols[ind] = np.zeros(len(input_data), dtype=int)
    remapped = pd.DataFrame(cols)

    app_logger.info(
        f"Remapped InterVA5 ({input_data.shape[1]} cols) -> InterVA2022 ({remapped.shape[1]} cols)"
    )
    return remapped


def _adapt_interva6_results(results: dict) -> list:
    """Convert InterVA6 in-memory results to the same dict format as InterVA5 CSV output."""
    rcd = []
    for va in results.get("VA2022", []):
        rcd.append({
            "ID": va["ID"],
            "MALPREV": va.get("MALPREV", ""),
            "HIVPREV": va.get("HIVPREV", ""),
            "COVIDPREV": va.get("COVIDPREV", ""),
            "PREGSTAT": va.get("PREGSTAT", ""),
            "PREGLIK": va.get("PREGLIK", ""),
            "CAUSE1": va.get("CAUSE1", " "),
            "LIK1": va.get("LIK1", " "),
            "CAUSE2": va.get("CAUSE2", " "),
            "LIK2": va.get("LIK2", " "),
            "CAUSE3": va.get("CAUSE3", " "),
            "LIK3": va.get("LIK3", " "),
            "INDET": va.get("INDET", 100),
        })
    return rcd


def _build_interva6_adapter(results: dict, input_data: pd.DataFrame):
    """
    Build an adapter object that wraps InterVA6 results so that the existing
    csmf() and compile_ccva_results() functions work unchanged.

    csmf() expects:
      - adapter.results["VA5"]: DataFrame with 15 columns incl. WHOLEPROB (Series)
      - adapter.dem_group: DataFrame indexed by ID with 'age' and 'sex' columns
    """
    from app.ccva.utilits.interva.utils import _get_dem_groups

    va2022_list = results.get("VA2022", [])
    if not va2022_list:
        # Return a minimal adapter with empty results
        class _EmptyAdapter:
            def __init__(self):
                self.results = {"VA5": pd.DataFrame()}
                self.dem_group = pd.DataFrame(columns=["age", "sex"])
        return _EmptyAdapter()

    # Load causetext to build proper Series index for WHOLEPROB
    causetext_path = os.path.join(os.path.dirname(__file__), "..", "utilits", "interva", "data", "causetext2022.csv")
    causetext_path = os.path.normpath(causetext_path)
    try:
        causetext_df = pd.read_csv(causetext_path)
        if 'description' in causetext_df.columns:
            cause_names = causetext_df['description'].tolist()
        elif causetext_df.shape[1] > 1:
            cause_names = causetext_df.iloc[:, 1].tolist()
        else:
            cause_names = [f"Cause_{i+1}" for i in range(67)]
    except Exception:
        cause_names = [f"Cause_{i+1}" for i in range(67)]

    # Pad cause_names to match wholeprob length if needed
    # InterVA6 wholeprob has entries for all 67 causes (3 pregnancy + 64 causes)
    # while InterVA5 has 70 (67 + 6 extra). We pad with zeros for compatibility.
    cause_index = pd.Index(cause_names[:67])

    rows = []
    for va in va2022_list:
        wholeprob_raw = va.get("wholeprob", [])
        # Build a Series matching InterVA5 format (70 entries, pad extras with 0)
        probs = list(wholeprob_raw[:67])
        # Zero out pregnancy probabilities (indices 0-2) so they don't distort the
        # CSMF calculation. InterVA6 normalizes pregnancy and cause probabilities
        # separately; including both would double-count.
        for idx in range(min(3, len(probs))):
            probs[idx] = 0.0
        while len(probs) < 70:
            probs.append(0.0)
        # Use cause_names padded to 70 for the index
        padded_names = list(cause_names[:67])
        while len(padded_names) < 70:
            padded_names.append(f"Extra_{len(padded_names)}")
        wholeprob_series = pd.Series(probs, index=pd.Index(padded_names))

        rows.append({
            "ID": va["ID"],
            "MALPREV": va.get("MALPREV", ""),
            "HIVPREV": va.get("HIVPREV", ""),
            "PREGSTAT": va.get("PREGSTAT", ""),
            "PREGLIK": va.get("PREGLIK", ""),
            "CAUSE1": va.get("CAUSE1", " "),
            "LIK1": va.get("LIK1", " "),
            "CAUSE2": va.get("CAUSE2", " "),
            "LIK2": va.get("LIK2", " "),
            "CAUSE3": va.get("CAUSE3", " "),
            "LIK3": va.get("LIK3", " "),
            "INDET": va.get("INDET", 100),
            "COMCAT": " ",
            "COMNUM": " ",
            "WHOLEPROB": wholeprob_series,
        })

    va5_df = pd.DataFrame(rows)

    # Build dem_group from the input_data.
    # InterVA2022 uses i019b (female) and i019c (intersex) instead of i019a (male)/i019b (female).
    # We infer male when i019b=0 and i019c=0.
    yes_vals = {1, "y", "Y", "yes", "Yes", "YES"}
    dem_list = []
    for _, row in input_data.iterrows():
        record_id = row.iloc[0] if len(row) > 0 else "unknown"
        # Sex
        is_female = row.get("i019b") in yes_vals if "i019b" in row.index else False
        is_intersex = row.get("i019c") in yes_vals if "i019c" in row.index else False
        if is_female:
            sex = "female"
        elif is_intersex:
            sex = "unknown"
        else:
            sex = "male"
        # Age
        age = "unknown"
        age_map = {"i022a": "adult", "i022b": "adult", "i022c": "adult",
                   "i022d": "child", "i022e": "child", "i022f": "child",
                   "i022g": "neonate"}
        for col, grp in age_map.items():
            if col in row.index and row.get(col) in yes_vals:
                age = grp
                break
        dem_list.append({"ID": record_id, "age": age, "sex": sex})

    # Only keep dem entries whose ID is in the results
    result_ids = set(va["ID"] for va in va2022_list)
    dem_list = [d for d in dem_list if d["ID"] in result_ids]

    dem_group = pd.DataFrame(dem_list)
    if not dem_group.empty:
        dem_group = dem_group.set_index("ID")

    class InterVA6Adapter:
        """Wraps InterVA6 results to be compatible with csmf() and compile_ccva_results()."""
        def __init__(self, va5_df, dem_group):
            self.results = {"VA5": va5_df}
            self.dem_group = dem_group

    return InterVA6Adapter(va5_df, dem_group)

        

# Function to compile the results from InterVA5
def compile_ccva_results(iv5out, top=10, undetermined=True,start_time:timedelta=None,
                         task_id:str=None,
                         data_processed_with_results:int=0,
                         total_records:int=0, rangeDates: Dict={},
                         error_logs: Optional[any]=None,
                         db: StandardDatabase=None,
                         user_id: str = "unknown"):
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
        "user_id": user_id,
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
    
    call_update_callback(lambda p: websocket_broadcast(task_id, p), {"progress": 100, "message": "Finish CCVA analysis...", "status": 'completed', "data": ccva_results ,"elapsed_time": f"{(datetime.now() - start_time).seconds // 3600}:{(datetime.now() - start_time).seconds // 60 % 60}:{(datetime.now() - start_time).seconds % 60}", "task_id": task_id, "error": False})

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


def process_ccva_errorlogs_v6(log_file_path: str, task_id: str = None):
    """Process InterVA6 (InterVA2022) error logs which have a simpler format."""
    log_entries = []

    if not os.path.exists(log_file_path):
        return log_entries

    error_pattern = r'([\w:/-]+)\s+Error in (indicators|sex indicator|age indicator):\s+(.+)'
    discrepancy_pattern = r'([\w:/-]+)\s+(.+)'

    current_group = None

    with open(log_file_path, 'r') as file:
        logs = file.readlines()

    for log in logs:
        log = log.strip()

        if "incomplete and excluded from further processing" in log:
            current_group = "incomplete_records"
            continue

        if "data discrepancies were identified and handled" in log:
            current_group = "data_discrepancies"
            continue

        if not log or log.startswith("Error & warning log built"):
            continue

        if current_group == "incomplete_records":
            match = re.search(error_pattern, log)
        elif current_group == "data_discrepancies":
            match = re.search(discrepancy_pattern, log)
        else:
            continue

        if match:
            uuid = match.group(1)
            if current_group == "incomplete_records":
                error_type = f"Error in {match.group(2)}"
                error_message = match.group(3)
            else:
                error_type = "data discrepancy"
                error_message = match.group(2)

            log_entry = {
                "uuid": uuid,
                "task_id": task_id,
                "error_type": error_type,
                "error_message": error_message,
                "group": current_group
            }
            log_entries.append(log_entry)

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
        def execute_query():
            cursor = db.aql.execute(query, cache=True)
            return cursor.next()

        # Retrieve the result (first result since RETURN only outputs one document)
        result = await run_in_threadpool(execute_query)

        return result

    except Exception as e:
        app_logger.error(f"Error fetching CCVA results and error logs: {e}")
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
    # Execute the query with caching
    def execute_va_data_query():
        cursor = db.aql.execute(query, cache=True)
        return {doc['uid']: doc for doc in cursor}

    # Convert the cursor to a dictionary keyed by UID for easy lookup
    va_data_map = await run_in_threadpool(execute_va_data_query)
    
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



from app.ccva.services.ccva_upload import insert_all_csv_data
from app.ccva.services.ccva_services import get_record_to_run_ccva
import io

async def process_upload_and_run_ccva(
    file_contents: bytes,
    unique_id: str,
    current_user: dict,
    start_date: Optional[date],
    end_date: Optional[date],
    top: Optional[int],
    date_type: Optional[str],
    malaria_status: str,
    hiv_status: str,
    ccva_algorithm: str,
    task_id: str,
    task_results: dict,
    db: StandardDatabase,
    covid_status: Optional[str] = None
):
    """
    Background task to process CSV upload and then run CCVA.
    Emits socket progress updates starting from the upload phase.
    """
    user_id = current_user.get('uid') or current_user.get('id') or "unknown" if isinstance(current_user, dict) else "unknown"
    
    # Helper for broadcasting progress
    async def broadcast_progress(progress_val, message, status="running", error=False, data=None, log=None):
        start_time_ref = datetime.now() # Approximate, or pass start_time if needed
        # In a real scenario, we might want to track absolute elapsed time from when the request started.
        # For now, we use a simple elapsed time calculation or just reuse the logic from run_ccva style if needed.
        # But run_ccva does its own timing. For the upload phase, we can just say "0:00:xx".
        
        progress_data = {
            "progress": progress_val,
            "message": message,
            "status": status,
            "error": error,
            "task_id": task_id,
            "user_id": user_id,
            "log": log  # Include log in progress data
        }
        if data:
            progress_data.update(data)
            
        await websocket_broadcast(task_id, progress_data)
        # We also save to DB for persistence if needed, but for "Reading CSV" usually socket is enough.
        # If we want full persistence, we use TaskProgressService.
        await TaskProgressService.save_progress(db, task_id, progress_data)

    try:
        # Phase 1: Reading CSV
        await broadcast_progress(1, "Reading CSV file...", status="running")
        
        df = pd.read_csv(io.StringIO(file_contents.decode('utf-8')), low_memory=False)
        
        # Validations
        if 'instanceID' in df.columns:
            df['instanceid'] = df['instanceID']
            df.drop(columns=['instanceID'], inplace=True)
            
        if 'instanceid' not in df.columns:
            raise Exception("Instance ID (instanceid) not found in the uploaded CSV")
            
        if unique_id not in df.columns:
             raise Exception("Unique ID not found in the uploaded CSV")

        # Phase 2: Processing Data Frame
        await broadcast_progress(5, "Processing CSV data...", status="running")
        
        df['vman_data_source'] = 'uploaded_csv'
        df['vman_data_name'] = 't'
        df['__id'] = df[unique_id]
        df['version_number'] = '1.0'
        df['trackid'] = task_id

        recordsDF = df.to_dict(orient='records')
        if top:
            recordsDF = recordsDF[:top]
            
        total_csv_records = len(recordsDF)

        if total_csv_records == 0:
             raise Exception("Uploaded CSV is empty")

        # Phase 3: Inserting Data
        await broadcast_progress(5, f"Inserting {total_csv_records} records...", status="running")
        # await insert_all_csv_data(recordsDF)

        # Phase 4: Fetching for CCVA
        await broadcast_progress(10, "Preparing data for analysis...", status="running")
        # records = await get_record_to_run_ccva(current_user, db, 'uploaded_csv', task_id, task_results, start_date, end_date, date_type=date_type)
        records = ResponseMainModel(data=recordsDF, message="Data prepared from CSV")
        if not records.data:
            raise Exception("No records found after insertion")

        # Phase 5: Run CCVA
        # run_ccva will take over progress updates (starting typically at progress=1 or similar)
        # We can pass an initial state if run_ccva supports it, effectively it starts its own progress flow.
        await run_ccva(
            db,
            records,
            task_id,
            task_results,
            start_date,
            end_date,
            malaria_status,
            hiv_status,
            ccva_algorithm,
            user_id,
            covid_status
        )

    except Exception as e:
        print(f"Error in process_upload_and_run_ccva: {e}")
        await broadcast_progress(0, str(e), status="error", error=True)