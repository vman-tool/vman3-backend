import json
from typing import Dict
from arango.database import StandardDatabase
from interva.interva5 import InterVA5
from pycrossva.transform import transform
from interva.utils import csmf
import os
import pandas as pd
import numpy as np
from app.shared.services.va_records import shared_fetch_va_records

async def run_ccva(db: StandardDatabase, task_id: str, task_results: Dict = {}):
    
    records = await shared_fetch_va_records(paging=False, include_assignment=False, format_records=False, db=db)
    # task_results[task_id] = records
    database_dataframe = pd.read_json(json.dumps(records.data))
    
    task_results[task_id] = runCCVA(odk_raw = database_dataframe, file_id = task_id)
    # return records
    # runCCVA()



def runCCVA(odk_raw, id_col: str = None, instrument: str = '2016WHOv151', algorithm: str = 'InterVA5', top=10, undetermined: bool = True, malaria: str = "h", hiv: str = "h", file_id:str = "unnamed_file"):
    if id_col:
        input_data = transform((instrument, algorithm), odk_raw, raw_data_id=id_col)
    else:
        input_data = transform((instrument, algorithm), odk_raw)
    #input_data = transform(("2016WHOv151", "InterVA5"), odk_raw, raw_data_id="vaid"p)
    output_folder = "../ccva_files/"
    
    iv5out = InterVA5(input_data, hiv=hiv, malaria=malaria, write=True, directory=output_folder, filename=file_id)

    iv5out.run()

    ## This is the direct method of getting total csmf all cause all groups
    # all = {
    #     "index":iv5out.get_csmf(top=top).index.tolist(),
    #     "values":iv5out.get_csmf(top=top).tolist()
    # }

    # for the below options, use the following combinations
    # age = 'adult, child, neonate'
    # sex = 'male,female'


    all = {
        "index":csmf(iv5out, top=top, age=None, sex=None).index.tolist(),
        "values":csmf(iv5out, top=top, age=None, sex=None).tolist()
    }
    if not undetermined:
        top +=1
        index = np.array(csmf(iv5out, top=top, age=None, sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age=None, sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        
        if len(idx) != 0:
            index = np.delete(index,idx)
            values = np.delete(values,idx)
            
            all = {
                "index":index.tolist(),
                "values":values.tolist()
            }
    
    male = {
        "index":csmf(iv5out, top=top, age=None, sex='male').index.tolist(),
        "values":csmf(iv5out, top=top, age=None, sex='male').tolist()
    }
    if not undetermined:
        top +=1
        index = np.array(csmf(iv5out, top=top, age=None, sex='male').index.tolist())
        values = np.array(csmf(iv5out, top=top, age=None, sex='male').tolist())
        idx = np.argwhere(index == "Undetermined")
        
        
        if len(idx) != 0:
            index = np.delete(index,idx)
            values = np.delete(values,idx)
            
            male = {
                "index":index.tolist(),
                "values":values.tolist()
            }
    
    female = {
        "index":csmf(iv5out, top=top, age=None, sex='female').index.tolist(),
        "values":csmf(iv5out, top=top, age=None, sex='female').tolist()
    }
    if not undetermined:
        top +=1
        index = np.array(csmf(iv5out, top=top, age=None, sex='female').index.tolist())
        values = np.array(csmf(iv5out, top=top, age=None, sex='female').tolist())
        idx = np.argwhere(index == "Undetermined")
        
        if len(idx) != 0:
            index = np.delete(index,idx)
            values = np.delete(values,idx)
            
            female = {
                "index":index.tolist(),
                "values":values.tolist()
            }
    
    adult = {
        "index":csmf(iv5out, top=top, age='adult', sex=None).index.tolist(),
        "values":csmf(iv5out, top=top, age='adult', sex=None).tolist()
    }
    if not undetermined:
        top +=1
        index = np.array(csmf(iv5out, top=top, age='adult', sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age='adult', sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        
        if len(idx) != 0:
            index = np.delete(index,idx)
            values = np.delete(values,idx)
            
            adult = {
                "index":index.tolist(),
                "values":values.tolist()
            }
    
    child = {
        "index":csmf(iv5out, top=top, age='child', sex=None).index.tolist(),
        "values":csmf(iv5out, top=top, age='child', sex=None).tolist()
    }

    if not undetermined:
        top +=1
        index = np.array(csmf(iv5out, top=top, age='child', sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age='child', sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        
        if len(idx) != 0:
            index = np.delete(index,idx)
            values = np.delete(values,idx)
            
            child = {
                "index":index.tolist(),
                "values":values.tolist()
            }


    neonate = {
        "index":csmf(iv5out, top=top, age='neonate', sex=None).index.tolist(),
        "values":csmf(iv5out, top=top, age='neonate', sex=None).tolist()
    }
    if not undetermined:
        top +=1
        index = np.array(csmf(iv5out, top=top, age='neonate', sex=None).index.tolist())
        values = np.array(csmf(iv5out, top=top, age='neonate', sex=None).tolist())
        idx = np.argwhere(index == "Undetermined")
        
        if len(idx) != 0:
            index = np.delete(index,idx)
            values = np.delete(values,idx)
            
            neonate = {
                "index":index.tolist(),
                "values":values.tolist()
            }

    merged_df = pd.concat([csmf(iv5out, top=top, age=None, sex=None), 
                           csmf(iv5out, top=top, age=None, sex='male'),
                           csmf(iv5out, top=top, age=None, sex='female'),
                           csmf(iv5out, top=top, age='adult', sex=None),
                           csmf(iv5out, top=top, age='child', sex=None),
                           csmf(iv5out, top=top, age='neonate', sex=None)], axis=1)
    
    merged_df.columns = ['all','male','female','adult','child','neonate']
    merged_df.fillna(0, inplace=True)

    merged_arr = []
    for col in merged_df.columns:
        merged_arr.append(merged_df[col].tolist())

    merged = {
        "index": merged_df.index.tolist(),
        "values": merged_arr
    }

    ccva_results = {
        "all":all,
        "male":male,
        "female":female,
        "adult":adult,
        "child":child,
        "neonate":neonate,
        "merged":merged
    }

    return ccva_results
