import os
import shutil
from typing import List
import uuid

from fastapi import UploadFile
from app.shared.configs.constants import Special_Constants


def save_file(file: UploadFile, valid_file_extensions: List[str] = None, delete_extisting: str = None, reconstruct_filename: bool = True):
    """
        This function saves file in the folder named in special constants within the project working directory:

        :params file: file to save
        :params valid_file_extensions: list of valid file extensions (default: all)
        :params delete_extisting: path to the existing file to be deleted (default: False)
        :params reconstruct_filename: reconstruct filename to use uuid version 4 (default: True)

        RETURN
        path to the file
    
    """
    file_extension = file.filename.split('.')[-1]
    if valid_file_extensions and file_extension not in valid_file_extensions:
        raise ValueError(f"Invalid file. Expected one of: {', '.join(valid_file_extensions)}")
    
    folder = Special_Constants.UPLOAD_FOLDER if Special_Constants.UPLOAD_FOLDER.startswith("/") else f"/{Special_Constants.UPLOAD_FOLDER}"
    
    filename =  f"{str(uuid.uuid4())}.{file_extension}" if reconstruct_filename else file.filename
        
    file_location = os.path.join(f"{os.getcwd()}/app{folder}", filename)

    existing_location = f"{os.getcwd()}/app{delete_extisting}" if delete_extisting else None

    if delete_extisting and existing_location and os.path.isfile(existing_location):
        os.remove(existing_location)
    
    with open(file_location, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return f"{folder}/{filename}"

def delete_file(path: str = None):
    try:
        existing_location = f"{os.getcwd()}/app{path}" if path else None
        if path and existing_location and os.path.isfile(existing_location):
            os.remove(existing_location)
    except:
        raise FileNotFoundError(f"No file could be found in path {path}")