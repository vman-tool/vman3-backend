import os
from pathlib import Path
import shutil
from typing import List
import uuid

from fastapi import UploadFile
from PIL import Image, UnidentifiedImageError

from app.shared.configs.constants import Special_Constants


def validate_image(
    file: UploadFile,
    max_size_bytes: int,
    min_width: int = 1,
    min_height: int = 1,
) -> None:
    """Validates an uploaded image is within the size limit and is a
    genuinely decodable image of at least the given dimensions - catches
    corrupted files and files whose extension doesn't match their actual
    content, not just files with the wrong extension (see save_file's own
    extension check for that).

    SVG is vector, not raster - Pillow can't decode it, and its dimensions
    aren't a fixed pixel grid, so it only gets a lightweight sanity check.

    Rewinds file.file back to the start before returning, so a subsequent
    save_file() call still reads the whole file.

    :raises ValueError: on any validation failure
    """
    file.file.seek(0, os.SEEK_END)
    size = file.file.tell()
    file.file.seek(0)

    if size > max_size_bytes:
        raise ValueError(
            f"File is too large ({size / 1024 / 1024:.1f} MB). "
            f"Maximum allowed is {max_size_bytes / 1024 / 1024:.1f} MB."
        )

    file_extension = (file.filename or "").split(".")[-1].lower()
    if file_extension == "svg":
        head = file.file.read(2048)
        file.file.seek(0)
        if b"<svg" not in head.lower():
            raise ValueError("File is not a valid SVG image.")
        return

    try:
        with Image.open(file.file) as image:
            image.verify()
        # verify() leaves the image unusable for further reads (and, per
        # Pillow's own docs, shouldn't be used before it) - re-open to read
        # actual dimensions.
        file.file.seek(0)
        with Image.open(file.file) as image:
            width, height = image.size
    except UnidentifiedImageError:
        raise ValueError("File is not a valid image.")
    except Exception:
        raise ValueError("File could not be read as an image - it may be corrupted.")
    finally:
        file.file.seek(0)

    if width < min_width or height < min_height:
        raise ValueError(
            f"Image is too small ({width}x{height}px). "
            f"Minimum size is {min_width}x{min_height}px."
        )


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
    
    file_path = Path(f"{os.getcwd()}/app{folder}/{filename}")


    if delete_extisting:
        delete_file(delete_extisting)
        
    
    
    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return f"{Special_Constants.FILE_URL}/{filename}"

def delete_file(path: str = None):
    try:
        if not path:
            raise ValueError("Path is required for deletion")
        file_name = path.rsplit(Special_Constants.UPLOAD_FOLDER)[1]
        existing_location = f"{os.getcwd()}/app{Special_Constants.UPLOAD_FOLDER}{file_name}" if path else None
        if path and existing_location and os.path.isfile(existing_location):
            os.remove(existing_location)
    except:
        raise FileNotFoundError(f"No file could be found in path {path}")