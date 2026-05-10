from io import BytesIO
import json
import uuid as uuid_lib

import pandas as pd
from arango.database import StandardDatabase
from fastapi import HTTPException, UploadFile

from app.pcva.models.pcva_models import PCVAImportsDetails
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel, VManBaseModel
from app.odk.services.data_download import insert_many_data_to_arangodb
from app.users.models.user import User
from app.pcva.responses.va_imports_response_classes import PCVAImportDetailsResponseClass


async def import_va_records_from_file(
    file: UploadFile,
    current_user: User,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    try:
        file_extension = file.filename.split(".")[-1].lower()

        content = await file.read()
        file_content = BytesIO(content)

        if file_extension == "csv":
            df = pd.read_csv(file_content)
        elif file_extension in ["xlsx", "xls"]:
            df = pd.read_excel(file_content)
        elif file_extension == "json":
            data = json.loads(content)
            df = pd.json_normalize(data)
        else:
            raise HTTPException(status_code=400, detail="Invalid file type. Expected csv, xlsx, xls, or json")

        number_of_records = len(df)

        import_detail = PCVAImportsDetails(
            fileName=file.filename,
            extension=file_extension,
            numberOfRecords=0,
            created_by=current_user.get("uuid") if current_user else None,
        )

        saved_import_detail = await import_detail.save(db=db)
        import_detail_uuid = saved_import_detail.get("uuid")

        df.columns = df.columns.str.lower()
        df = df.dropna(axis=1, how='all')
        df = df.loc[:, ~df.columns.duplicated()]
        
        df["detail_uuid"] = import_detail_uuid
        
        if 'instanceid' in df.columns:
            df['__id'] = df['instanceid']
        else:
            df['__id'] = [f"uuid:{uuid_lib.uuid4()}" for _ in range(len(df))]

        records = json.loads(df.to_json(orient='records'))

        if records:
            print(records)
            await insert_many_data_to_arangodb(records, overwrite_mode='replace', collection_name=db_collections.IMPORTS_VA_TABLE)
            
        saved_import_detail["numberOfRecords"] = number_of_records
        
        await PCVAImportsDetails(**saved_import_detail).update(current_user.get("uuid") if current_user else None, db=db)

        return ResponseMainModel(
            data=await PCVAImportDetailsResponseClass.get_structured_pcva_import_detail(pcva_import_detail=saved_import_detail, db=db),
            message=f"File imported successfully with {number_of_records} records",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import file: {e}")


async def get_import_details(
    paging: bool = True,
    page_number: int = 1,
    limit: int = 10,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    try:
        import_details = await PCVAImportsDetails.get_many(
            paging=paging,
            page_number=page_number,
            limit=limit,
            include_deleted=False,
            db=db,
        )
        total = await PCVAImportsDetails.count(include_deleted=False, db=db)
        return ResponseMainModel(
            data=[await PCVAImportDetailsResponseClass.get_structured_pcva_import_detail(import_detail) for import_detail in import_details],
            total=total,
            message="Import details fetched successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch import details: {e}")


async def get_imports_va_by_import_detail(
    import_detail_uuid: str,
    paging: bool = True,
    page_number: int = 1,
    limit: int = 10,
    db: StandardDatabase = None,
) -> ResponseMainModel:
    try:
        paginator = ""
        bind_vars = {"import_detail_uuid": import_detail_uuid}

        if paging:
            paginator = "LIMIT @offset, @limit"
            offset = (page_number - 1) * limit
            bind_vars.update({"offset": offset, "limit": limit})

        query = f"""
            FOR doc IN {db_collections.IMPORTS_VA_TABLE}
            FILTER doc.detail_uuid == @import_detail_uuid
            {paginator}
            RETURN doc
        """

        va_records = await VManBaseModel.run_custom_query(query=query, bind_vars=bind_vars, db=db)
        records = [record for record in va_records]

        count_query = f"""
            FOR doc IN {db_collections.IMPORTS_VA_TABLE}
            FILTER doc.detail_uuid == @import_detail_uuid
            COLLECT WITH COUNT INTO length
            RETURN length
        """
        count_cursor = await VManBaseModel.run_custom_query(query=count_query, bind_vars={"import_detail_uuid": import_detail_uuid}, db=db)
        total = count_cursor.next() if count_cursor else len(records)

        return ResponseMainModel(
            data=records,
            total=total,
            message="VA records fetched successfully",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch VA records: {e}")
