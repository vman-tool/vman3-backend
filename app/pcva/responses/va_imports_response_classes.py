from typing import Union

from app.shared.configs.models import BaseResponseModel, ResponseUser
from arango.database import StandardDatabase

from app.shared.configs.constants import db_collections
from app.shared.utils.response import populate_user_fields


class PCVAImportDetailsResponseClass(BaseResponseModel):
    uuid: str
    fileName: str
    extension: str
    numberOfRecords: int
    created_by: Union[ResponseUser, None] = None
    updated_by: Union[ResponseUser, None] = None
    created_at: str

    @classmethod
    def get_pcva_import_details(cls, db: StandardDatabase = None):
        
        query = f"""
        FOR doc IN {db_collections.IMPORTS_VA_DETAILS}
            RETURN {{
                uuid: doc.uuid,
                fileName: doc.fileName,
                extension: doc.extension,
                numberOfRecords: doc.numberOfRecords,
                created_by: doc.created_by,
                created_at: doc.created_on
            }}
        """
        cursor = db.aql.execute(query)
        import_details_data = []
        for import_detail in cursor:
            import_details_data.append(cls.populate_user_fields(db, import_detail))
        return [cls(**subject) for subject in import_details_data]
    
    @classmethod
    async def get_structured_pcva_import_detail(cls, pcva_import_detail_uuid = None, pcva_import_detail = None, db: StandardDatabase = None):
        pcva_import_detail_data = pcva_import_detail
        if not pcva_import_detail_data:
            query = f"""
            FOR doc IN {db_collections.IMPORTS_VA_DETAILS}
                FILTER doc.uuid == @pcva_import_detail_uuid
                RETURN doc
            """
            bind_vars = {'pcva_import_detail_uuid': pcva_import_detail_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            pcva_import_detail_data = cursor.next()
        populated_category_type_data = await populate_user_fields(data = pcva_import_detail_data, db = db)
        return cls(**populated_category_type_data)