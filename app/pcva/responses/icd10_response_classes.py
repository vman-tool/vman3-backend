from typing import Dict, Optional
from fastapi import HTTPException
from pydantic import BaseModel
from arango.database import StandardDatabase

from app.shared.configs.models import BaseResponseModel
from app.shared.utils.response import populate_user_fields
from app.shared.configs.constants import db_collections
from app.users.models.user import User

class ICD10CategoryResponseClass(BaseResponseModel):
    name: str
    created_at: str
    updated_at: str

    @classmethod
    def get_icd10_categories(cls, db: StandardDatabase = None):
        
        query = f"""
        FOR category IN {db_collections.ICD10_CATEGORY}
            RETURN {{
                uuid: category.uuid,
                name: category.name,
                created_by: category.created_by,
                created_at: category.created_on
            }}
        """
        cursor = db.aql.execute(query)
        categories_data = [cls.populate_user_fields(db, category) for category in cursor]
        return [cls(**subject) for subject in categories_data]
    
    @classmethod
    def get_structured_category(cls, icd10_category_uuid = None, icd10_category = None, db: StandardDatabase = None):
        category_data = icd10_category
        if not category_data:
            query = f"""
            FOR category IN {db_collections.ICD10_CATEGORY}
                FILTER category.uuid == @icd10_category_uuid
                RETURN category
            """
            bind_vars = {'icd10_category_uuid': icd10_category_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            category_data = cursor.next()

        populated_category_data = populate_user_fields(category_data, db)
        return cls(**populated_category_data)