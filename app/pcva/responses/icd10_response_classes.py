from typing import Dict, Optional
from fastapi import HTTPException
from pydantic import BaseModel
from arango.database import StandardDatabase

from app.shared.configs.models import BaseResponseModel
from app.shared.utils.response import populate_user_fields
from app.shared.configs.constants import db_collections
from app.users.models.user import User


class ICD10CategoryFieldClass(BaseModel):
    uuid: str
    name: str

    @classmethod
    def get_icd10_category(cls, category_uuid, db: StandardDatabase = None):
        
        query = f"""
        FOR category IN {db_collections.ICD10_CATEGORY}
            FILTER category.uuid == @category_uuid
            RETURN {{
                uuid: category.uuid,
                name: category.name,
            }}
        """
        bind_vars = {'category_uuid': category_uuid}
        cursor = db.aql.execute(query, bind_vars=bind_vars)
        category_data = cursor.next()
        return cls(**category_data)

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

class ICD10ResponseClass(BaseResponseModel):
    name: str
    created_at: str
    updated_at: Optional[str]
    category: Optional[ICD10CategoryFieldClass] = None

    @classmethod
    def get_icd10_codes(cls, db: StandardDatabase = None):
        
        query = f"""
        FOR code IN {db_collections.ICD10}
            RETURN {{
                uuid: category.uuid,
                name: category.name,
                created_at: category.created_at,
                created_by: category.created_by,
                category: category.category
            }}
        """
        cursor = db.aql.execute(query)
        codes_data = []
        for code in cursor:
            code['category'] = ICD10CategoryFieldClass.get_icd10_category(code['category'], db)
            codes_data.append[cls.populate_user_fields(db, code)]
        return [cls(**code) for code in codes_data]
    
    @classmethod
    def get_structured_code(cls, icd10_code_uuid = None, icd10_code = None, db: StandardDatabase = None):
        code_data = icd10_code
        if not code_data:
            query = f"""
            FOR code IN {db_collections.ICD10}
                FILTER code.uuid == @icd10_code_uuid
                RETURN code
            """
            bind_vars = {'icd10_code_uuid': icd10_code_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            code_data = cursor.next()
        populated_code_data = populate_user_fields(code_data, db)
        populated_code_data['category'] = ICD10CategoryFieldClass.get_icd10_category(populated_code_data['category'], db)
        return cls(**populated_code_data)