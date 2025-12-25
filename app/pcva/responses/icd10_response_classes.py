from typing import Optional, Union

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.shared.configs.constants import db_collections
from app.shared.configs.models import BaseResponseModel, ResponseUser
from app.shared.utils.response import populate_user_fields


class ICD10CategoryTypeFieldClass(BaseModel):
    uuid: str
    name: str

    @classmethod
    def get_icd10_category_type(cls, category_type_uuid, db: StandardDatabase = None):
        if not category_type_uuid:
            return None
        
        query = f"""
            FOR category_type IN {db_collections.ICD10_CATEGORY_TYPE}
                FILTER category_type.uuid == @category_type_uuid
                RETURN {{
                    uuid: category_type.uuid,
                    name: category_type.name,
                }}
        """
        bind_vars = {'category_type_uuid': category_type_uuid}
        cursor = db.aql.execute(query, bind_vars=bind_vars,cache=True)
        category_data = cursor.next()
        return cls(**category_data)

class ICD10CategoryTypeResponseClass(BaseResponseModel):
    uuid: str
    name: str
    created_by: Union[ResponseUser, None] = None
    updated_by: Union[ResponseUser, None] = None

    @classmethod
    def get_icd10_category_types(cls, db: StandardDatabase = None):
        
        query = f"""
        FOR category IN {db_collections.ICD10_CATEGORY_TYPE}
            RETURN {{
                uuid: category.uuid,
                name: category.name,
                created_by: category.created_by,
                created_at: category.created_on
            }}
        """
        cursor = db.aql.execute(query)
        category_types_data = []
        for category_type in cursor:
            category_types_data.append(cls.populate_user_fields(db, category_type))
        return [cls(**subject) for subject in category_types_data]
    
    @classmethod
    async def get_structured_category_type(cls, icd10_category_type_uuid = None, icd10_category_type = None, db: StandardDatabase = None):
        category_type_data = icd10_category_type
        if not category_type_data:
            query = f"""
            FOR category_type IN {db_collections.ICD10_CATEGORY_TYPE}
                FILTER category_type.uuid == @icd10_category_type_uuid
                RETURN category_type
            """
            bind_vars = {'icd10_category_type_uuid': icd10_category_type_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            category_type_data = cursor.next()
            category_type_data["type"] = ICD10CategoryTypeFieldClass.get_icd10_category_type(category_type_data["type"], db)
        populated_category_type_data = await populate_user_fields(data = category_type_data, db = db)
        return cls(**populated_category_type_data)


class ICD10CategoryFieldClass(BaseModel):
    uuid: str
    name: str
    type: Union[ICD10CategoryTypeFieldClass, None] = None

    @classmethod
    def get_icd10_category(cls, category_uuid, include_type: bool = False, db: StandardDatabase = None):
        
        query = f"""
        FOR category IN {db_collections.ICD10_CATEGORY}
            FILTER category.uuid == @category_uuid
            RETURN {{
                uuid: category.uuid,
                name: category.name,
                type: category.type
            }}
        """
        bind_vars = {'category_uuid': category_uuid}
        cursor = db.aql.execute(query, bind_vars=bind_vars,cache=True)
        category_data = cursor.next()
        if include_type:
            category_data["type"] = ICD10CategoryTypeFieldClass.get_icd10_category_type(category_data.get("type", ""), db)
        else:
            category_data["type"] = None
        return cls(**category_data)

class ICD10CategoryResponseClass(BaseResponseModel):
    uuid: str
    name: str
    created_by: Union[ResponseUser, None] | None
    updated_by: Union[ResponseUser, None] | None
    type: Union[ICD10CategoryTypeFieldClass, None] = None

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
        categories_data = []
        for category in cursor:
            categories_data.append(cls.populate_user_fields(db, category))
            categories_data["type"] = ICD10CategoryTypeFieldClass.get_icd10_category_type(category["type"], db)
        return [cls(**subject) for subject in categories_data]
    
    @classmethod
    async def get_structured_category(cls, icd10_category_uuid = None, icd10_category = None, db: StandardDatabase = None):
        category_data = icd10_category
        if not category_data:
            query = f"""
            FOR category IN {db_collections.ICD10_CATEGORY}
                FILTER category.uuid == @icd10_category_uuid
                RETURN category
            """
            bind_vars = {'icd10_category_uuid': icd10_category_uuid}
            def execute_category_query():
                cursor = db.aql.execute(query, bind_vars=bind_vars)
                return cursor.next()

            category_data = await run_in_threadpool(execute_category_query)
        populated_category_data = await populate_user_fields(data = category_data, db = db)
        return cls(**populated_category_data)

class ICD10ResponseClass(BaseResponseModel):
    code: str
    name: str
    created_at: str
    updated_at: Optional[str]
    category: Optional[ICD10CategoryFieldClass] = None

    @classmethod
    def get_icd10_codes(cls, db: StandardDatabase = None):
        
        query = f"""
        FOR code IN {db_collections.ICD10}
            RETURN {{
                uuid: code.uuid,
                name: code.name,
                created_at: code.created_at,
                created_by: code.created_by,
                category: code.category
            }}
        """
        cursor = db.aql.execute(query)
        codes_data = []
        for code in cursor:
            code['category'] = ICD10CategoryFieldClass.get_icd10_category(category_uuid=code['category'], include_type=True, db=db)
            codes_data.append[cls.populate_user_fields(db, code)]
        return [cls(**code) for code in codes_data]
    
    @classmethod
    async def get_structured_code(cls, icd10_code_uuid = None, icd10_code = None, db: StandardDatabase = None):
        code_data = icd10_code
        if not code_data:
            query = f"""
            FOR code IN {db_collections.ICD10}
                FILTER code.uuid == @icd10_code_uuid
                RETURN code
            """
            bind_vars = {'icd10_code_uuid': icd10_code_uuid}
            
            def execute_code_query():
                cursor = db.aql.execute(query, bind_vars=bind_vars)
                return cursor.next()

            code_data = await run_in_threadpool(execute_code_query)
        populated_code_data = await populate_user_fields(code_data, db=db)
        populated_code_data['category'] = await run_in_threadpool(
            ICD10CategoryFieldClass.get_icd10_category, populated_code_data['category'], True, db
        )
        return cls(**populated_code_data)