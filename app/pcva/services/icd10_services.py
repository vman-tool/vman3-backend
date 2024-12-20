from typing import Dict, List
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.pcva_models import ICD10, ICD10Category
from app.pcva.responses.icd10_response_classes import ICD10CategoryResponseClass, ICD10ResponseClass
from app.shared.configs.constants import db_collections
from app.shared.configs.models import Pager, ResponseMainModel
from app.users.models.user import User
from app.shared.utils.database_utilities import replace_object_values

async def create_icd10_categories_service(categories, user, db: StandardDatabase = None):
    try:
        created_categories = []
        for category in categories:
            category = category.model_dump()
            category['created_by']=user['uuid']
            saved_category = await ICD10Category(**category).save(db)
            created_category = ICD10CategoryResponseClass.get_structured_category(icd10_category = saved_category, db = db)
            created_categories.append(created_category)
        return ResponseMainModel(data=created_categories, message="Categories created successfully", total=len(created_categories))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")

async def get_icd10_categories_service(paging: bool = True,  page_number: int = 1, filters: Dict= {}, limit: int = 10, include_deleted: bool = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        data = [
            await ICD10CategoryResponseClass.get_structured_category(icd10_category = icd10_category, db = db) 
            for icd10_category in await ICD10Category.get_many(
                paging = paging, 
                page_number = page_number, 
                limit = limit, 
                filters=filters,
                include_deleted = include_deleted,
                db = db
            )]
        count_data = await ICD10Category.count(filters=filters, include_deleted=include_deleted, db=db)
        return ResponseMainModel(data=data, total=count_data, message="ICD10 Categories fetched successfully", pager=Pager(page=page_number, limit=limit))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to get codes: {e}")


async def update_icd10_categories_service(categories, user, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        updated_categories = []
        for category in categories:
            category_json = category.model_dump()
            saved_category = ICD10Category(**category_json).update(user['uuid'], db)
            created_category = await ICD10CategoryResponseClass.get_structured_category(icd10_category = saved_category, db = db)
            updated_categories.append(created_category)
        return ResponseMainModel(data=updated_categories, message="Categories updated successfully", total=len(updated_categories))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update categories: {e}")

async def get_icd10_codes(paging: bool = True,  page_number: int = 1, limit: int = 10, filters: Dict= None, include_deleted: bool = None, db: StandardDatabase = None) -> List[ICD10ResponseClass]:
    try:
        data = [
            await ICD10ResponseClass.get_structured_code(icd10_code = icd10_code, db = db) 
            for icd10_code in await ICD10.get_many(
                paging = paging, 
                page_number = page_number, 
                limit = limit, 
                filters = filters,
                include_deleted = include_deleted,
                db = db
            )]
        count_data = await ICD10.count(filters=filters, include_deleted=include_deleted, db=db)
        return ResponseMainModel(data=data, total=count_data, message="ICD10 fetched successfully", pager=Pager(page=page_number, limit=limit))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to get codes: {e}")

async def create_icd10_codes(codes, user, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        created_codes = []
        for code in codes:
            code = code.model_dump()
            code['created_by']=user['uuid']
            code.pop("uuid") if 'uuid' in code else code
            categories = await ICD10Category.get_many(
                filters={
                    "or_conditions": [
                        {"name": code.get("category", "")}, 
                        {"uuid": code.get("category", "")}
                    ]
                }, db=db)
            if(len(categories) > 0):
                code["category"] = categories[0]['uuid']
            else:
                raise HTTPException(status_code=400, detail="Category not found.")
            saved_code = await ICD10(**code).save(db)
            created_code = await ICD10ResponseClass.get_structured_code(icd10_code = saved_code, db = db)
            created_codes.append(created_code)
        return ResponseMainModel(data=created_codes, message="ICD10 Codes created successfully", total=len(created_codes))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create icd10 codes: {e}")

async def update_icd10_codes(codes, user, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        updated_codes = []
        for code in codes:
            code_json = code.model_dump()
            saved_code = ICD10(**code_json).update(user['uuid'], db)
            created_code = await ICD10ResponseClass.get_structured_code(icd10_code = saved_code, db = db)
            updated_codes.append(created_code)
        return ResponseMainModel(data=updated_codes, message="ICD10 Codes updated successfully", total=len(updated_codes))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update icd10 codes: {e}")
    
async def create_or_icd10_codes_from_file(codes, user, db: StandardDatabase):
    try:
        created_codes = []
        for code in codes:
            if "category" in code:
                filters = {"or_conditions": [{"name": code.get("category", "")}, {"uuid": code.get("category", "")}]}
                existing_category = await ICD10Category.get_many(filters=filters, include_deleted=False, db = db)
                if len(existing_category) > 0:
                        category = existing_category[0]
                else:
                    to_save_category  = {
                        "name": code.get("category", ""),
                        "created_by": user['uuid'],
                    }
                    category = await ICD10Category(**to_save_category).save(db = db)
                
                code["category"] = category.get("uuid", "")

                existing_code = await ICD10.get_many(include_deleted=False, filters={"code": code.get("code", "")}, db = db)
                if len(existing_code) > 0:
                    existing_code = existing_code[0]
                    code = replace_object_values(code, existing_code)
                    saved_code = await ICD10(**code).update(updated_by=user['uuid'], db = db)
                    created_code = await ICD10ResponseClass.get_structured_code(icd10_code = saved_code, db = db)
                else:
                    code['created_by']=user['uuid']
                    saved_code = await ICD10(**code).save(db)
                    created_code = await ICD10ResponseClass.get_structured_code(icd10_code = saved_code, db = db)
                created_codes.append(created_code)
            else:
                raise HTTPException(status_code=400, detail="Missing 'category' field in the uploaded codes")
        return ResponseMainModel(data=created_codes, message="Data imported successfully", total=len(created_codes))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import file: {e}")