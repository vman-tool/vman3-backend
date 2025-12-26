from typing import Dict, List
from arango import ArangoError
from arango.database import StandardDatabase
from fastapi import HTTPException

from app.pcva.models.pcva_models import ICD10, ICD10Category, ICD10CategoryType
from app.pcva.responses.icd10_response_classes import ICD10CategoryResponseClass, ICD10CategoryTypeResponseClass, ICD10ResponseClass
from app.shared.configs.constants import db_collections
from app.shared.configs.models import Pager, ResponseMainModel
from app.users.models.user import User
from app.shared.utils.database_utilities import replace_object_values
from app.pcva.requests.icd10_request_classes import ICD10CategoryRequestClass

async def create_icd10_category_types_service(category_types, user, db: StandardDatabase = None):
    try:
        created_category_types = []
        for category_type in category_types:
            category_type = category_type.model_dump()
            category_type['created_by']=user.get('uuid', "")
            saved_category_type = await ICD10CategoryType(**category_type).save(db)
            created_category_type = await ICD10CategoryTypeResponseClass.get_structured_category_type(icd10_category_type = saved_category_type, db = db)
            created_category_types.append(created_category_type)
        return ResponseMainModel(data=created_category_types, message="Categories types created successfully", total=len(created_category_types))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")

async def get_icd10_category_types_service(paging: bool = True,  page_number: int = 1, filters: Dict= {}, limit: int = 10, include_deleted: bool = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        categoryTypesData = await ICD10CategoryType.get_many(
            paging = paging, 
            page_number = page_number, 
            limit = limit, 
            filters=filters,
            include_deleted = include_deleted,
            db = db
        )
        data = [await ICD10CategoryTypeResponseClass.get_structured_category_type(icd10_category_type = icd10_category_type, db = db) for icd10_category_type in categoryTypesData]
        count_data = await ICD10CategoryType.count(filters=filters, include_deleted=include_deleted, db=db)
        return ResponseMainModel(data=data, total=count_data, message="ICD10 Categories Types fetched successfully", pager=Pager(page=page_number, limit=limit))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to get codes: {e}")


async def update_icd10_category_types_service(category_types, user, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        updated_category_types = []
        for category_type in category_types:
            category_json = category_type.model_dump()
            saved_category_types = ICD10CategoryType(**category_json).update(user['uuid'], db)
            created_category_types = await ICD10CategoryTypeResponseClass.get_structured_category_type(icd10_category_type = saved_category_types, db = db)
            updated_category_types.append(created_category_types)
        return ResponseMainModel(data=updated_category_types, message="Category types updated successfully", total=len(updated_category_types))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update categories: {e}")


async def create_icd10_categories_service(categories: List[ICD10CategoryRequestClass], user, db: StandardDatabase = None):
    try:
        created_categories = []
        for category in categories:
            category = category.model_dump()
            category['created_by']=user.get('uuid', "")
            saved_category = await ICD10Category(**category).save(db)
            created_category = await ICD10CategoryResponseClass.get_structured_category(icd10_category = saved_category, db = db)
            created_categories.append(created_category)
        return ResponseMainModel(data=created_categories, message="Categories created successfully", total=len(created_categories))
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create categories: {e}")

async def get_icd10_categories_service(paging: bool = True,  page_number: int = 1, filters: Dict= {}, limit: int = 10, include_deleted: bool = None, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        categoriesData = await ICD10Category.get_many(
            paging = paging, 
            page_number = page_number, 
            limit = limit, 
            filters=filters,
            include_deleted = include_deleted,
            db = db
        )
        data = [await ICD10CategoryResponseClass.get_structured_category(icd10_category = icd10_category, db = db) for icd10_category in categoriesData]
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

async def includeTypeFilter(type: str = None, filters: Dict = {}, db: StandardDatabase = None) -> Dict:
    if type and type != "":
        extracted_types = await ICD10CategoryType.get_many(filters={"uuid": type}, include_deleted=False, db = db)
        if len(extracted_types) > 0:
            type_obj = extracted_types[0]
            categories_of_type = await ICD10Category.get_many(filters={"type": type_obj.get("uuid", "")}, include_deleted=False, db = db)
            selected_category = None
            if "category" in filters:
                selected_category = filters.pop("category")
            category_uuids = [category.get("uuid", "") for category in categories_of_type]
            if selected_category not in category_uuids and selected_category is not None:
                category_uuids.append(selected_category)
            if "in_conditions" in filters:
                filters["in_conditions"].extend([{"category": category_uuids}])
            else:
                filters["in_conditions"] = [{"category": category_uuids}]
    return filters


async def get_icd10_codes(paging: bool = True,  page_number: int = 1, limit: int = 10, filters: Dict= None, include_deleted: bool = None, type: str = None, db: StandardDatabase = None) -> List[ICD10ResponseClass]:
    try:
        if filters is None:
            filters = {}
        filters = await includeTypeFilter(type=type, filters=filters, db=db)

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
        return ResponseMainModel(data=data, total=count_data, message="ICD10 fetched successfully", pager=Pager(page=page_number, limit=limit) if paging else None)
    except ArangoError as e:
        raise HTTPException(status_code=500, detail=f"Failed to get codes: {e}")
    
async def create_or_icd10_categories_from_file(categories, user, db: StandardDatabase):
    try:
        created_categories = []
        for category in categories:
            if "type" in category:
                filters = {"or_conditions": [{"name": category.get("type", "")}, {"uuid": category.get("type", "")}]}
                existing_category_type = await ICD10CategoryType.get_many(filters=filters, include_deleted=False, db = db)
                if len(existing_category_type) > 0:
                        category_type = existing_category_type[0]
                else:
                    to_save_category_type  = {
                        "name": category.get("type", ""),
                        "created_by": user['uuid'],
                    }
                    category_type = await ICD10CategoryType(**to_save_category_type).save(db = db)

                category["type"] = category_type.get("uuid", "")

                existing_category = await ICD10Category.get_many(include_deleted=False, filters={"name": category.get("name", "")}, db = db)
                if len(existing_category) > 0:
                    existing_category = existing_category[0]
                    category = replace_object_values(category, existing_category)
                    saved_category = await ICD10Category(**category).update(updated_by=user['uuid'], db = db)
                    created_category = await ICD10CategoryResponseClass.get_structured_category(icd10_category = saved_category, db = db)
                else:
                    category['created_by']=user['uuid']
                    saved_category = await ICD10Category(**category).save(db)
                    created_category = await ICD10CategoryResponseClass.get_structured_category(icd10_category = saved_category, db = db)
                created_categories.append(created_category)
            else:
                raise HTTPException(status_code=400, detail="Missing 'type' column in your uploaded file")
        return ResponseMainModel(data=created_categories, message="Data imported successfully", total=len(created_categories))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to import file: {e}")

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
            category_type = None
            print("Type of nan: ", type(code.get("type", "")), " for", code.get("type", ""))
            if "type" in code and code.get("type", "") != None and code.get("type", "") != "":
                filters = {"or_conditions": [{"name": code.get("type", "")}, {"uuid": code.get("type", "")}]}
                print("Ite went through Types: ", filters)
                existing_category_type = await ICD10CategoryType.get_many(filters=filters, include_deleted=False, db = db)
                if len(existing_category_type) > 0:
                        category_type = existing_category_type[0]
                else:
                    to_save_category_type  = {
                        "name": code.get("type", ""),
                        "created_by": user['uuid'],
                    }
                    category_type = await ICD10CategoryType(**to_save_category_type).save(db = db)
            
            if "category" in code and code.get("category", "") != None and code.get("category", "") != "":
                print("Ite went through Categories")
                filters = {"or_conditions": [{"name": code.get("category", "")}, {"uuid": code.get("category", "")}]}
                existing_category = await ICD10Category.get_many(filters=filters, include_deleted=False, db = db)
                if len(existing_category) > 0:
                        category = existing_category[0]
                else:
                    to_save_category  = {
                        "name": code.get("category", ""),
                        "created_by": user['uuid'],
                        "type": category_type.get("uuid", "") if category_type and "uuid" in category_type else None,
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