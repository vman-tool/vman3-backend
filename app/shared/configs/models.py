import uuid
from datetime import datetime
from fastapi import HTTPException
from typing import Any, Dict, List, Optional, TypeVar, Union

from arango.database import StandardDatabase
from pydantic import BaseModel, Field

from app.shared.configs.constants import db_collections
from app.shared.utils.database_utilities import add_query_filters, replace_object_values


class Pager(BaseModel):
    page: Union[int, None] = None
    limit: Union[int, None] = None

class ResponseMainModel(BaseModel):
    data: Any = None
    message: str
    error: Optional[Any] = None
    total: Optional[int] = None
    pager: Optional[Pager]= None
    
    
class VManBaseModel(BaseModel):
    """
    This class abstracts the CRUD operations for the Vman Models that are used to interract with the database.
    """

    uuid: Optional[str] = Field(default=None, unique=True)
    created_by: Union[str, int, None] = None
    created_at: Union[str, datetime, None] = None
    updated_by: Union[str, int, None] = None
    updated_at: Union[str, datetime, None] = None
    deleted_by: Union[str, int, None] = None
    deleted_at: Union[str, datetime, None] = None
    is_deleted: bool = False

    @classmethod
    def get_collection_name(cls) -> str:
        """
        Implement this method in order to be able to specify the collection name for Arango DB
        """
        raise NotImplementedError("Derived classes must define the collection name.")



    @classmethod
    def init_collection(cls, db):
        if not db.has_collection(cls.get_collection_name()):
            collection = db.create_collection(cls.get_collection_name())
            collection.add_hash_index(fields=['uuid'], unique=True)
        else:
            collection = db.collection(cls.get_collection_name())
            indexes = collection.indexes()
            if not any(index['fields'] == ['uuid'] for index in indexes):
                collection.add_hash_index(fields=['uuid'], unique=True)

    async def save(self, db: StandardDatabase, unique_field: str = None):
        """
        This method saved the data and returns the new saved data.

        :param db: Standard database object
        :param unique_field: for insert or update, specify a field to be used to check if record exists for it to update or not to insert new

        :return Newly saved data
        """
        self.init_collection(db)
        collection = db.collection(self.get_collection_name())
        self.uuid = str(uuid.uuid4())
        self.created_at = datetime.now().isoformat()
        doc = self.model_dump()

        existing_doc = await self.get_many(filters = {unique_field: doc.get(unique_field)}, db=db) if unique_field else None

        if existing_doc:
            doc["_key"] = existing_doc[0]["_key"]
            new_doc = collection.update(doc, return_new=True)
        else:
            new_doc = collection.insert(doc, return_new=True)
        return new_doc['new'] if 'new' in new_doc else new_doc

    @classmethod
    async def get(cls, doc_id: str = None, doc_uuid: str = None, db: StandardDatabase = None):
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        if doc_id:
            doc = collection.get(doc_id)
        if doc_uuid:
            query = f"""
                LET doc = FIRST(FOR doc IN {collection.name} FILTER doc.uuid == @uuid RETURN doc)
                RETURN doc
            """
            bind_vars = {'uuid': doc_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            doc = cursor.next()
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.__name__} not found")
        return doc
   
    @classmethod
    async def get_many(cls, paging: bool = None, page_number: int = None, limit: int = None, filters: Dict[str, Any] = {}, include_deleted: bool = None, db: StandardDatabase = None):
        """
        Fetch records from the specified collection using dynamic filters.

        :param db: The ArangoDB database instance.
        :param collection_name: The name of the collection to query.
        :param filters: A dictionary of filters to apply to the query.
            - include array of object inside or_conditions key in filters object to incorporate OR filter
            - include array of object inside in_conditions key in filters object to incorporate IN filter
            - include field with object that key is in ['==', '!=', '<', '<=', '>', '>=', "$gt", "$lt", "$lte", "$eq", "$ne", "$gte"] for comparison fields
        :return: A list of records matching the filters.
        """
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        query, bind_vars = cls.build_query(
            collection_name = collection.name, 
            filters = filters,
            paging = paging, 
            page_number = page_number, 
            limit = limit,
            include_deleted = include_deleted
        )
        cursor = db.aql.execute(query, bind_vars=bind_vars)
        records = list(cursor)
        if not records:
            return []
        return records
    
    @classmethod
    async def count(cls, filters: Dict[str, Any] = {}, include_deleted: bool = None, db: StandardDatabase = None):
        """
        Counts the number of records from the specified collection using dynamic filters.

        :param db: The ArangoDB database instance.
        :param collection_name: The name of the collection to query.
        :param filters: A dictionary of filters to apply to the query.
            - include array of object inside or_conditions key in filters object to incorporate OR filter
            - include array of object inside in_conditions key in filters object to incorporate IN filter
            - include field with object that key is in ['==', '!=', '<', '<=', '>', '>=', "$gt", "$lt", "$lte", "$eq", "$ne", "$gte"] for comparison fields
        :return: A list of records matching the filters.
        """
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        bind_vars = {}
        aql_filters = []

        query = f"""
            RETURN LENGTH(FOR doc in {collection.name}
        """


        aql_filters, vars = add_query_filters(filters, bind_vars)

        bind_vars.update(vars)
        
        if not include_deleted:
            aql_filters.append("doc.is_deleted == false")
        
        if aql_filters:
            query += " FILTER " + " AND ".join(aql_filters)

        query += " RETURN 1)"
        
        cursor = db.aql.execute(query, bind_vars=bind_vars)

        return cursor.next()

    async def update(self, updated_by: str = None, db: StandardDatabase = None):
        """
        Update the record with the provided updated_by.

        :param updated_by: The user who made the update.
        :param db: The ArangoDB database instance.
        """
        self.init_collection(db)
        collection = db.collection(self.get_collection_name())
        self.updated_by = updated_by
        self.updated_at = datetime.now().isoformat()
        doc = self.model_dump()

        if ("id" in doc and not doc['id']) or ("_key" in doc and not doc['_key']) or ("id" not in doc and "_key" not in doc):
            query = f"""
                LET doc = FIRST(FOR doc IN {self.get_collection_name()} FILTER doc.uuid == @uuid RETURN doc)
                RETURN doc
            """
            bind_vars = {'uuid': self.uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            original_doc = cursor.next()
        else:
            key = doc["_key"] if "_key" in doc else doc["id"]
            original_doc = collection.get({'_key': key})
        
        original_doc = replace_object_values(doc, original_doc)
        
        return collection.update(original_doc, return_new=True)["new"]

    @classmethod
    async def delete(cls, doc_id: str = None, doc_uuid: str = None, deleted_by: str = None, hard_delete: bool = False, db: StandardDatabase = None):
        """
        Delete the record with the provided id or uuid and user uuid.

        :param doc_id: The ID of the record to delete.
        :param doc_uuid: The UUID of the record to delete.
        :param deleted_by: The user UUID deleting the record.
        """
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())

        if doc_id:
            doc = collection.get(doc_id)
        if doc_uuid:
            query = f"""
                LET doc = FIRST(FOR doc IN {collection.name} FILTER doc.uuid == @uuid RETURN doc)
                RETURN doc
            """
            bind_vars = {'uuid': doc_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            doc = cursor.next()
        if hard_delete:
            return collection.delete(doc, silent=True)
        doc["is_deleted"] = True
        doc["deleted_by"] = deleted_by
        doc["deleted_at"] = datetime.now().isoformat()
        
        return collection.update(doc, silent=True)

    @classmethod
    async def restore(cls, doc_id: str = None, doc_uuid: str = None, restored_by: str= None, db: StandardDatabase = None):
        """
         Restore the record with the provided id or uuid and user uuid.

        :param doc_id: The ID of the deleted record to restore.
        :param doc_uuid: The UUID of the deleted record to restore.
        :param restored_by: The user UUID restoring the record.
        
        """
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        if doc_id:
            doc = collection.get(doc_id)
        if doc_uuid:
            query = f"""
                LET doc = FIRST(FOR doc IN {collection.name} FILTER doc.uuid == @uuid RETURN doc)
                RETURN doc
            """
            bind_vars = {'uuid': doc_uuid}
            cursor = db.aql.execute(query, bind_vars=bind_vars)
            doc = cursor.next()
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.get_collection_name()} not found")
        doc['is_deleted'] = False
        doc['updated_by'] = restored_by
        doc['updated_at'] = datetime.now().isoformat()
        doc["deleted_by"] = None
        doc["deleted_at"] = None
        
        return collection.update(doc, return_new=True)["new"]
    
    @classmethod
    def build_query(cls, collection_name: str, filters: Dict[str, Any] = {}, paging: bool = None, page_number: Optional[int] = None, limit: Optional[int] = None, include_deleted: bool = None):
        """
         Build a query from filters object and pagination values.

        :param collection_name: The ID of the deleted record to restore.
        :param paging: Boolean value to allow paging.
        :param page_number: Integer value for page number.
        :param limit: Integer value for number of records to be returned in a page.
        :param include_deleted: Boolean value to include or exclude soft deleted objects.
        :param filters: Dictionary to build conditional query. Include or_conditions objects with key, value pairs. (keys being field names).
        
        :return query: A string of the built query
        :return bind_vars: A dictionary of bind variables for the query.
        """
        query = f"FOR doc IN {collection_name}"
    
        bind_vars = {}
        aql_filters = []

        aql_filters, vars = add_query_filters(filters, bind_vars)

        bind_vars.update(vars)
        
        if not include_deleted:
            aql_filters.append("doc.is_deleted == false")
        
        if aql_filters:
            query += " FILTER " + " AND ".join(aql_filters)
        
        if paging and page_number is not None and limit is not None:
            offset = (page_number - 1) * limit
            query += " LIMIT @offset, @size"
            bind_vars.update({
                "offset": offset,
                "size": limit
            })
        
        if limit is not None and not paging:
            offset = limit
            query += " LIMIT @offset"
            bind_vars.update({
                "offset": offset
            }) 

        
        query += " RETURN doc"
        
        return query, bind_vars
    
    @classmethod
    async def run_custom_query(cls, query: str = None, bind_vars: Dict = None,count: bool = True, db: StandardDatabase = None):
        """
            This function will run a custom query and return the results.

            :params query: Arango custom query
            :params bind_vars: Bind variables for the query
            :params db: ArangoDB database instance

            RETURN
            Database return value
        """
        try:
            cursor = db.aql.execute(query=query, bind_vars=bind_vars, count=count)
            return cursor
        except Exception as e:
            raise e

class ResponseUser(BaseModel):
    uuid: str
    name: str

class BaseResponseModel(BaseModel):
    """
    Use this class to standardize the response of your API endpoints.
    """
    uuid: Union[str, None] = None
    created_by: Union[ResponseUser, None] = None
    updated_by: Union[ResponseUser, None] = None
    deleted_by: Union[ResponseUser, None] = None

    @classmethod
    def get_user(cls, user_uuid: str, db: StandardDatabase) -> ResponseUser:
        query = rf"""
        FOR user IN {db_collections.USERS}
            FILTER user.uuid == @user_uuid
            RETURN {{
                "uuid": user.uuid,
                "name": user.name
            }}
        """
        bind_vars = {'user_uuid': user_uuid}
        cursor = db.aql.execute(query, bind_vars=bind_vars)
        user_data = [user for user in cursor]
        if len(user_data) == 0:
            return ResponseUser(**{"uuid": "", "name": ""})
        user_data = user_data[0]
        return ResponseUser(**user_data)

    @classmethod
    def populate_user_fields(cls, data: Dict = {}, specific_fields: List[str] = None, db: StandardDatabase = None) -> Dict:
        """
            Use this method to populate user fields as a standard response.
        """
        data['created_by'] = cls.get_user(data['created_by'], db) if 'created_by' in data and data['created_by'] else None
        
        data['updated_by'] = cls.get_user(data['updated_by'], db) if 'updated_by' in data and data['updated_by'] else None
        
        data['deleted_by'] = cls.get_user(data['deleted_by'], db) if 'deleted_by' in data and data['deleted_by'] else None


        for field in specific_fields:
            if field in data and data[field]:
                data[field] = cls.get_user(data[field], db)
        return data