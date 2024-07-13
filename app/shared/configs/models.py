from datetime import datetime
from http.client import HTTPException
from typing import Optional
import uuid
from pydantic import BaseModel, Field
from arango.database import StandardDatabase

class VmanBaseModel(BaseModel):
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True)
    create_by: str
    created_on: datetime = Field(default_factory=datetime.now)
    updated_by: str
    updated_on: datetime = Field(default_factory=datetime.now)
    deleted_by: Optional[str] = None
    deleted_on: Optional[datetime] = None
    is_deleted: bool = False

    collection_name: str

    @classmethod
    def init_collection(cls, db):
        if not db.has_collection(cls.collection_name):
            collection = db.create_collection(cls.collection_name)
            collection.add_hash_index(fields=['uuid'], unique=True)
        else:
            collection = db.collection(cls.collection_name)
            indexes = collection.indexes()
            if not any(index['fields'] == ['uuid'] for index in indexes):
                collection.add_hash_index(fields=['uuid'], unique=True)

    def save(self, db: StandardDatabase):
        self.init_collection()
        collection = db.collection(self.collection_name)
        doc = self.model_dump()
        doc['_key'] = str(doc.pop('id', None))
        collection.insert(doc)
        return doc

    @classmethod
    def get(cls, doc_id: str, db: StandardDatabase):
        cls.init_collection()
        collection = db.collection(cls.collection_name)
        doc = collection.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.collection_name} not found")
        return doc

    def update(self, updated_by: str, db: StandardDatabase):
        self.init_collection()
        collection = db.collection(self.collection_name)
        self.updated_by = updated_by
        self.updated_on = datetime.now()
        doc = self.model_dump()
        collection.update({'_key': doc['id'], **doc})
        return doc

    @classmethod
    def delete(cls, doc_id: str, deleted_by: str, db: StandardDatabase):
        cls.init_collection()
        collection = db.collection(cls.collection_name)
        doc = collection.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.collection_name} not found")
        doc['is_deleted'] = True
        doc['deleted_by'] = deleted_by
        doc['deleted_on'] = datetime.now()
        collection.update(doc)
        return doc

    @classmethod
    def restore(cls, doc_id: str, updated_by: str, db: StandardDatabase):
        cls.init_collection()
        collection = db.collection(cls.collection_name)
        doc = collection.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.collection_name} not found")
        doc['is_deleted'] = False
        doc['updated_by'] = updated_by
        doc['updated_on'] = datetime.now()
        doc['deleted_by'] = None
        doc['deleted_on'] = None
        collection.update(doc)
        return doc
