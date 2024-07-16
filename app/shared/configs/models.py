import uuid
from datetime import datetime
from http.client import HTTPException
from typing import Optional

from arango.database import StandardDatabase
from pydantic import BaseModel, Field


class VmanBaseModel(BaseModel):
    """
    This class abstracts the CRUD operations for the Vman Models that are used to interract with the database.
    """

    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True)
    created_by: str
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_by: Optional[str] = None
    updated_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    deleted_by: Optional[str] = None
    deleted_at: Optional[str] = None
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

    def save(self, db: StandardDatabase):
        """
        This method saved the data and returns the new saved data.
        
        """
        self.init_collection(db)
        collection = db.collection(self.get_collection_name())
        doc = self.model_dump()
        # doc['_key'] = str(doc.pop('id', None))
        return collection.insert(doc, return_new=True)["new"]
        

    @classmethod
    def get(cls, doc_id: str, db: StandardDatabase):
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        doc = collection.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.get_collection_name()} not found")
        return doc

    def update(self, updated_by: str, db: StandardDatabase):
        self.init_collection()
        collection = db.collection(self.collection_name)
        self.updated_by = updated_by
        self.updated_at = datetime.now()
        doc = self.model_dump()
        collection.update({'_key': doc['id'], **doc})
        return doc

    @classmethod
    def delete(cls, doc_id: str, deleted_by: str, db: StandardDatabase):
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        doc = collection.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.get_collection_name()} not found")
        doc['is_deleted'] = True
        doc['deleted_by'] = deleted_by
        doc['deleted_at'] = datetime.now()
        collection.update(doc)
        return doc

    @classmethod
    def restore(cls, doc_id: str, updated_by: str, db: StandardDatabase):
        cls.init_collection(db)
        collection = db.collection(cls.get_collection_name())
        doc = collection.get(doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail=f"{cls.get_collection_name()} not found")
        doc['is_deleted'] = False
        doc['updated_by'] = updated_by
        doc['updated_at'] = datetime.now()
        doc['deleted_by'] = None
        doc['deleted_at'] = None
        collection.update(doc)
        return doc
