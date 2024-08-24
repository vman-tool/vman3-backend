
import logging
from typing import AsyncGenerator

from arango import ArangoClient
from arango.database import StandardDatabase
from decouple import config
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import collections_with_indexes

logger = logging.getLogger(__name__)

class ArangoDBClient:
    def __init__(self):
        self.client = ArangoClient(hosts=config("DB_URL", default="http://localhost:8529"))
        self.db_name = config("DB_NAME", default="vman3")
        self.username = config("DB_ROOT_USER", default="root")
        self.password = config("ARANGO_ROOT_PASSWORD", default="welcome2vman")
        self.db = None

    async def connect(self):
        # Run the synchronous connect method in a thread pool
        await run_in_threadpool(self._connect_sync)

    def _connect_sync(self):
        sys_db = self.client.db("_system", username=self.username, password=self.password)
        
        # Check if the database exists and create it if not
        if self.db_name not in sys_db.databases():
            sys_db.create_database(self.db_name)
        if self.db is None:
        # Connect to the specified database
            self.db = self.client.db(self.db_name, username=self.username, password=self.password)

    async def create_collections(self):
        # This method remains synchronous, so we run it in a thread pool
        await run_in_threadpool(self._create_collections_sync)

    def _create_collections_sync(self):
        create_collections_and_indexes(self.db, collections_with_indexes)

    async def insert_many(self, collection_name: str, documents: list[dict]):
        # Wrap the synchronous insert_many in a thread pool
        return await run_in_threadpool(self._insert_many_sync, collection_name, documents)

    def _insert_many_sync(self, collection_name: str, documents: list[dict]):
        collection = self.db.collection(collection_name)
        result = collection.insert_many(documents)
        return result

    async def replace_one(self, collection_name: str, document: dict):
        # Wrap the synchronous replace_one in a thread pool
        return await run_in_threadpool(self._replace_one_sync, collection_name, document)
    def _replace_one_sync(self, collection_name: str, document: dict):
        try:
            aql_query = """
            UPSERT { __id: @id }
            INSERT @document
            UPDATE @document IN @@collection
            OPTIONS { exclusive: true }
            """
            bind_vars = {
                '@collection': collection_name,
                'id': document['__id'],
                'document': document
            }
            cursor = self.db.aql.execute(aql_query, bind_vars=bind_vars)
            result = [doc for doc in cursor]
            return result
        except Exception as e:
            logger.error(f"Error during replace_one: {e}")
            raise e

# Non-async function to create collections and indexes
def create_collections_and_indexes(db: StandardDatabase, collections_with_indexes: dict):
    for collection_name, indexes in collections_with_indexes.items():
        if not db.has_collection(collection_name):
            db.create_collection(collection_name)
        
        collection = db.collection(collection_name)
        
        for index in indexes:
            try:
                existing_indexes = collection.indexes()
                existing_index = next((idx for idx in existing_indexes if idx['fields'] == index['fields']), None)
                
                if existing_index:
                    index_type_match = existing_index['type'] == index['type']
                    unique_match = existing_index['unique'] == index.get('unique', False)
                    name_match = existing_index.get('name', None) == index.get('name', None)

                    if not (index_type_match and unique_match and name_match):
                        collection.delete_index(existing_index['id'])
                        logger.info(f"Dropped existing index: {existing_index['id']} due to property mismatch")

                        if index['type'] == 'hash':
                            collection.add_hash_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                        elif index['type'] == 'skiplist':
                            collection.add_skiplist_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                        elif index['type'] == 'persistent':
                            collection.add_persistent_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))

                        logger.info(f"Created updated index: {index['name']} with fields: {index['fields']}")
                else:
                    if index['type'] == 'hash':
                        collection.add_hash_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                    elif index['type'] == 'skiplist':
                        collection.add_skiplist_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                    elif index['type'] == 'persistent':
                        collection.add_persistent_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))

                    logger.info(f"Created new index: {index['name']} with fields: {index['fields']}")

            except Exception as e:
                logger.error(f"Error processing index {index}: {e}")
                pass

# Asynchronous generator for session management
async def get_arangodb_client() -> ArangoDBClient:
    client = ArangoDBClient()
    await client.connect()
    return client

async def get_arangodb_session() -> AsyncGenerator[StandardDatabase, None]:
    client = await get_arangodb_client()
    try:
        yield client.db
    finally:
        pass  # No explicit closing needed        pass  # No explicit closing needed