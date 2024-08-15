from datetime import datetime
from typing import AsyncGenerator

from arango import ArangoClient
from arango.database import StandardDatabase
from decouple import config

from app.shared.configs.constants import collections_with_indexes


class ArangoDBClient:
    def __init__(self):
        self.client = ArangoClient(hosts=config("ARANGODB_URL", default="http://localhost:8529"))
        self.db_name = config("DB_NAME", default="vman3")
        self.username = config("ARANGO_ROOT_USER", default="root")
        self.password = config("ARANGO_ROOT_PASSWORD", default="password")
        self.db = None

    async def connect(self):
        sys_db = self.client.db("_system", username=self.username, password=self.password)
        # print(sys_db.databases())

        
        # # Check if the database exists and create it if not
        if self.db_name not in sys_db.databases():
            sys_db.create_database(self.db_name)
        
        # Connect to the specified database
        self.db = self.client.db(self.db_name, username=self.username, password=self.password)
    async def create_collections(self):
        await create_collections_and_indexes(self.db, collections_with_indexes)  
    async def insert_many(self, collection_name:str, documents: list[dict]):
        collection = self.db.collection(collection_name)
        result = collection.insert_many(documents)
        return result
    
    # async def replace_one(self, collection_name:str, document:dict):
    #     try:
    #         aql_query = """
    #         UPSERT { _key: @key }
    #         INSERT @document
    #         UPDATE @document IN @@collection
    #         OPTIONS { exclusive: true }
    #         // LET opType = IS_NULL(OLD) ? "insert" : "update"
    #        // RETURN { _key: NEW._key, type: opType }
    #         """
    #         bind_vars = {
    #             '@collection': collection_name,
    #             'key': document['__id'],
    #             'document': document
    #         }
    #         cursor = self.db.aql.execute(aql_query, bind_vars=bind_vars)
    #         result = [doc for doc in cursor]
    #         return result
    #     except Exception as e:
    #         print(e)
    #         raise e
async def replace_one(self, collection_name: str, document: dict):
    try:
        # Ensure the key is a string
        document['_key'] = str(document['__id'])
        
        # Convert datetime fields to ISO 8601 strings, and ensure numeric fields are numbers
        for key, value in document.items():
            if isinstance(value, datetime):
                document[key] = value.isoformat()
            elif isinstance(value, str) and value.isdigit():
                document[key] = int(value)

        aql_query = """
        UPSERT { _key: @key }
        INSERT @document
        UPDATE @document IN @@collection
        OPTIONS { exclusive: true }
        """
        
        bind_vars = {
            '@collection': collection_name,
            'key': document['_key'],
            'document': document
        }
        
        cursor = await self.db.aql.execute(aql_query, bind_vars=bind_vars)
        result = [doc for doc in cursor]
        return result
    
    except Exception as e:
        print(f"Error during replace_one: {e}")
        raise e

    
   
   
       # async def replace_one(self, collection_name:str, document:dict):
    #     try:
    #         aql_query = """
    #         UPSERT { _key: @key }
    #         INSERT @document
    #         UPDATE @document IN @@collection
    #         OPTIONS { exclusive: true }
    #         // LET opType = IS_NULL(OLD) ? "insert" : "update"
    #        // RETURN { _key: NEW._key, type: opType }
    #         """
    #         bind_vars = {
    #             '@collection': collection_name,
    #             'key': document['__id'],
    #             'document': document
    #         }
    #         cursor = self.db.aql.execute(aql_query, bind_vars=bind_vars)
    #         result = [doc for doc in cursor]
    #         return result
    #     except Exception as e:
    #         print(e)
    #         raise e
    # async def replace_one(self, collection_name: str, document: dict):
    #     try:
    #         # Ensure the key is a string
    #         document['_key'] = str(document['__id'])
            
    #         # Convert datetime fields to ISO 8601 strings, and ensure numeric fields are numbers
    #         for key, value in document.items():
    #             if isinstance(value, datetime):
    #                 document[key] = value.isoformat()
    #             elif isinstance(value, str) and value.isdigit():
    #                 document[key] = int(value)

    #         aql_query = """
    #         UPSERT { _key: @key }
    #         INSERT @document
    #         UPDATE @document IN @@collection
    #         OPTIONS { exclusive: true }
    #         """
            
    #         bind_vars = {
    #             '@collection': collection_name,
    #             'key': document['_key'],
    #             'document': document
    #         }
            
    #         cursor =  self.db.aql.execute(aql_query, bind_vars=bind_vars)
    #         result = [doc for doc in cursor]
    #         return result
        
    #     except Exception as e:
    #         print(f"Error during replace_one: {e}")
    #         raise e
   
    
async def create_collections_and_indexes(db: StandardDatabase, collections_with_indexes: dict):
    for collection_name, indexes in collections_with_indexes.items():
        if not db.has_collection(collection_name):
            db.create_collection(collection_name)
        
        collection = db.collection(collection_name)
        
        for index in indexes:
            try:
                existing_indexes = collection.indexes()
                existing_index = next((idx for idx in existing_indexes if idx['fields'] == index['fields']), None)
                
                if existing_index:
                    # Check if the existing index properties match the desired ones
                    index_type_match = existing_index['type'] == index['type']
                    unique_match = existing_index['unique'] == index.get('unique', False)
                    name_match = existing_index.get('name', None) == index.get('name', None)

                    if not (index_type_match and unique_match and name_match):
                        # Properties do not match, drop and recreate the index
                        collection.delete_index(existing_index['id'])
                        print(f"Dropped existing index: {existing_index['id']} due to property mismatch")

                        # Create the new index with updated properties
                        if index['type'] == 'hash':
                            collection.add_hash_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                        elif index['type'] == 'skiplist':
                            collection.add_skiplist_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                        elif index['type'] == 'persistent':
                            collection.add_persistent_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))

                        print(f"Created updated index: {index['name']} with fields: {index['fields']}")
                    else:
                        continue
                        # print(f"Index already exists with matching properties: {existing_index['name']} with fields: {existing_index['fields']}")
                else:
                    # Index does not exist, create it
                    if index['type'] == 'hash':
                        collection.add_hash_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                    elif index['type'] == 'skiplist':
                        collection.add_skiplist_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))
                    elif index['type'] == 'persistent':
                        collection.add_persistent_index(fields=index['fields'], unique=index.get('unique', False), name=index.get('name', None))

                    print(f"Created new index: {index['name']} with fields: {index['fields']}")

            except Exception as e:
                print(f"Error processing index {index}: {e}")
                pass


async def get_arangodb_client()->ArangoDBClient:
    client = ArangoDBClient()
    await client.connect()
    return client    



# @asynccontextmanager()
async def get_arangodb_session() -> AsyncGenerator[StandardDatabase, None]:
    client = await get_arangodb_client()
    try:
        yield client.db
    finally:
        pass  # ArangoDB sessions don't need to be explicitly closed like SQLAlchemy