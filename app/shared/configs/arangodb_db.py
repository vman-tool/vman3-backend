from typing import AsyncGenerator

from arango import ArangoClient
from arango.database import StandardDatabase
from decouple import config


class ArangoDBClient:
    def __init__(self):
        self.client = ArangoClient(hosts=config("ARANGODB_URL", default="http://localhost:8529"))
        self.db_name = config("DB_NAME", default="vman3")
        self.username = "root"
        self.password = '1029' or config("ARANGO_ROOT_PASSWORD")
        self.db = None

    async def connect(self):
        sys_db = self.client.db("_system", username=self.username, password=self.password)
        print(sys_db.databases())

        
        # # Check if the database exists and create it if not
        if self.db_name not in sys_db.databases():
            sys_db.create_database(self.db_name)
        
        # Connect to the specified database
        self.db = self.client.db(self.db_name, username=self.username, password=self.password)
        await create_collections(self.db, ["form_submissions", "download_tracker", "download_process_tracker"])

    async def insert_many(self, collection_name:str, documents: list[dict]):
        collection = self.db.collection(collection_name)
        result = collection.insert_many(documents)
        return result
    
    async def replace_one(self, collection_name:str, document:dict):
        try:
            aql_query = """
            UPSERT { __id: @key }
            INSERT @document
            UPDATE @document IN @@collection
            OPTIONS { exclusive: true }
            // LET opType = IS_NULL(OLD) ? "insert" : "update"
           // RETURN { _key: NEW._key, type: opType }
            """
            bind_vars = {
                '@collection': collection_name,
                'key': document['__id'],
                'document': document
            }
            cursor = self.db.aql.execute(aql_query, bind_vars=bind_vars)
            result = [doc for doc in cursor]
            return result
        except Exception as e:
            print(e)
            raise e

# Function to create collections if they do not exist
async def create_collections(db, collections):
    for collection_name in collections:
        if not db.has_collection(collection_name):
            db.create_collection(collection_name)

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