from arango.database import StandardDatabase

from app.settings.models.odk_configs import OdkConfigModel
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


async def fetch_odk_config(db: StandardDatabase) -> OdkConfigModel:
    config_data = db.collection(db_collections.SYSTEM_CONFIGS).get('odk_config')  # Assumes 'odk_config' is the key
    if not config_data:
        raise ValueError("ODK configuration not found in the database")

    # Ensure config_data is a dictionary
    if isinstance(config_data, dict):
        return OdkConfigModel(**config_data)
    else:
        raise ValueError("ODK configuration data is not in the expected format")


async def fetch_configs_settings(db: StandardDatabase = None):
    try:
        config_data= await fetch_odk_config(db)
        return ResponseMainModel(
            data=config_data.model_dump(),
            message="Config fetched successfully",
            total=None
        )

    except Exception as e:
        return ResponseMainModel(
            data=None,
            message="Failed to fetch records",
            error=str(e),
            total=None
        )

async def add_configs_settings(configData: OdkConfigModel, db: StandardDatabase = None) -> ResponseMainModel:
    try:
        data={
            '_key': 'odk_config',  # Unique key to ensure only one config exists
        **configData.model_dump()
        }
        
        doc= db.collection(db_collections.SYSTEM_CONFIGS).insert(data, overwrite=True)
        print(doc)
        # db:ArangoDBClient = await get_arangodb_client()
   
        # doc= await db.replace_one(db_collections.SYSTEM_CONFIGS, document={
        #     '_key':'1', # for reasons that  we will only have one config
        #     '__id':'1',
        #     'url': configData.url,
        #     'username': configData.username,
        #     'password': configData.password,
        #     'formId': configData.formId
        # })
    
        return ResponseMainModel(
            data={"config_id": ''},
            message="Config saved successfully",
            # total=total_records
        )

    except Exception as e:
        return ResponseMainModel(
            data=None,
            message="Failed to fetch records",
            error=str(e),
            total=None
        )