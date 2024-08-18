from types import SimpleNamespace

from arango.database import StandardDatabase
from fastapi import HTTPException, status

from app.settings.models.odk_configs import OdkConfigModel
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.utilits.odk_client import ODKClientAsync


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
        print(config_data)
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

        data = {
            '_key': 'odk_config',  # Unique key to ensure only one config exists
            **configData.model_dump()
        }

        # Convert data_dict to an object
        data_simpleSpace = SimpleNamespace(**data)

        async with ODKClientAsync(data_simpleSpace) as odk_client:
            
            # try:    
                data_for_count = await odk_client.getFormSubmissions(top=1, order_by='__system/submissionDate', order_direction='asc')
                if not data_for_count:
                    raise ValueError("Failed to save config", "Invalid ODK configuration")
                    
                    
                        
            # except Exception:
            #             raise ValueError("Failed to save config", "Invalid ODK configuration")
                    
        print(data_for_count, 'data_for_count')
        if not data_for_count:
            raise ValueError("Failed to save config", "Invalid ODK configuration")
        
        db.collection(db_collections.SYSTEM_CONFIGS).insert(data, overwrite=True)

        return ResponseMainModel(
            data={"config_id": 'odk_config'},
            message="Config saved successfully"
        )

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,  # Or another appropriate status code
            detail=str(e)
        )
