import json
from types import SimpleNamespace

from arango.database import StandardDatabase
from fastapi import HTTPException, status

from app.odk.utils.odk_client import ODKClientAsync
from app.settings.models.settings import SettingsConfigData
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel


async def fetch_odk_config(db: StandardDatabase) -> SettingsConfigData:
    config_data = db.collection(db_collections.SYSTEM_CONFIGS).get('vman_config')  # Assumes 'vman_config' is the key
    if not config_data:
        raise ValueError("ODK configuration not found in the database")
    # Ensure config_data is a dictionary
    if isinstance(config_data, dict):
        return  SettingsConfigData(**config_data)  
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




async def add_configs_settings(configData: SettingsConfigData, db: StandardDatabase = None) -> ResponseMainModel:
    
    try:
        # Prepare the base data dictionary with a unique key
        data = {'_key': 'vman_config'}


        # Determine which configuration to insert based on the 'type' field
        if configData.type == 'odk_api_configs' and configData.odk_api_configs:
            odk_data = configData.odk_api_configs.model_dump()
            data['odk_api_configs'] = odk_data
           

            # Flatten odk_api_configs data for SimpleNamespace
            data_simpleSpace = SimpleNamespace(**odk_data)

            # Validate ODK configuration
            async with ODKClientAsync(data_simpleSpace) as odk_client:
                data_for_count = await odk_client.getFormSubmissions(top=1, order_by='__system/submissionDate', order_direction='asc')
                if not data_for_count:
                    raise ValueError("Invalid ODK configuration")

        elif configData.type == 'system_configs' and configData.system_configs:
            data['system_configs'] = configData.system_configs.model_dump()
            

        elif configData.type == 'field_mapping' and configData.field_mapping:
            data['field_mapping'] = configData.field_mapping.model_dump()
        
        elif configData.type == 'va_summary' and configData.va_summary:
            data['va_summary'] = configData.va_summary
            

        else:
            raise ValueError("Invalid type or missing configuration data")

        # Insert the filtered config data into the database
        aql_query = """
        UPSERT { _key: @key }
        INSERT @document
        UPDATE @document IN @@collection
        OPTIONS { exclusive: true }
        """
        bind_vars = {
            '@collection': db_collections.SYSTEM_CONFIGS,
            'key':  data['_key'],
            'document': data
        }
        cursor = db.aql.execute(aql_query, bind_vars=bind_vars)
        result = [doc for doc in cursor]
        # db.collection(db_collections.SYSTEM_CONFIGS).insert(data, overwrite=False)

        # Return success response
        return ResponseMainModel(
            data={"config_id": 'vman_config'},
            message="Config saved successfully"
        )

    except Exception as e:
        print(e)
        # Handle any exceptions and return an HTTP error response
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


async def get_questioners_fields(db: StandardDatabase = None):
    try:
        config = await fetch_odk_config(db)
        # logger.info(f"ODK Config: {config}")
        
            
        async with ODKClientAsync(config.odk_api_configs) as odk_client:
            data_for_count = await odk_client.getFormSubmissions(top= 1, skip= None , order_by='__system/submissionDate', order_direction='asc')
            if not data_for_count:
                raise ValueError("Invalid ODK configuration")
            return ResponseMainModel(
                data=data_for_count,
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