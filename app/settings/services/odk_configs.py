from types import SimpleNamespace

from arango.database import StandardDatabase
from fastapi import HTTPException, status
from fastapi.concurrency import run_in_threadpool

from app.odk.utils.odk_client import ODKClientAsync
from app.settings.models.settings import ImagesConfigData, SettingsConfigData
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.middlewares.exceptions import BadRequestException
from app.shared.utils.database_utilities import replace_object_values
from app.shared.utils.cache import ttl_cache, invalidate_cache

def validate_configs(config: SettingsConfigData):
    if (config.field_mapping.submitted_date is None or 
        config.field_mapping.death_date is None or 
        config.field_mapping.interview_date is None):
        raise BadRequestException("Either Submitted, Death, or Interview date field is not set in the configuration")

    if (config.field_mapping.location_level1 is None or 
        config.field_mapping.is_adult is None or 
        config.field_mapping.is_child is None or 
        config.field_mapping.is_neonate is None):
        raise BadRequestException("One or more required fields are not set in the configuration")
    
#@log_to_db(context="fetch_odk_config", log_args=True)
async def fetch_odk_config(db: StandardDatabase, is_validate_configs: bool = False) -> SettingsConfigData:
    try:
        
        def get_config_data():
            return db.collection(db_collections.SYSTEM_CONFIGS).get('vman_config')

        config_data = await run_in_threadpool(get_config_data)
        if not config_data:
            await db_logger.log(
            message="ODK configuration not found in the database" ,
            level=db_logger.LogLevel.ERROR,
            context="fetch_odk_config",
            data={} 
    )
            raise ValueError("ODK configuration not found in the database")

        # Ensure config_data is a dictionary
        if isinstance(config_data, dict):
            config = SettingsConfigData(**config_data)
            
            # Validate required fields in field_mapping
            if is_validate_configs:
                validate_configs(config)

            return config
        else:
            print("ODK configuration data is not in the expected format")
            await db_logger.log(
            message="ODK configuration data is not in the expected format" ,
            level=db_logger.LogLevel.ERROR,
            context="fetch_odk_config",
            data={} 
    )
            raise ValueError("ODK configuration data is not in the expected format")
    except Exception as e:
        print(e)
        raise ValueError(e)


@ttl_cache(ttl=3600, key_prefix="system_configs")
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
        
        elif configData.type == 'field_labels' and configData.field_labels:

            aql_query = f"""
            FOR settings in  {db_collections.SYSTEM_CONFIGS}
            RETURN settings.field_labels[0]
            """
            def execute_field_labels_query():
                cursor = db.aql.execute(aql_query, bind_vars={}, cache=True)
                return [doc for doc in cursor]

            existing_field_labels_settings = await run_in_threadpool(execute_field_labels_query)
            field_label_data = []
            if len(existing_field_labels_settings) > 0 and existing_field_labels_settings[0] is not None:
                existing_field_label_dict = {field['field_id']: field for field in existing_field_labels_settings}

                for field_label in configData.model_dump().get('field_labels', ""):
                    if "field_id" not in field_label:
                        continue
                    field_id = field_label['field_id']

                    if field_id in existing_field_label_dict:
                        existing_field_label_dict[field_id] = replace_object_values(field_label, existing_field_label_dict[field_id])
                    else:
                         existing_field_label_dict[field_id] = field_label
                
                field_label_data = list(existing_field_label_dict.values())
            else:
                field_label_data = configData.model_dump().get('field_labels', "")
            
            data['field_labels'] = field_label_data
                # Add handling for cron settings
        elif configData.type == 'cron_settings' and configData.cron_settings:
            data['cron_settings'] = configData.cron_settings.model_dump()
            
        # Add handling for backup settings
        elif configData.type == 'backup_settings' and configData.backup_settings:
            data['backup_settings'] = configData.backup_settings.model_dump()
            
        # Add handling for sync status
        elif configData.type == 'sync_status' and configData.sync_status:
            data['sync_status'] = configData.sync_status.model_dump()
            
        else:
            raise ValueError("Invalid type or missing configuration data")

        # db.collection(db_collections.SYSTEM_CONFIGS).insert(data, overwrite=False)
        results = await save_system_settings(data = data, db = db)
        
        # Invalidate Configs Cache
        await invalidate_cache("system_configs")

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


async def save_system_settings(data, db: StandardDatabase = None):
    try:
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
        def execute_save_settings():
            cursor = db.aql.execute(aql_query, bind_vars=bind_vars)
            return [doc for doc in cursor]
            
        return await run_in_threadpool(execute_save_settings)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
    
@ttl_cache(ttl=3600, key_prefix="system_images") # Cache for 1 hour
async def get_system_images(db: StandardDatabase = None):
    aql_query = f"""
        FOR settings in  {db_collections.SYSTEM_CONFIGS}
        RETURN settings.system_images
    """
    def execute_get_images_query():
        cursor = db.aql.execute(aql_query, bind_vars={})
        return [doc for doc in cursor]

    data = await run_in_threadpool(execute_get_images_query)
    return data

async def save_system_images(data: ImagesConfigData, reset: bool = False, db: StandardDatabase = None):
    try:
        if not data and not reset:
            raise ValueError("No system images provided")
        saving_data = {'_key': 'vman_config'}
        saving_data['system_images'] = data.model_dump()

        existing_images = await get_system_images(db)
        if len(existing_images) > 0 and existing_images[0] is not None:
            
            updated_images = replace_object_values(saving_data['system_images'], existing_images[0], force=reset)
            saving_data['system_images'] = updated_images
            await save_system_settings(saving_data, db)
            
            # Invalidate cache for smart update
            await invalidate_cache("system_images")
            
            return await get_system_images(db)
        else:
            await save_system_settings(saving_data, db)
            # Invalidate cache for smart update
            await invalidate_cache("system_images")

            return await get_system_images(db)
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Couldn't save system images")