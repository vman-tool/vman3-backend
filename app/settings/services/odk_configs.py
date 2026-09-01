from types import SimpleNamespace
from typing import List, Optional

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

# Fields the app depends on across CCVA, PCVA, statistics, and DQA - an
# empty mapping here doesn't fail at save time (they're typed as plain str,
# not Optional, so "" satisfies pydantic) but breaks downstream AQL queries
# that interpolate the mapped field name directly, and that failure surfaces
# far from here (e.g. mid-CCVA-run) with no indication it's a config gap.
REQUIRED_FIELD_MAPPING_LABELS = {
    'location_level1': 'Location Level 1',
    'is_adult': 'Is Adult',
    'is_child': 'Is Child',
    'is_neonate': 'Is Neonate',
}


def _is_blank(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == '')


def validate_configs(config: SettingsConfigData):
    fm = config.field_mapping
    if fm is None:
        raise BadRequestException("Field mapping configuration is missing.")

    if _is_blank(fm.submitted_date) and _is_blank(fm.death_date) and _is_blank(fm.interview_date):
        raise BadRequestException(
            "At least one of Submitted Date, Death Date, or Interview Date must be mapped."
        )

    missing = [label for field, label in REQUIRED_FIELD_MAPPING_LABELS.items() if _is_blank(getattr(fm, field))]
    if missing:
        raise BadRequestException(
            f"The following required fields are not mapped: {', '.join(missing)}. "
            "These are used across CCVA, PCVA, and Data Quality analysis and must be set before saving."
        )
    
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


# @ttl_cache(ttl=3600, key_prefix="system_configs")
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

            # Clear any cached session before validating — the new server won't
            # accept a token issued by the old server.
            import os as _os
            if _os.path.exists("session.json"):
                _os.remove("session.json")

            # Validate ODK configuration
            async with ODKClientAsync(data_simpleSpace) as odk_client:
                data_for_count = await odk_client.getFormSubmissions(top=1, order_by='__system/submissionDate', order_direction='asc')
                if not data_for_count:
                    raise ValueError("Invalid ODK configuration")

        elif configData.type == 'system_configs' and configData.system_configs:
            data['system_configs'] = configData.system_configs.model_dump()
            

        elif configData.type == 'field_mapping' and configData.field_mapping:
            validate_configs(configData)
            data['field_mapping'] = configData.field_mapping.model_dump()
        
        elif configData.type == 'va_summary' and configData.va_summary:
            data['va_summary'] = configData.va_summary
        
        elif configData.type == 'field_labels' and configData.field_labels:

            # field_labels is one array holding an entry per relabelled
            # field_id (region, district, ward, ...); the merge below needs
            # that whole array to preserve every other field's relabels.
            # Two bugs previously combined to silently drop data on every
            # save past the first: (1) `field_labels[0]` fetched only the
            # array's first ENTRY, not the whole array, and (2) the fetch
            # scanned every document in the collection with no `_key`
            # filter, so it could read a stray/older document instead of
            # the one `save_system_settings` actually upserts (by
            # `_key: 'vman_config'`, same pattern as fetch_odk_config).
            def get_existing_field_labels():
                doc = db.collection(db_collections.SYSTEM_CONFIGS).get('vman_config')
                return (doc or {}).get('field_labels')

            existing_field_labels_array = await run_in_threadpool(get_existing_field_labels)
            field_label_data = []
            if existing_field_labels_array:
                existing_field_label_dict = {
                    field['field_id']: field for field in existing_field_labels_array
                    if field and 'field_id' in field
                }

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

        elif configData.type == 'dqa_thresholds' and configData.dqa_thresholds:
            data['dqa_thresholds'] = configData.dqa_thresholds.model_dump()

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

    except HTTPException:
        # Preserve validation errors raised above (e.g. BadRequestException
        # from validate_configs) as-is - the blanket handler below stringifies
        # everything it catches, and str() on an HTTPException produces
        # "<status_code>: <detail>", duplicating the code into the message.
        raise
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
            return cursor

            
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

async def save_system_images(data: ImagesConfigData, reset: bool = False, fields_to_reset: Optional[List[str]] = None, db: StandardDatabase = None):
    """
    :param reset: force every field in `data` onto the existing record,
        including nulls - used to reset ALL images at once.
    :param fields_to_reset: reset just these specific fields to null,
        leaving the rest of the existing record (and anything else merged
        in from `data`) untouched - used to reset a single image.
    """
    try:
        if not data and not reset:
            raise ValueError("No system images provided")
        saving_data = {'_key': 'vman_config'}
        saving_data['system_images'] = data.model_dump()

        existing_images = await get_system_images(db)
        if len(existing_images) > 0 and existing_images[0] is not None:

            updated_images = replace_object_values(saving_data['system_images'], existing_images[0], force=reset)
            if fields_to_reset:
                for field in fields_to_reset:
                    updated_images[field] = None
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