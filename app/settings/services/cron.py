from types import SimpleNamespace
from typing import Dict, List, Any
from arango.database import StandardDatabase
from fastapi import HTTPException, status
from app.odk.utils.odk_client import ODKClientAsync
from app.settings.models.settings import CronSettings, BackupSettings, SettingsConfigData
from app.settings.services.odk_configs import save_system_settings
from app.shared.configs.constants import db_collections
from app.shared.configs.models import ResponseMainModel
from app.shared.utils.database_utilities import replace_object_values


# Add these models to your existing settings.py file

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
            cursor = db.aql.execute(aql_query, bind_vars={},cache=True)
            existing_field_labels_settings = [doc for doc in cursor]
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
            
        else:
            raise ValueError("Invalid type or missing configuration data")

        # Save the settings
        results = await save_system_settings(data=data, db=db)

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


async def fetch_cron_settings(db: StandardDatabase) -> List[Dict[str, Any]]:
    """Fetch cron settings from the database"""
    try:
        # Query to get cron settings from ArangoDB
        query = """
        FOR doc IN settings
            FILTER doc.type == 'cron_settings'
            RETURN doc
        """
        cursor = await db.aql.execute(query)
        results = [doc async for doc in cursor]
        
        # If no settings found, return default settings
        if not results:
            return [{
                "days": [],
                "time": "00:00"
            }]
        
        return results
    except Exception as e:
        print(f"Error fetching cron settings: {str(e)}")
        raise e

async def save_cron_settings(settings: CronSettings, db: StandardDatabase) -> Dict[str, Any]:
    """Save cron settings to the database"""
    try:
        # Check if settings already exist
        query = """
        FOR doc IN settings
            FILTER doc.type == 'cron_settings'
            RETURN doc
        """
        cursor = await db.aql.execute(query)
        existing_settings = [doc async for doc in cursor]
        
        if existing_settings:
            # Update existing settings
            doc_id = existing_settings[0]['_id']
            settings_dict = settings.dict()
            settings_dict['type'] = 'cron_settings'
            
            await db.update_document(doc_id, settings_dict)
            return settings_dict
        else:
            # Create new settings
            settings_dict = settings.dict()
            settings_dict['type'] = 'cron_settings'
            
            result = await db.insert_document('settings', settings_dict)
            return settings_dict
    except Exception as e:
        print(f"Error saving cron settings: {str(e)}")
        raise e

async def fetch_backup_settings(db: StandardDatabase) -> List[Dict[str, Any]]:
    """Fetch backup settings from the database"""
    try:
        # Query to get backup settings from ArangoDB
        query = """
        FOR doc IN settings
            FILTER doc.type == 'backup_settings'
            RETURN doc
        """
        cursor = await db.aql.execute(query)
        results = [doc async for doc in cursor]
        
        # If no settings found, return default settings
        if not results:
            return [{
                "frequency": "daily",
                "time": "00:00",
                "location": "local"
            }]
        
        return results
    except Exception as e:
        print(f"Error fetching backup settings: {str(e)}")
        raise e

async def save_backup_settings(settings: BackupSettings, db: StandardDatabase) -> Dict[str, Any]:
    """Save backup settings to the database"""
    try:
        # Check if settings already exist
        query = """
        FOR doc IN settings
            FILTER doc.type == 'backup_settings'
            RETURN doc
        """
        cursor = await db.aql.execute(query)
        existing_settings = [doc async for doc in cursor]
        
        if existing_settings:
            # Update existing settings
            doc_id = existing_settings[0]['_id']
            settings_dict = settings.dict()
            settings_dict['type'] = 'backup_settings'
            
            await db.update_document(doc_id, settings_dict)
            return settings_dict
        else:
            # Create new settings
            settings_dict = settings.dict()
            settings_dict['type'] = 'backup_settings'
            
            result = await db.insert_document('settings', settings_dict)
            return settings_dict
    except Exception as e:
        print(f"Error saving backup settings: {str(e)}")
        raise e