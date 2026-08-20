from typing import Dict, List, Any
from arango.database import StandardDatabase
from app.settings.models.settings import CronSettings, BackupSettings


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