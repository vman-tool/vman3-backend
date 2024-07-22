from typing import Dict
from arango.database import StandardDatabase

async def record_exists(collection_name: str, uuid: str = None, id: str = None, custom_fields: Dict = {}, db: StandardDatabase = None) -> bool:
    """
    Use this method to check existence of record with given fields and their values

    :collection_name: name of the collection
    :uuid: uuid of the record if you need to use that
    :id: _key/id of the record if you need to use that
    :custom_fields: Use this variable whenever you have more than one field (including uuid, id), Otherwise the check will use only these
    """
    bind_vars = {}
    
    if custom_fields or (uuid and id):
        aql_filters = []

        if uuid:
            custom_fields['uuid'] = uuid
        if id:
            custom_fields['_key'] = id
        
        # Add user-defined filters
        for field, value in custom_fields.items():
            aql_filters.append(f"doc.{field} == @{field}")
            bind_vars[field] = value
        query = f"""
        RETURN LENGTH(
            FOR doc IN {collection_name}
        """

        query += " FILTER " + " AND ".join(aql_filters)

        query += """
            RETURN doc
        ) > 0"""
    if id and not custom_fields:
        query = f"""
        RETURN LENGTH(
            FOR doc IN {collection_name}
            FILTER doc._key == @id
            RETURN doc
        ) > 0
        """
        bind_vars = {'id': id}
    if uuid and not custom_fields:
        query = f"""
        RETURN LENGTH(
            FOR doc IN {collection_name}
            FILTER doc.uuid == @uuid
            RETURN doc
        ) > 0
        """
        bind_vars = {'uuid': uuid}
    
    cursor = db.aql.execute(query, bind_vars=bind_vars)
    exists = cursor.next()
    return exists

async def replace_object_values(new_dict: Dict, old_dict: Dict):
    """
    Use this method to replace the values from old_dict with new_dict

    :new_dict : Dictionary with values to replace
    :old_dict : Dictionary with values to be replaced
    :Returns the old dictionary with new values
    """
    try:
        for key, value in new_dict.items():
                if value:
                    old_dict[key] = value

        return old_dict
    except Exception as e:
        print(f"Error occurred while replacing values: {str(e)}")
        return None