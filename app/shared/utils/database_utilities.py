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
    aql_filters = []
    
    if custom_fields or (uuid and id):
        
        query = f"""
        RETURN LENGTH(
            FOR doc IN {collection_name}
        """
        if uuid:
            custom_fields['uuid'] = uuid
        if id:
            custom_fields['_key'] = id
        
        aql_filters, bind_vars = add_query_filters(query, custom_fields, bind_vars = bind_vars)
        
        if aql_filters:
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
    
def add_query_filters(filters: Dict = {}, bind_vars: Dict = {}):
    aql_filters = []

        
    or_conditions = filters.pop("or_conditions", [])
    
    for field, value in filters.items():
        if isinstance(value, list):
            or_conditions.append({field: v} for v in value)
        else:
            aql_filters.append(f"doc.{field} == @{field}")
            bind_vars[field] = value
    
    if or_conditions:
        or_clauses = []
        for i, condition_set in enumerate(or_conditions):
            sub_conditions = []
            for sub_field, sub_value in condition_set.items():
                bind_var_key = f"{sub_field}_or_{i}"
                sub_conditions.append(f"doc.{sub_field} == @{bind_var_key}")
                bind_vars[bind_var_key] = sub_value
            or_clauses.append(" AND ".join(sub_conditions))
        aql_filters.append(f"({' OR '.join(or_clauses)})")
    
    
    return aql_filters, bind_vars