from typing import Any, Dict, List
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
    
    if custom_fields:
        
        query = f"""
        RETURN LENGTH(
            FOR doc IN {collection_name}
        """
        if uuid:
            custom_fields['uuid'] = uuid
        if id:
            custom_fields['_key'] = id
        
        aql_filters, bind_vars = add_query_filters(custom_fields, bind_vars=bind_vars)

        if aql_filters:
            query += " FILTER " + " AND ".join(aql_filters)
        
        query += """
            RETURN doc
        ) > 0"""

    elif id and not custom_fields:
        query = f"""
        RETURN LENGTH(
            FOR doc IN {collection_name}
            FILTER doc._key == @id
            RETURN doc
        ) > 0
        """
        bind_vars = {'id': id}

    elif uuid and not custom_fields:
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

def replace_object_values(new_dict: Dict, old_dict: Dict, force: bool = False):
    """
    Use this method to replace the values from old_dict with new_dict

    :param new_dict : Dictionary with values to replace
    :param old_dict : Dictionary with values to be replaced
    :param force: This will replace all keys available in new dictionary without checking for null value
    :Returns the old dictionary with new values
    """
    try:
        for key, value in new_dict.items():
                if value and not force:
                    old_dict[key] = value
                elif force:
                    old_dict[key] = value

        return old_dict
    except Exception as e:
        print(f"Error occurred while replacing values: {str(e)}")
        return None
    
def add_query_filters(filters: Dict = None, bind_vars: Dict = None, document_name: str = 'doc'):
    """
    This function creates a string of query filters for ArangoDB as well as bind variables

    :param filters: Input dictionary of fields and their values (Use actual names of collection fields)
    :param bind_vars: Input dictionary to hold bind variables, input yours to update the already available values
    :param document_name: Input string of the doc variable eg: FOR v in documents, in this case v is the input, by default doc is used

    include array of object inside or_conditions key in filters object to incorporate OR filter
    include array of object inside in_conditions key in filters object to incorporate IN filter
    include field with object that key is in ['==', '!=', '<', '<=', '>', '>=', "$gt", "$lt", "$lte", "$eq", "$ne", "$gte"] for comparison fields

    Returns
    :aql_filters: A list of string filters arranges accordingly
    :bind_vars: A dictionary of bind variables, updated with the values from input filters
    """
    aql_filters = []
        
    or_conditions = filters.pop("or_conditions", [])
    in_conditions = filters.pop("in_conditions", [])

    def add_comparison_filter(field: str, value: Any, op: str):
        aql_filters.append(f"{document_name}.{field} {op} @{field}")
        bind_vars[field] = value

    def reassign_operation(op):
        if op == '$gt':
            op = '>'
        if op == "$lt":
            op = '<'
        if op == "$lte":
            op = '<='
        if op == "$eq":
            op = '=='
        if op == "$ne":
            op = '!='
        if op == "$gte":
            op = '>='        
        return op
    
    for field, value in filters.items():
        if isinstance(value, list):
            or_conditions.append({field: v} for v in value)
        else:
            if isinstance(value, dict):
                for op, comp_value in value.items():
                    if op in ['==', '!=', '<', '<=', '>', '>=', "$gt", "$lt", "$lte", "$eq", "$ne", "$gte"]:
                        op = reassign_operation(op)
                        add_comparison_filter(field, comp_value, op)
                    else:
                        raise ValueError(f"Unsupported operator: {op}")
            else:
                add_comparison_filter(field, value, '==')

    if or_conditions:
        or_clauses = []
        for i, condition_set in enumerate(or_conditions):
            sub_conditions = []
            for sub_field, sub_value in condition_set.items():
                bind_var_key = f"{sub_field}_or_{i}"
                sub_conditions.append(f"{document_name}.{sub_field} == @{bind_var_key}")
                bind_vars[bind_var_key] = sub_value
            or_clauses.append(" AND ".join(sub_conditions))
        aql_filters.append(f"({' OR '.join(or_clauses)})")
    
    if in_conditions:
        in_clauses = []
        for field, values in in_conditions.items():
            bind_var_key = f"{field}_in_values"
            in_clauses.append(f"{document_name}.{field} IN @{bind_var_key}")
            bind_vars[bind_var_key] = values
        aql_filters.append(" AND ".join(in_clauses))
    
    return aql_filters, bind_vars