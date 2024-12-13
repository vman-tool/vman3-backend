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
                if (value or type(value) == bool) and not force:
                    old_dict[key] = value
                elif force:
                    old_dict[key] = value

        return old_dict
    except Exception as e:
        print(f"Error occurred while replacing values: {str(e)}")
        return None
    
def add_query_filters(filters: Dict = None, bind_vars: Dict = None, document_name: str = 'doc'):
    """
    Function to build ArangoDB AQL filters and bind variables.
    
    :param filters: Input dictionary of fields and their values.
    :param bind_vars: Bind variable dictionary to be updated.
    :param document_name: AQL document name, default is 'doc'.
    
    :return: aql_filters, bind_vars

    Example:
        > Use `and_conditions` for fields that are actually the same with multiple filters to be able to use some thing like BETWEEN for dates and numbers especially 
        ```
        filters = {
            'and_conditions': [
                {'date': {'>=': '2024-01-01'}},
                {'date': {'<=': '2024-12-31'}} 
            ],
            'or_conditions': [
                {'status': {'==': 'active'}},
                {'type': {'==': 'internal'}},
                {'role': "admin"},
            ],
            'in_conditions': [
                {'role': ['admin', 'manager']},
            ],
            'includes_conditions': [
            {
                'tags': {
                    'and': ['important', 'urgent'],
                    'or': ['project1', 'project2'] 
                }
            },
            {
                'description': {
                    'and': ['key1', 'key2'],
                    'or': ['term1', 'term2']
                }
            }
        ]
        }
        ```
    """
    aql_filters = []
        
    and_conditions = filters.pop("and_conditions", [])
    or_conditions = filters.pop("or_conditions", [])
    in_conditions = filters.pop("in_conditions", [])
    includes_conditions = filters.pop("includes_conditions", [])

    def add_comparison_filter(field: str, value: Any, op: str, condition_type: str, i: int):
        bind_var_key = f"{field}_{condition_type}_{i}"
        aql_filters.append(f"{document_name}.{field} {op} @{bind_var_key}")
        bind_vars[bind_var_key] = value

    
    def generate_includes_filter(field, include_conditions):
        """Generate filter for include conditions with AND/OR logic"""
        sub_filters = []
        
        if 'and' in include_conditions:
            and_values = include_conditions['and']
            and_sub_conditions = []
            for i, value in enumerate(and_values):
                bind_var_key = f"{field}_and_includes_{i}"
                and_sub_conditions.append(f"CONTAINS({document_name}.{field}, @{bind_var_key})")
                bind_vars[bind_var_key] = value
            
            sub_filters.append(f"({' AND '.join(and_sub_conditions)})")
        
        if 'or' in include_conditions:
            or_values = include_conditions['or']
            or_sub_conditions = []
            for i, value in enumerate(or_values):
                bind_var_key = f"{field}_or_includes_{i}"
                or_sub_conditions.append(f"CONTAINS({document_name}.{field}, @{bind_var_key})")
                bind_vars[bind_var_key] = value
            sub_filters.append(f"({' OR '.join(or_sub_conditions)})")
        
        return sub_filters

    def reassign_operation(op):
        operations = {
            '$gt': '>',
            "$lt": '<',
            "$lte": '<=',
            "$eq": '==',
            "$ne": '!=',
            "$gte": '>='
        }
        return operations.get(op, op)

    for field, value in filters.items():
        if isinstance(value, dict):
            for op, comp_value in value.items():
                op = reassign_operation(op)
                add_comparison_filter(field, comp_value, op, 'field', 0)
        else:
            add_comparison_filter(field, value, '==', 'field', 0)

    if and_conditions:
        for i, condition_set in enumerate(and_conditions):
            for field, value in condition_set.items():
                if isinstance(value, dict):
                    for op, comp_value in value.items():
                        op = reassign_operation(op)
                        add_comparison_filter(field, comp_value, op, 'and', i)
                else:
                    add_comparison_filter(field, value, '==', 'and', i)

    if or_conditions:
        or_clauses = []
        for i, condition_set in enumerate(or_conditions):
            sub_conditions = []
            for field, value in condition_set.items():
                if isinstance(value, dict):
                    for op, comp_value in value.items():
                        op = reassign_operation(op)
                        sub_conditions.append(f"{document_name}.{field} {op} @{field}_or_{i}")
                        bind_vars[f"{field}_or_{i}"] = comp_value
                else:
                    sub_conditions.append(f"{document_name}.{field} == @{field}_or_{i}")
                    bind_vars[f"{field}_or_{i}"] = value
            or_clauses.append(f"({' AND '.join(sub_conditions)})")
        aql_filters.append(f"({' OR '.join(or_clauses)})")

    if in_conditions:
        in_clauses = []
        for condition in in_conditions:
            for field, values in condition.items():
                bind_var_key = f"{field}_in_values"
                in_clauses.append(f"{document_name}.{field} IN @{bind_var_key}")
                bind_vars[bind_var_key] = values
            aql_filters.append(" AND ".join(in_clauses))
    
    if includes_conditions:
        includes_clauses = []
        for condition in includes_conditions:
            for field, include_conditions in condition.items():
                if isinstance(include_conditions, dict):
                    field_filters = generate_includes_filter(field, include_conditions)
                    includes_clauses.extend(field_filters)

                elif isinstance(include_conditions, (list, str)):
                    values = include_conditions if isinstance(include_conditions, list) else [include_conditions]
                    sub_conditions = []
                    for i, value in enumerate(values):
                        bind_var_key = f"{field}_includes_{i}"
                        sub_conditions.append(f"CONTAINS({document_name}.{field}, @{bind_var_key})")
                        bind_vars[bind_var_key] = value
                    includes_clauses.append(f"({' OR '.join(sub_conditions)})")
        
        if includes_clauses:
            aql_filters.append(" AND ".join(includes_clauses))

    return aql_filters, bind_vars


