import random
import json 
import copy

QUERY_TEMPLATE_CACHE = {}

def _fill_template(template, value_map):
    """
    Recursively fills placeholders in a query template by working
    directly with the dictionary, which is much safer than string replacement.
    """
    query = copy.deepcopy(template)
    def _substitute(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                obj[key] = _substitute(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = _substitute(item)
        elif isinstance(obj, str) and obj in value_map:
            return value_map[obj]
        return obj
    return _substitute(query)

def select_queries(field_names, field_types, pk_field, optimized):
    """
    Generates and caches select query templates.
    - Optimized mode: Returns only optimized templates.
    - Unoptimized mode: Returns a mix of optimized and ineffective templates.
    """
    cache_key = f"select-{optimized}-{pk_field}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    optimized_templates = []
    ineffective_templates = []
    projection_templates = []
    optimized_base = {pk_field: "{pk_value}"}

    for i in range(1, len(field_names)):
        field = field_names[i]; bson_type = field_types[i]; value_placeholder = f"{{{field}_value}}"

        # Always generate optimized templates
        if bson_type in ["int", "long", "double", "decimal"]:
            optimized_templates.append({**optimized_base, field: value_placeholder})
            optimized_templates.append({**optimized_base, field: {"$gt": value_placeholder}})
            optimized_templates.append({**optimized_base, field: {"$lt": value_placeholder}})
            optimized_templates.append({**optimized_base, field: {"$gte": value_placeholder, "$lte": f"{{{field}_high_value}}"}})
        elif bson_type == "string":
            optimized_templates.append({**optimized_base, field: value_placeholder})
            optimized_templates.append({**optimized_base, field: {"$regex": value_placeholder}})
        elif bson_type in ["bool", "date", "timestamp", "objectId"]:
            optimized_templates.append({**optimized_base, field: value_placeholder})
        elif bson_type == "array":
            optimized_templates.append({**optimized_base, field: {"$in": value_placeholder}})
        else:
            optimized_templates.append({**optimized_base, field: value_placeholder})

        # Only generate ineffective templates if NOT in optimized mode
        if not optimized:
            if bson_type in ["int", "long", "double", "decimal"]:
                ineffective_templates.append({field: value_placeholder})
                ineffective_templates.append({field: {"$gt": value_placeholder}})
                ineffective_templates.append({field: {"$lt": value_placeholder}})
                ineffective_templates.append({field: {"$gte": value_placeholder, "$lte": f"{{{field}_high_value}}"}})
            elif bson_type == "string":
                ineffective_templates.append({field: value_placeholder})
                ineffective_templates.append({field: {"$regex": value_placeholder}})
            elif bson_type in ["bool", "date", "timestamp", "objectId"]:
                ineffective_templates.append({field: value_placeholder})
            elif bson_type == "array":
                ineffective_templates.append({field: {"$in": value_placeholder}})
            else:
                ineffective_templates.append({field: value_placeholder})
        
        projection_templates.append({pk_field: 1, field: 1, "_id": 0})

    # Add base queries
    optimized_templates.insert(0, {pk_field: "{pk_value}"})
    if not optimized:
        ineffective_templates.insert(0, {pk_field: {"$exists": True}})
    projection_templates.insert(0, {pk_field: 1, "_id": 0})

    # Combine lists for unoptimized mode
    final_templates = optimized_templates + ineffective_templates
    
    result = (final_templates, projection_templates)
    QUERY_TEMPLATE_CACHE[cache_key] = result
    return result

def update_queries(field_names, field_types, primary_key, shard_keys, optimized):
    """
    Generates and caches update query templates.
    - Optimized mode: Returns only optimized templates.
    - Unoptimized mode: Returns a mix of optimized and ineffective templates.
    """
    cache_key = f"update-{optimized}-{primary_key}-{'-'.join(shard_keys)}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    templates = []
    optimized_filter = {primary_key: "{pk_value}"}
    ineffective_filter = {}

    for i in range(len(field_names)):
        field, ftype = field_names[i], field_types[i]
        if field in shard_keys: continue

        value_placeholder = f"{{{field}_value}}"
        update_op_templates = []

        if ftype in ["int", "long", "double", "decimal"]:
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$inc": {field: f"{{{field}_increment}}"}})
        elif ftype == "bool":
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$set": {field: f"{{{field}_not_value}}"}})
        elif ftype == "array":
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$push": {field: {"$each": value_placeholder}}})
        else: # Covers string, date, timestamp, objectId, and others
            update_op_templates.append({"$set": {field: value_placeholder}})

        for update_template in update_op_templates:
            # Always add the optimized version
            templates.append({"filter": optimized_filter, "update": update_template})
            # If not in optimized mode, also add the ineffective version
            if not optimized:
                templates.append({"filter": ineffective_filter, "update": update_template})
    
    QUERY_TEMPLATE_CACHE[cache_key] = templates
    return templates

def delete_queries(field_names, field_types, pk_field, optimized):
    """
    Generates and caches delete query templates.
    - Optimized mode: Returns only optimized templates.
    - Unoptimized mode: Returns a mix of optimized and ineffective templates.
    """
    cache_key = f"delete-{optimized}-{pk_field}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    templates = []
    optimized_base = {pk_field: "{pk_value}"}
    
    # Always add the base optimized query
    templates.append({pk_field: "{pk_value}"})
    # If not in optimized mode, add the base ineffective query
    if not optimized:
        templates.append({})

    for i in range(len(field_names)):
        field, ftype = field_names[i], field_types[i]
        if field == pk_field: continue
        
        value_placeholder = f"{{{field}_value}}"
        
        # Always generate optimized templates
        if ftype in ["int", "long", "double", "decimal"]:
            templates.append({**optimized_base, field: value_placeholder})
            templates.append({**optimized_base, field: {"$gt": value_placeholder}})
        elif ftype == "string":
            templates.append({**optimized_base, field: value_placeholder})
            templates.append({**optimized_base, field: {"$regex": value_placeholder}})
        else:
            templates.append({**optimized_base, field: value_placeholder})
        
        # Only generate ineffective templates if NOT in optimized mode
        if not optimized:
            if ftype in ["int", "long", "double", "decimal"]:
                templates.append({field: value_placeholder})
                templates.append({field: {"$gt": value_placeholder}})
            elif ftype == "string":
                templates.append({field: value_placeholder})
                templates.append({field: {"$regex": value_placeholder}})
            else:
                templates.append({field: value_placeholder})
                
    QUERY_TEMPLATE_CACHE[cache_key] = templates
    return templates