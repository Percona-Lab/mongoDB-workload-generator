# This file generates dynamic queries for the workload. You can add new query formats to the appropriate function and they'll be randomly chosen
# while the workload is running. 
# The queries below have a mix of "optimized" and "ineffective" queries. The good queries always use the primary/shard key 
# The slow queries do not use the primary/shard key (on purpose) in order to create workload that's not optimal

import random
import json 
import copy

QUERY_TEMPLATE_CACHE = {}

def _fill_template(template, value_map):
    """
    Recursively fills placeholders in a query template by working
    directly with the dictionary, which is much safer than string replacement.
    """
    # Create a deep copy to ensure the cached template is never modified
    query = copy.deepcopy(template)

    # This is a helper function that will walk through the dictionary/list structure
    def _substitute(obj):
        if isinstance(obj, dict):
            # If it's a dict, recurse into its values
            for key, value in obj.items():
                obj[key] = _substitute(value)
        elif isinstance(obj, list):
            # If it's a list, recurse into its items
            for i, item in enumerate(obj):
                obj[i] = _substitute(item)
        elif isinstance(obj, str) and obj in value_map:
            # This is our target: a string that is a key in our value_map
            # We replace the placeholder string with the actual value.
            return value_map[obj]
        
        # Return the object unchanged if it's not a placeholder
        return obj

    return _substitute(query)


# SELECT queries
def select_queries(field_names, field_types, pk_field):
    """
    Generates and caches lists of optimized and ineffective select query TEMPLATES
    and their corresponding PROJECTION templates.
    """
    cache_key = f"select-{pk_field}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    optimized_templates = []
    ineffective_templates = []
    projection_templates = []

    # Base templates use placeholders that will be filled at runtime
    optimized_base = {pk_field: "{pk_value}"}

    for i in range(1, len(field_names)):
        field = field_names[i]
        bson_type = field_types[i]
        # Generic placeholder for the value of the current field
        value_placeholder = f"{{{field}_value}}"

        if bson_type in ["int", "long", "double", "decimal"]:
            # Templates for numeric range queries
            optimized_templates.append({**optimized_base, field: value_placeholder})
            optimized_templates.append({**optimized_base, field: {"$gt": value_placeholder}})
            optimized_templates.append({**optimized_base, field: {"$lt": value_placeholder}})
            optimized_templates.append({**optimized_base, field: {"$gte": value_placeholder, "$lte": f"{{{field}_high_value}}"}})

            ineffective_templates.append({field: value_placeholder})
            ineffective_templates.append({field: {"$gt": value_placeholder}})
            ineffective_templates.append({field: {"$lt": value_placeholder}})
            ineffective_templates.append({field: {"$gte": value_placeholder, "$lte": f"{{{field}_high_value}}"}})

        elif bson_type == "string":
            optimized_templates.append({**optimized_base, field: value_placeholder})
            optimized_templates.append({**optimized_base, field: {"$regex": value_placeholder}})
            
            ineffective_templates.append({field: value_placeholder})
            ineffective_templates.append({field: {"$regex": value_placeholder}})

        elif bson_type == "bool":
            optimized_templates.append({**optimized_base, field: value_placeholder})
            ineffective_templates.append({field: value_placeholder})

        elif bson_type in ["date", "timestamp", "objectId"]:
            optimized_templates.append({**optimized_base, field: value_placeholder})
            ineffective_templates.append({field: value_placeholder})

        elif bson_type == "array":
            optimized_templates.append({**optimized_base, field: {"$in": value_placeholder}})
            ineffective_templates.append({field: {"$in": value_placeholder}})
        
        else: # Fallback for any other types
            optimized_templates.append({**optimized_base, field: value_placeholder})
            ineffective_templates.append({field: value_placeholder})
        
        # Create a projection template for each field combination
        projection_templates.append({pk_field: 1, field: 1, "_id": 0})

    # Add base queries for just the primary key
    optimized_templates.insert(0, {pk_field: "{pk_value}"})
    ineffective_templates.insert(0, {pk_field: {"$exists": True}})
    projection_templates.insert(0, {pk_field: 1, "_id": 0})

    # Store the tuple of lists in the cache
    result = (optimized_templates, ineffective_templates, projection_templates)
    QUERY_TEMPLATE_CACHE[cache_key] = result
    return result

# UPDATE queries
def update_queries(field_names, field_types, primary_key, shard_keys):
    """
    Generates and caches lists of optimized and ineffective update query TEMPLATES.
    This version will NOT generate templates that modify shard key fields.
    """
    # The cache key is now more specific to be safe
    cache_key = f"update-{primary_key}-{'-'.join(shard_keys)}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    optimized_templates = []
    ineffective_templates = []

    optimized_filter = {primary_key: "{pk_value}"}
    ineffective_filter = {}

    for i in range(len(field_names)):
        field = field_names[i]
        ftype = field_types[i]

        # If the field is part of the shard key, skip it and do not create an update template.
        if field in shard_keys:
            continue

        value_placeholder = f"{{{field}_value}}"
        update_op_templates = []

        if ftype in ["int", "long", "double", "decimal"]:
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$inc": {field: f"{{{field}_increment}}"}})
        elif ftype == "bool":
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$set": {field: f"{{{field}_not_value}}"}})
        elif ftype == "string":
            update_op_templates.append({"$set": {field: value_placeholder}})
        elif ftype in ["date", "timestamp"]:
            update_op_templates.append({"$set": {field: value_placeholder}})
        elif ftype == "array":
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$push": {field: {"$each": value_placeholder if isinstance(value_placeholder, list) else [value_placeholder]}}})
        elif ftype == "objectId":
            update_op_templates.append({"$set": {field: value_placeholder}})
        else:
            update_op_templates.append({"$set": {field: value_placeholder}})

        for update_template in update_op_templates:
            optimized_templates.append({"filter": optimized_filter, "update": update_template})
            ineffective_templates.append({"filter": ineffective_filter, "update": update_template})
    
    result = (optimized_templates, ineffective_templates)
    QUERY_TEMPLATE_CACHE[cache_key] = result
    return result

# delete queries
def delete_queries(field_names, field_types, pk_field):
    """
    Generates and caches lists of optimized and ineffective delete query TEMPLATES.
    """
    cache_key = f"delete-{pk_field}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    optimized_templates = []
    ineffective_templates = []

    # Base template for optimized queries
    optimized_base = {pk_field: "{pk_value}"}

    # Add the simplest templates first
    optimized_templates.append({pk_field: "{pk_value}"})
    ineffective_templates.append({})  # For a wide delete_many

    # Iterate through other fields to create more specific templates
    for i in range(len(field_names)):
        field = field_names[i]
        ftype = field_types[i]
        value_placeholder = f"{{{field}_value}}"

        if field == pk_field:
            continue

        if ftype in ["int", "long", "double", "decimal"]:
            optimized_templates.append({**optimized_base, field: value_placeholder})
            optimized_templates.append({**optimized_base, field: {"$gt": value_placeholder}})
            ineffective_templates.append({field: value_placeholder})
            ineffective_templates.append({field: {"$gt": value_placeholder}})

        elif ftype == "string":
            optimized_templates.append({**optimized_base, field: value_placeholder})
            optimized_templates.append({**optimized_base, field: {"$regex": value_placeholder}})
            ineffective_templates.append({field: value_placeholder})
            ineffective_templates.append({field: {"$regex": value_placeholder}})

        elif ftype in ["bool", "date", "timestamp", "objectId"]:
            optimized_templates.append({**optimized_base, field: value_placeholder})
            ineffective_templates.append({field: value_placeholder})

        elif ftype == "array":
            optimized_templates.append({**optimized_base, field: {"$in": value_placeholder}})
            ineffective_templates.append({field: {"$in": value_placeholder}})

        else: # Fallback
            optimized_templates.append({**optimized_base, field: value_placeholder})
            ineffective_templates.append({field: value_placeholder})
    
    # Store the result in the cache
    result = (optimized_templates, ineffective_templates)
    QUERY_TEMPLATE_CACHE[cache_key] = result
    return result

