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
    - Optimized mode: Returns ONLY primary key equality queries.
    - Unoptimized mode: Returns a mix of optimized and ineffective templates.
    """
    cache_key = f"select-{optimized}-{pk_field}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    if optimized:
        # In optimized mode, ONLY generate the fastest possible query: a point lookup on the primary key.
        final_templates = [{pk_field: "{pk_value}"}]
        projection_templates = [{pk_field: 1, "_id": 0}]
        result = (final_templates, projection_templates)
        QUERY_TEMPLATE_CACHE[cache_key] = result
        return result

    # --- This block now only runs in UNOPTIMIZED mode ---
    optimized_templates = []
    ineffective_templates = []
    projection_templates = []
    optimized_base = {pk_field: "{pk_value}"}

    for i in range(len(field_names)):
        if field_names[i] == pk_field: continue
        field = field_names[i]; bson_type = field_types[i]; value_placeholder = f"{{{field}_value}}"

        # Generate compound index queries (still relatively optimized)
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

        # Generate ineffective templates (collection scans)
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
    - Optimized mode: The filter always uses the primary key.
    - Unoptimized mode: Includes filters that may cause collection scans.
    """
    cache_key = f"update-{optimized}-{primary_key}-{'-'.join(shard_keys)}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    templates = []
    # This filter is already the most optimal way to target a single document.
    optimized_filter = {primary_key: "{pk_value}"}
    # This filter will target multiple documents, forcing a different execution plan.
    ineffective_filter = {}

    for i in range(len(field_names)):
        field, ftype = field_names[i], field_types[i]
        if field in shard_keys or field == primary_key: continue

        value_placeholder = f"{{{field}_value}}"
        update_op_templates = []

        if ftype in ["int", "long", "double", "decimal"]:
            update_op_templates.append({"$set": {field: value_placeholder}})
            update_op_templates.append({"$inc": {field: f"{{{field}_increment}}"}})
        elif ftype == "bool":
            update_op_templates.append({"$set": {field: value_placeholder}})
        elif ftype == "array":
            update_op_templates.append({"$push": {field: value_placeholder}})
        else: # Covers string, date, timestamp, objectId, and others
            update_op_templates.append({"$set": {field: value_placeholder}})

        for update_template in update_op_templates:
            # Always add the optimized version (targets one doc)
            templates.append({"filter": optimized_filter, "update": update_template})

            # If NOT in optimized mode, also add the ineffective version (targets many docs)
            if not optimized:
                # To make this truly un-optimized, we set a filter that doesn't use the PK
                ineffective_filter[field] = value_placeholder
                templates.append({"filter": ineffective_filter, "update": update_template})
                ineffective_filter = {} # Reset for next loop

    QUERY_TEMPLATE_CACHE[cache_key] = templates
    return templates

def delete_queries(field_names, field_types, pk_field, optimized):
    """
    Generates and caches delete query templates.
    - Optimized mode: Returns ONLY primary key equality queries.
    - Unoptimized mode: Returns a mix of optimized and ineffective templates.
    """
    cache_key = f"delete-{optimized}-{pk_field}-{'-'.join(field_names)}"
    if cache_key in QUERY_TEMPLATE_CACHE:
        return QUERY_TEMPLATE_CACHE[cache_key]

    if optimized:
        # In optimized mode, ONLY generate the fastest possible query: a point lookup on the primary key.
        templates = [{pk_field: "{pk_value}"}]
        QUERY_TEMPLATE_CACHE[cache_key] = templates
        return templates

    # --- This block now only runs in UNOPTIMIZED mode ---
    templates = []
    optimized_base = {pk_field: "{pk_value}"}

    # Always add the base optimized query
    templates.append({pk_field: "{pk_value}"})
    # Add the base ineffective query
    templates.append({})

    for i in range(len(field_names)):
        field, ftype = field_names[i], field_types[i]
        if field == pk_field: continue

        value_placeholder = f"{{{field}_value}}"

        # Generate compound index queries (still relatively optimized)
        if ftype in ["int", "long", "double", "decimal"]:
            templates.append({**optimized_base, field: value_placeholder})
            templates.append({**optimized_base, field: {"$gt": value_placeholder}})
        elif ftype == "string":
            templates.append({**optimized_base, field: value_placeholder})
            templates.append({**optimized_base, field: {"$regex": value_placeholder}})
        else:
            templates.append({**optimized_base, field: value_placeholder})

        # Generate ineffective templates (collection scans)
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