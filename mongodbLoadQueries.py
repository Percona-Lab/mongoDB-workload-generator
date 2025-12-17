import random
# This file generates dynamic queries for the workload. You can add new query formats to the appropriate function and they'll be randomly chosen
# while the workload is running. 
# The queries below have a mix of "optimized" and "ineffective" queries. The good queries always use the primary/shard key 
# The slow queries do not use the primary/shard key (on purpose) in order to create workload that's not optimal

# SELECT queries
def select_queries(param_list, field_names, field_types):
    """
    Generate lists of optimized and ineffective select queries.
    """
    optimized_queries = []
    ineffective_queries = []
    query_projections = []

    if not param_list or not field_names or len(param_list) != len(field_names) or len(field_types) != len(field_names):
        return [], [], []

    pk_value = param_list[0]
    pk_field = field_names[0]

    for i in range(1, len(param_list)):
        field = field_names[i]
        value = param_list[i]
        bson_type = field_types[i]

        # OPTIMIZED queries always include PK
        base_query = {pk_field: pk_value}
        
        if bson_type in ["int", "long", "double", "decimal"]:
            # Generate a high value to be used by gte lte queries:
            increment = random.randint(1, 100000)  # Random int between 1 and 100000
            high_value = value + increment
            # Numeric range queries
            optimized_queries.append({**base_query, field: value})
            optimized_queries.append({**base_query, field: {"$gt": value}})
            optimized_queries.append({**base_query, field: {"$lt": value}})
            optimized_queries.append({**base_query, field: {"$gte": value, "$lte": high_value}})

            ineffective_queries.append({field: value})
            ineffective_queries.append({field: {"$gt": value}})
            ineffective_queries.append({field: {"$lt": value}})
            ineffective_queries.append({field: {"$gte": value, "$lte": high_value}})

        elif bson_type == "string":
            optimized_queries.append({**base_query, field: value})
            optimized_queries.append({**base_query, field: {"$regex": value}})
            
            ineffective_queries.append({field: value})
            ineffective_queries.append({field: {"$regex": value}})

        elif bson_type == "bool":
            optimized_queries.append({**base_query, field: value})
            ineffective_queries.append({field: value})

        elif bson_type in ["date", "timestamp"]:
            optimized_queries.append({**base_query, field: value})
            ineffective_queries.append({field: value})

        elif bson_type == "objectId":
            optimized_queries.append({**base_query, field: value})

        elif bson_type == "array":
            optimized_queries.append({**base_query, field: {"$in": value}})
            ineffective_queries.append({field: {"$in": value}})

        else:
            # Fallback to exact match
            optimized_queries.append({**base_query, field: value})
            ineffective_queries.append({field: value})

        # projection for the field and pk
        query_projections.append({pk_field: 1, field: 1, "_id": 0})

    # Base queries
    optimized_queries.insert(0, {pk_field: pk_value})
    ineffective_queries.insert(0, {pk_field: {"$exists": True}})
    query_projections.insert(0, {pk_field: 1, "_id": 0})

    return optimized_queries, ineffective_queries, query_projections

# UPDATE queries
def update_queries(field_names, values, field_types, primary_key, pk_value):
    """
    Generate lists of optimized and ineffective update queries.
    Each query dict contains "filter" and "update" keys.
    """
    optimized_updates = []
    ineffective_updates = []

    if not (field_names and values and field_types) or not (len(field_names) == len(values) == len(field_types)):
        return [], []

    for i in range(len(field_names)):
        field = field_names[i]
        value = values[i]
        ftype = field_types[i]

        if value is None:
            continue

        updates_for_field = []

        if ftype in ["int", "long", "double", "decimal"]:
            updates_for_field.append({"$set": {field: value}})
            increment = random.randint(1, 100)
            updates_for_field.append({"$inc": {field: increment}})

        elif ftype == "string":
            updates_for_field.append({"$set": {field: value}})

        elif ftype == "bool":
            if isinstance(value, bool):
                updates_for_field.append({"$set": {field: not value}})
                updates_for_field.append({"$set": {field: value}})
            else:
                updates_for_field.append({"$set": {field: bool(value)}})

        elif ftype in ["date", "timestamp"]:
            updates_for_field.append({"$set": {field: value}})

        elif ftype == "array":
            updates_for_field.append({"$set": {field: value}})
            updates_for_field.append({"$push": {field: {"$each": value if isinstance(value, list) else [value]}}})

        elif ftype == "objectId":
            updates_for_field.append({"$set": {field: value}})

        else:
            updates_for_field.append({"$set": {field: value}})

        # Build optimized and ineffective update queries with filters
        for update_op in updates_for_field:
            # Defensive: ensure update operators exist
            if not any(k.startswith("$") for k in update_op.keys()):
                update_op = {"$set": update_op}

            optimized_updates.append({
                "filter": {primary_key: pk_value},
                "update": update_op
            })

            ineffective_updates.append({
                "filter": {},  # No primary key filter
                "update": update_op
            })

    return optimized_updates, ineffective_updates


def delete_queries(param_list, field_names, field_types, primary_key_name, primary_key_value): # Added pk_name, pk_value
    """
    Generate optimized and ineffective delete queries.
    Optimized queries always include the primary key.
    Ineffective queries omit the primary key.
    """
    optimized_queries = []
    ineffective_queries = []

    if not param_list or not field_names or not field_types or \
       len(param_list) != len(field_names) or len(field_types) != len(field_names):
        return [], []

    # The primary key and its value should be explicitly passed
    # as they are the cornerstone for optimized deletes.
    pk_field = primary_key_name
    pk_value = primary_key_value

    # Always add a simple primary key only delete to optimized queries
    optimized_queries.append({pk_field: pk_value})
    # Add an empty filter for ineffective for delete_many
    ineffective_queries.append({})


    # Iterate through other fields to create more specific queries
    # We use field_names directly, param_list contains values in the same order
    for i in range(len(field_names)):
        field = field_names[i]
        value = param_list[i] # param_list has the values
        ftype = field_types[i]

        # Skip the primary key field itself as it's handled above or by direct filters
        if field == pk_field:
            continue

        # Base for optimized queries: always include the primary key
        optimized_base_filter = {pk_field: pk_value}

        if ftype in ["int", "long", "double", "decimal"]:
            # Optimized numeric exact and range deletes
            optimized_queries.append({**optimized_base_filter, field: value})
            optimized_queries.append({**optimized_base_filter, field: {"$gt": value}})
            optimized_queries.append({**optimized_base_filter, field: {"$lt": value}})

            # Ineffective numeric deletes (no primary key)
            ineffective_queries.append({field: value})
            ineffective_queries.append({field: {"$gt": value}})
            ineffective_queries.append({field: {"$lt": value}})

        elif ftype == "string":
            # Optimized string exact and regex deletes
            optimized_queries.append({**optimized_base_filter, field: value})
            optimized_queries.append({**optimized_base_filter, field: {"$regex": value}})

            # Ineffective string deletes (no primary key)
            ineffective_queries.append({field: value})
            ineffective_queries.append({field: {"$regex": value}})

        elif ftype == "bool":
            # Optimized boolean exact match
            optimized_queries.append({**optimized_base_filter, field: value})

            # Ineffective boolean exact match
            ineffective_queries.append({field: value})

        elif ftype in ["date", "timestamp"]:
            # Optimized date exact match
            optimized_queries.append({**optimized_base_filter, field: value})

            # Ineffective date exact match
            ineffective_queries.append({field: value})

        elif ftype == "objectId":
            # Optimized ObjectId exact match
            optimized_queries.append({**optimized_base_filter, field: value})

            # Ineffective ObjectId exact match
            ineffective_queries.append({field: value}) # Still want these for non-optimized runs

        elif ftype == "array":
            # Optimized array deletes
            if isinstance(value, list) and value: # Ensure value is a non-empty list
                 optimized_queries.append({**optimized_base_filter, field: {"$in": value}})
            else: # If not a list, treat as single item for $in
                 optimized_queries.append({**optimized_base_filter, field: {"$in": [value]}})

            # Ineffective array deletes
            if isinstance(value, list) and value:
                ineffective_queries.append({field: {"$in": value}})
            else:
                ineffective_queries.append({field: {"$in": [value]}})

        else:
            # Fallback exact match for other types
            optimized_queries.append({**optimized_base_filter, field: value})
            ineffective_queries.append({field: value})

    return optimized_queries, ineffective_queries

