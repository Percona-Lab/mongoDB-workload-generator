#!/usr/bin/env python3
import json
import logging
import pprint
import re
import sys
import os
import motor # type: ignore
from bson import ObjectId # type: ignore
import asyncio
import random
from mongo_client import get_client

def _process_query_placeholders(query, fake, generate_random_value, known_pk_values, pk_field, field_schema):
    """
    Recursively processes a query object to replace placeholders.
    - If known_pk_values exist, replaces '<pk>' with a cached primary key.
    - If known_pk_values is empty, replaces '<pk>' with a randomly generated value of the correct type.
    - Replaces other placeholders like '<int>' or '<name>' with random data.
    """
    if isinstance(query, dict):
        processed_query = {}
        for key, value in query.items():
            if isinstance(value, str):
                # 1. Check for the special '<pk>' placeholder on the primary key field
                if key == pk_field and value == "<pk>":
                    if known_pk_values:
                        # Ideal case: Use a known key from the cache
                        processed_query[key] = random.choice(known_pk_values)
                    else:
                        # Fallback case: No inserts in workload, generate a random value.
                        # This is now a debug message to reduce noise.
                        logging.debug(f"No keys cached for '{pk_field}'. Falling back to random value generation for '<pk>'.")
                        pk_props = field_schema.get(pk_field, {})
                        pk_type = pk_props.get("type", "string") # Default to string
                        processed_query[key] = generate_random_value(pk_type)

                # 2. Check for 'faker:' syntax
                elif value.startswith("faker:"):
                    provider_name = value.split(":")[1]
                    if provider_name == "bson.ObjectId":
                        processed_query[key] = ObjectId()
                    elif hasattr(fake, provider_name):
                        processed_query[key] = getattr(fake, provider_name)()
                    else:
                        logging.warning(f"Faker provider '{provider_name}' not found. Using original value.")
                        processed_query[key] = value
                # 3. Check for generic '<placeholder>' syntax
                elif value.startswith("<") and value.endswith(">"):
                    placeholder = value[1:-1]
                    if hasattr(fake, placeholder):
                        processed_query[key] = getattr(fake, placeholder)()
                    else:
                        processed_query[key] = generate_random_value(placeholder)
                else:
                    processed_query[key] = value
            else:
                processed_query[key] = _process_query_placeholders(value, fake, generate_random_value, known_pk_values, pk_field, field_schema)
        return processed_query
    elif isinstance(query, list):
        return [_process_query_placeholders(item, fake, generate_random_value, known_pk_values, pk_field, field_schema) for item in query]
    else:
        return query

async def execute_user_query_async(args, query_def, fake, generate_random_value, inserted_keys_cache, primary_keys_map, metadata_cache):
    """
    Processes placeholders and executes a single user-defined query.
    """
    db_name = query_def.get("database")
    collection_name = query_def.get("collection")
    operation = query_def.get("operation")
    filter_query = query_def.get("filter", {})
    update_doc = query_def.get("update")
    projection = query_def.get("projection")

    client = get_client()
    db = client[db_name]
    collection = db[collection_name]

    # Get all necessary metadata for this collection
    primary_key = primary_keys_map.get((db_name, collection_name), "_id")
    known_pk_values = inserted_keys_cache.get((db_name, collection_name), [])
    metadata = metadata_cache.get((db_name, collection_name), {})
    field_schema = metadata.get("field_schema", {})


    # Process all placeholders
    processed_filter = _process_query_placeholders(filter_query, fake, generate_random_value, known_pk_values, primary_key, field_schema)
    processed_update = _process_query_placeholders(update_doc, fake, generate_random_value, known_pk_values, primary_key, field_schema) if update_doc else None

    op_count = 0
    docs_affected = 0
    op_type = None

    try:
        if args.debug:
            logging.debug(f"\n--- [DEBUG] Running {operation} on: {db_name}.{collection_name} ---")
            logging.debug(f"Filter: {pprint.pformat(processed_filter)}")
            if projection:
                logging.debug(f"Projection: {pprint.pformat(projection)}")
            if processed_update and operation in ["updateOne", "updateMany"]:
                logging.debug(f"Update Document: {pprint.pformat(processed_update)}")

        if operation == "find":
            cursor = collection.find(processed_filter, projection)
            documents = await cursor.to_list(length=None)
            docs_affected = len(documents)
            op_count = 1
            op_type = "find"

        elif operation == "count":
            docs_affected = await collection.count_documents(processed_filter)
            op_count = 1
            op_type = "count"

        elif operation == "aggregate":
            pipeline = _process_query_placeholders(query_def.get("pipeline", []), fake, generate_random_value, known_pk_values, primary_key, field_schema)
            if args.debug:
                logging.debug(f"Pipeline: {pprint.pformat(pipeline)}")
            cursor = collection.aggregate(pipeline)
            documents = await cursor.to_list(length=None)
            docs_affected = len(documents)
            op_count = 1
            op_type = "aggregate"

        elif operation == "updateOne":
            result = await collection.update_one(processed_filter, processed_update)
            docs_affected = result.modified_count
            op_count = 1
            op_type = "updateOne"

        elif operation == "updateMany":
            result = await collection.update_many(processed_filter, processed_update)
            docs_affected = result.modified_count
            op_count = 1
            op_type = "updateMany"

        elif operation == "deleteOne":
            result = await collection.delete_one(processed_filter)
            docs_affected = result.deleted_count
            op_count = 1
            op_type = "deleteOne"

        elif operation == "deleteMany":
            result = await collection.delete_many(processed_filter)
            docs_affected = result.deleted_count
            op_count = 1
            op_type = "deleteMany"

        else:
            logging.error(f"Unknown operation: {operation}")

    except Exception as e:
        logging.error(f"Error executing query {query_def}: {e}")

    return op_type, op_count, docs_affected