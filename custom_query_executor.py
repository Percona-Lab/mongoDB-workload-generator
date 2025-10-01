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
from mongo_client import get_client


async def execute_user_query_async(args, query_def, fake, generate_random_value):
    """
    Processes placeholders and executes a single user-defined query.

    Args:
        query_def (dict): A dictionary defining the query.
        fake (Faker): The Faker instance for generating provider-based data.
        generate_random_value_func (function): The function for generating type-based data.

    Returns:
        tuple: (operation_type, operation_count, documents_affected).
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
    
    processed_filter = _process_query_with_faker(filter_query, fake, generate_random_value)
    
    op_count = 0
    docs_affected = 0
    op_type = None

    try:
        if args.debug:
            logging.debug(f"\n--- [DEBUG] Running {operation} on: {db_name}.{collection_name} ---")
            logging.debug(f"Filter: {pprint.pformat(processed_filter)}")
            if projection:
                logging.debug(f"Projection: {pprint.pformat(projection)}")
            if update_doc and operation in ["updateOne", "updateMany"]:
                 # We process the update doc just for logging, it will be re-processed below
                processed_update_for_log = _process_query_with_faker(update_doc, fake, generate_random_value)
                logging.debug(f"Update Document: {pprint.pformat(processed_update_for_log)}")

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
            pipeline = _process_query_with_faker(query_def.get("pipeline", []), fake, generate_random_value)
            if args.debug: # Add specific logging for aggregate pipeline
                logging.debug(f"Pipeline: {pprint.pformat(pipeline)}")
            cursor = collection.aggregate(pipeline)
            documents = await cursor.to_list(length=None)
            docs_affected = len(documents)
            op_count = 1
            op_type = "aggregate"

        elif operation == "updateOne":
            processed_update = _process_query_with_faker(update_doc, fake, generate_random_value)
            result = await collection.update_one(processed_filter, processed_update)
            docs_affected = result.modified_count
            op_count = 1
            op_type = "updateOne"

        elif operation == "updateMany":
            processed_update = _process_query_with_faker(update_doc, fake, generate_random_value)
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

def _process_query_with_faker(query, fake, generate_random_value):
    if isinstance(query, dict):
        processed_query = {}
        for key, value in query.items():
            if isinstance(value, str) and value.startswith("faker:"):
                provider_name = value.split(":")[1]
                if provider_name == "bson.ObjectId":
                    processed_query[key] = ObjectId()
                elif hasattr(fake, provider_name):
                    processed_query[key] = getattr(fake, provider_name)()
                else:
                    logging.warning(f"Faker provider '{provider_name}' not found. Using generic value.")
                    processed_query[key] = generate_random_value("string")
            else:
                processed_query[key] = _process_query_with_faker(value, fake, generate_random_value)
        return processed_query
    elif isinstance(query, list):
        return [_process_query_with_faker(item, fake, generate_random_value) for item in query]
    else:
        return query

