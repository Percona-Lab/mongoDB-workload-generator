#!/usr/bin/env python3
import pymongo
import json
import logging
from bson import json_util
import copy
import pprint
import args as args_module
import os

import mongo_client
mongo_client.init()

def load_queries_from_path(path):
    """
    Loads queries from a specific file or all .json files in a directory.

    Args:
        path (str): The path to a file or directory.

    Returns:
        list: A consolidated list of all query definitions, or None on error.
    """
    all_queries = []
    files_to_load = []

    if not os.path.exists(path):
        logging.error(f"User query path does not exist: {path}")
        return None

    # Handle a single file
    if os.path.isfile(path):
        if path.endswith('.json'):
            files_to_load.append(path)
        else:
            logging.error(f"Provided path is not a JSON file: {path}")
            return None
            
    # Handle a directory
    elif os.path.isdir(path):
        for filename in os.listdir(path):
            if filename.endswith('.json'):
                files_to_load.append(os.path.join(path, filename))

    if not files_to_load:
        logging.warning(f"No .json query files found in path: {path}")
        return []

    # Load from all identified files
    for f_path in files_to_load:
        try:
            with open(f_path, 'r') as f:
                queries = json.load(f, object_hook=json_util.object_hook)
                if isinstance(queries, list):
                    all_queries.extend(queries)
                    logging.info(f"Successfully loaded {len(queries)} queries from {f_path}.")
                else:
                    logging.warning(f"Skipping file '{f_path}': content is not a JSON list.")
        except Exception as e:
            logging.error(f"Failed to load or parse query file {f_path}: {e}")
            # Continue to next file instead of failing the whole batch
            continue
            
    return all_queries

def _resolve_placeholder(placeholder_value, fake, generate_random_value_func):
    """
    Resolves a placeholder by first checking for a known type, then for a provider.
    """
    # List of known BSON types from your generate_random_value function
    known_types = [
        "string", "int", "double", "bool", "date", "objectId", 
        "array", "object", "timestamp", "long", "decimal"
    ]

    # 1. Check if the placeholder is a known BSON type
    if placeholder_value in known_types:
        return generate_random_value_func(placeholder_value)
    
    # 2. If not a known type, assume it's a Faker provider
    else:
        faker_func = getattr(fake, placeholder_value, None)
        if callable(faker_func):
            return faker_func()
        else:
            # 3. If it's neither, log a warning and return the placeholder
            logging.warning(f"Unknown type or provider '{placeholder_value}'. Keeping placeholder.")
            return f"<{placeholder_value}>"

def _process_placeholders(data, fake, generate_random_value_func):
    """
    Recursively traverses a data structure to find and replace placeholders.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = _process_placeholders(value, fake, generate_random_value_func)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            data[i] = _process_placeholders(item, fake, generate_random_value_func)
    elif isinstance(data, str) and data.startswith('<') and data.endswith('>'):
        placeholder_value = data[1:-1]
        return _resolve_placeholder(placeholder_value, fake, generate_random_value_func)
            
    return data

def execute_user_query(args, query_def, fake, generate_random_value_func):
    """
    Processes placeholders and executes a single user-defined query.

    Args:
        query_def (dict): A dictionary defining the query.
        fake (Faker): The Faker instance for generating provider-based data.
        generate_random_value_func (function): The function for generating type-based data.

    Returns:
        tuple: (operation_type, operation_count, documents_affected).
    """
    processed_query = copy.deepcopy(query_def)
    _process_placeholders(processed_query, fake, generate_random_value_func)

    client = mongo_client.get_client()
    db_name = processed_query.get("database")
    collection_name = processed_query.get("collection")
    operation = processed_query.get("operation")

    if not all([db_name, collection_name, operation]):
        logging.warning(f"Skipping invalid query (missing db, collection, or operation): {processed_query}")
        return None, 0, 0

    # --- DEBUG LOGGING FOR USER QUERIES ---
    if args.debug:
        # ---- ADD THIS LINE FOR DIAGNOSTICS ----
        # logging.debug(f"Executing query on client connected to: {client.HOST}:{client.PORT}")
        # ---------------------------------------
        
        logging.debug(f"\n--- [DEBUG] Running USER QUERY on: {db_name}.{collection_name} ---")
        logging.debug(f"Operation: {operation}")
        for key, value in processed_query.items():
            if key not in ["database", "collection", "operation"]:
                logging.debug(f"{key.capitalize()}: {pprint.pformat(value)}")

    collection = client[db_name][collection_name]
    op_type, docs_affected = None, 0

    try:
        results = None
        if operation == "find":
            op_type = "select"
            cursor = collection.find(processed_query.get("filter", {}), processed_query.get("projection"))
            if "limit" in processed_query:
                cursor = cursor.limit(processed_query["limit"])
            results = list(cursor)
            docs_affected = len(results)

        elif operation == "insertOne":
            op_type = "insert"
            result = collection.insert_one(processed_query["document"])
            docs_affected = 1 if result.inserted_id else 0
            results = {"inserted_id": result.inserted_id}

        elif operation == "updateOne":
            op_type = "update"
            result = collection.update_one(processed_query["filter"], processed_query["update"])
            docs_affected = result.modified_count
            results = {"matched_count": result.matched_count, "modified_count": result.modified_count}
        
        elif operation == "updateMany":
            op_type = "update"
            result = collection.update_many(processed_query["filter"], processed_query["update"])
            docs_affected = result.modified_count
            results = {"matched_count": result.matched_count, "modified_count": result.modified_count}

        elif operation == "deleteOne":
            op_type = "delete"
            result = collection.delete_one(processed_query["filter"])
            docs_affected = result.deleted_count
            results = {"deleted_count": result.deleted_count}

        elif operation == "deleteMany":
            op_type = "delete"
            result = collection.delete_many(processed_query["filter"])
            docs_affected = result.deleted_count
            results = {"deleted_count": result.deleted_count}

        elif operation == "aggregate":
            op_type = "select"
            cursor = collection.aggregate(processed_query["pipeline"])
            results = list(cursor)
            docs_affected = len(results)

        else:
            logging.warning(f"Unsupported operation '{operation}' in query file.")
            return None, 0, 0
        
        if args.debug:
            logging.debug(f"Result: {pprint.pformat(results)}")
            logging.debug("---------------------------------------------------\n")

        return op_type, 1, docs_affected

    except pymongo.errors.PyMongoError as e:
        logging.error(f"PyMongo error on {db_name}.{collection_name} with query {processed_query}: {e}")
    except KeyError as e:
        logging.error(f"Missing key '{e}' in query definition for operation '{operation}': {processed_query}")
    except Exception as e:
        logging.error(f"Unexpected error executing user query {processed_query}: {e}")

    return None, 0, 0