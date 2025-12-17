#!/usr/bin/env python3
# from args import args
import pymongo # type: ignore
from datetime import datetime
import random
import string
import bson # type: ignore
from faker import Faker # type: ignore
from customProvider import CustomProvider # Custom providers
import time
import threading
import logging
import textwrap
import pprint
import signal
import sys
import re
from urllib.parse import urlencode  # Properly format URL parameters
from multiprocessing import Lock
from mongodbCreds import dbconfig 
from mongo_client import get_client
import args as args_module


import custom_query_executor

# Initialize MongoClient once globally.
import mongo_client
mongo_client.init()

# Global stop event for clean shutdown
stop_event = threading.Event()

# Import the query module, this has all select queries this script will randomly execute
import mongodbLoadQueries  

# Initialize a lock for logging
log_lock = Lock()

fake = Faker()
fake.add_provider(CustomProvider) # Add our custom providers

process_id = 0
insert_count = 0
update_count = 0
delete_count = 0
select_count = 0
collection_primary_keys = {}       # (db, collection) primary key field
inserted_primary_keys = {}        # (db, collection) list of primary key values
collection_shard_metadata = {}
docs_deleted = 0
docs_inserted = 0
docs_updated = 0
docs_selected = 0
lock = threading.Lock()

def handle_exit(signum, frame):
    """Handle Ctrl+C gracefully by signaling threads to stop."""
    print("\n[!] Ctrl+C detected! Stopping workload...")
    stop_event.set()  # Signal all threads to stop
    sys.exit(0)

letters = string.ascii_lowercase
def random_string(max_length):
    return ''.join(random.choice(letters) for _ in range(max_length))

def generate_aircraft_context():
    plane_type, total_seats, num_passengers, seats_available = fake.aircraft_and_seats()
    return {
        "plane_type": plane_type,
        "total_seats": total_seats,
        "num_passengers": num_passengers,
        "seats_available": seats_available
    }

def requires_aircraft_context(field_schema):
    needed_methods = {"passengers", "equip", "total_seats", "seats_available"}
    for props in field_schema.values():
        provider = props.get("provider")
        if provider in needed_methods:
            return True
    # seats_available may be a field without a provider, so check explicitly
    if "seats_available" in field_schema:
        return True
    return False

######################################################################
# Function to prepend shard key to each index if not already included
######################################################################
def prepend_shard_key_to_index(index_keys, shard_key):
    updated_keys = []
    for sk_field, sk_order in shard_key.items():
        updated_keys.append((sk_field, sk_order))

    for field, order in index_keys.items():
        if field not in shard_key:
            updated_keys.append((field, order))
    return updated_keys

###################################################
# Function to find out the collection's primary key
###################################################
def get_primary_key_from_collection(coll):
    # Check if shard key is defined and exists in fieldName
    shard_key = coll.get("shardConfig", {}).get("key")
    fields = coll.get("fieldName", {})

    if shard_key:
        for key_field in shard_key.keys():
            if key_field in fields:
                return key_field
            else:
                logging.warning(f"Shard key '{key_field}' not found in fieldName.")
    
    # Check for explicit primary key markers
    for field, props in fields.items():
        if props.get("isPrimaryKey") or props.get("unique") is True:
            return field

    # Fall back to "_id" only if no other option
    # logging.warning("Falling back to '_id' as primary key (no shardKey or unique key found in schema)")
    return "_id"

#####################################################################
# Inspects the collection being used and adds to a list the following
# - Whether it is sharded
# - What the shard keys are (if sharded)
#####################################################################
def collect_shard_key_metadata(random_db,random_collection):
    global collection_shard_metadata  
    collection_shard_metadata = {}   
    client = get_client()
    db = client[random_db]
    
    ns = f"{random_db}.{random_collection}"

    try:
        # First, check if the collection is sharded
        coll_stats = db.command("collstats", random_collection)
        is_sharded = coll_stats.get("sharded", False)

        shard_keys = []

        if is_sharded:
            # Get shard key info
            config_db = client["config"]
            sharding_info = config_db["collections"].find_one({"_id": ns})
            if sharding_info and "key" in sharding_info:
                shard_keys = list(sharding_info["key"].keys())

        # Store in global metadata dictionary
        collection_shard_metadata[(random_db, random_collection)] = {
            "sharded": is_sharded,
            "shard_keys": shard_keys
        }

    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error retrieving shard metadata for {ns}: {e}")


####################
# Create collections
####################
def create_collection(collection_def, collections=1, recreate=False):
    created_collections = []
    global collection_primary_keys

    # Normalize input: if a single dict is passed, wrap in list
    if isinstance(collection_def, dict):
        collection_def = [collection_def]

    for entry in collection_def:
        base_collection_name = entry["collectionName"]
        db_name = entry["databaseName"]
        indexes = entry.get("indexes", [])
        shard_config = entry.get("shardConfig")

        client = get_client()
        db = client[db_name]

        for i in range(1, collections + 1):
            collection_name = f"{base_collection_name}_{i}" if collections > 1 else base_collection_name
            collection = db[collection_name]

            try:
                if recreate and collection_name in db.list_collection_names():
                    collection.drop()

                if collection_name not in db.list_collection_names():
                    db.create_collection(collection_name)
                    logging.info(f"Collection '{collection_name}' created in DB '{db_name}'")

                    if not dbconfig.get("replicaSet") and shard_config:
                        shard_collection(db_name, collection_name, shard_config)

                    primary_key_field = None
                    for index in indexes:
                        index_keys = index["keys"]
                        options = index.get("options", {})
                        keys = list(index_keys.items())

                        # Determine primary key from first unique index
                        if options.get("unique", False) and keys and not primary_key_field:
                            primary_key_field = keys[0][0]

                        # logging.info(f"Creating index on {collection_name} with keys={keys}, options={options}")
                        try:
                            index_name = collection.create_index(keys, **options)
                            logging.info(f"Successfully created index: '{index_name}'")
                        except Exception as e:
                            logging.error(f"Failed to create index {keys} on {collection_name}: {e}")

                    if not primary_key_field:
                        primary_key_field = "_id"

                    collection_primary_keys[(db_name, collection_name)] = primary_key_field
                created_collections.append((db_name, collection_name))

            except pymongo.errors.PyMongoError as e:
                        logging.error(f"Error creating collection '{collection_name}': {e}")

    return created_collections

###################
# Shard collections
###################
def shard_collection(db_name, collection_name, shard_config):
    client = get_client()
    try:
        keys = list(shard_config["key"].items())
        client[db_name][collection_name].create_index(keys)
        client.admin.command("enableSharding", db_name)
        client.admin.command("shardCollection", f"{db_name}.{collection_name}", key=shard_config["key"])
        logging.info(f"Sharding configured for '{db_name}.{collection_name}' with key {shard_config['key']}")
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error sharding collection '{db_name}.{collection_name}': {e}")

########################
# Random value generator
########################
def generate_random_value(type_val):
    """A simple helper to generate a random value based on a BSON type string."""
    match type_val:
        case "string":
            return fake.word()
        case "int":
            return random.randint(1, 10000)
        case "double":
            return round(random.uniform(10.0, 10000.0), 2)
        case "bool":
            return random.choice([True, False])
        case "date":
            return fake.date_time()
        case "objectId":
            return bson.ObjectId()
        case "array":
            # This is the generic fallback for an array
            return [fake.word() for _ in range(random.randint(1, 3))]
        case "object":
            # This is the generic fallback for an object
            return {"randomKey": fake.word()}
        case "timestamp":
            return datetime.utcnow()
        case "long":
            return random.randint(10000000000, 99999999999)
        case "decimal":
            return bson.Decimal128(str(round(random.uniform(0.1, 9999.99), 2)))
        case _:
            return None

##################################################
# Create random data based on datatype an provider
##################################################
def generate_random_document(field_schema, context=None):
    doc = {}
    context = context or {}

    for field, props in field_schema.items():
        provider = props.get("provider")
        
        # 1. Prioritize using the provider if it's defined in the schema
        if provider:
            # Handle special context-aware providers from your original code
            if provider == "passengers":
                doc[field] = fake.passengers(
                    total_seats=context.get("total_seats", 100),
                    num_passengers=context.get("num_passengers", 10),
                    fake=fake
                )
            elif provider == "equip":
                doc[field] = fake.equip(
                    context.get("plane_type", "Airbus A320"),
                    context.get("total_seats", 100)
                )
            elif provider == "total_seats":
                doc[field] = str(context.get("total_seats", 100))
            elif provider == "seats_available":
                doc[field] = context.get("seats_available", 0)
            
            # Handle all other normal providers (like drivers, rental_info, etc.)
            else:
                provider_func = getattr(fake, provider, None)
                if callable(provider_func):
                    doc[field] = provider_func()
                else:
                    logging.warning(f"Provider '{provider}' not found for field '{field}'.")
                    doc[field] = None
        
        # 2. If NO provider is specified, fall back to the basic type
        else:
            bson_type = props.get("type", "string")
            doc[field] = generate_random_value(bson_type)

    # Add a final safety check for context fields that might not have a provider
    if "seats_available" in field_schema and "seats_available" not in doc:
        doc["seats_available"] = context.get("seats_available", 0)

    return doc

################
# CRUD Functions
################

##############
# Insert Docs
##############
def insert_documents(args,base_collection, random_db, random_collection, collection_def, batch_size=10):
    global insert_count, docs_inserted, inserted_primary_keys, collection_primary_keys

    documents = []
    collection = get_client()[random_db][random_collection]

    coll_entry = next(
        (item for item in collection_def
         if item.get("databaseName") == random_db and item.get("collectionName") == base_collection),
        None
    )
    if not coll_entry:
        logging.error(f"No schema definition found for {random_db}.{base_collection}")
        return

    field_schema = coll_entry.get("fieldName", {})
    primary_key = get_primary_key_from_collection(coll_entry)
    collection_primary_keys[(random_db, random_collection)] = primary_key

    need_context = requires_aircraft_context(field_schema)

    for _ in range(batch_size):
        context = generate_aircraft_context() if need_context else {}
        doc = generate_random_document(field_schema, context=context)

        if primary_key != "_id" and primary_key not in doc:
            pk_type = field_schema.get(primary_key, {}).get("type", "string")
            doc[primary_key] = generate_random_value(pk_type)

        documents.append(doc)

    try:
        result = collection.insert_many(documents)
        with lock:
            insert_count += 1
            docs_inserted += len(result.inserted_ids)

            if (random_db, random_collection) not in inserted_primary_keys:
                inserted_primary_keys[(random_db, random_collection)] = []

            if primary_key == "_id":
                inserted_primary_keys[(random_db, random_collection)].extend(result.inserted_ids)
            else:
                inserted_primary_keys[(random_db, random_collection)].extend(
                    doc[primary_key] for doc in documents if primary_key in doc
                )

    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error inserting documents into {random_db}.{random_collection}: {e}")


##############
# Select Docs
##############
def select_documents(args, base_collection, random_db, random_collection, collection_def, optimized):
    global select_count, docs_selected, collection_shard_metadata
    
    client = mongo_client.get_client()
    collection = client[random_db][random_collection]

    # Find the matching collection schema
    coll_entry = next(
        (item for item in collection_def
         if item.get("databaseName") == random_db and item.get("collectionName") == base_collection),
        None
    )
    if not coll_entry:
        logging.error(f"No schema found for {random_db}.{base_collection}")
        return

    field_schema = coll_entry.get("fieldName", {})
    primary_key = get_primary_key_from_collection(coll_entry)
    primary_key_type = field_schema.get(primary_key, {}).get("type", "string")

    # Use existing primary key if available
    pk_values = inserted_primary_keys.get((random_db, random_collection), [])
    if pk_values:
        pk_value = random.choice(pk_values)
    else:
        pk_value = generate_random_value(primary_key_type)

    # Build full query parameter list
    query_params = [pk_value]
    query_fields = [primary_key]
    query_types = [primary_key_type]

    for field_name, field_info in field_schema.items():
        if field_name == primary_key:
            continue
        bson_type = field_info.get("bsonType") or field_info.get("type", "string")
        value = generate_random_value(bson_type)
        query_params.append(value)
        query_fields.append(field_name)
        query_types.append(bson_type)

    # Generate select queries
    optimized_queries, ineffective_queries, query_projections = mongodbLoadQueries.select_queries(
        query_params, query_fields, query_types
    )

    try:
        if optimized and optimized_queries:
            query = random.choice(optimized_queries)

            # Shard-awareness check
            shard_info = collection_shard_metadata.get((random_db, random_collection), {})
            if shard_info.get("sharded"):
                missing_keys = [k for k in shard_info.get("shard_keys", []) if k not in query]
                if missing_keys:
                    logging.debug(f"Skipping select on sharded collection {random_db}.{random_collection}: Query missing shard keys {missing_keys}.")
                    return

            # --- DEBUG LOGGING ---
            if args.debug:
                # ---- ADD THIS LINE FOR DIAGNOSTICS ----
                # logging.debug(f"Executing query on client connected to: {client.HOST}:{client.PORT}")
                # ---------------------------------------

                # Use logging.debug for these messages
                logging.debug(f"\n--- [DEBUG] Running COUNT on: {random_db}.{random_collection} ---")
                logging.debug(f"Query: {pprint.pformat(query)}")

            count = collection.count_documents(query)
            
            if args.debug:
                # Use logging.debug for these messages
                logging.debug(f"Result: Found {count} document(s).")
                logging.debug("---------------------------------------------------\n")
            
            if count:
                with lock:
                    docs_selected += count

        elif ineffective_queries:
            query_index = random.randint(0, len(ineffective_queries) - 1)
            query = ineffective_queries[query_index]
            projection = query_projections[query_index] if query_index < len(query_projections) else None

            # --- DEBUG LOGGING ---
            if args.debug:
                # Use logging.debug for these messages
                logging.debug(f"\n--- [DEBUG] Running FIND on: {random_db}.{random_collection} ---")
                logging.debug(f"Query: {pprint.pformat(query)}")
                if projection:
                    logging.debug(f"Projection: {pprint.pformat(projection)}")

            cursor = collection.find(query, projection).limit(5)
            results = list(cursor)
            result_count = len(results)

            if args.debug:
                # Use logging.debug for these messages
                logging.debug(f"Result: Found {result_count} document(s).")
                if results:
                    logging.debug(f"Documents:\n{pprint.pformat(results)}")
                logging.debug("-------------------------------------------------\n")

            if result_count:
                with lock:
                    docs_selected += result_count

        with lock:
            select_count += 1

    except pymongo.errors.PyMongoError as e:
        logging.error(f"Error selecting from collection {random_db}.{random_collection}: {e}")

##############
# Update Docs
##############
def update_documents(args, base_collection, random_db, random_collection, collection_def, optimized):
    global update_count, docs_updated, collection_shard_metadata

    collection = get_client()[random_db][random_collection]

    coll_entry = next(
        (item for item in collection_def
         if item.get("databaseName") == random_db and item.get("collectionName") == base_collection),
        None
    )
    if not coll_entry:
        logging.error(f"No schema found for {random_db}.{base_collection}")
        return

    field_schema = coll_entry.get("fieldName", {})
    primary_key = get_primary_key_from_collection(coll_entry)
    primary_key_type = field_schema.get(primary_key, {}).get("type", "string")

    # Select PK value from inserted keys or generate one
    pk_values = inserted_primary_keys.get((random_db, random_collection), [])
    if pk_values:
        pk_value = random.choice(pk_values)
    else:
        pk_value = generate_random_value(primary_key_type)

    # Prepare fields excluding primary key
    update_fields = [f for f in field_schema if f != primary_key]
    if not update_fields:
        logging.warning(f"No updateable fields found for {random_db}.{random_collection}")
        return

    num_fields_to_update = random.randint(1, min(5, len(update_fields)))
    selected_fields = random.sample(update_fields, num_fields_to_update)

    new_values = []
    new_types = []

    # --- MODIFIED SECTION FOR PROVIDER HANDLING IN UPDATES ---
    # Need to generate context *if* any of the selected_fields require it.
    # This might require fetching the existing document to get its context values.
    # For simplicity and to fix the immediate error, we'll mimic the generate_random_document
    # context generation, which assumes new values. If you need to update based on *existing*
    # document values (e.g., reduce seats available), you'd need to fetch the document first.
    
    # For this fix, we'll generate a fresh context for providers that need it.
    # This will generate new random plane_type and total_seats values for 'equip'.
    # If the update should be relative to the existing document, this needs more logic.
    need_context = any(field_schema[f].get("provider") in ["passengers", "equip", "total_seats", "seats_available"] for f in selected_fields)
    context = generate_aircraft_context() if need_context else {} # Assuming generate_aircraft_context exists and provides these.

    for f in selected_fields:
        props = field_schema[f]
        provider = props.get("provider")
        ftype = props.get("type", "string")

        new_val = None
        if provider:
            # Handle special context-aware providers first, similar to generate_random_document
            if provider == "passengers":
                new_val = fake.passengers(
                    total_seats=context.get("total_seats", 100),
                    num_passengers=context.get("num_passengers", 10),
                    fake=fake
                )
            elif provider == "equip":
                # Ensure context exists for these arguments
                plane_type = context.get("plane_type", "Airbus A320")
                total_seats = context.get("total_seats", 100)
                new_val = fake.equip(plane_type, total_seats)
                logging.debug(f"Update: Field '{f}': Used provider '{provider}' with plane_type={plane_type}, total_seats={total_seats}.")
            elif provider == "total_seats":
                new_val = str(context.get("total_seats", 100)) # Ensure type consistency if 'str' is expected
            elif provider == "seats_available":
                new_val = context.get("seats_available", 0)
            else: # All other general providers (like 'drivers', 'rental_options')
                provider_func = getattr(fake, provider, None)
                if callable(provider_func):
                    new_val = provider_func()
                    logging.debug(f"Update: Field '{f}': Used general provider '{provider}'.")
                else:
                    logging.warning(f"Update: Provider '{provider}' not found or not callable on 'fake' object for field '{f}'. Falling back to generic type '{ftype}'.")
                    new_val = generate_random_value(ftype)
        else:
            # Fallback to generic generate_random_value if no provider
            logging.debug(f"Update: Field '{f}': No provider specified. Using generic type '{ftype}'.")
            new_val = generate_random_value(ftype)
        
        new_values.append(new_val)
        new_types.append(ftype)
    # --- END MODIFIED SECTION ---

    # Generate optimized and ineffective update queries
    optimized_updates, ineffective_updates = mongodbLoadQueries.update_queries(
        selected_fields, new_values, new_types, primary_key, pk_value
    )

    update_candidates = optimized_updates if optimized else ineffective_updates

    if not update_candidates:
        logging.warning("No update queries generated")
        return

    chosen_update = random.choice(update_candidates)
    filter_query = chosen_update.get("filter", {primary_key: pk_value}) if optimized else chosen_update.get("filter", {})
    update_doc = chosen_update.get("update")

    if not update_doc:
        logging.error("Update document missing in chosen update query.")
        return

    # Check shard key metadata
    shard_info = collection_shard_metadata.get((random_db, random_collection), {})
    is_sharded = shard_info.get("sharded", False)
    shard_keys = shard_info.get("shard_keys", [])

    # Determine if update modifies a shard key field
    modified_fields = [
        field
        for op in update_doc.values()
        if isinstance(op, dict)
        for field in op.keys()
    ]

    modifies_shard_key = any(f in shard_keys for f in modified_fields)

    # Skip update if shard key is modified without full shard key filter
    # This usually happens when creating workloads for both sharded and non-sharded collections
    if is_sharded and modifies_shard_key:
        missing_keys = [k for k in shard_keys if k not in filter_query]
        if missing_keys:
            # logging.warning(
            #     f"Skipping update for {random_db}.{random_collection}: "
            #     f"attempts to modify shard key fields {shard_keys} "
            #     f"without including all in filter. Missing: {missing_keys}. "
            #     f"Filter: {filter_query}, Update: {update_doc}"
            # )
            return

    try:
        if optimized or modifies_shard_key:
            result = collection.update_one(filter_query, update_doc)
        else:
            result = collection.update_many(filter_query, update_doc)

        with lock:
            update_count += 1
            if result.modified_count > 0:
                docs_updated += result.modified_count
    except Exception as e:
        logging.error(f"Error updating document {primary_key}={pk_value}: {e}")

##############
# Delete Docs
##############
def delete_documents(args, base_collection, random_db, random_collection, collection_def, optimized):
    global delete_count, docs_deleted, collection_shard_metadata
    collection = get_client()[random_db][random_collection]

    # Find schema
    coll_entry = next(
        (item for item in collection_def
         if item.get("databaseName") == random_db and item.get("collectionName") == base_collection),
        None
    )
    if not coll_entry:
        logging.error(f"No schema found for {random_db}.{base_collection}")
        return

    field_schema = coll_entry.get("fieldName", {})
    primary_key = get_primary_key_from_collection(coll_entry)
    primary_key_type = field_schema.get(primary_key, {}).get("type", "string")

    # Build values for all fields
    pk_values = inserted_primary_keys.get((random_db, random_collection), [])
    if pk_values:
        pk_value = random.choice(pk_values)
    else:
        # If no inserted keys, generate a random one. This delete might not match an existing doc.
        pk_value = generate_random_value(primary_key_type)
        logging.debug(f"No inserted PKs found for {random_db}.{random_collection}. Generating random PK for delete: {pk_value}")


    query_params = []
    query_fields = []
    query_types = []

    # Collect parameters for all fields, including the primary key, for delete query generation
    for field_name, field_info in field_schema.items():
        ftype = field_info.get("bsonType") or field_info.get("type", "string")
        if field_name == primary_key:
            val = pk_value # Use the selected PK value
        else:
            val = generate_random_value(ftype) # Generate random value for other fields
        query_params.append(val)
        query_fields.append(field_name)
        query_types.append(ftype)


    # Generate delete queries, passing primary_key and pk_value explicitly
    optimized_queries, ineffective_queries = mongodbLoadQueries.delete_queries(
        query_params, query_fields, query_types, primary_key, pk_value # <-- Pass primary_key and pk_value
    )

    try:
        query = {} # Initialize query to avoid UnboundLocalError
        if optimized and optimized_queries:
            query = random.choice(optimized_queries)
            delete_op_type = "one" # Optimized deletes should use delete_one
        elif ineffective_queries:
            query = random.choice(ineffective_queries)
            delete_op_type = "many" # Ineffective deletes should use delete_many
        else:
            logging.warning("No delete queries generated")
            return

        # Shard-awareness: check if query has full shard key
        shard_info = collection_shard_metadata.get((random_db, random_collection), {})
        is_sharded = shard_info.get("sharded", False)
        shard_keys = shard_info.get("shard_keys", [])

        # If sharded and we are doing an "ineffective" delete (which means delete_many)
        # without a full shard key, we should ideally skip it or be aware of its implications.
        # The current check is for *any* missing shard key in the query, which is fine
        # for `delete_one`. For `delete_many` on sharded collections, if the query
        # doesn't contain the shard key, it will scatter reads/writes.
        if is_sharded and shard_keys and delete_op_type == "one": # Only check for `delete_one` as `delete_many` is by design broader
            missing_keys = [k for k in shard_keys if k not in query]
            if missing_keys:
                logging.debug(
                    f"Skipping delete_one on sharded collection {random_db}.{random_collection}: "
                    f"Query missing shard key fields {missing_keys}. Query: {query}"
                )
                return

        if delete_op_type == "one":
            result = collection.delete_one(query)
        else: # delete_op_type == "many"
            # For delete_many, we don't strictly require shard key in query
            # but it's important to understand this will be a broadcast operation if not present.
            result = collection.delete_many(query)

        with lock:
            delete_count += 1
            if result.deleted_count > 0:
                docs_deleted += result.deleted_count
                # If a document was deleted by its primary key, remove it from inserted_primary_keys
                if primary_key in query and query[primary_key] == pk_value:
                    if (random_db, random_collection) in inserted_primary_keys and pk_value in inserted_primary_keys[(random_db, random_collection)]:
                        inserted_primary_keys[(random_db, random_collection)].remove(pk_value)
                        logging.debug(f"Removed PK {pk_value} from cache after successful delete.")

    except Exception as e:
        logging.error(f"Error deleting documents with query {query}: {e}")


#######################
# End of CRUD functions
#######################

#######################################
# Validate and configure workload ratio
#######################################
def workload_ratio_config(args):
    # Default ratios for unspecified values
    default_ratios = {
        "insert_ratio": 10,
        "update_ratio": 20,
        "delete_ratio": 10,
        "select_ratio": 60
    }
    # Store custom workload ratios
    ratio_args = {
        "insert_ratio": args.insert_ratio,
        "update_ratio": args.update_ratio,
        "delete_ratio": args.delete_ratio,
        "select_ratio": args.select_ratio,
    }
    # Store workloads to skip if any
    skip_args = {
        "skip_update": args.skip_update,
        "skip_delete": args.skip_delete,
        "skip_insert": args.skip_insert,
        "skip_select": args.skip_select,
    }
    # Ensure all required keys exist in the ratio_args, defaulting to None if missing
    ratios = {
        "insert_ratio": ratio_args.get("insert_ratio", None),
        "update_ratio": ratio_args.get("update_ratio", None),
        "delete_ratio": ratio_args.get("delete_ratio", None),
        "select_ratio": ratio_args.get("select_ratio", None),
    }
    # Take into account if the user wants to skip specific workloads
    skip = {
        "skip_update": skip_args.get("skip_update", False),
        "skip_delete": skip_args.get("skip_delete", False),
        "skip_insert": skip_args.get("skip_insert", False),
        "skip_select": skip_args.get("skip_select", False),
    }
    # Set ratios to 0 for skipped workloads
    for key, skip_flag in skip.items():
        if skip_flag:
            ratio_key = key.replace("skip_", "") + "_ratio"
            ratios[ratio_key] = 0
    # Calculate sum of specified ratios
    specified_ratios = {k: v for k, v in ratios.items() if v is not None}
    specified_sum = sum(specified_ratios.values())
    # If the sum exceeds 100%, warn and reset to defaults
    if specified_sum > 100:
        with log_lock:  # This ensures only one process can log at a time
            logging.warning(f"The total workload ratio is {round(specified_sum, 2)}%, which exceeds 100%. "
                            "Each workload ratio will be adjusted to their default values.")
        return default_ratios  # Reset to default ratios if sum exceeds 100%

    # Calculate the remaining percentage for unspecified ratios
    remaining_percentage = round(100 - specified_sum, 10)

    # Count the number of unspecified ratios
    unspecified_keys = [key for key in ratios if ratios[key] is None]

    if unspecified_keys:
        # Distribute the remaining percentage proportionally based on default values
        total_default = sum(default_ratios[key] for key in unspecified_keys)
        for key in unspecified_keys:
            ratios[key] = round((default_ratios[key] / total_default) * remaining_percentage, 10)

    # Ensure the total sum is exactly 100%
    total_weight = sum(ratios.values())
    if total_weight != 100:
        with log_lock:  # This ensures only one process can log at a time
            logging.info(f"The adjusted workload ratio is {round(total_weight, 10)}%, which is not 100%. "
                         "Rebalancing the ratios...")
        scale_factor = 100 / total_weight
        for key in ratios:
            ratios[key] = round(ratios[key] * scale_factor, 10)  # Round to avoid floating point precision issues

    # Re-assign the adjusted ratios to args after validation and remove floating point precision
    args.insert_ratio = ratios["insert_ratio"] 
    args.update_ratio = ratios["update_ratio"]
    args.delete_ratio = ratios["delete_ratio"]
    args.select_ratio = ratios["select_ratio"]
   
    return ratios

###############################
# Output workload configuration
###############################
def log_workload_config(collection_def, args, shard_enabled, workload_length, workload_ratios, workload_logged):
    # Check if the function has already been executed
    if workload_logged:
        return

    if isinstance(collection_def, dict):
        collection_def = [collection_def]

    # Extract and format collection and database names
    # Create a single-line string with all collections and databases
    collection_info = " | ".join(
    [f"{item['databaseName']}.{item['collectionName']}" for item in collection_def]
    )

    table_width = 115
    workload_details = textwrap.dedent(f"""\n 
    Duration: {workload_length}
    CPUs: {args.cpu}
    Threads: (Per CPU: {args.threads} | Total: {args.cpu * args.threads})    
    Database and Collection: ({collection_info})   
    Instances of the same collection: {"Disabled" if args.custom_queries else args.collections}
    Configure Sharding: {shard_enabled}
    Insert batch size: {args.batch_size}
    Optimized workload: {"Disabled" if args.custom_queries else args.optimized}
    Workload ratio: (SELECTS: {int(round(float(workload_ratios['select_ratio']), 0))}% | INSERTS: {int(round(float(workload_ratios['insert_ratio']), 0))}% | UPDATES: {int(round(float(workload_ratios['update_ratio']), 0))}% | DELETES: {int(round(float(workload_ratios['delete_ratio']), 0))}%)
    Report frequency: {args.report_interval} seconds
    Report logfile: {args.log}\n
    {'=' * table_width}
    {' Workload Started':^{table_width - 2}}
    {'=' * table_width}\n""")    
    # with log_lock:  # This ensures only one process can log at a time
    logging.info(workload_details)
    # Set the flag to True to prevent further logging
    workload_logged = True  
 

##########################################
# Calculate operations per report_interval
##########################################
def calculate_ops_per_interval(args, allThreads, report_interval=5, process_id=0, total_ops_dict=None, lock=None):
    last_insert_count = 0
    last_update_count = 0
    last_delete_count = 0
    last_select_count = 0
    while not stop_event.is_set() and any(thread.is_alive() for thread in allThreads):
        time.sleep(report_interval)  # Wait for report_interval seconds
        with lock:
            inserts_per_sec = (insert_count - last_insert_count) / report_interval
            updates_per_sec = (update_count - last_update_count) / report_interval
            deletes_per_sec = (delete_count - last_delete_count) / report_interval
            selects_per_sec = (select_count - last_select_count) / report_interval

            total_ops_per_sec = selects_per_sec + inserts_per_sec + updates_per_sec + deletes_per_sec
            last_insert_count = insert_count
            last_update_count = update_count
            last_delete_count = delete_count
            last_select_count = select_count

            # Update shared dictionary for total operations tracking
            if total_ops_dict is not None:
                total_ops_dict['insert'][process_id] = inserts_per_sec
                total_ops_dict['update'][process_id] = updates_per_sec
                total_ops_dict['delete'][process_id] = deletes_per_sec
                total_ops_dict['select'][process_id] = selects_per_sec

        

##############################################
# Output total operations across all CPUs
##############################################
def log_total_ops_per_interval(args, total_ops_dict, stop_event, lock):
    while not stop_event.is_set():
        time.sleep(args.report_interval)
        with lock:
            total_selects = sum(total_ops_dict['select'])
            total_inserts = sum(total_ops_dict['insert'])
            total_updates = sum(total_ops_dict['update'])
            total_deletes = sum(total_ops_dict['delete'])
            total_ops = total_selects + total_inserts + total_updates + total_deletes

            if total_ops: # We only provide the output if the total ops isn't zero
                logging.info(
                    f"AVG Operations last {args.report_interval}s ({args.cpu} CPUs): {total_ops:.2f} "
                    f"(SELECTS: {total_selects:.2f}, INSERTS: {total_inserts:.2f}, "
                    f"UPDATES: {total_updates:.2f}, DELETES: {total_deletes:.2f})"
                )

#####################################################################
# Obtain real-time workload stats for each CPU. This is stored in the 
# output_queue which is later summarized by the main application file
##################################################################### 
def workload_stats(select_count, insert_count, update_count, delete_count, process_id, output_queue):
    # Create the dictionary with the required structure
    stats_dict = {
        "process_id": process_id,
        "stats": {
            "select": select_count,
            "insert": insert_count,
            "delete": delete_count,
            "update": update_count,
            "docs_inserted": docs_inserted,
            "docs_selected": docs_selected,
            "docs_updated": docs_updated,
            "docs_deleted": docs_deleted
        }
    }
    output_queue.put(stats_dict)

##########################################################################
# Obtain stats for all collections but only when the workload has finished
# We only need to collect this from only one of the running CPUs since the 
# collection information would be the same. This is stored in the 
# collection_queue which is later summarized by the main application file
##########################################################################
def collection_stats(collection_def, collections, collection_queue):
    collstats_dict = {}  # Dictionary to store all collection stats

    for entry in collection_def:
        base_collection_name = entry["collectionName"]
        db_name = entry["databaseName"]

        client = get_client()
        db = client[db_name]

        for i in range(1, collections + 1):
            collection_name_with_suffix = f"{base_collection_name}_{i}" if collections > 1 else base_collection_name
            try:
                collstats = db.command("collstats", collection_name_with_suffix)
                collstats_dict[collection_name_with_suffix] = {
                    "db": db_name,
                    "sharded": collstats.get("sharded", False),
                    "size": collstats.get("size", 0),
                    "documents": collstats.get("count", 0),
                }
            except pymongo.errors.PyMongoError as e:
                print(f"Error retrieving stats for {db_name}.{collection_name_with_suffix}: {e}")

    # Put full aggregated stats after all definitions are processed
    collection_queue.put(collstats_dict)

#############################################################
# Randomly choose operations and collections for the workload
#############################################################

#############################################################
# WORKER FOR RANDOMIZED WORKLOAD (No user query file)
#############################################################
def random_worker(args, created_collections, collection_def):
    runtime = args.runtime 
    batch_size = args.batch_size
    skip_update = args.skip_update
    skip_delete = args.skip_delete
    skip_insert = args.skip_insert
    skip_select = args.skip_select 
    insert_ratio = args.insert_ratio if args.insert_ratio is not None else 10
    update_ratio = args.update_ratio if args.update_ratio is not None else 20
    delete_ratio = args.delete_ratio if args.delete_ratio is not None else 10 
    select_ratio = args.select_ratio if args.select_ratio is not None else 60 
    optimized = bool(args.optimized)
    
    work_start = time.time()
    operations = ["insert", "update", "delete", "select"]
    weights = [insert_ratio, update_ratio, delete_ratio, select_ratio]

    while time.time() - work_start < runtime and not stop_event.is_set():  # Ensure graceful exit
        operation = random.choices(operations, weights=weights, k=1)[0] # randomly choose what kind of operation based on the workload ratio
        random_db, random_collection = random.choice(created_collections) # choose collections randomly
        field_schema = collection_def[0]["fieldName"]  # All fields + types in the collection definition JSON file
        # Gather collection metadata
        # collection_shard_metadata = collect_shard_key_metadata(collection_def, args.collections)
        collection_shard_metadata = collect_shard_key_metadata(random_db,random_collection)
        # Remove numeric suffix from collection name (e.g., _1, _2, etc.)
        # The suffix is used to create variations of the same collection, but we only need the base name to obtain the collection definition from the JSON file.
        if args.collections > 1:
            base_collection = re.sub(r'_\d+$', '', random_collection)
        else:
            base_collection = random_collection

        if operation == "insert" and not skip_insert:
            insert_documents(args, base_collection, random_db, random_collection, collection_def, batch_size=10)
        elif operation == "update" and not skip_update:
            update_documents(args, base_collection, random_db, random_collection, collection_def, optimized)
        elif operation == "delete" and not skip_delete:
            delete_documents(args, base_collection, random_db, random_collection, collection_def, optimized)
        elif operation == "select" and not skip_select:
            select_documents(args, base_collection, random_db,random_collection, collection_def, optimized)


#############################################################
# WORKER FOR CUSTOM QUERY MODE (User query file provided)
# Insert calls still use our random generator
#############################################################
def custom_worker(args, created_collections, collection_def, user_queries):
    global select_count, insert_count, update_count, delete_count, docs_selected, docs_inserted, docs_updated, docs_deleted
    runtime = args.runtime
    
    # 1. Pre-process and categorize the user queries from the JSON file
    select_queries = [q for q in user_queries if q.get("operation") in ["find", "aggregate"]]
    update_queries = [q for q in user_queries if q.get("operation") in ["updateOne", "updateMany"]]
    delete_queries = [q for q in user_queries if q.get("operation") in ["deleteOne", "deleteMany"]]

    # 2. Set up the operations and weights based on ratios
    operations = []
    weights = []
    if not args.skip_select and args.select_ratio > 0 and select_queries:
        operations.append("select")
        weights.append(args.select_ratio)
    if not args.skip_update and args.update_ratio > 0 and update_queries:
        operations.append("update")
        weights.append(args.update_ratio)
    if not args.skip_insert and args.insert_ratio > 0:
        operations.append("insert")
        weights.append(args.insert_ratio)
    if not args.skip_delete and args.delete_ratio > 0 and delete_queries:
        operations.append("delete")
        weights.append(args.delete_ratio)

    if not operations:
        logging.warning("No operations available for the given ratios and user query file. Worker is idle.")
        return

    if args.debug:
        # ---- ADD THIS LINE FOR DIAGNOSTICS ----
        logging.debug(f"Hybrid worker started. Operations enabled: {operations}")
    work_start = time.time()

    while time.time() - work_start < runtime and not stop_event.is_set():
        # 3. Choose an operation type based on the workload ratio
        chosen_op = random.choices(operations, weights=weights, k=1)[0]
        
        # 4. Execute the chosen operation
        if chosen_op == "select":
            query_def = random.choice(select_queries)
            op_type, op_count, docs_affected = custom_query_executor.execute_user_query(args, query_def, fake, generate_random_value)
            if op_type:
                with lock:
                    select_count += op_count
                    docs_selected += docs_affected
        
        elif chosen_op == "update":
            query_def = random.choice(update_queries)
            op_type, op_count, docs_affected = custom_query_executor.execute_user_query(args, query_def, fake, generate_random_value)
            if op_type:
                with lock:
                    update_count += op_count
                    docs_updated += docs_affected

        elif chosen_op == "delete":
            query_def = random.choice(delete_queries)
            op_type, op_count, docs_affected = custom_query_executor.execute_user_query(args, query_def, fake, generate_random_value)
            if op_type:
                with lock:
                    delete_count += op_count
                    docs_deleted += docs_affected            

        # Insert functionality can be our own random and doesn't require the user providing theirs since we will be inserting random records
        elif chosen_op == "insert":
            random_db, random_collection = random.choice(created_collections)
            base_collection = re.sub(r'_\d+$', '', random_collection) if args.collections > 1 else random_collection
            # Fallback to original random insert function
            insert_documents(args, base_collection, random_db, random_collection, collection_def, args.batch_size)


####################
# Start the workload
####################
def start_workload(args, process_id="", completed_processes="",output_queue="", collection_queue="", total_ops_dict=None, collection_def=None, created_collections=None, user_queries=None):
    # Handler for Ctrl+C
    signal.signal(signal.SIGINT, handle_exit)
 
    try:
        # Start multiple worker threads
        allThreads = []

        # Decide which worker to use based on whether user_queries were provided
        if user_queries:
            target_worker = custom_worker
            worker_args = (args, created_collections, collection_def, user_queries)
        else:
            target_worker = random_worker
            worker_args = (args, created_collections, collection_def)

        for _ in range(args.threads):
            thread = threading.Thread(target=target_worker, args=worker_args)
            thread.start()
            allThreads.append(thread)

        # Start the thread to calculate QPS AFTER initializing the worker threads
        logging_thread = threading.Thread(target=calculate_ops_per_interval, args=(args, allThreads,args.report_interval,process_id, total_ops_dict, lock), daemon=True)
        logging_thread.start()

        # Wait for all threads to finish
        for thread in allThreads:
            thread.join()

    except KeyboardInterrupt:
        stop_event.set()  # Ensure the thread stops
        logging_thread.join()  # Wait for the thread to finish gracefully

    finally:
        time.sleep(5) # We sleep a few seconds to make sure not to overlap with the real-time workload report
        # Mark this process as complete
        completed_processes[process_id] = True
        # Get collection stats after the workload has completed
        collection_stats(collection_def, args.collections, collection_queue)
        # Get workload stats
        workload_stats(select_count, insert_count, update_count, delete_count, process_id, output_queue)



####################
# Start the workload
####################
if __name__ == "__main__":
    start_workload()
    