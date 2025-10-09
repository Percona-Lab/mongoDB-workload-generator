#!/usr/bin/env python3
import asyncio
import motor  # type: ignore
from datetime import datetime
import random
import string
import bson  # type: ignore
from faker import Faker  # type: ignore
from customProvider import CustomProvider
import time
import logging
import textwrap
import pprint
import signal
import sys
import re
from urllib.parse import urlencode
from multiprocessing import Lock
from mongodbCreds import dbconfig
from mongo_client import get_client, init_async
import args as args_module
from pymongo.errors import PyMongoError # type: ignore
import custom_query_executor
import mongodbLoadQueries
from mongodbWorkload import Bcolors
import os

fake = Faker()
fake.add_provider(CustomProvider)

insert_count = 0
update_count = 0
delete_count = 0
select_count = 0
collection_metadata_cache = {}
collection_primary_keys = {}
inserted_primary_keys = {}
collection_shard_metadata = {}
docs_deleted = 0
docs_inserted = 0
docs_updated = 0
docs_selected = 0
lock = asyncio.Lock()

def handle_exit(signum, frame):
    print("\n[!] Ctrl+C detected! Stopping workload...")
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
    shard_key = coll.get("shardConfig", {}).get("key")
    fields = coll.get("fieldName", {})

    if shard_key:
        for key_field in shard_key.keys():
            if key_field in fields:
                return key_field
            else:
                logging.warning(f"Shard key '{key_field}' not found in fieldName.")

    for field, props in fields.items():
        if props.get("isPrimaryKey") or props.get("unique") is True:
            return field
    return "_id"

#####################################################################
# Get collection metadata and store in a cache
#####################################################################
async def collect_shard_key_metadata(random_db,random_collection):
    global collection_shard_metadata
    collection_shard_metadata = {}
    client = get_client()
    db = client[random_db]

    ns = f"{random_db}.{random_collection}"

    try:
        coll_stats = await db.command("collstats", random_collection)
        is_sharded = coll_stats.get("sharded", False)

        shard_keys = []

        if is_sharded:
            config_db = client["config"]
            sharding_info = await config_db["collections"].find_one({"_id": ns})
            if sharding_info and "key" in sharding_info:
                shard_keys = list(sharding_info["key"].keys())

        collection_shard_metadata[(random_db, random_collection)] = {
            "sharded": is_sharded,
            "shard_keys": shard_keys
        }

    except PyMongoError as e:
        logging.error(f"Error retrieving shard metadata for {ns}: {e}")

async def pre_compute_collection_metadata(args, created_collections, collection_def):
    global collection_metadata_cache, collection_primary_keys
    client = get_client()

    for db_name, collection_name in created_collections:
        base_collection_name = re.sub(r'_\d+$', '', collection_name) if args.collections > 1 else collection_name
        coll_entry = next(
            (item for item in collection_def
             if item.get("databaseName") == db_name and item.get("collectionName") == base_collection_name),
            None
        )
        if not coll_entry:
            logging.error(f"No schema found for {db_name}.{base_collection_name}")
            continue

        primary_key = get_primary_key_from_collection(coll_entry)
        collection_primary_keys[(db_name, collection_name)] = primary_key

        is_sharded = False
        shard_keys = []
        try:
            coll_stats = await client[db_name].command("collstats", collection_name)
            is_sharded = coll_stats.get("sharded", False)
            if is_sharded:
                sharding_info = await client["config"]["collections"].find_one({"_id": f"{db_name}.{collection_name}"})
                if sharding_info and "key" in sharding_info:
                    shard_keys = list(sharding_info["key"].keys())
        except PyMongoError as e:
            logging.error(f"Error retrieving shard metadata for {db_name}.{collection_name}: {e}")

        collection_metadata_cache[(db_name, collection_name)] = {
            "primary_key": primary_key,
            "is_sharded": is_sharded,
            "shard_keys": shard_keys,
            "field_schema": coll_entry.get("fieldName", {}),
            "coll_entry": coll_entry
        }


##########################################################################################################
# Iterate through collection definitions and calls the query template generators once for each collection. 
# This will fully populate the QUERY_TEMPLATE_CACHE before any worker process starts
##########################################################################################################
def pre_cache_all_queries(args,collection_def):
    """
    Iterates through all collection definitions and calls the query generation
    functions to populate the cache before the workload starts.
    """
    if args.debug:
        logging.debug("Pre-populating the query template cache for all collections...")
    for coll_entry in collection_def:
        field_schema = coll_entry.get("fieldName", {})
        if not field_schema:
            logging.warning(f"Skipping query caching for {coll_entry.get('collectionName')} due to missing 'fieldName' schema.")
            continue

        field_names = list(field_schema.keys())
        field_types = [v.get('type', 'string') for v in field_schema.values()]
        primary_key = get_primary_key_from_collection(coll_entry)
        shard_keys = list(coll_entry.get("shardConfig", {}).get("key", {}).keys())

        # Cache templates for both optimized and unoptimized modes to cover all possibilities
        for optimized_mode in [True, False]:
            mongodbLoadQueries.select_queries(field_names, field_types, primary_key, optimized_mode)
            mongodbLoadQueries.update_queries(field_names, field_types, primary_key, shard_keys, optimized_mode)
            mongodbLoadQueries.delete_queries(field_names, field_types, primary_key, optimized_mode)

    if args.debug:
        logging.debug(f"Query cache populated. Total cached template sets: {len(mongodbLoadQueries.QUERY_TEMPLATE_CACHE)}")



####################
# Create collections
####################
async def create_collection(collection_def, collections=1, recreate=False):
    created_collections = []
    global collection_primary_keys

    if isinstance(collection_def, dict):
        collection_def = [collection_def]

    for entry in collection_def:
        base_collection_name = entry["collectionName"]
        db_name = entry["databaseName"]
        indexes = entry.get("indexes", [])
        shard_config = entry.get("shardConfig")

        client = get_client()
        db = client[db_name]

        tasks = []

        for i in range(1, collections + 1):
            async def create_task(i):
                collection_name = f"{base_collection_name}_{i}" if collections > 1 else base_collection_name
                collection = db[collection_name]
                try:
                    if recreate and collection_name in await db.list_collection_names():
                        await collection.drop()

                    if collection_name not in await db.list_collection_names():
                        await db.create_collection(collection_name)
                        logging.info(f"Collection '{collection_name}' created in DB '{db_name}'")

                        if not dbconfig.get("replicaSet") and shard_config:
                            await shard_collection_async(db_name, collection_name, shard_config)

                        primary_key_field = get_primary_key_from_collection(entry)
                        collection_primary_keys[(db_name, collection_name)] = primary_key_field

                        for index in indexes:
                            index_keys = index["keys"]
                            options = index.get("options", {})
                            keys = list(index_keys.items())
                            try:
                                index_name = await collection.create_index(keys, **options)
                                logging.info(f"Successfully created index: '{index_name}'")
                            except PyMongoError as e:
                                logging.error(f"Failed to create index {keys} on {collection_name}: {e}")

                    created_collections.append((db_name, collection_name))
                except PyMongoError as e:
                            logging.error(f"Error creating collection '{collection_name}': {e}")
            tasks.append(create_task(i))

        await asyncio.gather(*tasks)

    return created_collections

###################
# Shard collections
###################
async def shard_collection_async(db_name, collection_name, shard_config):
    client = get_client()
    try:
        keys = list(shard_config["key"].items())
        await client[db_name][collection_name].create_index(keys)
        await client.admin.command("enableSharding", db_name)
        await client.admin.command("shardCollection", f"{db_name}.{collection_name}", key=shard_config["key"])
        logging.info(f"Sharding configured for '{db_name}.{collection_name}' with key {shard_config['key']}")
    except PyMongoError as e:
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
            return [fake.word() for _ in range(random.randint(1, 3))]
        case "object":
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

        if provider:
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
            else:
                provider_func = getattr(fake, provider, None)
                if callable(provider_func):
                    doc[field] = provider_func()
                else:
                    logging.warning(f"Provider '{provider}' not found for field '{field}'.")
                    doc[field] = None
        else:
            bson_type = props.get("type", "string")
            doc[field] = generate_random_value(bson_type)

    if "seats_available" in field_schema and "seats_available" not in doc:
        doc["seats_available"] = context.get("seats_available", 0)

    return doc

################
# CRUD Functions
################

##############
# Insert Docs
##############
async def insert_documents(args, base_collection, random_db, random_collection, collection_def, batch_size=10):
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
        result = await collection.insert_many(documents)
        insert_count += 1
        docs_inserted += len(result.inserted_ids)

        # Cache the primary keys of the documents we just inserted
        async with lock:
            if (random_db, random_collection) not in inserted_primary_keys:
                inserted_primary_keys[(random_db, random_collection)] = []

            key_field = collection_primary_keys.get((random_db, random_collection), "_id")
            if key_field == "_id":
                inserted_primary_keys[(random_db, random_collection)].extend(result.inserted_ids)
            else:
                inserted_primary_keys[(random_db, random_collection)].extend(
                    [doc[key_field] for doc in documents if key_field in doc]
                )

    except PyMongoError as e:
        logging.error(f"Error inserting documents into {random_db}.{random_collection}: {e}")

##############
# Select Docs
##############
async def select_documents(args, base_collection, random_db, random_collection, collection_def, optimized, prebuilt_query=None):
    global select_count, docs_selected
    collection = get_client()[random_db][random_collection]

    if prebuilt_query:
        # --- NEW FAST PATH ---
        # If a query is provided, execute it immediately and skip all generation.
        try:
            if args.debug:
                logging.debug(f"\n--- [DEBUG] Running COUNT on: {random_db}.{random_collection} ---")
                logging.debug(f"Pre-built Query: {pprint.pformat(prebuilt_query)}")
            
            count = await collection.count_documents(prebuilt_query)
            select_count += 1
            docs_selected += count
        except PyMongoError as e:
            logging.error(f"Error selecting from collection {random_db}.{random_collection}: {e}")
        return

    # --- Original logic remains as a fallback ---
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

    # Get shard key info from the correct cache
    shard_info = collection_metadata_cache.get((random_db, random_collection), {})
    shard_keys = shard_info.get("shard_keys", [])

    # Get the templates from the caching function
    field_names = list(field_schema.keys())
    field_types = [v.get('type', 'string') for v in field_schema.values()]
    
    # --- CHANGE: Pass 'optimized' and expect back one list of templates ---
    templates, projection_templates = mongodbLoadQueries.select_queries(
        field_names, field_types, primary_key, optimized
    )

    try:
        if not templates:
            logging.warning(f"No select query templates generated for {random_db}.{random_collection}")
            return

        template = random.choice(templates)
        
        # Generate all possible values the template might need
        pk_value = generate_random_value(field_schema.get(primary_key, {}).get('type', 'string'))
        value_map = {"{pk_value}": pk_value}
        for i in range(len(field_names)):
            field, bson_type = field_names[i], field_types[i]
            value = generate_random_value(bson_type)
            value_map[f"{{{field}_value}}"] = value
            if bson_type in ["int", "long", "double", "decimal"]:
                    value_map[f"{{{field}_high_value}}"] = value + random.randint(1, 100000)

        query = mongodbLoadQueries._fill_template(template, value_map)

        # Logic now correctly branches based on the 'optimized' flag
        if optimized:
            if shard_keys:
                missing_keys = [k for k in shard_keys if k not in query]
                if missing_keys:
                    logging.debug(f"Skipping optimized select on sharded collection {random_db}.{random_collection}: Query missing shard keys {missing_keys}.")
                    return

            if args.debug:
                logging.debug(f"\n--- [DEBUG] Running COUNT on: {random_db}.{random_collection} ---")
                logging.debug(f"Query: {pprint.pformat(query)}")

            count = await collection.count_documents(query)
            select_count += 1
            docs_selected += count
            
        else: # Unoptimized
            projection = random.choice(projection_templates)

            if args.debug:
                logging.debug(f"\n--- [DEBUG] Running FIND on: {random_db}.{random_collection} ---")
                logging.debug(f"Query: {pprint.pformat(query)}")

            results = await collection.find(query, projection).limit(5).to_list(None)
            result_count = len(results)
            select_count += 1
            docs_selected += result_count

    except PyMongoError as e:
        logging.error(f"Error selecting from collection {random_db}.{random_collection}: {e}")

##############
# Update Docs
##############
async def update_documents(args, base_collection, random_db, random_collection, collection_def, optimized):
    global update_count, docs_updated, collection_metadata_cache, inserted_primary_keys

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

    shard_info = collection_metadata_cache.get((random_db, random_collection), {})
    shard_keys = shard_info.get("shard_keys", [])

    field_names = list(field_schema.keys())
    field_types = [v.get('type', 'string') for v in field_schema.values()]

    # --- CHANGE: Pass 'optimized' and receive a single list of templates ---
    update_candidates = mongodbLoadQueries.update_queries(
        field_names, field_types, primary_key, shard_keys, optimized
    )

    if not update_candidates:
        logging.warning(f"No update queries available for {random_collection} (shard key fields are excluded).")
        return

    chosen_template = random.choice(update_candidates)

    pk_values_for_coll = inserted_primary_keys.get((random_db, random_collection), [])
    if pk_values_for_coll:
        pk_value = random.choice(pk_values_for_coll)
    else:
        pk_value = generate_random_value(field_schema.get(primary_key, {}).get('type', 'string'))

    value_map = {"{pk_value}": pk_value}
    for i in range(len(field_names)):
        field, bson_type = field_names[i], field_types[i]
        value = generate_random_value(bson_type)
        value_map[f"{{{field}_value}}"] = value
        if bson_type in ["int", "long", "double", "decimal"]:
             value_map[f"{{{field}_increment}}"] = random.randint(1, 100)
        if bson_type == "bool" and isinstance(value, bool):
             value_map[f"{{{field}_not_value}}"] = not value

    query_object = mongodbLoadQueries._fill_template(chosen_template, value_map)
    filter_query = query_object["filter"]
    update_doc = query_object["update"]

    try:
        if optimized:
            result = await collection.update_one(filter_query, update_doc)
        else:
            result = await collection.update_many(filter_query, update_doc)

        update_count += 1
        docs_updated += result.modified_count

    except Exception as e:
        logging.error(f"Error updating document {primary_key}={pk_value}: {e}, full error: {e.details if hasattr(e, 'details') else ''}")

##############
# Delete Docs
##############
async def delete_documents(args, base_collection, random_db, random_collection, collection_def, optimized):
    global delete_count, docs_deleted, collection_metadata_cache, inserted_primary_keys, lock
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

    field_names = list(field_schema.keys())
    field_types = [v.get('type', 'string') for v in field_schema.values()]

    # --- CHANGE: Pass 'optimized' and receive a single list of templates ---
    delete_candidates = mongodbLoadQueries.delete_queries(
        field_names, field_types, primary_key, optimized
    )

    if not delete_candidates:
        logging.warning("No delete queries generated")
        return

    chosen_template = random.choice(delete_candidates)

    pk_values_for_coll = inserted_primary_keys.get((random_db, random_collection), [])
    if pk_values_for_coll:
        pk_value = random.choice(pk_values_for_coll)
    else:
        pk_value = generate_random_value(field_schema.get(primary_key, {}).get('type', 'string'))

    value_map = {"{pk_value}": pk_value}
    for i in range(len(field_names)):
        field, bson_type = field_names[i], field_types[i]
        value = generate_random_value(bson_type)
        value_map[f"{{{field}_value}}"] = value

    query = mongodbLoadQueries._fill_template(chosen_template, value_map)

    try:
        shard_info = collection_metadata_cache.get((random_db, random_collection), {})
        shard_keys = shard_info.get("shard_keys", [])

        if optimized:
            if shard_keys:
                missing_keys = [k for k in shard_keys if k not in query]
                if missing_keys:
                    return
            result = await collection.delete_one(query)
        else:
            result = await collection.delete_many(query)

        delete_count += 1
        docs_deleted += result.deleted_count

        if result.deleted_count > 0 and pk_value in query.values():
            async with lock:
                if (random_db, random_collection) in inserted_primary_keys and pk_value in inserted_primary_keys.get((random_db, random_collection), []):
                    inserted_primary_keys[(random_db, random_collection)].remove(pk_value)

    except Exception as e:
        logging.error(f"Error deleting documents with query {query}: {e}")

#######################
# End of CRUD functions
#######################

#######################################
# Validate and configure workload ratio
#######################################
def workload_ratio_config(args):
    default_ratios = {
        "insert_ratio": 10,
        "update_ratio": 20,
        "delete_ratio": 10,
        "select_ratio": 60
    }
    ratio_args = {
        "insert_ratio": args.insert_ratio,
        "update_ratio": args.update_ratio,
        "delete_ratio": args.delete_ratio,
        "select_ratio": args.select_ratio,
    }
    skip_args = {
        "skip_update": args.skip_update,
        "skip_delete": args.skip_delete,
        "skip_insert": args.skip_insert,
        "skip_select": args.skip_select,
    }
    ratios = {
        "insert_ratio": ratio_args.get("insert_ratio", None),
        "update_ratio": ratio_args.get("update_ratio", None),
        "delete_ratio": ratio_args.get("delete_ratio", None),
        "select_ratio": ratio_args.get("select_ratio", None),
    }
    skip = {
        "skip_update": skip_args.get("skip_update", False),
        "skip_delete": skip_args.get("skip_delete", False),
        "skip_insert": skip_args.get("skip_insert", False),
        "skip_select": skip_args.get("skip_select", False),
    }
    for key, skip_flag in skip.items():
        if skip_flag:
            ratio_key = key.replace("skip_", "") + "_ratio"
            ratios[ratio_key] = 0
    specified_ratios = {k: v for k, v in ratios.items() if v is not None}
    specified_sum = sum(specified_ratios.values())
    if specified_sum > 100:
        logging.warning(f"The total workload ratio is {round(specified_sum, 2)}%, which exceeds 100%. "
                        "Each workload ratio will be adjusted to their default values.")
        return default_ratios
    remaining_percentage = round(100 - specified_sum, 10)
    unspecified_keys = [key for key in ratios if ratios[key] is None]
    if unspecified_keys:
        total_default = sum(default_ratios[key] for key in unspecified_keys)
        for key in unspecified_keys:
            ratios[key] = round((default_ratios[key] / total_default) * remaining_percentage, 10)
    total_weight = sum(ratios.values())
    if total_weight != 100:
        logging.info(f"The adjusted workload ratio is {round(total_weight, 10)}%, which is not 100%. "
                     "Rebalancing the ratios...")
        scale_factor = 100 / total_weight
        for key in ratios:
            ratios[key] = round(ratios[key] * scale_factor, 10)
    args.insert_ratio = ratios["insert_ratio"]
    args.update_ratio = ratios["update_ratio"]
    args.delete_ratio = ratios["delete_ratio"]
    args.select_ratio = ratios["select_ratio"]
    return ratios

###############################
# Output workload configuration
###############################
def log_workload_config(collection_def, args, shard_enabled, workload_length, workload_ratios):

    if isinstance(collection_def, dict):
        collection_def = [collection_def]

    collection_info = " | ".join(
    [f"{Bcolors.BOLD}{Bcolors.SETTING_VALUE}{item['databaseName']}.{item['collectionName']}{Bcolors.ENDC}" for item in collection_def]
    )

    table_width = 115
    workload_details = textwrap.dedent(f"""\n
    {Bcolors.WORKLOAD_SETTING}Duration:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{workload_length} seconds{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}CPUs:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.cpu}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Threads:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}(Per CPU: {args.threads} | Total: {args.cpu * args.threads}{Bcolors.ENDC})
    {Bcolors.WORKLOAD_SETTING}Database and Collection:{Bcolors.ENDC} ({collection_info})
    {Bcolors.WORKLOAD_SETTING}Instances of the same collection:{Bcolors.ENDC} {Bcolors.BOLD}{(Bcolors.DISABLED if args.custom_queries else Bcolors.SETTING_VALUE)}{"Disabled" if args.custom_queries else args.collections}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Configure Sharding:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{shard_enabled}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Insert batch size:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.batch_size}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Optimized workload:{Bcolors.ENDC} {Bcolors.BOLD}{(Bcolors.DISABLED if args.custom_queries else Bcolors.SETTING_VALUE)}{"Disabled" if args.custom_queries else args.optimized}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Workload ratio:{Bcolors.ENDC} ({Bcolors.BOLD}{Bcolors.SETTING_VALUE}SELECTS: {int(round(float(workload_ratios['select_ratio']), 0))}% {Bcolors.ENDC}|{Bcolors.BOLD}{Bcolors.SETTING_VALUE} INSERTS: {int(round(float(workload_ratios['insert_ratio']), 0))}% {Bcolors.ENDC}|{Bcolors.BOLD}{Bcolors.SETTING_VALUE} UPDATES: {int(round(float(workload_ratios['update_ratio']), 0))}% {Bcolors.ENDC}|{Bcolors.BOLD}{Bcolors.SETTING_VALUE} DELETES: {int(round(float(workload_ratios['delete_ratio']), 0))}%{Bcolors.ENDC})
    {Bcolors.WORKLOAD_SETTING}Report frequency:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.report_interval} seconds{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Report logfile:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.log}{Bcolors.ENDC}\n
    {Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
    {Bcolors.BOLD}{Bcolors.HEADER}{' Workload Started':^{table_width - 2}}{Bcolors.ENDC}
    {Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}\n""")
    logging.info(workload_details)

##########################################
# Calculate operations per report_interval
##########################################
async def update_stats_periodically(process_id, total_ops_dict, stop_event, report_interval):
    """Calculates the delta of operations since last check and updates the shared dict."""
    last_counts = {"insert": 0, "update": 0, "delete": 0, "select": 0}
    while not stop_event.is_set():
        await asyncio.sleep(report_interval)

        inserts_per_sec = (insert_count - last_counts["insert"]) / report_interval
        updates_per_sec = (update_count - last_counts["update"]) / report_interval
        deletes_per_sec = (delete_count - last_counts["delete"]) / report_interval
        selects_per_sec = (select_count - last_counts["select"]) / report_interval

        last_counts["insert"] = insert_count
        last_counts["update"] = update_count
        last_counts["delete"] = delete_count
        last_counts["select"] = select_count

        try:
            total_ops_dict['insert'][process_id] = inserts_per_sec
            total_ops_dict['update'][process_id] = updates_per_sec
            total_ops_dict['delete'][process_id] = deletes_per_sec
            total_ops_dict['select'][process_id] = selects_per_sec
        except Exception:
            break

##############################################
# Output total operations across all CPUs
##############################################
def log_total_ops_per_interval(args, total_ops_dict, stop_event, lock):
    """This function is now only responsible for printing throughput stats when they are available."""
    while not stop_event.is_set():
        time.sleep(args.report_interval)
        with lock:
            total_selects = sum(total_ops_dict['select'])
            total_inserts = sum(total_ops_dict['insert'])
            total_updates = sum(total_ops_dict['update'])
            total_deletes = sum(total_ops_dict['delete'])
            total_ops = total_selects + total_inserts + total_updates + total_deletes

            if total_ops > 0:
                logging.info(
                    f"{Bcolors.GRAY_TEXT}Throughput last {args.report_interval}s ({args.cpu} CPUs): {Bcolors.BOLD}{Bcolors.HIGHLIGHT}{total_ops:.2f} ops/sec{Bcolors.ENDC}{Bcolors.GRAY_TEXT} "
                    f"(SELECTS: {total_selects:.2f}, INSERTS: {total_inserts:.2f}, "
                    f"UPDATES: {total_updates:.2f}, DELETES: {total_deletes:.2f}){Bcolors.ENDC}"
                )

#####################################################################
# Obtain real-time workload stats for each CPU. This is stored in the
# output_queue which is later summarized by the main application file
#####################################################################
def workload_stats(select_count, insert_count, update_count, delete_count, process_id, output_queue):
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
async def collection_stats_async(collection_def, collections, collection_queue):
    collstats_dict = {}

    for entry in collection_def:
        base_collection_name = entry["collectionName"]
        db_name = entry["databaseName"]

        client = get_client()
        db = client[db_name]

        for i in range(1, collections + 1):
            collection_name_with_suffix = f"{base_collection_name}_{i}" if collections > 1 else base_collection_name
            try:
                collstats = await db.command("collstats", collection_name_with_suffix)
                collstats_dict[collection_name_with_suffix] = {
                    "db": db_name,
                    "sharded": collstats.get("sharded", False),
                    "size": collstats.get("size", 0),
                    "documents": collstats.get("count", 0),
                }
            except PyMongoError as e:
                print(f"Error retrieving stats for {db_name}.{collection_name_with_suffix}: {e}")

    collection_queue.put(collstats_dict)

#############################################################
# Randomly choose operations and collections for the workload
#############################################################

##############################################################################
# WORKER FOR RANDOMIZED WORKLOAD (Used when a user query file is not provided)
##############################################################################
# In app.py, replace your random_worker_async function with this corrected version

async def random_worker_async(args, created_collections, collection_def, stop_event):
    runtime = args.runtime
    skip_update = args.skip_update
    skip_delete = args.skip_delete
    skip_insert = args.skip_insert
    skip_select = args.skip_select
    insert_ratio = args.insert_ratio if args.insert_ratio is not None else 10
    update_ratio = args.update_ratio if args.update_ratio is not None else 20
    delete_ratio = args.delete_ratio if args.delete_ratio is not None else 10
    select_ratio = args.select_ratio if args.select_ratio is not None else 60
    optimized = bool(args.optimized)

    # --- PRE-BUILD A POOL OF REUSABLE QUERIES ---
    query_pool = []
    if select_ratio > 0 and not skip_select:
        logging.debug(f"Worker process {os.getpid()} is pre-building a query pool for SELECT operations...")
        num_queries_to_build = 1000 

        for _ in range(num_queries_to_build):
            random_db, random_collection = random.choice(created_collections)
            
            metadata = collection_metadata_cache.get((random_db, random_collection))
            if not metadata: continue

            field_schema = metadata.get("field_schema", {})
            primary_key = metadata.get("primary_key", "_id")
            
            # --- THIS IS THE CORRECTED LINE ---
            # Pass empty lists instead of None to prevent the TypeError
            templates, _ = mongodbLoadQueries.select_queries([], [], primary_key, optimized=True)
            if not templates: continue
            template = templates[0]

            pk_type = field_schema.get(primary_key, {}).get("type", "string")
            pk_value = generate_random_value(pk_type)
            value_map = {"{pk_value}": pk_value}
            final_query = mongodbLoadQueries._fill_template(template, value_map)
            
            query_pool.append((random_db, random_collection, final_query))
    
    if not query_pool and select_ratio > 0 and not skip_select:
        logging.warning("Query pool is empty. SELECT operations may not run.")

    # WARM-UP PHASE
    if insert_ratio > 0:
        if args.debug:
            logging.debug("Worker starting warm-up phase to pre-populate data...")
        for db_name, collection_name in created_collections:
            base_collection = re.sub(r'_\d+$', '', collection_name) if args.collections > 1 else collection_name
            await insert_documents(args, base_collection, db_name, collection_name, collection_def, args.batch_size)
        if args.debug:
            logging.debug("Warm-up complete. Starting main timed workload.")

    # --- MODIFIED WORKER LOOP ---
    work_start = time.time()
    operations = ["insert", "update", "delete", "select"]
    weights = [insert_ratio, update_ratio, delete_ratio, select_ratio]
    task_batch_size = 100 
    pool_size = len(query_pool)
    query_index = 0

    while time.time() - work_start < runtime and not stop_event.is_set():
        tasks = []
        for _ in range(task_batch_size):
            operation = random.choices(operations, weights=weights, k=1)[0]
            
            if operation == "select" and not skip_select and pool_size > 0:
                db, coll, query = query_pool[query_index % pool_size]
                query_index += 1
                tasks.append(select_documents(args, None, db, coll, None, optimized, prebuilt_query=query))
            
            elif operation == "insert" and not skip_insert:
                random_db, random_collection = random.choice(created_collections)
                base_collection = re.sub(r'_\d+$', '', random_collection) if args.collections > 1 else random_collection
                tasks.append(insert_documents(args, base_collection, random_db, random_collection, collection_def, batch_size=10))

            elif operation == "update" and not skip_update:
                random_db, random_collection = random.choice(created_collections)
                base_collection = re.sub(r'_\d+$', '', random_collection) if args.collections > 1 else random_collection
                tasks.append(update_documents(args, base_collection, random_db, random_collection, collection_def, optimized))

            elif operation == "delete" and not skip_delete:
                random_db, random_collection = random.choice(created_collections)
                base_collection = re.sub(r'_\d+$', '', random_collection) if args.collections > 1 else random_collection
                tasks.append(delete_documents(args, base_collection, random_db, random_collection, collection_def, optimized))
            
        if tasks:
            await asyncio.gather(*tasks)

####################################################################################################
# WORKER FOR CUSTOM QUERY MODE (This is used whenthe user has provided their own custom query file)
# Insert calls still use our random generator
####################################################################################################
async def custom_worker_async(args, created_collections, collection_def, user_queries, stop_event):
    global select_count, insert_count, update_count, delete_count, docs_selected, docs_inserted, docs_updated, docs_deleted
    runtime = args.runtime

    select_queries = [q for q in user_queries if q.get("operation") in ["find", "aggregate", "count"]]
    update_queries = [q for q in user_queries if q.get("operation") in ["updateOne", "updateMany"]]
    delete_queries = [q for q in user_queries if q.get("operation") in ["deleteOne", "deleteMany"]]

    operations = []
    weights = []

    select_ratio = args.select_ratio if args.select_ratio is not None else 0
    insert_ratio = args.insert_ratio if args.insert_ratio is not None else 0
    update_ratio = args.update_ratio if args.update_ratio is not None else 0
    delete_ratio = args.delete_ratio if args.delete_ratio is not None else 0

    if not args.skip_select and select_ratio > 0 and select_queries:
        operations.append("select")
        weights.append(select_ratio)
    if not args.skip_update and update_ratio > 0 and update_queries:
        operations.append("update")
        weights.append(update_ratio)
    if not args.skip_insert and insert_ratio > 0:
        operations.append("insert")
        weights.append(insert_ratio)
    if not args.skip_delete and delete_ratio > 0 and delete_queries:
        operations.append("delete")
        weights.append(delete_ratio)

    if not operations:
        logging.warning("No operations available for the given ratios and user query file. Worker is idle.")
        return

    # WARM-UP PHASE: Only run if inserts are part of the workload.
    if insert_ratio > 0:
        if args.debug:
            logging.debug("Worker starting warm-up phase to pre-populate data...")

        for db_name, collection_name in created_collections:
            base_collection = re.sub(r'_\d+$', '', collection_name) if args.collections > 1 else collection_name
            await insert_documents(args, base_collection, db_name, collection_name, collection_def, args.batch_size)

        if args.debug:
            logging.debug("Warm-up complete. Starting main timed workload.")

    work_start = time.time()
    task_batch_size = 100 # How many concurrent tasks to run at once

    while time.time() - work_start < runtime and not stop_event.is_set():
        tasks = []
        for _ in range(task_batch_size):
            chosen_op = random.choices(operations, weights=weights, k=1)[0]

            if chosen_op == "select":
                query_def = random.choice(select_queries)
                # Note: We append the coroutine, but don't await it here
                tasks.append(custom_query_executor.execute_user_query_async(args, query_def, fake, generate_random_value, inserted_primary_keys, collection_primary_keys, collection_metadata_cache))
            elif chosen_op == "update":
                query_def = random.choice(update_queries)
                tasks.append(custom_query_executor.execute_user_query_async(args, query_def, fake, generate_random_value, inserted_primary_keys, collection_primary_keys, collection_metadata_cache))
            elif chosen_op == "delete":
                query_def = random.choice(delete_queries)
                tasks.append(custom_query_executor.execute_user_query_async(args, query_def, fake, generate_random_value, inserted_primary_keys, collection_primary_keys, collection_metadata_cache))
            elif chosen_op == "insert":
                random_db, random_collection = random.choice(created_collections)
                base_collection = re.sub(r'_\d+$', '', random_collection) if args.collections > 1 else random_collection
                tasks.append(insert_documents(args, base_collection, random_db, random_collection, collection_def, args.batch_size))
        
        if tasks:
            # We must handle the results from custom queries to update the counters
            results = await asyncio.gather(*tasks)
            for result in results:
                if result is None: continue # Handles insert tasks which don't return counters

                op_type, op_count, docs_affected = result
                if op_type in ["find", "aggregate", "count"]:
                    select_count += op_count
                    docs_selected += docs_affected
                elif op_type in ["updateOne", "updateMany"]:
                    update_count += op_count
                    docs_updated += docs_affected
                elif op_type in ["deleteOne", "deleteMany"]:
                    delete_count += op_count
                    docs_deleted += docs_affected

####################
# Start the workload
####################
async def start_workload_async(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections, user_queries=None, stop_event=None):
    await init_async(args) # Each process initializes its own client
    await pre_compute_collection_metadata(args, created_collections, collection_def)

    try:
        if user_queries:
            if args.debug:
                logging.debug(f"Process {process_id} is starting with {args.threads} worker coroutines for custom queries.")
            workers = [asyncio.create_task(custom_worker_async(args, created_collections, collection_def, user_queries, stop_event)) for _ in range(args.threads)]
        else:
            if args.debug:
                logging.debug(f"Process {process_id} is starting with {args.threads} worker coroutines for random queries.")
            workers = [asyncio.create_task(random_worker_async(args, created_collections, collection_def, stop_event)) for _ in range(args.threads)]

        stats_updater_task = asyncio.create_task(update_stats_periodically(process_id, total_ops_dict, stop_event, args.report_interval))
        workers.append(stats_updater_task)

        await asyncio.gather(*workers)

    except asyncio.CancelledError:
        pass
    finally:
        workload_stats(select_count, insert_count, update_count, delete_count, process_id, output_queue)
        if process_id == 0:
            await collection_stats_async(collection_def, args.collections, collection_queue)