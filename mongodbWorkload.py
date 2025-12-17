#!/usr/bin/env python3
from args import parser
import args as args_module  # so we can save the parsed args globally
from joblib import Parallel, delayed # type: ignore
import multiprocessing
import app  
# import custom as app
import logging
import time
import textwrap
import argparse
import sys
import os
import json 

from custom_query_executor import load_queries_from_path

args = parser.parse_args()
args_module.args = args

# If a user query path is given, a collection definition must also be provided.
if args.custom_queries and not args.collection_definition:
    logging.fatal(
        "Error: The --collection_definition parameter is required when using --custom_queries."
    )
    sys.exit(1)

# If a user query path is provided, force the number of collections to 1.
if args.custom_queries and args.collections > 1:
    logging.info(
        f"User query path provided. Forcing --collections value from {args.collections} to 1."
    )
    args.collections = 1

from logger import configure_logging
# ---- SET LOGGING LEVEL AND CONFIGURE HANDLERS ----
log_level = logging.DEBUG if args.debug else logging.INFO
configure_logging(log_file=args.log if hasattr(args, "log") else None, level=log_level)
# --------------------------------------------------

import mongo_client
mongo_client.init()

# Validate --log argument
if args.log is True:  # If --log is used but no file is provided
    logging.error(f"Error: The --log option requires a filename and path (e.g., /tmp/report.log).")
    sys.exit(1)

collection_def = []
shard_enabled = False
COLLECTION_DEF_DIR = 'collections/'  # Default folder
CUSTOM_QUERIES_DIR = 'queries/' # Default queries folder

def load_collection_definitions(path_or_file=None):
    definitions = []

    # Default: load all files in collections/ if nothing provided or --collection_definition used without value
    if path_or_file is None or path_or_file == 'collections':
        folder = COLLECTION_DEF_DIR
        if not os.path.isdir(folder):
            logging.error(f"Error: Default collection definition directory '{folder}' not found.")
            sys.exit(1)
        files_to_load = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.endswith('.json') and os.path.isfile(os.path.join(folder, f))
        ]
        if not files_to_load:
            logging.error(f"No JSON files found in directory '{folder}'")
            sys.exit(1)

    # If user provides a file
    elif path_or_file.endswith('.json'):
        # Prepend default directory if it's just a filename
        if not os.path.isabs(path_or_file) and '/' not in path_or_file:
            path_or_file = os.path.join(COLLECTION_DEF_DIR, path_or_file)

        if os.path.isfile(path_or_file):
            files_to_load = [path_or_file]
        else:
            logging.error(f"Error: JSON file '{path_or_file}' not found.")
            sys.exit(1)

    # If user provides a folder
    elif os.path.isdir(path_or_file):
        files_to_load = [
            os.path.join(path_or_file, f)
            for f in os.listdir(path_or_file)
            if f.endswith('.json') and os.path.isfile(os.path.join(path_or_file, f))
        ]
        if not files_to_load:
            logging.error(f"No JSON files found in directory '{path_or_file}'")
            sys.exit(1)
    else:
        logging.error(f"Error: '{path_or_file}' is not a valid JSON file or directory.")
        sys.exit(1)

    # Load and validate each file
    for filepath in files_to_load:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

                if isinstance(data, dict):
                    data = [data]
                elif not isinstance(data, list):
                    logging.warning(f"Skipping file '{filepath}': Root element must be a dict or list of dicts.")
                    continue

                for item in data:
                    database = item.get("databaseName")
                    collection = item.get("collectionName")
                    shard_config = item.get("shardConfig")

                    if not database or not collection:
                        logging.error(f"Invalid collection definition in file '{filepath}': Missing 'databaseName' or 'collectionName'.")
                        sys.exit(1)

                    if shard_config:
                        global shard_enabled
                        shard_enabled = True

                    definitions.append(item)

                logging.info(f"Loaded {len(data)} collection definition(s) from '{filepath}'")

        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON in file '{filepath}': {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Unexpected error while loading '{filepath}': {e}")
            sys.exit(1)

    if not definitions:
        logging.error("No valid collection definitions found after loading.")
        sys.exit(1)

    return definitions

def load_custom_queries(path_or_file=None):
    queries = []

    # Default: load all files in queries/ if nothing provided or --custom_queries used without value
    if path_or_file is None or path_or_file == 'queries':
        folder = CUSTOM_QUERIES_DIR
        if not os.path.isdir(folder):
            logging.error(f"Error: Default custom queries directory '{folder}' not found.")
            sys.exit(1)
        files_to_load = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.endswith('.json') and os.path.isfile(os.path.join(folder, f))
        ]
        if not files_to_load:
            logging.error(f"No JSON files found in directory '{folder}'")
            sys.exit(1)
    # If user provides a file
    elif path_or_file.endswith('.json'):
        if not os.path.isabs(path_or_file) and '/' not in path_or_file:
            path_or_file = os.path.join(CUSTOM_QUERIES_DIR, path_or_file) # Consider if this is the desired behavior for queries
        if os.path.isfile(path_or_file):
            files_to_load = [path_or_file]
        else:
            logging.error(f"Error: JSON query file '{path_or_file}' not found.")
            sys.exit(1)
    # If user provides a folder
    elif os.path.isdir(path_or_file):
        files_to_load = [
            os.path.join(path_or_file, f)
            for f in os.listdir(path_or_file)
            if f.endswith('.json') and os.path.isfile(os.path.join(path_or_file, f))
        ]
        if not files_to_load:
            logging.error(f"No JSON files found in directory '{path_or_file}'")
            sys.exit(1)
    else:
        logging.error(f"Error: '{path_or_file}' is not a valid JSON query file or directory.")
        sys.exit(1)

    for filepath in files_to_load:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    logging.warning(f"Skipping file '{filepath}': Root element for queries must be a list of dicts.")
                    continue
                queries.extend(data)
                logging.info(f"Loaded {len(data)} custom queries from '{filepath}'")
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing JSON in file '{filepath}': {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Unexpected error while loading '{filepath}': {e}")
            sys.exit(1)

    if not queries:
        logging.error("No valid custom queries found after loading.")
        sys.exit(1)

    return queries

################################################
# Obtain workload summary and provide the output
################################################
def workload_summary(workload_output,elapsed_time):
    # Initialize a dictionary to store the total sums
    total_stats = {"select": 0, "insert": 0, "delete": 0, "update": 0, "docs_inserted": 0, "docs_selected": 0, "docs_updated": 0, "docs_deleted": 0}
    # Iterate through each process entry in the workload list
    for entry in workload_output:
        stats = entry["stats"]
        # Sum up each stat
        total_stats["select"] += stats["select"]
        total_stats["insert"] += stats["insert"]
        total_stats["delete"] += stats["delete"]
        total_stats["update"] += stats["update"]
        total_stats["docs_inserted"] += stats["docs_inserted"]
        total_stats["docs_selected"] += stats["docs_selected"]
        total_stats["docs_updated"] += stats["docs_updated"]
        total_stats["docs_deleted"] += stats["docs_deleted"]
    
    table_width = 115

    if elapsed_time < 60:
        runtime = f"{elapsed_time:.2f} seconds"
        # logging.info(f"Workload Runtime: {elapsed_time:.2f} seconds")
    else:
        elapsed_time_minutes = elapsed_time / 60
        runtime = f"{elapsed_time_minutes:.2f} minutes"
        # logging.info(f"Workload Runtime: {elapsed_time_minutes:.2f} minutes")

    workload_stats = textwrap.dedent(f"""
{'=' * table_width}
{' Combined Workload Stats ':^{table_width - 2}}
{'=' * table_width}
Workload Runtime: {runtime}
Total Operations: {total_stats["select"] + total_stats["insert"] + total_stats["update"] + total_stats["delete"]} (SELECT: {total_stats["select"]}, INSERT: {total_stats["insert"]}, UPDATE: {total_stats["update"]}, DELETE: {total_stats["delete"]})
AVG Operations: {(total_stats["select"] + total_stats["insert"] + total_stats["update"] + total_stats["delete"]) / elapsed_time:.2f} (SELECTS: {total_stats["select"] / elapsed_time:.2f}, INSERTS: {total_stats["insert"] / elapsed_time:.2f}, UPDATES: {total_stats["update"] / elapsed_time:.2f}, DELETES: {total_stats["delete"] / elapsed_time:.2f})
Total: (Documents Inserted: {total_stats["docs_inserted"]} | Documents Found: {total_stats["docs_selected"]} | Documents Updated: {total_stats["docs_updated"]} | Documents Deleted: {total_stats["docs_deleted"]})
{'=' * table_width}\n""")
    logging.info(workload_stats)

##################################################
# Obtain collection summary and provide the output
##################################################   
def collection_summary(collection_output):
    unique_coll_stats = []
    seen = set()
    # Dedupe the collections (multiple CPU processes could run the same stats query)
    for item in collection_output:
        collection_name = (list(item.keys())[0])
        if collection_name not in seen:
            seen.add(collection_name)
            unique_coll_stats.append(item)

    # Build the entire table as a single string
    table = "\n"
    table += "="*100 + "\n"
    table += f"|{'Collection Stats':^98}| \n"
    table += "="*100 + "\n"
    table += f"| {'Database':^20} | {'Collection':^20} | {'Sharded':^16} | {'Size':^14} | {'Documents':^15}|\n"
    table += "="*100 + "\n"

    # Print the unique stats for each collection
    for coll in unique_coll_stats:
        for coll_name, stats in coll.items():
            # Create collection stats for each collection in the dictionary
            # The strange ident below is so it lines up nicely with the rest of the other output messages
            size_in_mb = stats["size"] / 1024 / 1024  # Size in MB
            if size_in_mb >= 1024:
                # Convert to GB if the size is larger than 1024MB
                size_display = f"{size_in_mb / 1024:.2f} GB"
            else:
                # Otherwise, display in MB
                size_display = f"{size_in_mb:.2f} MB"

            # Add the collection's stats to the table
            table += f"| {str(stats['db']):^20} | {coll_name:^20} | {str(stats['sharded']):^16} | {size_display:^14} | {stats['documents']:^15}|\n"

    table += "="*100 + "\n"
    # Output the entire table in one call
    logging.info(table)    

########################################
# Monitor the workload for each CPU used
########################################
def monitor_completion(completed_processes):
    try:
        while not all(completed_processes):
            time.sleep(0.2)
        stop_event.set()
        total_ops_logger.join()  # Gracefully wait for logging process to finish
        total_ops_logger.close()  # Close process explicitly    

        table_width = 115
        workload_finished = textwrap.dedent(f"""
        {'=' * table_width}
        {' Workload Finished':^{table_width - 2}}
        {'=' * table_width}\n""")

        logging.info(workload_finished)
    except KeyboardInterrupt:
        logging.info("Monitoring interrupted. Cleaning up...")

#####################################
# Make the call to start the workload
# We use a slightly delayed start for each CPU to prevent some of the logging to get duplicated
#####################################
def delayed_start(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections, user_queries=None):
    time.sleep(0.2)
    return app.start_workload(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections, user_queries)


###############################
# Main section to start the app
###############################
if __name__ == "__main__":
    # Parse collection definitions
    # collection_definition_path = getattr(args, 'collection_definition', None)
    # collection_def = load_collection_definitions(collection_definition_path)

    if args.collection_definition:
        collection_def = load_collection_definitions(args.collection_definition)
    else:
        collection_def = load_collection_definitions() # Load default if no argument provided

    # Validate CPU count
    available_cpus = os.cpu_count()
    if args.cpu > available_cpus:
        logging.info(f"Cannot set CPU to {args.cpu} as there are only {available_cpus} available. Workload will be configured to use {available_cpus} CPUs.")
        args.cpu = available_cpus

    # Parse runtime
    if args.runtime.endswith("m"):
        duration = int(args.runtime[:-1])        
        args.runtime = duration * 60
        workload_length = str(duration) + " minutes"
    elif args.runtime.endswith("s"):
        duration = int(args.runtime[:-1])
        args.runtime = duration
        workload_length = str(duration) + " seconds"
    else:
        raise ValueError("Invalid time format. Use '60s' for seconds or '5m' for minutes.")    

    # Create collections (run once)
    created_collections = app.create_collection(collection_def, args.collections, args.recreate)

    # Load user queries if the file is provided
    user_queries = None
    # if args.custom_queries:
    #     user_queries = load_queries_from_path(args.custom_queries)
    #     if user_queries is None: # None indicates a fatal error during loading
    #         sys.exit(1)

    if args.custom_queries:
        user_queries = load_custom_queries(args.custom_queries)
        if user_queries is None: # None indicates a fatal error during loading
                sys.exit(1)

    # After loading both definitions and queries, validate them.
    if user_queries:
        # Create a set of valid "database.collection" names
        valid_collections = {f"{c['databaseName']}.{c['collectionName']}" for c in collection_def}
        
        for query in user_queries:
            target_coll = f"{query.get('database')}.{query.get('collection')}"
            
            if target_coll not in valid_collections:
                logging.fatal(
                    f"Validation Error: A query targets collection '{target_coll}', "
                    f"but this collection is not defined in your --collection_definition files."
                )
                sys.exit(1)
        logging.info("All custom queries were successfully validated against collection definitions.")

    start_time = time.time()

    # Configure workload ratio 
    workload_ratios = app.workload_ratio_config(args)
        
    app.log_workload_config(collection_def, args, shard_enabled, workload_length, workload_ratios, workload_logged=False)


    workload_output = []
    collection_output = []

    with multiprocessing.Manager() as manager:
        completed_processes = manager.list([False] * args.cpu)
        output_queue = manager.Queue()
        collection_queue = manager.Queue()
        workload_logged = manager.Value('b', False)
        collection_logged = manager.Value('b', False)

        total_ops_dict = manager.dict({
            'insert': manager.list([0] * args.cpu),
            'update': manager.list([0] * args.cpu),
            'delete': manager.list([0] * args.cpu),
            'select': manager.list([0] * args.cpu),
        })

        lock = multiprocessing.Lock()
        stop_event = multiprocessing.Event()

        total_ops_logger = multiprocessing.Process(
            target=app.log_total_ops_per_interval,
            args=(args, total_ops_dict, stop_event, lock)
        )
        total_ops_logger.start()

        # Launch workload in parallel
        parallel_executor = Parallel(n_jobs=args.cpu)
        parallel_executor(
            delayed(delayed_start)(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections, user_queries)
            for process_id in range(args.cpu)
        )

        # Gather CPU outputs
        while not output_queue.empty():
            workload_output.append(output_queue.get())
        while not collection_queue.empty():
            collection_output.append(collection_queue.get())

        monitor_completion(completed_processes)

    # Summaries
    elapsed_time = time.time() - start_time
    collection_summary(collection_output)
    workload_summary(workload_output, elapsed_time)

    
