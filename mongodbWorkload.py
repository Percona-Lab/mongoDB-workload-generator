#!/usr/bin/env python3
import asyncio
import argparse
import sys
import os
import json
import logging
import multiprocessing
import time
import textwrap
import re

from logger import configure_logging
from mongo_client import init_async, close_client_async
import app
import custom_query_executor

class Bcolors:
    # Based on Percona Aqua Palette
    # Using ANSI 256-color escape codes for richer, more specific colors
    # Format: \033[38;5;<COLOR_CODE>m

    # Grays/Neutrals (for less critical info, or default text)
    GRAY_TEXT = '\033[38;5;242m' # A medium gray, similar to aqua-900 text but readable
    LIGHT_GRAY_TEXT = '\033[38;5;250m' # Very light gray for subtle details

    # Aqua Shades for structure and main info
    # Lightest to Darkest Aqua/Green Shades
    AQUA_50  = '\033[38;5;195m'  # Very light, pale cyan
    AQUA_100 = '\033[38;5;158m'  # Light mint green
    AQUA_200 = '\033[38;5;121m'  # Pale seafoam green
    AQUA_300 = '\033[38;5;85m'   # Light seafoam green
    AQUA_400 = '\033[38;5;49m'   # Bright seafoam green
    AQUA_500 = '\033[38;5;36m'   # Core vibrant aqua
    AQUA_600 = '\033[38;5;30m'   # Richer, slightly darker aqua
    AQUA_700 = '\033[38;5;29m'   # Deeper teal
    AQUA_800 = '\033[38;5;23m'   # Dark teal/forest green
    AQUA_900 = '\033[38;5;22m'   # Very dark forest green

    # Specific use cases
    HEADER = AQUA_600     # Headers for sections
    STATS_HEADER = AQUA_700 # Header for each Collection Stats
    WORKLOAD_SETTING = GRAY_TEXT # Workload setting names
    SETTING_VALUE = AQUA_700 # The workload setting value
    HIGHLIGHT = AQUA_500  # Main throughput numbers, key results
    SECONDARY_HIGHLIGHT = LIGHT_GRAY_TEXT # This can be used to highlight something, but not as bright
    ACCENT = AQUA_600     # Table borders, separation lines
    WARNING = '\033[38;5;208m' # Orange for warnings (standard ANSI orange/gold)
    ERROR = '\033[38;5;196m'   # Red for errors (standard ANSI red)

    # Styles
    ENDC = '\033[0m'       # Reset color
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

args = None
collection_def = None
user_queries = None
shard_enabled = False

COLLECTION_DEF_DIR = 'collections/'
CUSTOM_QUERIES_DIR = 'queries/'

def load_collection_definitions(path_or_file=None):
    definitions = []
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
    elif path_or_file.endswith('.json'):
        if not os.path.isabs(path_or_file) and '/' not in path_or_file:
            path_or_file = os.path.join(COLLECTION_DEF_DIR, path_or_file)

        if os.path.isfile(path_or_file):
            files_to_load = [path_or_file]
        else:
            logging.error(f"Error: JSON file '{path_or_file}' not found.")
            sys.exit(1)
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
    elif path_or_file.endswith('.json'):
        if not os.path.isabs(path_or_file) and '/' not in path_or_file:
            path_or_file = os.path.join(CUSTOM_QUERIES_DIR, path_or_file)
        if os.path.isfile(path_or_file):
            files_to_load = [path_or_file]
        else:
            logging.error(f"Error: JSON query file '{path_or_file}' not found.")
            sys.exit(1)
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
# Get workload summary and provide the output
################################################
def workload_summary(workload_output,elapsed_time, specified_duration_str):
    total_stats = {"select": 0, "insert": 0, "delete": 0, "update": 0, "docs_inserted": 0, "docs_selected": 0, "docs_updated": 0, "docs_deleted": 0}
    for entry in workload_output:
        stats = entry["stats"]
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
    else:
        elapsed_time_minutes = elapsed_time / 60
        runtime = f"{elapsed_time_minutes:.2f} minutes"

    workload_stats = textwrap.dedent(f"""
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.ACCENT}{Bcolors.BOLD}{Bcolors.HEADER}{' Combined Workload Stats ':^{table_width - 4}}{Bcolors.ENDC}{Bcolors.ACCENT}
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.GRAY_TEXT}Specified Duration:{Bcolors.ENDC} {specified_duration_str}
{Bcolors.GRAY_TEXT}Total Elapsed Time:{Bcolors.ENDC} {runtime}
{Bcolors.GRAY_TEXT}Total Operations:{Bcolors.ENDC} {Bcolors.BOLD}{total_stats["select"] + total_stats["insert"] + total_stats["update"] + total_stats["delete"]}{Bcolors.ENDC} (SELECT: {total_stats["select"]}, INSERT: {total_stats["insert"]}, UPDATE: {total_stats["update"]}, DELETE: {total_stats["delete"]})
{Bcolors.GRAY_TEXT}Overall Throughput:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.HIGHLIGHT}{(total_stats["select"] + total_stats["insert"] + total_stats["update"] + total_stats["delete"]) / elapsed_time:.2f} ops/sec{Bcolors.ENDC} {Bcolors.GRAY_TEXT}({Bcolors.ENDC}{Bcolors.LIGHT_GRAY_TEXT}SELECTS: {Bcolors.ENDC}{Bcolors.BOLD}{total_stats["select"] / elapsed_time:.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT},{Bcolors.ENDC} {Bcolors.LIGHT_GRAY_TEXT}INSERTS: {Bcolors.ENDC}{Bcolors.BOLD}{total_stats["insert"] / elapsed_time:.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT},{Bcolors.ENDC} {Bcolors.LIGHT_GRAY_TEXT}UPDATES: {Bcolors.ENDC}{Bcolors.BOLD}{total_stats["update"] / elapsed_time:.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT},{Bcolors.ENDC} {Bcolors.LIGHT_GRAY_TEXT}DELETES: {Bcolors.ENDC}{Bcolors.BOLD}{total_stats["delete"] / elapsed_time:.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT}){Bcolors.ENDC}
{Bcolors.GRAY_TEXT}Total:{Bcolors.ENDC} (Documents Inserted: {Bcolors.BOLD}{total_stats["docs_inserted"]}{Bcolors.ENDC} | Documents Found: {Bcolors.BOLD}{total_stats["docs_selected"]}{Bcolors.ENDC} | Documents Updated: {Bcolors.BOLD}{total_stats["docs_updated"]}{Bcolors.ENDC} | Documents Deleted: {Bcolors.BOLD}{total_stats["docs_deleted"]}{Bcolors.ENDC})
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}\n""")
    logging.info(workload_stats)

##################################################
# Get collection summary and provide the output
##################################################  
def collection_summary(collection_output):
    unique_coll_stats = []
    seen = set()
    for item in collection_output:
        collection_name = (list(item.keys())[0])
        if collection_name not in seen:
            seen.add(collection_name)
            unique_coll_stats.append(item)
    
    table = "\n"
    table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
    table += f"{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.HEADER}{'Collection Stats':^98}{Bcolors.ENDC}{Bcolors.ACCENT}| \n"
    table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
    table += f"{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Database':^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Collection':^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Sharded':^16}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Size':^14}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Documents':^15}{Bcolors.ENDC}{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC}\n"
    table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
    
    for coll in unique_coll_stats:
        for coll_name, stats in coll.items():
            size_in_mb = stats["size"] / 1024 / 1024
            if size_in_mb >= 1024:
                size_display = f"{size_in_mb / 1024:.2f} GB"
            else:
                size_display = f"{size_in_mb:.2f} MB"
            table += f"{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.LIGHT_GRAY_TEXT}{str(stats['db']):^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{coll_name:^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{str(stats['sharded']):^16}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{size_display:^14}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{stats['documents']:^15}{Bcolors.ENDC}{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC}\n"
    
    table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
    logging.info(table)

########################################
# Monitor the workload for each CPU used
########################################
def monitor_completion(completed_processes):
    try:
        while not all(completed_processes):
            time.sleep(0.2)
        table_width = 115
        workload_finished = textwrap.dedent(f"""
        {'=' * table_width}
        {' Workload Finished':^{table_width - 2}}
        {'=' * table_width}\n""")
        logging.info(workload_finished)
    except KeyboardInterrupt:
        logging.info("Monitoring interrupted. Cleaning up...")

def logger_process_target(args, total_ops_dict, stop_event, lock):
    """Initializes logging and then starts the reporting loop."""
    log_level = logging.DEBUG if args.debug else logging.INFO
    configure_logging(log_file=args.log, level=log_level)
    app.log_total_ops_per_interval(args, total_ops_dict, stop_event, lock)

#####################################
# Make the call to start the workload
#####################################
def start_process(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections, user_queries, stop_event):
    log_level = logging.DEBUG if args.debug else logging.INFO
    configure_logging(log_file=args.log, level=log_level)
    asyncio.run(app.start_workload_async(args, process_id, completed_processes, output_queue, collection_queue, total_ops_dict, collection_def, created_collections, user_queries, stop_event))

async def main_workload_async(args, collection_def, user_queries, specified_duration_str):
    await init_async() # Main process connects
    created_collections = await app.create_collection(collection_def, args.collections, args.recreate)
    await close_client_async() # Main process disconnects BEFORE forking
    ratios = app.workload_ratio_config(args)

    with multiprocessing.Manager() as manager:
        completed_processes = manager.list([False] * args.cpu)
        output_queue = manager.Queue()
        collection_queue = manager.Queue()
        stop_event = manager.Event()

        total_ops_dict = manager.dict({
            'insert': manager.list([0] * args.cpu),
            'update': manager.list([0] * args.cpu),
            'delete': manager.list([0] * args.cpu),
            'select': manager.list([0] * args.cpu),
        })

        app.log_workload_config(collection_def, args, shard_enabled, args.runtime, ratios)

        start_time = time.time()
        
        processes = []
        for process_id in range(args.cpu):
            p = multiprocessing.Process(
                target=start_process,
                args=(
                    args, process_id, completed_processes, output_queue,
                    collection_queue, total_ops_dict, collection_def,
                    created_collections, user_queries, stop_event
                )
            )
            processes.append(p)
            p.start()
        
        lock = manager.Lock()
        logger_process = multiprocessing.Process(
            target=logger_process_target,
            args=(args, total_ops_dict, stop_event, lock)
        )
        logger_process.start()
        
        end_time = start_time + args.runtime
        try:
            # Wait for the runtime duration while being responsive to Ctrl+C
            while time.time() < end_time:
                time.sleep(1) 
        except KeyboardInterrupt:
            logging.info("\n[!] Ctrl+C detected! Stopping workload...")
        finally:
            stop_event.set()

        for p in processes:
            p.join()
        
        logger_process.join()
        
        elapsed_time = time.time() - start_time
        workload_output = []
        while not output_queue.empty():
            workload_output.append(output_queue.get())
        
        collection_output = []
        while not collection_queue.empty():
            collection_output.append(collection_queue.get())
            
        collection_summary(collection_output)
        workload_summary(workload_output, elapsed_time, specified_duration_str)


def main():        
    parser = argparse.ArgumentParser(description="MongoDB Workload Generator")
    parser.add_argument("--runtime", default="60s", help="The total duration to run the workload (e.g., 60s, 5m).")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads per process. (Total threads = threads * cpu)")
    parser.add_argument("--cpu", type=int, default=1, help="Number of CPUs/processes to use.")
    parser.add_argument("--custom_queries", help="Path to a JSON file with custom queries.")
    parser.add_argument("--collections", type=int, default=1, help="Number of collections to use.")
    parser.add_argument("--recreate", action="store_true", help="Drops the collections before starting the workload.")
    parser.add_argument("--batch_size", type=int, default=10, help="Number of documents to insert in each batch.")
    parser.add_argument("--optimized", action="store_true", help="Use more efficient queries (i.e. 'find_one', 'update_one', 'delete_one').")
    parser.add_argument("--insert_ratio", type=float, default=None, help="Workload ratio for insert operations.")
    parser.add_argument("--select_ratio", type=float, default=None, help="Workload ratio for select operations.")
    parser.add_argument("--update_ratio", type=float, default=None, help="Workload ratio for update operations.")
    parser.add_argument("--delete_ratio", type=float, default=None, help="Workload ratio for delete operations.")
    parser.add_argument("--report_interval", type=int, default=5, help="Frequency (in seconds) to report operations per second.")
    parser.add_argument("--log", help="Path and filename for log output.")
    parser.add_argument("--skip_insert", action="store_true", help="Skip all insert operations.")
    parser.add_argument("--skip_select", action="store_true", help="Skip all select operations.")
    parser.add_argument("--skip_update", action="store_true", help="Skip all update operations.")
    parser.add_argument("--skip_delete", action="store_true", help="Skip all delete operations.")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode to show detailed output.")
    parser.add_argument("--collection_definition", help="Path to a JSON file or directory with collection definitions.")
    
    args = parser.parse_args()

    user_queries = None
    
    log_level = logging.DEBUG if args.debug else logging.INFO
    configure_logging(log_file=args.log, level=log_level)
    
    if args.custom_queries and not args.collection_definition:
        logging.fatal("Error: The --collection_definition parameter is required when using --custom_queries.")
        sys.exit(1)
    if args.custom_queries and args.collections > 1:
        logging.info(f"User query path provided. Forcing --collections value from {args.collections} to 1.")
        args.collections = 1
    if args.log is True:
        logging.error(f"Error: The --log option requires a filename and path (e.g., /tmp/report.log).")
        sys.exit(1)
        
    available_cpus = os.cpu_count()
    if args.cpu > available_cpus:
        logging.info(f"Cannot set CPU to {args.cpu} as there are only {available_cpus} available. Workload will be configured to use {available_cpus} CPUs.")
        args.cpu = available_cpus

    specified_duration_str = args.runtime # Default value

    if args.runtime.endswith("m"):
        duration = int(args.runtime[:-1])
        args.runtime = duration * 60
        specified_duration_str = f"{duration:.2f} minutes" # User-friendly string
    elif args.runtime.endswith("s"):
        duration = int(args.runtime[:-1])
        args.runtime = duration
        specified_duration_str = f"{duration:.2f} seconds" # User-friendly string
        
    else:
        raise ValueError("Invalid time format. Use '60s' for seconds or '5m' for minutes.")

    if args.collection_definition:
        collection_def = load_collection_definitions(args.collection_definition)
    else:
        collection_def = load_collection_definitions()
    
    if args.custom_queries:
        user_queries = load_custom_queries(args.custom_queries)
        if user_queries is None:
            sys.exit(1)
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

    asyncio.run(main_workload_async(args, collection_def, user_queries, specified_duration_str))

###############################
# Main section to start the app
###############################
if __name__ == "__main__":
    if sys.platform != 'win32':
        multiprocessing.set_start_method('fork')
    main()    