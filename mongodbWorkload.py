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
import reporting
from logger import configure_logging
from mongo_client import init_async, close_client_async
import app
import custom_query_executor
import generic_workload
from colors import Bcolors
import queue


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


###################################
# Function to run generic workload
###################################
def run_generic_workload(args):
    """Orchestrates the multiprocessing for the 'run' phase of the generic workload."""

    specified_duration_str = args.runtime
    if isinstance(args.runtime, str):
        try:
            if args.runtime.endswith("m"): args.runtime = int(args.runtime[:-1]) * 60
            elif args.runtime.endswith("s"): args.runtime = int(args.runtime[:-1])
            else: args.runtime = int(args.runtime)
        except (ValueError, AttributeError):
            logging.error(f"Invalid time format for --runtime: '{args.runtime}'.")
            sys.exit(1)

    target_ids = asyncio.run(generic_workload.get_target_ids(args))
    if not target_ids and args.type in ['find', 'update', 'delete']:
         print("Warning: Could not fetch target IDs. Find, update, and delete workloads will not run.")

    collection_def = " | ".join([f"{args.db}.{args.collection}"])

    reporting.log_workload_config(collection_def, args, shard_enabled=None, workload_length=None, workload_ratios=None)
      

    with multiprocessing.Manager() as manager:
        stats_queue = manager.Queue()
        stop_event = manager.Event()

        processes = []
        for i in range(args.cpu):
            p = multiprocessing.Process(
                target=start_generic_process,
                args=(args, target_ids, stats_queue, stop_event)
            )
            processes.append(p)
            p.start()

        start_time = time.time()
        last_report_time = start_time

        total_ops, total_select_ops, total_insert_ops, total_update_ops, total_delete_ops = 0, 0, 0, 0, 0
        total_docs_found, total_docs_inserted, total_docs_modified, total_docs_deleted = 0, 0, 0, 0

        try:
            while time.time() - start_time < args.runtime:
                time.sleep(1) 

                if time.time() - last_report_time < args.report_interval:
                    continue

                select_ops_interval, insert_ops_interval, update_ops_interval, delete_ops_interval = 0, 0, 0, 0

                while not stats_queue.empty():
                    try:
                        stats = stats_queue.get_nowait()
                        ops_in_report = stats.get("total_ops", 0)

                        if "docs_found" in stats:
                            select_ops_interval += ops_in_report
                            total_docs_found += stats.get("docs_found", 0)
                        elif "docs_inserted" in stats:
                            insert_ops_interval += ops_in_report
                            total_docs_inserted += stats.get("docs_inserted", 0)
                        elif "docs_modified" in stats:
                            update_ops_interval += ops_in_report
                            total_docs_modified += stats.get("docs_modified", 0)
                        elif "docs_deleted" in stats:
                            delete_ops_interval += ops_in_report
                            total_docs_deleted += stats.get("docs_deleted", 0)
                    except queue.Empty:
                        break

                total_select_ops += select_ops_interval
                total_insert_ops += insert_ops_interval
                total_update_ops += update_ops_interval
                total_delete_ops += delete_ops_interval

                elapsed = time.time() - last_report_time

                total_throughput = (select_ops_interval + insert_ops_interval + update_ops_interval + delete_ops_interval) / elapsed if elapsed > 0 else 0
                select_throughput = select_ops_interval / elapsed if elapsed > 0 else 0
                insert_throughput = insert_ops_interval / elapsed if elapsed > 0 else 0
                update_throughput = update_ops_interval / elapsed if elapsed > 0 else 0
                delete_throughput = delete_ops_interval / elapsed if elapsed > 0 else 0

                if total_throughput > 0:
                    logging.info(
                        f"{Bcolors.GRAY_TEXT}Throughput last {elapsed:.1f}s ({args.cpu} CPUs): {Bcolors.BOLD}{Bcolors.HIGHLIGHT}{total_throughput:.2f} ops/sec{Bcolors.ENDC}{Bcolors.GRAY_TEXT} "
                        f"(SELECTS: {select_throughput:.2f}, INSERTS: {insert_throughput:.2f}, "
                        f"UPDATES: {update_throughput:.2f}, DELETES: {delete_throughput:.2f}){Bcolors.ENDC}"
                    )
                last_report_time = time.time()

        except KeyboardInterrupt:
            logging.info("\n[!] Ctrl+C detected! Stopping workload...")
        finally:
            stop_event.set()
            for p in processes:
                p.join(timeout=5)

        end_time = time.time()

        while not stats_queue.empty():
            try:
                stats = stats_queue.get_nowait()
                ops_in_report = stats.get("total_ops", 0)

                if "docs_found" in stats:
                    total_select_ops += ops_in_report
                    total_docs_found += stats.get("docs_found", 0)
                elif "docs_inserted" in stats:
                    total_insert_ops += ops_in_report
                    total_docs_inserted += stats.get("docs_inserted", 0)
                elif "docs_modified" in stats:
                    total_update_ops += ops_in_report
                    total_docs_modified += stats.get("docs_modified", 0)
                elif "docs_deleted" in stats:
                    total_delete_ops += ops_in_report
                    total_docs_deleted += stats.get("docs_deleted", 0)
            except queue.Empty:
                break

        total_ops = total_select_ops + total_insert_ops + total_update_ops + total_delete_ops
        duration = end_time - start_time

        asyncio.run(reporting.fetch_and_log_collection_stats(args))

        reporting.log_generic_summary(
            total_ops, total_select_ops, total_insert_ops, total_update_ops, total_delete_ops,
            total_docs_found, total_docs_inserted, total_docs_modified, total_docs_deleted,
            duration, specified_duration_str
        )

def start_generic_process(args, target_ids, output_queue, stop_event):
    """Wrapper to call the async process worker from app.py."""
    asyncio.run(app.start_generic_workload_async(args, target_ids, output_queue, stop_event))





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

    app.pre_cache_all_queries(args,collection_def) # Pre-populate the query cache
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

        reporting.log_workload_config(collection_def, args, shard_enabled, args.runtime, ratios)

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


# In mongodbWorkload.py, replace the main() function

def main():
    parser = argparse.ArgumentParser(description="MongoDB Workload Generator")
    
    # --- New Flag to select workload type ---
    parser.add_argument("--generic", action="store_true", help="Run the high-throughput generic point-query workload.")

    original_group = parser.add_argument_group('Default and Custom Workload Options')
    original_group.add_argument("--custom_queries", help="Path to a JSON file with custom queries.")
    original_group.add_argument("--collection_definition", help="Path to a JSON file or directory with collection definitions.")
    original_group.add_argument("--collections", type=int, default=1, help="Number of collections to use.")
    original_group.add_argument("--recreate", action="store_true", help="Drops the collections before starting the workload.")
    original_group.add_argument("--optimized", action="store_true", help="Use more efficient queries (i.e. 'find_one', 'update_one', 'delete_one').")
    original_group.add_argument("--insert_ratio", type=float, default=None, help="Workload ratio for insert operations.")
    original_group.add_argument("--select_ratio", type=float, default=None, help="Workload ratio for select operations.")
    original_group.add_argument("--update_ratio", type=float, default=None, help="Workload ratio for update operations.")
    original_group.add_argument("--delete_ratio", type=float, default=None, help="Workload ratio for delete operations.")
    original_group.add_argument("--skip_insert", action="store_true", help="Skip all insert operations.")
    original_group.add_argument("--skip_select", action="store_true", help="Skip all select operations.")
    original_group.add_argument("--skip_update", action="store_true", help="Skip all update operations.")
    original_group.add_argument("--skip_delete", action="store_true", help="Skip all delete operations.")

    # --- Arguments for the generic workload ---
    generic_group = parser.add_argument_group('Generic Workload Options (used with --generic)')
    generic_group.add_argument("command", nargs='?', choices=["prepare", "run", "cleanup"], help="The command for the generic workload: 'prepare', 'run', or 'cleanup'.")
    generic_group.add_argument("--db", default="benchmark", help="Database name for the generic workload.")
    generic_group.add_argument("--collection", default="pointquery", help="Collection name for the generic workload.")
    generic_group.add_argument("--num_docs", type=int, default=100000, help="Number of documents for the 'prepare' step.")

    generic_group.add_argument('--type', 
                type=str, 
                default='find', 
                choices=['find', 'update', 'delete', 'insert', 'mixed'], 
                help='The type of generic workload to run: find, update, delete, insert, or mixed.')


    # --- General arguments for both workloads ---
    parser.add_argument("--runtime", default="60s", help="The total duration to run the workload (e.g., 60s, 5m). Default 60s")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads (coroutines) per process. Default 4")
    parser.add_argument("--cpu", type=int, default=1, help="Number of CPUs/processes to use. Default 1")
    parser.add_argument("--batch_size", type=int, default=100, help="Number of documents to insert in each batch. Default 100")
    parser.add_argument("--report_interval", type=int, default=5, help="Frequency (in seconds) to report operations per second. Default 5s")
    parser.add_argument("--log", help="Path and filename for log output.")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode to show detailed output.")

    args = parser.parse_args()
    
    log_level = logging.DEBUG if args.debug else logging.INFO
    configure_logging(log_file=args.log, level=log_level)

    # --- Main Logic Branch ---
    if args.generic:
        if not args.command:
            parser.error("The '--generic' flag requires a command: 'prepare', 'run', or 'cleanup'.")
        
        # Execute the generic workload command
        try:
            if args.command == "prepare":
                asyncio.run(generic_workload.prepare(args))
            elif args.command == "run":
                # The 'run' command needs its own main function to manage processes
                run_generic_workload(args)
            elif args.command == "cleanup":
                asyncio.run(generic_workload.cleanup(args))
        except KeyboardInterrupt:
            print("\nOperation cancelled by user.")
        sys.exit(0)

    # --- Else, run the original workload (existing logic) ---
    else:
        user_queries = None
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
        
        specified_duration_str = args.runtime
        if args.runtime.endswith("m"):
            duration = int(args.runtime[:-1])
            args.runtime = duration * 60
            specified_duration_str = f"{duration:.2f} minutes"
        elif args.runtime.endswith("s"):
            duration = int(args.runtime[:-1])
            args.runtime = duration
            specified_duration_str = f"{duration:.2f} seconds"
        else:
            raise ValueError("Invalid time format. Use '60s' for seconds or '5m' for minutes.")

        if args.collection_definition:
            collection_def = load_collection_definitions(args.collection_definition)
        else:
            collection_def = load_collection_definitions()

        if args.custom_queries:
            user_queries = load_custom_queries(args.custom_queries)
            # ... (rest of your original validation logic)
        
        asyncio.run(main_workload_async(args, collection_def, user_queries, specified_duration_str))

###############################
# Main section to start the app
###############################
if __name__ == "__main__":
    if sys.platform != 'win32':
        multiprocessing.set_start_method('fork')
    main()