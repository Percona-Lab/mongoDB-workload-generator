import textwrap
import logging
import asyncio
import time
import mongo_client
from colors import Bcolors


def log_workload_config(collection_def, args, shard_enabled=None, workload_length=None, workload_ratios=None):
    """Prints the initial configuration block for any workload."""
    
    if isinstance(args.runtime, int):
        duration_str = f"{args.runtime} seconds"
    else:
        duration_str = args.runtime

    collection_info = "N/A"  # Initialize with a default value for invalid types.

    # First, handle the string case directly.
    if isinstance(collection_def, str):
        collection_info = collection_def
        
    # If a single dictionary make it into a list so it can be processed the same way.
    elif isinstance(collection_def, dict):
        collection_def = [collection_def] # Convert dict to a list with one item

    # Now, if the input was a list (or was just converted from a dict), process it.
    if isinstance(collection_def, list):
        collection_info = " | ".join([
            f"{item.get('databaseName', '?')}.{item.get('collectionName', '?')}" 
            for item in collection_def
        ])

    if shard_enabled:
        status_color = Bcolors.SETTING_VALUE
        status_text = shard_enabled
    else:
        status_color = Bcolors.DISABLED
        status_text = "Disabled"


    if args.custom_queries or args.generic:
        instances_color = Bcolors.DISABLED
        instances_text = "Disabled"
    else:
        instances_color = Bcolors.SETTING_VALUE
        instances_text = args.collections

        
    settings = [
        f"{Bcolors.WORKLOAD_SETTING}Configure Sharding:{Bcolors.ENDC} {Bcolors.BOLD}{status_color}{status_text}{Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}Database and Collection:{Bcolors.ENDC}{Bcolors.BOLD}{Bcolors.SETTING_VALUE}({collection_info}){Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}Insert batch size:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.batch_size}{Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}Instances of the same collection:{Bcolors.ENDC} {Bcolors.BOLD}{instances_color}{instances_text}{Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}Duration:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{duration_str}{Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}CPUs:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.cpu}{Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}Threads:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}(Per CPU: {args.threads} | Total: {args.cpu * args.threads}{Bcolors.ENDC})",
        f"{Bcolors.WORKLOAD_SETTING}Report frequency:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.report_interval} seconds{Bcolors.ENDC}",
        f"{Bcolors.WORKLOAD_SETTING}Report logfile:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.log}{Bcolors.ENDC}"
    ]

    
    workload_type_str = ""
    if args.generic:
        workload_type_str = "Generic"
        optimized_status = "Disabled"
        optimized_color = Bcolors.DISABLED 
    elif args.custom_queries or args.collection_definition:
        workload_type_str = "Custom"
        optimized_status = "Disabled"
        optimized_color = Bcolors.DISABLED
    else:
        workload_type_str = "Default" # Default original workload
        optimized_status = args.optimized
        optimized_color = Bcolors.SETTING_VALUE

  
    settings.insert(0, f"{Bcolors.WORKLOAD_SETTING}Workload Type:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{workload_type_str}{Bcolors.ENDC}")
    settings.insert(1, f"{Bcolors.WORKLOAD_SETTING}Optimized workload:{Bcolors.ENDC} {Bcolors.BOLD}{optimized_color}{optimized_status}{Bcolors.ENDC}")
       
    if workload_ratios:
        ratio_str = (f"({Bcolors.BOLD}{Bcolors.SETTING_VALUE}SELECTS: {int(round(float(workload_ratios['select_ratio']), 0))}% {Bcolors.ENDC}|"
                     f"{Bcolors.BOLD}{Bcolors.SETTING_VALUE} INSERTS: {int(round(float(workload_ratios['insert_ratio']), 0))}% {Bcolors.ENDC}|"
                     f"{Bcolors.BOLD}{Bcolors.SETTING_VALUE} UPDATES: {int(round(float(workload_ratios['update_ratio']), 0))}% {Bcolors.ENDC}|"
                     f"{Bcolors.BOLD}{Bcolors.SETTING_VALUE} DELETES: {int(round(float(workload_ratios['delete_ratio']), 0))}%{Bcolors.ENDC})")
        settings.insert(2, f"{Bcolors.WORKLOAD_SETTING}Workload ratio:{Bcolors.ENDC} {ratio_str}")
    else:
        ratio_status = "Disabled"
        ratio_color = Bcolors.DISABLED
        settings.insert(2, f"{Bcolors.WORKLOAD_SETTING}Workload ratio:{Bcolors.ENDC} {Bcolors.BOLD}{ratio_color}{ratio_status}{Bcolors.ENDC}")

    table_width = 115
    config_details = "\n" + "\n".join(settings)
    config_details += f"""\n
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.BOLD}{Bcolors.HEADER}{' Live Workload Monitoring ':^{table_width - 2}}{Bcolors.ENDC}
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}\n"""
    logging.info(config_details)
    logging.info(f"{Bcolors.WARNING}Workload starting...{Bcolors.ENDC}")



async def fetch_and_log_collection_stats(args):
    """Connects to the DB, gets collStats for the generic collection, and prints the summary table."""
    await mongo_client.init_async(args)
    client = mongo_client.get_client()
    db = client[args.db]
    
    try:
        collstats = await db.command("collstats", args.collection)
        coll_data = {
            args.collection: {
                "db": args.db,
                "sharded": collstats.get("sharded", False),
                "size": collstats.get("size", 0),
                "documents": collstats.get("count", 0),
            }
        }
        
        table = "\n"
        table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
        table += f"{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.HEADER}{'Collection Stats':^98}{Bcolors.ENDC}{Bcolors.ACCENT}| \n"
        table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
        table += f"{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Database':^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Collection':^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Sharded':^16}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Size':^14}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.STATS_HEADER}{'Documents':^15}{Bcolors.ENDC}{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC}\n"
        table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"

        for coll_name, stats in coll_data.items():
            size_in_mb = stats["size"] / 1024 / 1024
            size_display = f"{size_in_mb / 1024:.2f} GB" if size_in_mb >= 1024 else f"{size_in_mb:.2f} MB"
            table += f"{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.LIGHT_GRAY_TEXT}{str(stats['db']):^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{coll_name:^20}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{str(stats['sharded']):^16}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{size_display:^14}{Bcolors.ENDC} {Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC} {Bcolors.SECONDARY_HIGHLIGHT}{stats['documents']:^15,}{Bcolors.ENDC}{Bcolors.ACCENT}|{Bcolors.BOLD}{Bcolors.ENDC}\n"

        table += f"{Bcolors.ACCENT}{'='*100}{Bcolors.ENDC}\n"
        logging.info(table)

    except Exception as e:
        logging.error(f"Could not retrieve collection stats for {args.db}.{args.collection}: {e}")
    finally:
        await mongo_client.close_client_async()


def log_generic_summary(total_ops, select_ops, insert_ops, update_ops, delete_ops, 
                        docs_found, docs_inserted, docs_modified, docs_deleted, 
                        elapsed_time, specified_duration_str):
    """Prints the final summary block for the generic workload."""
    table_width = 115
    
    overall_throughput = total_ops / elapsed_time if elapsed_time > 0 else 0
    select_throughput = select_ops / elapsed_time if elapsed_time > 0 else 0
    insert_throughput = insert_ops / elapsed_time if elapsed_time > 0 else 0
    update_throughput = update_ops / elapsed_time if elapsed_time > 0 else 0
    delete_throughput = delete_ops / elapsed_time if elapsed_time > 0 else 0
    
    summary_details = textwrap.dedent(f"""
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.ACCENT}{Bcolors.BOLD}{Bcolors.HEADER}{' Combined Workload Stats ':^{table_width - 4}}{Bcolors.ENDC}{Bcolors.ACCENT}
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.GRAY_TEXT}Specified Duration:{Bcolors.ENDC} {specified_duration_str}
{Bcolors.GRAY_TEXT}Total Elapsed Time:{Bcolors.ENDC} {elapsed_time:.2f} seconds
{Bcolors.GRAY_TEXT}Total Operations:{Bcolors.ENDC} {Bcolors.BOLD}{total_ops:,}{Bcolors.ENDC} (SELECT: {Bcolors.BOLD}{select_ops:,}{Bcolors.ENDC}, INSERT: {Bcolors.BOLD}{insert_ops:,}{Bcolors.ENDC}, UPDATE: {Bcolors.BOLD}{update_ops:,}{Bcolors.ENDC}, DELETE: {Bcolors.BOLD}{delete_ops:,}{Bcolors.ENDC})
{Bcolors.GRAY_TEXT}Overall Throughput:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.HIGHLIGHT}{overall_throughput:,.2f} ops/sec{Bcolors.ENDC} {Bcolors.GRAY_TEXT}({Bcolors.ENDC}{Bcolors.LIGHT_GRAY_TEXT}SELECTS: {Bcolors.ENDC}{Bcolors.BOLD}{select_throughput:,.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT}, INSERTS: {Bcolors.ENDC}{Bcolors.BOLD}{insert_throughput:,.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT}, UPDATES: {Bcolors.ENDC}{Bcolors.BOLD}{update_throughput:,.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT}, DELETES: {Bcolors.ENDC}{Bcolors.BOLD}{delete_throughput:,.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT}){Bcolors.ENDC}
{Bcolors.GRAY_TEXT}Total:{Bcolors.ENDC} (Documents Found: {Bcolors.BOLD}{docs_found:,}{Bcolors.ENDC} | Documents Inserted: {Bcolors.BOLD}{docs_inserted:,}{Bcolors.ENDC} | Documents Updated: {Bcolors.BOLD}{docs_modified:,}{Bcolors.ENDC} | Documents Deleted: {Bcolors.BOLD}{docs_deleted:,}{Bcolors.ENDC})
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}\n""")
    logging.info(summary_details)