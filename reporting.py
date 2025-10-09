import textwrap
import logging
import asyncio
import time

# Reuse your existing modules
import mongo_client
from colors import Bcolors

def log_generic_config(args):
    """Prints the initial configuration block for the generic workload."""
    
    # Convert runtime back to a user-friendly string for display
    if isinstance(args.runtime, int):
        duration_str = f"{args.runtime} seconds"
    else:
        duration_str = args.runtime

    table_width = 115
    config_details = textwrap.dedent(f"""\n
    {Bcolors.WORKLOAD_SETTING}Workload Type:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}Generic Point-Query{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Duration:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{duration_str}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}CPUs:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.cpu}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Threads:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}(Per CPU: {args.threads} | Total: {args.cpu * args.threads}{Bcolors.ENDC})
    {Bcolors.WORKLOAD_SETTING}Database and Collection:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.db}.{args.collection}{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Report frequency:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.report_interval} seconds{Bcolors.ENDC}
    {Bcolors.WORKLOAD_SETTING}Report logfile:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.SETTING_VALUE}{args.log}{Bcolors.ENDC}\n
    {Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
    {Bcolors.BOLD}{Bcolors.HEADER}{' Workload Started':^{table_width - 2}}{Bcolors.ENDC}
    {Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}\n""")
    logging.info(config_details)


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
        
        # This reuses the same table format as the original workload's summary function
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


def log_generic_summary(total_ops, total_docs_found, elapsed_time, specified_duration_str):
    """Prints the final summary block for the generic workload."""
    table_width = 115
    overall_throughput = total_ops / elapsed_time if elapsed_time > 0 else 0
    
    summary_details = textwrap.dedent(f"""
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.ACCENT}{Bcolors.BOLD}{Bcolors.HEADER}{' Combined Workload Stats ':^{table_width - 4}}{Bcolors.ENDC}{Bcolors.ACCENT}
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}
{Bcolors.GRAY_TEXT}Specified Duration:{Bcolors.ENDC} {specified_duration_str}
{Bcolors.GRAY_TEXT}Total Elapsed Time:{Bcolors.ENDC} {elapsed_time:.2f} seconds
{Bcolors.GRAY_TEXT}Total Operations:{Bcolors.ENDC} {Bcolors.BOLD}{total_ops:,}{Bcolors.ENDC} (SELECT: {total_ops:,}, INSERT: 0, UPDATE: 0, DELETE: 0)
{Bcolors.GRAY_TEXT}Overall Throughput:{Bcolors.ENDC} {Bcolors.BOLD}{Bcolors.HIGHLIGHT}{overall_throughput:,.2f} ops/sec{Bcolors.ENDC} {Bcolors.GRAY_TEXT}({Bcolors.ENDC}{Bcolors.LIGHT_GRAY_TEXT}SELECTS: {Bcolors.ENDC}{Bcolors.BOLD}{overall_throughput:,.2f}{Bcolors.ENDC}{Bcolors.GRAY_TEXT}, INSERTS: 0.00, UPDATES: 0.00, DELETES: 0.00){Bcolors.ENDC}
{Bcolors.GRAY_TEXT}Total:{Bcolors.ENDC} (Documents Found: {Bcolors.BOLD}{total_docs_found:,}{Bcolors.ENDC} | Documents Inserted: {Bcolors.BOLD}0{Bcolors.ENDC} | Documents Updated: {Bcolors.BOLD}0{Bcolors.ENDC} | Documents Deleted: {Bcolors.BOLD}0{Bcolors.ENDC})
{Bcolors.ACCENT}{'=' * table_width}{Bcolors.ENDC}\n""")
    logging.info(summary_details)