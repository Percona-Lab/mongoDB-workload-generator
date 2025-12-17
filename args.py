import argparse

parser = argparse.ArgumentParser(description="MongoDB Workload Generator")
parser.add_argument('--collections', type=int, default=1, help="How many collections to create (default 1).")
parser.add_argument('--collection_definition', type=str, nargs='?', const='collections',
    help="(Optional) Name of a JSON file (from collections/), full path to a file, or a directory. "
         "If omitted, all JSON files from 'collections/' will be used."
)
parser.add_argument('--recreate', action='store_true', help="Recreate the collection before running the test.")
parser.add_argument('--runtime', type=str, default="60s", help="Duration of the load test, specify in seconds (e.g., 60s) or minutes (e.g., 5m) (default 60s).")
parser.add_argument('--batch_size', type=int, default=10, help="Number of documents per batch insert (default 10).")
parser.add_argument('--threads', type=int, default=4, help="Number of threads for simultaneous operations (default 4).")
parser.add_argument('--skip_update', action='store_true', help="Skip update operations.")
parser.add_argument('--skip_delete', action='store_true', help="Skip delete operations.")
parser.add_argument('--skip_insert', action='store_true', help="Skip insert operations.")
parser.add_argument('--skip_select', action='store_true', help="Skip select operations.")
parser.add_argument('--insert_ratio', type=int, help="Percentage of insert operations (default 10).")
parser.add_argument('--update_ratio', type=int, help="Percentage of update operations (default 20).")
parser.add_argument('--delete_ratio', type=int, help="Percentage of delete operations (default 10).")
parser.add_argument('--select_ratio', type=int, help="Percentage of select operations (default 60).")
parser.add_argument('--report_interval', type=int, default=5, help="Interval (in seconds) between workload stats output (default 5s).")
parser.add_argument('--optimized', action='store_true', help="Run optimized workload only.")
parser.add_argument('--cpu', type=int, default=1, help="Number of CPUs to launch multiple instances in parallel (default 1).")
parser.add_argument("--log", nargs="?", const=True, help="Log filename and path (e.g., /tmp/report.log).")

parser.add_argument('--custom_queries', type=str, nargs='?', const='queries',
    help="(Optional) Path to a single JSON query file or a directory containing multiple .json query files. If no path provided, all JSON files from 'queries/' will be used."
)
parser.add_argument('--debug', action='store_true', help="Enable debug logging to show queries and results.")


args = None