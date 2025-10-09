# In generic_workload.py

import asyncio
import time
import random
import logging
import mongo_client
from colors import Bcolors

# Configuration
ID_SAMPLE_SIZE = 10000

async def prepare(args):
    """Drops, creates, and loads the generic benchmark collection."""
    print(f"{Bcolors.HEADER}--- Starting Generic Prepare Phase ---{Bcolors.ENDC}")
    await mongo_client.init_async(args)
    client = mongo_client.get_client()
    collection = client[args.db][args.collection]

    print(f"Dropping collection '{args.db}.{args.collection}'...")
    await collection.drop()
    print(f"Inserting {args.num_docs:,} documents...")

    batch_size = 1000
    tasks = []
    start_time = time.time()

    for i in range(1, args.num_docs + 1):
        tasks.append(collection.insert_one({"_id": i, "pad": "X" * 100}))
        if i % batch_size == 0:
            await asyncio.gather(*tasks)
            tasks = []
            print(f"  > Inserted {i:,}/{args.num_docs:,}", end='\r')
    
    if tasks:
        await asyncio.gather(*tasks)

    end_time = time.time()
    print(f"\n{Bcolors.OKGREEN}✔ Prepare complete.{Bcolors.ENDC} Inserted {args.num_docs:,} documents in {end_time - start_time:.2f} seconds.")
    await mongo_client.close_client_async()

async def cleanup(args):
    """Drops the generic benchmark collection."""
    print(f"{Bcolors.HEADER}--- Starting Generic Cleanup Phase ---{Bcolors.ENDC}")
    await mongo_client.init_async(args)
    client = mongo_client.get_client()
    print(f"Dropping collection '{args.db}.{args.collection}'...")
    await client[args.db].drop_collection(args.collection)
    print(f"{Bcolors.OKGREEN}✔ Cleanup complete.{Bcolors.ENDC}")
    await mongo_client.close_client_async()

async def get_target_ids(args):
    """Fetches a sample of _id values from the prepared collection."""
    print(f"Fetching a sample of {ID_SAMPLE_SIZE} IDs to use for queries...")
    await mongo_client.init_async(args)
    client = mongo_client.get_client()
    collection = client[args.db][args.collection]
    
    pipeline = [{"$sample": {"size": ID_SAMPLE_SIZE}}]
    target_ids = [doc["_id"] async for doc in collection.aggregate(pipeline)]
    
    await mongo_client.close_client_async()
    return target_ids

async def thread_worker(collection, target_ids, stop_event, output_queue, report_interval):
    """The ultra-light async worker that periodically reports its own progress."""
    local_op_count = 0
    local_docs_found = 0  # New: Track found documents
    batch_size = 100
    last_report_time = time.time()
    last_reported_op_count = 0
    last_reported_docs_found = 0 # New: Track for interval reporting

    while not stop_event.is_set():
        tasks = []
        for _ in range(batch_size):
            target_id = random.choice(target_ids)
            tasks.append(collection.find_one({"_id": target_id}))
        
        # New: Inspect the results
        results = await asyncio.gather(*tasks)
        local_op_count += batch_size
        local_docs_found += sum(1 for doc in results if doc is not None)

        # Periodically report both operations and docs found
        if time.time() - last_report_time >= report_interval:
            ops_in_interval = local_op_count - last_reported_op_count
            docs_in_interval = local_docs_found - last_reported_docs_found
            
            # New: Send a richer dictionary in the queue
            output_queue.put({"total_ops": ops_in_interval, "docs_found": docs_in_interval})
            
            last_report_time = time.time()
            last_reported_op_count = local_op_count
            last_reported_docs_found = local_docs_found
    
    # Before exiting, report any remaining operations
    final_ops = local_op_count - last_reported_op_count
    final_docs = local_docs_found - last_reported_docs_found
    if final_ops > 0:
        output_queue.put({"total_ops": final_ops, "docs_found": final_docs})