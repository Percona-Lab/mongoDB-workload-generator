# In generic_workload.py

import asyncio
import time
import random
import logging
import mongo_client
from colors import Bcolors
import itertools
import uuid

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
    print(f"Inserting {args.num_docs:,} documents with random UUIDs...")

    batch_size = 1000
    tasks = []
    start_time = time.time()

    for i in range(1, args.num_docs + 1):
        # The new document structure:
        # 1. No "_id" field - MongoDB will generate an ObjectId automatically.
        # 2. A new "indexed_uuid" field with a random UUID.
        doc = {
            "indexed_uuid": str(uuid.uuid4()),
            "pad": "X" * 100
        }
        tasks.append(collection.insert_one(doc))
        if i % batch_size == 0:
            await asyncio.gather(*tasks)
            tasks = []
            print(f"  > Inserted {i:,}/{args.num_docs:,}", end='\r')
    
    if tasks:
        await asyncio.gather(*tasks)

    insert_end_time = time.time()
    print(f"\n✔ Inserted {args.num_docs:,} documents in {insert_end_time - start_time:.2f} seconds.")

    # Create an index on the new UUID field for fast lookups
    print(f"Creating index on 'indexed_uuid' field...")
    await collection.create_index("indexed_uuid")
    index_end_time = time.time()

    print(f"{Bcolors.OKGREEN}✔ Prepare complete.{Bcolors.ENDC} Index created in {index_end_time - insert_end_time:.2f} seconds.")
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
    # print(f"Fetching a sample of {ID_SAMPLE_SIZE} IDs to use for queries...")
    await mongo_client.init_async(args)
    client = mongo_client.get_client()
    collection = client[args.db][args.collection]
    
    pipeline = [{"$sample": {"size": ID_SAMPLE_SIZE}}]
    target_ids = [doc["_id"] async for doc in collection.aggregate(pipeline)]
    
    await mongo_client.close_client_async()
    return target_ids

async def find_thread_worker(collection, target_ids, stop_event, output_queue, report_interval):
    """The ultra-light async worker for FIND operations."""
    local_op_count = 0
    local_docs_found = 0
    batch_size = 100
    last_report_time = time.time()
    last_reported_op_count = 0
    last_reported_docs_found = 0

    while not stop_event.is_set():
        tasks = []
        for _ in range(batch_size):
            target_id = random.choice(target_ids)
            tasks.append(collection.find_one({"_id": target_id}))
        
        results = await asyncio.gather(*tasks)
        local_op_count += batch_size
        local_docs_found += sum(1 for doc in results if doc is not None)

        if time.time() - last_report_time >= report_interval:
            ops_in_interval = local_op_count - last_reported_op_count
            docs_in_interval = local_docs_found - last_reported_docs_found
            
            output_queue.put({"total_ops": ops_in_interval, "docs_found": docs_in_interval})
            
            last_report_time = time.time()
            last_reported_op_count = local_op_count
            last_reported_docs_found = local_docs_found
    
    final_ops = local_op_count - last_reported_op_count
    final_docs = local_docs_found - last_reported_docs_found
    if final_ops > 0:
        output_queue.put({"total_ops": final_ops, "docs_found": final_docs})

async def update_thread_worker(collection, target_ids, stop_event, output_queue, report_interval):
    """The ultra-light async worker for UPDATE operations."""
    local_op_count = 0
    local_docs_modified = 0
    batch_size = 100
    last_report_time = time.time()
    last_reported_op_count = 0
    last_reported_docs_modified = 0

    while not stop_event.is_set():
        tasks = []
        for _ in range(batch_size):
            target_id = random.choice(target_ids)
            tasks.append(collection.update_one(
                {"_id": target_id},
                {"$set": {"pad": "Y" * 100}},
                upsert=True  # Make the update an upsert
            ))
        
        results = await asyncio.gather(*tasks)
        local_op_count += batch_size
        # An upsert can result in a modified doc or an upserted_id. Both count as success.
        successful_ops = sum(res.modified_count + (1 if res.upserted_id is not None else 0) for res in results)
        local_docs_modified += successful_ops

        if time.time() - last_report_time >= report_interval:
            ops_in_interval = local_op_count - last_reported_op_count
            docs_in_interval = local_docs_modified - last_reported_docs_modified
            
            output_queue.put({"total_ops": ops_in_interval, "docs_modified": docs_in_interval})
            
            last_report_time = time.time()
            last_reported_op_count = local_op_count
            last_reported_docs_modified = local_docs_modified
    
    final_ops = local_op_count - last_reported_op_count
    final_docs = local_docs_modified - last_reported_docs_modified
    if final_ops > 0:
        output_queue.put({"total_ops": final_ops, "docs_modified": final_docs})


async def delete_thread_worker(collection, target_ids, stop_event, output_queue, report_interval):
    """The ultra-light async worker for DELETE operations.
    
    To maintain a stable benchmark, this worker performs a delete-then-reinsert cycle.
    This keeps the collection size constant. One logical operation = 1 delete + 1 insert.
    """
    local_op_count = 0
    local_docs_deleted = 0
    batch_size = 100
    last_report_time = time.time()
    last_reported_op_count = 0
    last_reported_docs_deleted = 0

    while not stop_event.is_set():
        # Use random.sample to pick UNIQUE IDs for each batch.
        if len(target_ids) < batch_size:
            ids_to_operate_on = target_ids
        else:
            ids_to_operate_on = random.sample(target_ids, k=batch_size)

        # Phase 1: Batch Deletes (This remains the same)
        delete_tasks = [collection.delete_one({"_id": doc_id}) for doc_id in ids_to_operate_on]
        delete_results = await asyncio.gather(*delete_tasks)

        # Phase 2: Batch Re-Inserts using an atomic "upsert" operation.
        # This replaces the document if it exists or inserts it if it doesn't.
        replace_tasks = [
            collection.replace_one(
                {"_id": doc_id},                               # The filter to find the document
                {"_id": doc_id, "pad": "X" * 100},              # The new document to insert/replace with
                upsert=True                                    # The key option!
            ) for doc_id in ids_to_operate_on
        ]
        await asyncio.gather(*replace_tasks)
        
        local_op_count += len(ids_to_operate_on)
        local_docs_deleted += sum(res.deleted_count for res in delete_results)

        if time.time() - last_report_time >= report_interval:
            ops_in_interval = local_op_count - last_reported_op_count
            docs_in_interval = local_docs_deleted - last_reported_docs_deleted
            
            output_queue.put({"total_ops": ops_in_interval, "docs_deleted": docs_in_interval})
            
            last_report_time = time.time()
            last_reported_op_count = local_op_count
            last_reported_docs_deleted = local_docs_deleted
    
    final_ops = local_op_count - last_reported_op_count
    final_docs = local_docs_deleted - last_reported_docs_deleted
    if final_ops > 0:
        output_queue.put({"total_ops": final_ops, "docs_deleted": final_docs})


async def insert_thread_worker(collection, stop_event, output_queue, report_interval):
    """The ultra-light async worker for random INSERT operations."""
    local_op_count = 0
    batch_size = 100
    last_report_time = time.time()
    last_reported_op_count = 0
    
    # No longer need itertools.count, as MongoDB will generate the _id.

    while not stop_event.is_set():
        tasks = []
        for _ in range(batch_size):
            # Create a document with the indexed_uuid, letting MongoDB handle the _id.
            doc = {
                "indexed_uuid": str(uuid.uuid4()),
                "pad": "Z" * 100
            }
            tasks.append(collection.insert_one(doc))
        
        await asyncio.gather(*tasks)
        local_op_count += batch_size

        if time.time() - last_report_time >= report_interval:
            ops_in_interval = local_op_count - last_reported_op_count
            
            output_queue.put({"total_ops": ops_in_interval, "docs_inserted": ops_in_interval})
            
            last_report_time = time.time()
            last_reported_op_count = local_op_count
    
    final_ops = local_op_count - last_reported_op_count
    if final_ops > 0:
        output_queue.put({"total_ops": final_ops, "docs_inserted": final_ops})