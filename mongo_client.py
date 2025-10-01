#!/usr/bin/env python3
import motor.motor_asyncio as motor # type: ignore
import logging
import sys
from urllib.parse import urlencode
import os
import importlib.util
import random

# This will hold the single, shared client instance for each process.
# It's initialized to None.
_client = None

def _load_creds_explicitly():
    """
    Loads the dbconfig from a specific file path to avoid import issues.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(current_dir, 'mongodbCreds.py')

    if not os.path.exists(creds_path):
        logging.fatal(f"FATAL: Could not find credentials file at {creds_path}")
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("mongodbCreds", creds_path)
    mongodb_creds = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mongodb_creds)
    
    return mongodb_creds.dbconfig

def _create_new_client():
    """
    An internal function that builds the connection URI and returns a new async client.
    """
    dbconfig = _load_creds_explicitly()
    port = dbconfig.get("port")
    selected_host = random.choice(dbconfig["hosts"])
    if port:
        # Use the single selected host
        host_with_port = f"{selected_host}:{port}"
    else:
        # Assumes port is already in the hostname if the port field is empty
        host_with_port = selected_host

    if dbconfig.get("username") and dbconfig.get("password"):
        connection_uri = f"mongodb://{dbconfig['username']}:{dbconfig['password']}@{host_with_port}"
    else:
        connection_uri = f"mongodb://{host_with_port}"

    conn_params = {
        key: str(value)
        for key, value in dbconfig.items()
        if key not in {"hosts", "username", "password", "port"} and value is not None
    }
    if conn_params:
        connection_uri += "/?" + urlencode(conn_params)

    pid = os.getpid()
    logging.debug(f"Process {pid} is creating a new MongoClient.")
    return motor.AsyncIOMotorClient(connection_uri)

async def init_async():
    """
    Initializes the global async MongoDB client for the current process.
    """
    global _client
    if _client is None:
        try:
            _client = _create_new_client()
            # The hello command is cheap and does not require auth.
            await _client.admin.command('hello')
            logging.debug("MongoDB connection initialized successfully.") 
        except Exception as e:
            logging.fatal(f"Unable to connect to MongoDB. Please check your config.\nError: {e}")
            _client = None
            sys.exit(1)

def get_client():
    """
    Returns the initialized global client instance for the current process.
    Assumes init_async() has already been called.
    """
    return _client

async def close_client_async():
    """
    Closes the global client connection and resets the variable.
    """
    global _client
    if _client:
        _client.close()
        _client = None
        logging.debug("MongoDB connection closed.")