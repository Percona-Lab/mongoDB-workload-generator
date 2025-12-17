#!/usr/bin/env python3
import pymongo # type: ignore
import logging
import sys
from urllib.parse import urlencode
import threading
import os
import importlib.util

# Use thread-local storage to ensure a separate client per worker process.
local_data = threading.local()

def _load_creds_explicitly():
    """
    Loads the dbconfig from a specific file path to avoid import issues.
    """
    # Get the directory where this mongo_client.py file is located.
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the full path to mongodbCreds.py.
    creds_path = os.path.join(current_dir, 'mongodbCreds.py')

    if not os.path.exists(creds_path):
        logging.fatal(f"FATAL: Could not find credentials file at {creds_path}")
        sys.exit(1)

    # Use importlib to load the module from the specific path.
    spec = importlib.util.spec_from_file_location("mongodbCreds", creds_path)
    mongodb_creds = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mongodb_creds)
    
    return mongodb_creds.dbconfig

def _create_new_client():
    """
    An internal function that builds the connection URI and returns a new client.
    """
    # Load credentials using the new explicit method.
    dbconfig = _load_creds_explicitly()

    port = dbconfig.get("port")
    # If port is defined and non-empty, append it to each host; otherwise assume port is embedded in host string
    if port:
        hosts = ",".join([f"{host}:{port}" for host in dbconfig["hosts"]])
    else:
        hosts = ",".join(dbconfig["hosts"])


    if dbconfig.get("username") and dbconfig.get("password"):
        connection_uri = f"mongodb://{dbconfig['username']}:{dbconfig['password']}@{hosts}"
    else:
        connection_uri = f"mongodb://{hosts}"

    conn_params = {
        key: str(value)
        for key, value in dbconfig.items()
        if key not in {"hosts", "username", "password", "port"} and value is not None
    }
    if conn_params:
        connection_uri += "/?" + urlencode(conn_params)

    return pymongo.MongoClient(connection_uri)

def init():
    """
    Performs a one-time connection check from the main process.
    """
    try:
        client = _create_new_client()
        client.admin.command('ping')
        logging.debug("MongoDB connection credentials appear to be valid.") 
    except Exception as e:
        logging.fatal(f"Unable to connect to MongoDB. Please check your config.\nError: {e}")
        sys.exit(1)

def get_client():
    """
    Returns a process-safe MongoClient instance.
    """
    if not hasattr(local_data, "client"):
        pid = os.getpid()
        logging.debug(f"Process {pid} is creating a new MongoClient.")
        local_data.client = _create_new_client()
    
    return local_data.client

def get_db():
    """
    Returns a specific database handle from the process-local client.
    """
    return get_client()["config"]