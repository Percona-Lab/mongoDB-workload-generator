# Configuration for MongoDB connection
dbconfig = {
    "username": "your_mongodb_user",
    "password": "your_mongodb_password",
    # Standard port and host configuration
    # "port": "27017",
    # "hosts": [
    #     "da-cl01-mongodb-mongos00",
    #     "da-cl01-mongodb-mongos01"
    # ],
    # Configuration below if you are running the mongos nodes on the same host, but using different ports. Notice the port is empty since we specify it following the hostname
    "port": "",
    "hosts": [
        "localhost:55009",
        "localhost:55010"
    ],
    "serverSelectionTimeoutMS": 15000, # We need this to fail faster, otherwise the default is 30 seconds
    "connectTimeoutMS": 10000,  # Example timeout setting
    "maxPoolSize": 500, # Example pool setting
    # Leave replicaSet: None if connecting to mongos. Enter the appropriate replicaSet name if connecting to replicaSet instead of Mongos
    "replicaSet": None,  
    # "replicaSet": "rslab", 
    "authSource": "admin",  # Adjust for authentication
    "tls": "false",  # Example tls setting
}