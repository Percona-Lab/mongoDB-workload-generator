# Configuration for MongoDB connection
dbconfig = {
    "username": "mongo_user_here",
    "password": "mongo_password_here",
    # Standard port and host configuration
    "port": "27017",
    "hosts": [
        "dan-ps-lab-mongos00.tp.int.percona.com",
        "dan-ps-lab-mongos01.tp.int.percona.com"
    ],
    # Configuration below if you are running the mongos nodes on the same host, but using different ports. Notice the port is empty since we specify it following the hostname
    # "port": "",
    # "hosts": [
    #     "localhost:55009",
    #     "localhost:55010"
    # ],
    "serverSelectionTimeoutMS": 15000, # We need this to fail faster, otherwise the default is 30 seconds
    "connectTimeoutMS": 10000,  # Example timeout setting
    "maxPoolSize": 1000, # Example pool setting
    "minPoolSize": 50,
    # Leave replicaSet: None if connecting to mongos. Enter the appropriate replicaSet name if connecting to replicaSet instead of Mongos
    "replicaSet": None,  
    # "replicaSet": "rslab", 
    "authSource": "admin",  # Adjust for authentication
    # Set the 2 values below to true if using tls
    "tls": "false",  # Example tls setting
    "tlsAllowInvalidCertificates": "false"
}