import pymongo
import os

def insert_to_mongo(aggregated_file):

    # Initialize variables
    mongo_username = os.environ.get("MONGO_USERNAME")
    mongo_password =  os.environ.get("MONGO_PASSWORD")
    mongo_ip_address = os.environ.get("MONGO_IP")
    database_name = os.environ.get("MONGO_DB_NAME")
    collection_name = os.environ.get("MONGO_COLLECTION_NAME")

    # Initialize mongoDB atlas session.
    client = pymongo.MongoClient(f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_ip_address}")

    # Create a new DB.
    db = client[database_name]

    # Create a new collection.
    collection = db[collection_name]

    for doc in aggregated_file:
        collection.insert_one(doc)

if __name__=="__main__":
    insert_to_mongo()
