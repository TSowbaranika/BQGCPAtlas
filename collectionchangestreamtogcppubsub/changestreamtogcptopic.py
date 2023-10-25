import json
import os
import certifi
from pymongo import MongoClient
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from constants.constants import (
    MONGODB_CONNECTION_URI_STR,
    MONGODB_DATABASE_NAME,
    MONGODB_COLLECTION_NAME,
    GCP_TOPIC_PATH,
    FULLDOCUMENT_KEY
)

load_dotenv()
class CollectionChangeStreamToGCPTopic:
    def __init__(self):
        # Initializing the required details from .env file
        self.mongoconnstr = os.getenv(MONGODB_CONNECTION_URI_STR)
        self.dbname = os.getenv(MONGODB_DATABASE_NAME)
        self.collectionname = os.getenv(MONGODB_COLLECTION_NAME)
        self.gcptopicname = os.getenv(GCP_TOPIC_PATH)

   
    def to_perform_mongo_changestream_to_gcp_topics(self):
        # Creating connection to the MongoDb with its connection string
        mongo_client = MongoClient(self.mongoconnstr ,tlsCAFile=certifi.where())
        #Connecting to Database from the Established connection
        db = mongo_client[self.dbname]
        #Accessing the specific collection in the Database which is connected
        collection = db[self.collectionname]
        #Watcher to continusously watch all the operations that are happens in the specified collection
        change_stream = collection.watch(full_document="updateLookup")

        # Initialize Pub/Sub publisher
        publisher = pubsub_v1.PublisherClient()
        topic_path = self.gcptopicname

        # Listen to MongoDB change stream and publish to Pub/Sub
        for change in change_stream:
             # Get the change event document as a Pub/Sub message
            fulldocument = change[FULLDOCUMENT_KEY]
            
            # Deleting unique id which is specific to MongoDB
            del fulldocument["_id"]
            
            # Publish the message to Pub/Sub
            future = publisher.publish(topic_path, json.dumps(fulldocument).encode('utf-8'))
            
            # Optional: Handle errors and retries
            try:
                future.result()
            except Exception as e:
                print(f"Error publishing collection level changes to Pub/Sub: {e}")
