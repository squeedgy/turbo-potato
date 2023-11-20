from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import logging

class MongoDBConnector:
    def __init__(self, host, port, user, password, database, collection):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.collection = collection
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                authSource=self.database
            )
            self.db = self.client[self.database]
            logging.info(f"Connected to MongoDB database '{self.database}'")
        except ConnectionFailure as e:
            logging.error(f"Error connecting to MongoDB: {e}")

    def disconnect(self):
        if self.client is not None:
            self.client.close()
            logging.info("Disconnected from MongoDB")

    def insert_data(self, data):
        try:
            result = self.db[self.collection].insert_many(data)
            logging.info(f"Inserted {len(result.inserted_ids)} documents into '{self.collection}' collection.")
            return result.inserted_ids
        except Exception as e:
            logging.error(f"Error inserting data into MongoDB: {e}")