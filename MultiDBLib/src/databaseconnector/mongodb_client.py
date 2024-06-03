from pymongo import MongoClient
from .exceptions import *
from .db import Database
import logging


# Configure logging
logger = logging.getLogger('pymongo')
logger.setLevel(logging.INFO)

class MongoDBClient(Database):
    """
    MongoDB client class extending the generic Database class for MongoDB-specific operations.
    Manages connections to a MongoDB server and performs database operations on a default collection.
    """

    def __init__(self, host, port, database, collection_name):
        """
        Initializes a new instance of MongoDBClient.
        
        :param host: str, the hostname or IP address of the MongoDB server.
        :param port: int, the port number on which the MongoDB server is listening.
        :param database: str, the name of the database to use.
        :param collection: str, the default collection to use.
        """
        self.host = host
        self.port = port
        self.database = database
        self.collection_name = collection_name
        self.collection = None
        self.client = None
        self.db = None

    def connect(self):
        """
        Connects to the MongoDB server using the provided credentials and selects the database and collection.
        """
        try:
            self.client = MongoClient(self.host, self.port)
            self.db = self.client[self.database]
            self.collection = self.db[self.collection_name]
            logger.info(f"Connected to MongoDB at {self.host}:{self.port}")
        except ConnectionError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise ConnectionError(f"Could not connect to MongoDB: {e}")


    def close(self):
        """
        Closes the connection to the MongoDB server.
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")


    def insert_data(self, document):
        """
        Inserts a single document into the MongoDB collection.
        :param document: dict, the document to insert.
        :return: The ID of the inserted document.
        """
        try:
            result = self.collection.insert_one(document)
            logger.info(f"Inserted document with ID: {result.inserted_id}")
            return result.inserted_id
        except InsertionError as e:
            logger.error(f"Error inserting data: {e}")
            raise InsertionError(f"Error inserting data: {e}")

        
    def fetch_data(self, query):
        """
        Finds documents in the MongoDB collection based on a query.
        :param query: dict, the query criteria.
        :return: A list of documents that match the query.
        """
        try:
            results = self.collection.find(query)
            documents = [doc for doc in results]
            logger.info(documents)
            return documents
        except FetchError as e:
            logger.error(f"Error fetching data: {e}")
            raise FetchError(f"Error fetching data: {e}")

        

    def update_data(self, query, new_values):
        """
        Updates documents in the MongoDB collection based on a query.
        :param query: dict, the query to match the documents that need updating.
        :param new_values: dict, the new values to update in the matching documents.
        :return: The count of documents updated.
        """
        try:
            result = self.collection.update_many(query, {'$set': new_values})
            logger.info(f"Documents updated: {result.modified_count}")
            return result.modified_count
        except UpdateError as e:
            logger.error(f"Error updating data: {e}")
            raise UpdateError(f"Error updating data: {e}")


    def delete_data(self, query):
        """
        Deletes documents from the MongoDB collection based on a query.
        :param query: dict, the query to match the documents to be deleted.
        :return: The count of documents deleted.
        """
        try:
            result = self.collection.delete_many(query)
            logger.info(f"Documents deleted: {result.deleted_count}")
            return result.deleted_count
        except DeletionError as e:
            logger.error(f"Error deleting data: {e}")
            raise DeletionError(f"Error deleting data: {e}")
    
    def delete_all_data(self, query=None):
        """
        Deletes all documents from the MongoDB collection.
        :return: The count of documents deleted.
        """
        try:
            result = self.collection.delete_many({})
            logger.info(f"All documents deleted: {result.deleted_count}")
            return result.deleted_count
        except DeletionError as e:
            logger.error(f"Error deleting data: {e}")
            raise DeletionError(f"Error deleting data: {e}")



    def __str__(self):
        return f"MongoDBClient(host={self.host}, port={self.port}, database={self.database}, collection={self.collection_name})"
    
        