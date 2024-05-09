from pymongo import MongoClient
import pymongo.errors as mongo_errors
from app.MultiDBLib.src.exceptions import ConnectionError, QueryError
from .db import Database
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MongoDBClient(Database):
    """
    MongoDB client class extending the generic Database class for MongoDB-specific operations.
    This class manages connections to a MongoDB server and performs database operations.
    """

    def __init__(self, host, port, username, password, database):
        """
        Initializes a new instance of MongoDBClient.
        
        :param host: str, the hostname or IP address of the MongoDB server.
        :param port: int, the port number on which the MongoDB server is listening.
        :param username: str, the username for MongoDB authentication.
        :param password: str, the password for MongoDB authentication.
        :param database: str, the name of the database to use.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client = None
        self.db = None

    def connect(self):
        """
        Connects to the MongoDB server using the provided credentials and selects the database.
        """
        try:
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}"
            self.client = MongoClient(uri)
            self.db = self.client[self.database]
            logger.info(f"Connected to MongoDB at {self.host}:{self.port}")
        except mongo_errors.ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise ConnectionError(f"Could not connect to MongoDB: {e}")

    def close(self):
        """
        Closes the connection to the MongoDB server.
        """
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")

    def execute_query(self, query):
        """
        Executes a specified query on the connected MongoDB database.
        
        :param query: dict, a MongoDB query or command to execute.
        :return: The result of the query or command execution.
        """
        try:
            result = self.db.command(query)
            logger.info("Query executed successfully.")
            return result
        except mongo_errors.PyMongoError as e:
            logger.error(f"Query execution failed: {e}")
            raise QueryError(f"Failed to execute query: {e}")


    def insert_data(self, collection, document):
        """
        Inserts a single document into a MongoDB collection.
        
        :param collection: str, the name of the collection where the document will be inserted.
        :param document: dict, the document to insert.
        :return: The ID of the inserted document.
        """
        try:
            result = self.client.db[collection].insert_one(document)
            print(f"Document inserted with id: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            print(f"An error occurred: {e}")

    def fetch_data(self, collection, query):
        """
        Finds documents in a MongoDB collection based on a query.
        
        :param collection: str, the name of the collection to search.
        :param query: dict, the query criteria.
        :return: A list of documents that match the query.
        """
        try:
            results = self.client.db[collection].find(query)
            documents = [doc for doc in results]
            print(f"Found {len(documents)} documents")
            return documents
        except Exception as e:
            print(f"An error occurred: {e}")

    def update_data(self, collection, query, new_values):
        """
        Updates documents in a MongoDB collection based on a query.
        
        :param collection: str, the name of the collection where the update will occur.
        :param query: dict, the query to match the documents that need updating.
        :param new_values: dict, the new values to update in the matching documents.
        :return: The count of documents updated.
        """
        try:
            result = self.client.db[collection].update_many(query, {'$set': new_values})
            print(f"Documents updated: {result.modified_count}")
            return result.modified_count
        except Exception as e:
            print(f"An error occurred: {e}")

    def delete_data(self, collection, query):
        """
        Deletes documents from a MongoDB collection based on a query.
        
        :param collection: str, the name of the collection where the deletion will occur.
        :param query: dict, the query to match the documents to be deleted.
        :return: The count of documents deleted.
        """
        try:
            result = self.client.db[collection].delete_many(query)
            print(f"Documents deleted: {result.deleted_count}")
            return result.deleted_count
        except Exception as e:
            print(f"An error occurred: {e}")

