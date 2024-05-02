from pymongo import MongoClient
import pymongo.errors as mongo_errors
from src.exceptions import QueryError
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
