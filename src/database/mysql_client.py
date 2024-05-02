from src.exceptions import ConnectionError, QueryError
import logging
import mysql.connector
from .db import Database



# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLClient(Database):
    """
    MySQL client class extending the generic Database class for MySQL-specific operations.
    This class manages connections to a MySQL server and performs database operations.
    """

    def __init__(self, host, port, user, password, database):
        """
        Initialize the MySQL client with connection parameters.
        
        :param host: str, the hostname or IP address of the MySQL server.
        :param port: int, the port number on which the MySQL server is listening.
        :param user: str, the username for MySQL authentication.
        :param password: str, the password for MySQL authentication.
        :param database: str, the name of the database to use.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        """
        Establish a connection to the MySQL database using the provided credentials.
        """
        

    
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                logger.info(f"Connected to MySQL Server version {db_info}")
        except ConnectionError as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise ConnectionError(f"Could not connect to MySQL: {e}")

    def close(self):
        """
        Close the connection to the MySQL database.
        """
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("MySQL connection closed.")

    def execute_query(self, query):
        """
        Execute a SQL query on the MySQL server.
        
        :param query: str, a SQL query to execute.
        :return: A cursor object containing the results of the query.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()  # Use fetchone(), fetchmany() if needed
            cursor.close()
            logger.info("Query executed successfully.")
            return result
        except QueryError as e:
            logger.error(f"Failed to execute query: {e}")
            raise QueryError(f"Failed to execute query: {e}")