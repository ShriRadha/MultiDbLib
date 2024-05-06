import psycopg2
from app.MultiDBLib.src.exceptions import ConnectionError, QueryError
from .db import Database
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresClient(Database):
    """
    PostgreSQL client class extending the generic Database class for PostgreSQL-specific operations.
    This class manages connections to a PostgreSQL server and performs database operations.
    """

    def __init__(self, host, port, user, password, database):
        """
        Initialize the PostgreSQL client with connection parameters.
        
        :param host: str, the hostname or IP address of the PostgreSQL server.
        :param port: int, the port number on which the PostgreSQL server is listening.
        :param user: str, the username for PostgreSQL authentication.
        :param password: str, the password for PostgreSQL authentication.
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
        Establish a connection to the PostgreSQL database using the provided credentials.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}")
        except ConnectionError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise ConnectionError(f"Could not connect to PostgreSQL: {e}")

    def close(self):
        """
        Close the connection to the PostgreSQL database.
        """
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed.")

    def execute_query(self, query):
        """
        Execute a SQL query on the PostgreSQL server.
        
        :param query: str, a SQL query to execute.
        :return: A list containing the rows returned by the query.
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



    def insert_data(self, query, params=None):
        """
        Inserts data into a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for inserting data.
        :param params: tuple or None, parameters for the SQL query to prevent SQL injection.
        :return: int, the number of rows affected.
        """
        try:
            self.database.connect()
            cursor = self.database.connection.cursor()
            cursor.execute(query, params)
            self.database.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            return row_count
        finally:
            self.database.close()

    def fetch_data(self, query, params=None):
        """
        Fetches data from a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for fetching data.
        :param params: tuple or None, parameters for the SQL query to ensure safe queries.
        :return: list of tuple, the rows fetched from the database.
        """
        try:
            self.database.connect()
            cursor = self.database.connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        finally:
            self.database.close()

    def update_data(self, query, params=None):
        """
        Updates data in a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for updating data.
        :param params: tuple or None, parameters for the SQL query to prevent SQL injection.
        :return: int, the number of rows affected.
        """
        try:
            self.database.connect()
            cursor = self.database.connection.cursor()
            cursor.execute(query, params)
            self.database.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            return row_count
        finally:
            self.database.close()

    def delete_data(self, query, params=None):
        """
        Deletes data from a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for deleting data.
        :param params: tuple or None, parameters for the SQL query to ensure safe deletion.
        :return: int, the number of rows affected.
        """
        try:
            self.database.connect()
            cursor = self.database.connection.cursor()
            cursor.execute(query, params)
            self.database.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            return row_count
        finally:
            self.database.close()
