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
        """
        self.connection_string = f"host={host} port={port} user={user} password={password} dbname={database}"
        self.connection = None

    def connect(self):
        """
        Establishes a database connection.
        """
        if self.connection is None:
            self.connection = psycopg2.connect(self.connection_string)

    def close(self):
        """
        Closes the database connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None



    def insert_data(self, query, params=None):
        """
        Inserts data into a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for inserting data.
        :param params: tuple or None, parameters for the SQL query to prevent SQL injection.
        :return: int, the number of rows affected.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            return row_count
        finally:
            self.close()

    def fetch_data(self, query, params=None):
        """
        Fetches data from a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for fetching data.
        :param params: tuple or None, parameters for the SQL query to ensure safe queries.
        :return: list of tuple, the rows fetched from the database.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        finally:
            self.close()

    def update_data(self, query, params=None):
        """
        Updates data in a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for updating data.
        :param params: tuple or None, parameters for the SQL query to prevent SQL injection.
        :return: int, the number of rows affected.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            return row_count
        finally:
            self.close()

    def delete_data(self, query, params=None):
        """
        Deletes data from a PostgreSQL database.
        
        :param query: str, the SQL query string to execute for deleting data.
        :param params: tuple or None, parameters for the SQL query to ensure safe deletion.
        :return: int, the number of rows affected.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            return row_count
        finally:
            self.close()
