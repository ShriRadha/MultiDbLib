import pyodbc
from .exceptions import *
from .db import Database
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MSSQLClient(Database):
    """
    MSSQL client class extending the generic Database class for SQL Server-specific operations.
    This class manages connections to a SQL Server and performs database operations.
    """

    def __init__(self, host, port, user, password, database, driver):
        """
        Initialize the MSSQL client with connection parameters.
        """
        self.host = host
        self.port = port  # SQL Server default port is often 1433
        self.user = user
        self.password = password
        self.database = database
        self.driver = driver  # Ensure the correct ODBC driver is installed
        self.connection_string = f'DRIVER={self.driver};SERVER={self.host},{self.port};DATABASE={self.database};UID={self.user};PWD={self.password}'
        self.connection = None

    def connect(self):
        """
        Establishes a database connection.
        """
        if not self.connection:
            try:
                self.connection = pyodbc.connect(self.connection_string)
                logger.info(f"Connected to SQL Server at {self.host}:{self.port}")
            except ConnectionError as e:
                logger.error(f"Failed to connect to SQL Server: {e}")
                raise ConnectionError(f"Could not connect to SQL Server: {e}")

    def close(self):
        """
        Closes the database connection if it is open.
        """
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("SQL Server connection closed.")


    def insert_data(self, query, params=None):
        """
        Inserts data into a database.
        """


        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                self.connection.commit()  # Ensure commit happens while connection is still open
                row_count = cursor.rowcount
                logger.info(f"Inserted {row_count} rows.")
                return row_count
        except InsertionError as e:
            self.connection.rollback()
            raise InsertionError(f"Database operation failed: {e}")
        
    def fetch_data(self, query, params=None):
        """
        Fetches data from a database.
        """
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, params or ())
                rows = cursor.fetchall()
                logger.info(rows)
                return rows
            except FetchError as e:
                logger.error(f"Failed to fetch data: {e}")
                raise FetchError(f"Failed to fetch data: {e}")

    def update_data(self, query, params=None):
        """
        Updates data in a database.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                self.connection.commit()  # Ensure commit happens while connection is still open
                row_count = cursor.rowcount
                logger.info(f"Updated {row_count} rows.")
                return row_count
        except InsertionError as e:
            self.connection.rollback()
            raise InsertionError(f"Database operation failed: {e}")


    def delete_data(self, query, params=None):
        """
        Deletes data from a database.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                self.connection.commit()  # Ensure commit happens while connection is still open
                row_count = cursor.rowcount
                logger.info(f"Deleted {row_count} rows.")
                return row_count
        except InsertionError as e:
            self.connection.rollback()
            raise InsertionError(f"Database operation failed: {e}")



    def delete_all_data(self, table_name):
        """
        Deletes all data from a specified table.
        """
        try:
            with self.connection.cursor() as cursor:
                query = f"DELETE FROM {table_name}"
                cursor.execute(query)
                row_count = cursor.rowcount
                logger.info(f"Deleted all rows from {table_name}.")
                return row_count
        except InsertionError as e:
            self.connection.rollback()
            raise InsertionError(f"Database operation failed: {e}")

