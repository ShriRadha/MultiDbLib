import psycopg2
from app.MultiDBLib.src.exceptions import *
from .db import Database
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
            try:
                self.connection = psycopg2.connect(self.connection_string)
                logger.info(f"Connected to PostgreSQL at {self.connection_string}")
            except ConnectionError as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise ConnectionError(f"Could not connect to PostgreSQL: {e}")

    def close(self):
        """
        Closes the database connection.
        """
        if self.connection:
            try:

                self.connection.close()
                self.connection = None
                logger.info("PostgreSQL connection closed.")
            except ConnectionError as e:
                logger.error(f"Failed to close PostgreSQL connection: {e}")
                raise ConnectionError(f"Could not close PostgreSQL connection: {e}")



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
            logger.info(f"Inserted {row_count} rows into the database.")
            return row_count
        except InsertionError as e:
            logger.error(f"Error inserting data: {e}")
            raise InsertionError(f"Error inserting data: {e}")
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
            logger.info(f"Fetched {len(rows)} rows from the database.")
            return rows
        except FetchError as e:
            logger.error(f"Error fetching data: {e}")
            raise FetchError(f"Error fetching data: {e}")
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
            logger.info(f"Updated {row_count} rows in the database.")
            return row_count
        except UpdateError as e:
            logger.error(f"Error updating data: {e}")
            raise UpdateError(f"Error updating data: {e}")
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
            logger.info(f"Deleted {row_count} rows from the database.")
            return row_count
        except DeletionError as e:
            logger.error(f"Error deleting data: {e}")
            raise DeletionError(f"Error deleting data: {e}")
        finally:
            self.close()
    
    def delete_all_data(self, query):
        """
        Deletes all data from a PostgreSQL database.
        
        :return: int, the number of rows affected.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(f"DELETE FROM {query}")
            self.connection.commit()
            row_count = cursor.rowcount
            cursor.close()
            logger.info(f"Deleted {row_count} rows from the database.")
            return row_count
        except DeletionError as e:
            logger.error(f"Error deleting data: {e}")
            raise DeletionError(f"Error deleting data: {e}")
        finally:
            self.close()
