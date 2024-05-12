import mysql.connector
from app.MultiDBLib.src.exceptions import *
from .db import Database
import logging

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
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        """
        Establishes a database connection.
        """
        if self.connection is None:
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                logger.info(f"Connected to MySQL Server at {self.host}:{self.port}")
            except mysql.connector.Error as e:
                logger.error(f"Failed to connect to MySQL: {e}")
                raise ConnectionError(f"Could not connect to MySQL: {e}")

    def close(self):
        """
        Closes the database connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("MySQL connection closed.")

    def insert_data(self, query, params=None):
        """
        Inserts data into a MySQL database.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            row_count = cursor.rowcount
            return row_count
        except InsertionError as e:
            logger.error(f"Failed to execute insert query: {e}")
            raise InsertionError(f"Failed to execute insert query: {e}")
        finally:
            cursor.close()
            self.close()

    def fetch_data(self, query, params=None):
        """
        Fetches data from a MySQL database.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return rows
        except FetchError as e:
            logger.error(f"Failed to execute fetch query: {e}")
            raise FetchError(f"Failed to execute fetch query: {e}")
        finally:
            cursor.close()
            self.close()

    def update_data(self, query, params=None):
        """
        Updates data in a MySQL database.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            row_count = cursor.rowcount
            return row_count
        except UpdateError as e:
            logger.error(f"Failed to execute update query: {e}")
            raise UpdateError(f"Failed to execute update query: {e}")
        finally:
            cursor.close()
            self.close()

    def delete_data(self, query, params=None):
        """
        Deletes data from a MySQL database.
        """
        try:
            self.connect()
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            row_count = cursor.rowcount
            return row_count
        except DeletionError as e:
            logger.error(f"Failed to execute delete query: {e}")
            raise DeletionError(f"Failed to execute delete query: {e}")
        finally:
            cursor.close()
            self.close()
