import unittest
from unittest.mock import patch, MagicMock
from MultiDBLib.src.databaseconnector.mongodb_client import MongoDBClient
from MultiDBLib.src.databaseconnector.mssql_client import MSSQLClient
from MultiDBLib.src.databaseconnector.postgres_client import PostgresClient

class TestDatabaseClients(unittest.TestCase):

    @patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
    def test_mongodb_connection(self, mock_mongo_client):
        # Testing MongoDB connection establishment
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        db_client = MongoDBClient('localhost', 27017, 'testdb', 'testcollection')
        db_client.connect()
        mock_mongo_client.assert_called_with('localhost', 27017)
        self.assertTrue(db_client.client is not None)

    @patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
    def test_mssql_connection(self, mock_pyodbc_connect):
        """
        Test the connection method of the MSSQLClient class to ensure it successfully connects using pyodbc.
        """
        # Mock the pyodbc connection return value
        mock_connection_instance = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection_instance
        
        # Initialize MSSQLClient and attempt to connect
        db_client = MSSQLClient('localhost', 1433, 'user', 'password', 'testdb', 'ODBC Driver 17 for SQL Server')
        db_client.connect()
        
        # Assert that pyodbc.connect was called with the correct connection string
        mock_pyodbc_connect.assert_called_with("DRIVER=ODBC Driver 17 for SQL Server;SERVER=localhost,1433;DATABASE=testdb;UID=user;PWD=password")
        # Assert that the connection attribute is not None
        self.assertIsNotNone(db_client.connection)




        

    @patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2.connect')
    def test_postgres_connection(self, mock_psycopg2_connect):
        # Testing PostgreSQL client connection establishment
        mock_connection_instance = MagicMock()
        mock_psycopg2_connect.return_value = mock_connection_instance
        db_client = PostgresClient('localhost', 5432, 'user', 'password', 'testdb')
        db_client.connect()
        mock_psycopg2_connect.assert_called_with("host=localhost port=5432 user=user password=password dbname=testdb")
        self.assertIsNotNone(db_client.connection)


    def test_close_connection_mongodb(self):
        # Testing close method for MongoDB
        with patch.object(MongoDBClient, 'close', return_value=None) as mock_close:
            db_client = MongoDBClient('localhost', 27017, 'testdb', 'testcollection')
            db_client.close()
            mock_close.assert_called_once()


    def test_mssql_close_connection(self):
        """
        Test the close method of the MSSQLClient to ensure it properly closes the connection.
        """
        with patch.object(MSSQLClient, 'close', return_value=None) as mock_close:
            # Initialize MSSQLClient
            db_client = MSSQLClient('localhost', 1433, 'user', 'password', 'testdb', 'ODBC Driver 17 for SQL Server')
            db_client.close()
            mock_close.assert_called_once()


    def test_close_connection_postgres(self):
        # Testing close method for PostgreSQL
        with patch.object(PostgresClient, 'close', return_value=None) as mock_close:
            db_client = PostgresClient('localhost', 5432, 'user', 'password', 'testdb')
            db_client.close()
            mock_close.assert_called_once()
