import unittest
from unittest.mock import patch, MagicMock
from app.MultiDBLib.src.database.mongodb_client import MongoDBClient
from app.MultiDBLib.src.database.mysql_client import MySQLClient
from app.MultiDBLib.src.database.postgres_client import PostgresClient

class TestDatabaseClients(unittest.TestCase):

    @patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
    def test_mongodb_connection(self, mock_mongo_client):
        # Testing MongoDB connection establishment
        mock_client_instance = MagicMock()
        mock_mongo_client.return_value = mock_client_instance
        db_client = MongoDBClient('localhost', 27017, 'user', 'password', 'testdb')
        db_client.connect()
        mock_mongo_client.assert_called_with('mongodb://user:password@localhost:27017')
        self.assertTrue(db_client.client is not None)

    @patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
    def test_mysql_connection(self, mock_mysql_connect):
        # Testing MySQL client connection establishment
        mock_connection_instance = MagicMock()
        mock_mysql_connect.return_value = mock_connection_instance
        db_client = MySQLClient('localhost', 3306, 'user', 'password', 'testdb')
        db_client.connect()
        mock_mysql_connect.assert_called_with(host='localhost', port=3306, user='user', password='password', database='testdb')
        self.assertTrue(db_client.connection is not None)

    @patch('app.MultiDBLib.src.database.postgres_client.psycopg2.connect')
    def test_postgres_connection(self, mock_psycopg2_connect):
        # Testing PostgreSQL client connection establishment
        mock_connection_instance = MagicMock()
        mock_psycopg2_connect.return_value = mock_connection_instance
        db_client = PostgresClient('localhost', 5432, 'user', 'password', 'testdb')
        db_client.connect()
        mock_psycopg2_connect.assert_called_with(host='localhost', port=5432, user='user', password='password', database='testdb')
        self.assertTrue(db_client.connection is not None)

    def test_execute_query_mongodb(self):
        # Testing execute_query for MongoDB
        with patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient.execute_query') as mock_execute:
            db_client = MongoDBClient('localhost', 27017, 'user', 'password', 'testdb')
            db_client.execute_query({'find': 'test_collection'})
            mock_execute.assert_called_once()

    def test_execute_query_mysql(self):
        # Testing execute_query for MySQL
        with patch('app.MultiDBLib.src.database.mysql_client.MySQLClient.execute_query') as mock_execute:
            db_client = MySQLClient('localhost', 3306, 'user', 'password', 'testdb')
            db_client.execute_query("SELECT * FROM test_table")
            mock_execute.assert_called_once()

    def test_execute_query_postgres(self):
        # Testing execute_query for PostgreSQL
        with patch('app.MultiDBLib.src.database.postgres_client.PostgresClient.execute_query') as mock_execute:
            db_client = PostgresClient('localhost', 5432, 'user', 'password', 'testdb')
            db_client.execute_query("SELECT * FROM test_table")
            mock_execute.assert_called_once()

    def test_close_connection_mongodb(self):
        # Testing close method for MongoDB
        with patch.object(MongoDBClient, 'close', return_value=None) as mock_close:
            db_client = MongoDBClient('localhost', 27017, 'user', 'password', 'testdb')
            db_client.close()
            mock_close.assert_called_once()

    def test_close_connection_mysql(self):
        # Testing close method for MySQL
        with patch.object(MySQLClient, 'close', return_value=None) as mock_close:
            db_client = MySQLClient('localhost', 3306, 'user', 'password', 'testdb')
            db_client.close()
            mock_close.assert_called_once()

    def test_close_connection_postgres(self):
        # Testing close method for PostgreSQL
        with patch.object(PostgresClient, 'close', return_value=None) as mock_close:
            db_client = PostgresClient('localhost', 5432, 'user', 'password', 'testdb')
            db_client.close()
            mock_close.assert_called_once()
