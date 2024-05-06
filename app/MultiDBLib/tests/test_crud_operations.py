import pytest
import unittest
from unittest.mock import patch, MagicMock
from app.MultiDBLib.src.database.mongodb_client import MongoDBClient as mongodb_ops
from app.MultiDBLib.src.database.postgres_client import PostgresClient as postgres_ops
from app.MultiDBLib.src.database.mysql_client import MySQLClient as mysql_ops

# Define fixtures for test data
@pytest.fixture
def test_document():
    return {"name": "Test Document", "value": 123}

# Tests for MongoDB operations
@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_insert_data_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking insert_one method
    mock_insert_one = MagicMock()
    mock_client_instance.db.__getitem__.return_value.insert_one = mock_insert_one
    mock_insert_one.return_value.inserted_id = 'mock_id'

    inserted_id = mongodb_ops.insert_data(mock_client_instance, 'test_collection', test_document)
    assert inserted_id == 'mock_id'

@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_fetch_data_found_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking find method
    mock_find = MagicMock()
    mock_client_instance.db.__getitem__.return_value.find = mock_find
    mock_find.return_value = [test_document]

    found_documents = mongodb_ops.fetch_data(mock_client_instance, 'test_collection', {"name": "Test Document"})
    assert len(found_documents) == 1
    assert found_documents[0]["name"] == test_document["name"]

@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_fetch_data_not_found_mongo(mock_mongo_client):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking find method
    mock_find = MagicMock()
    mock_client_instance.db.__getitem__.return_value.find = mock_find
    mock_find.return_value = []

    found_documents = mongodb_ops.fetch_data(mock_client_instance, 'test_collection', {"name": "Nonexistent Document"})
    assert len(found_documents) == 0

@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_update_data_found_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking update_many method
    mock_update_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.update_many = mock_update_many
    mock_update_many.return_value.modified_count = 1

    updated_count = mongodb_ops.update_data(mock_client_instance, 'test_collection', {"name": "Test Document"}, {"value": 456})
    assert updated_count == 1

@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_update_data_not_found_mongo(mock_mongo_client):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking update_many method
    mock_update_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.update_many = mock_update_many
    mock_update_many.return_value.modified_count = 0

    updated_count = mongodb_ops.update_data(mock_client_instance, 'test_collection', {"name": "Nonexistent Document"}, {"value": 789})
    assert updated_count == 0

@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_delete_data_found_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking delete_many method
    mock_delete_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.delete_many = mock_delete_many
    mock_delete_many.return_value.deleted_count = 1

    deleted_count = mongodb_ops.delete_data(mock_client_instance, 'test_collection', {"name": "Test Document"})
    assert deleted_count == 1

@patch('app.MultiDBLib.src.database.mongodb_client.MongoDBClient')
def test_delete_data_not_found_mongo(mock_mongo_client):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking delete_many method
    mock_delete_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.delete_many = mock_delete_many
    mock_delete_many.return_value.deleted_count = 0

    deleted_count = mongodb_ops.delete_data(mock_client_instance, 'test_collection', {"name": "Nonexistent Document"})
    assert deleted_count == 0
















@pytest.fixture
def test_query():
    return "SELECT * FROM test_table"

# Tests for PostgreSQL operations
@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_insert_data_postgres(mock_postgres_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_client_instance.connection.cursor.return_value = mock_cursor

    inserted_rows = postgres_ops.insert_data(mock_client_instance, 'INSERT INTO test_table (column1, column2) VALUES (%s, %s)', ('value1', 'value2'))
    assert inserted_rows == 1

@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_fetch_data_postgres(mock_postgres_client, test_query):
     # Mocking PostgresClient instance
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking cursor and its methods
    mock_cursor = MagicMock()
    mock_fetchall = MagicMock(return_value=[('1', '2'), ('3', '4')])
    mock_cursor.fetchall = mock_fetchall
    mock_client_instance.connection.cursor.return_value = mock_cursor

    # Calling the function under test
    result = postgres_ops.fetch_data(mock_client_instance, 'SELECT * FROM test_table')

    # Assertions
    assert result == [('1', '2'), ('3', '4')]

@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_fetch_data_postgres_empty(mock_postgres_client, test_query):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = []

    result = postgres_ops.fetch_data(mock_client_instance, test_query)
    assert len(result) == 0

@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_update_data_postgres(mock_postgres_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_client_instance.connection.cursor.return_value = mock_cursor

    updated_rows = postgres_ops.update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'old_value'))
    assert updated_rows == 1

@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_update_data_postgres_empty(mock_postgres_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0
    mock_client_instance.connection.cursor.return_value = mock_cursor

    updated_rows = postgres_ops.update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'nonexistent_value'))
    assert updated_rows == 0

@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_delete_data_postgres(mock_postgres_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_client_instance.connection.cursor.return_value = mock_cursor

    deleted_rows = postgres_ops.delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('value',))
    assert deleted_rows == 1

@patch('app.MultiDBLib.src.database.postgres_client.PostgresClient')
def test_delete_data_postgres_empty(mock_postgres_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0
    mock_client_instance.connection.cursor.return_value = mock_cursor

    deleted_rows = postgres_ops.delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('nonexistent_value',))
    assert deleted_rows == 0














@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_insert_data_mysql(mock_mysql_client):

    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_client_instance.connection.cursor.return_value = mock_cursor

    inserted_rows = mysql_ops.insert_data(mock_client_instance, 'INSERT INTO test_table (column1, column2) VALUES (%s, %s)', ('value1', 'value2'))
    assert inserted_rows == 1

    
@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_fetch_data_mysql(mock_mysql_client, test_query):
     # Mocking PostgresClient instance
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking cursor and its methods
    mock_cursor = MagicMock()
    mock_fetchall = MagicMock(return_value=[('1', '2'), ('3', '4')])
    mock_cursor.fetchall = mock_fetchall
    mock_client_instance.connection.cursor.return_value = mock_cursor

    # Calling the function under test
    result = mysql_ops.fetch_data(mock_client_instance, 'SELECT * FROM test_table')

    # Assertions
    assert result == [('1', '2'), ('3', '4')]

@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_fetch_data_mysql_empty(mock_mysql_client, test_query):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = []

    result = mysql_ops.fetch_data(mock_client_instance, test_query)
    assert len(result) == 0

@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_update_data_mysql(mock_mysql_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_client_instance.connection.cursor.return_value = mock_cursor

    updated_rows = mysql_ops.update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'old_value'))
    assert updated_rows == 1

@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_update_data_mysql_empty(mock_mysql_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0
    mock_client_instance.connection.cursor.return_value = mock_cursor

    updated_rows = mysql_ops.update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'nonexistent_value'))
    assert updated_rows == 0

@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_delete_data_mysql(mock_mysql_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_client_instance.connection.cursor.return_value = mock_cursor

    deleted_rows = mysql_ops.delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('value',))
    assert deleted_rows == 1

@patch('app.MultiDBLib.src.database.mysql_client.MySQLClient')
def test_delete_data_mysql_empty(mock_mysql_client):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = None

    # Mocking rowcount property
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0
    mock_client_instance.connection.cursor.return_value = mock_cursor

    deleted_rows = mysql_ops.delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('nonexistent_value',))
    assert deleted_rows == 0
