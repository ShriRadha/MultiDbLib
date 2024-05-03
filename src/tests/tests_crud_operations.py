import pytest
import unittest
from unittest.mock import patch, MagicMock
from src.operations.mongodb_operations import insert_document, find_document, update_document, delete_document
from src.operations.postgres_operations import insert_data, fetch_data, update_data, delete_data
from src.operations.mysql_operations import insert_data, fetch_data, update_data, delete_data

# Define fixtures for test data
@pytest.fixture
def test_document():
    return {"name": "Test Document", "value": 123}

# Tests for MongoDB operations
@patch('src.operations.mongodb_operations.MongoDBClient')
def test_insert_document_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking insert_one method
    mock_insert_one = MagicMock()
    mock_client_instance.db.__getitem__.return_value.insert_one = mock_insert_one
    mock_insert_one.return_value.inserted_id = 'mock_id'

    inserted_id = insert_document(mock_client_instance, 'test_collection', test_document)
    assert inserted_id == 'mock_id'

@patch('src.operations.mongodb_operations.MongoDBClient')
def test_find_document_found_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking find method
    mock_find = MagicMock()
    mock_client_instance.db.__getitem__.return_value.find = mock_find
    mock_find.return_value = [test_document]

    found_documents = find_document(mock_client_instance, 'test_collection', {"name": "Test Document"})
    assert len(found_documents) == 1
    assert found_documents[0]["name"] == test_document["name"]

@patch('src.operations.mongodb_operations.MongoDBClient')
def test_find_document_not_found_mongo(mock_mongo_client):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking find method
    mock_find = MagicMock()
    mock_client_instance.db.__getitem__.return_value.find = mock_find
    mock_find.return_value = []

    found_documents = find_document(mock_client_instance, 'test_collection', {"name": "Nonexistent Document"})
    assert len(found_documents) == 0

@patch('src.operations.mongodb_operations.MongoDBClient')
def test_update_document_found_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking update_many method
    mock_update_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.update_many = mock_update_many
    mock_update_many.return_value.modified_count = 1

    updated_count = update_document(mock_client_instance, 'test_collection', {"name": "Test Document"}, {"value": 456})
    assert updated_count == 1

@patch('src.operations.mongodb_operations.MongoDBClient')
def test_update_document_not_found_mongo(mock_mongo_client):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking update_many method
    mock_update_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.update_many = mock_update_many
    mock_update_many.return_value.modified_count = 0

    updated_count = update_document(mock_client_instance, 'test_collection', {"name": "Nonexistent Document"}, {"value": 789})
    assert updated_count == 0

@patch('src.operations.mongodb_operations.MongoDBClient')
def test_delete_document_found_mongo(mock_mongo_client, test_document):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking delete_many method
    mock_delete_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.delete_many = mock_delete_many
    mock_delete_many.return_value.deleted_count = 1

    deleted_count = delete_document(mock_client_instance, 'test_collection', {"name": "Test Document"})
    assert deleted_count == 1

@patch('src.operations.mongodb_operations.MongoDBClient')
def test_delete_document_not_found_mongo(mock_mongo_client):
    # Mocking MongoDB client
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance

    # Mocking delete_many method
    mock_delete_many = MagicMock()
    mock_client_instance.db.__getitem__.return_value.delete_many = mock_delete_many
    mock_delete_many.return_value.deleted_count = 0

    deleted_count = delete_document(mock_client_instance, 'test_collection', {"name": "Nonexistent Document"})
    assert deleted_count == 0
















@pytest.fixture
def test_query():
    return "SELECT * FROM test_table"

# Tests for PostgreSQL operations
@patch('src.operations.postgres_operations.PostgresClient')
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

    inserted_rows = insert_data(mock_client_instance, 'INSERT INTO test_table (column1, column2) VALUES (%s, %s)', ('value1', 'value2'))
    assert inserted_rows == 1

@patch('src.operations.postgres_operations.PostgresClient')
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
    result = fetch_data(mock_client_instance, 'SELECT * FROM test_table')

    # Assertions
    assert result == [('1', '2'), ('3', '4')]

@patch('src.operations.postgres_operations.PostgresClient')
def test_fetch_data_postgres_empty(mock_postgres_client, test_query):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_postgres_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = []

    result = fetch_data(mock_client_instance, test_query)
    assert len(result) == 0

@patch('src.operations.postgres_operations.PostgresClient')
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

    updated_rows = update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'old_value'))
    assert updated_rows == 1

@patch('src.operations.postgres_operations.PostgresClient')
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

    updated_rows = update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'nonexistent_value'))
    assert updated_rows == 0

@patch('src.operations.postgres_operations.PostgresClient')
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

    deleted_rows = delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('value',))
    assert deleted_rows == 1

@patch('src.operations.postgres_operations.PostgresClient')
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

    deleted_rows = delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('nonexistent_value',))
    assert deleted_rows == 0














@patch('src.operations.mysql_operations.MySQLClient')
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

    inserted_rows = insert_data(mock_client_instance, 'INSERT INTO test_table (column1, column2) VALUES (%s, %s)', ('value1', 'value2'))
    assert inserted_rows == 1

    
@patch('src.operations.mysql_operations.MySQLClient')
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
    result = fetch_data(mock_client_instance, 'SELECT * FROM test_table')

    # Assertions
    assert result == [('1', '2'), ('3', '4')]

@patch('src.operations.mysql_operations.MySQLClient')
def test_fetch_data_mysql_empty(mock_mysql_client, test_query):
    # Mocking PostgreSQL client
    mock_client_instance = MagicMock()
    mock_mysql_client.return_value = mock_client_instance

    # Mocking execute_query method
    mock_execute_query = MagicMock()
    mock_client_instance.execute_query = mock_execute_query
    mock_execute_query.return_value = []

    result = fetch_data(mock_client_instance, test_query)
    assert len(result) == 0

@patch('src.operations.mysql_operations.MySQLClient')
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

    updated_rows = update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'old_value'))
    assert updated_rows == 1

@patch('src.operations.mysql_operations.MySQLClient')
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

    updated_rows = update_data(mock_client_instance, 'UPDATE test_table SET column1 = %s WHERE column2 = %s', ('new_value', 'nonexistent_value'))
    assert updated_rows == 0

@patch('src.operations.mysql_operations.MySQLClient')
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

    deleted_rows = delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('value',))
    assert deleted_rows == 1

@patch('src.operations.mysql_operations.MySQLClient')
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

    deleted_rows = delete_data(mock_client_instance, 'DELETE FROM test_table WHERE column1 = %s', ('nonexistent_value',))
    assert deleted_rows == 0
