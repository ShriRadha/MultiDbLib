import pytest
from unittest.mock import patch, MagicMock
from app.MultiDBLib.src.database.mongodb_client import MongoDBClient as mongodb_ops
from app.MultiDBLib.src.database.postgres_client import PostgresClient as postgres_ops
from app.MultiDBLib.src.database.mysql_client import MySQLClient as mysql_ops

@pytest.fixture
def test_document():
    return {"name": "Test Document", "value": 123}

@pytest.fixture
def mongo_client():
    client = mongodb_ops(host="localhost", port=27017, database="test_db", collection_name='test_collection')
    return client

@pytest.fixture
def postgres_client():
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    return client

@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_insert_data_mongo(mock_mongo, mongo_client, test_document):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_insert_one = MagicMock(return_value=MagicMock(inserted_id='mock_id'))
    mock_collection.insert_one = mock_insert_one
    mongo_client.connect() 

    inserted_id = mongo_client.insert_data(test_document)

    assert inserted_id == 'mock_id'

@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_fetch_data_mongo(mock_mongo, mongo_client, test_document):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_collection.find.return_value = [test_document]
    mongo_client.connect()

    found_documents = mongo_client.fetch_data({"name": "Test Document"})
    assert found_documents == [test_document]

@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_update_data_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_update_many = MagicMock(return_value=MagicMock(modified_count=1))
    mock_collection.update_many = mock_update_many
    mongo_client.connect()

    updated_count = mongo_client.update_data({"name": "Test Document"}, {"value": 456})
    assert updated_count == 1

@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_delete_data_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_delete_many = MagicMock(return_value=MagicMock(deleted_count=1))
    mock_collection.delete_many = mock_delete_many
    mongo_client.connect()

    deleted_count = mongo_client.delete_data({"name": "Test Document"})
    assert deleted_count == 1


@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_fetch_data_not_in_db_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_collection.find.return_value = []  # No documents found
    mongo_client.connect()

    found_documents = mongo_client.fetch_data({"name": "Nonexistent Document"})
    assert found_documents == []  # Expecting an empty list since no documents match

@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_update_data_not_in_db_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_update_many = MagicMock(return_value=MagicMock(modified_count=0))  # No documents updated
    mock_collection.update_many = mock_update_many
    mongo_client.connect()

    updated_count = mongo_client.update_data({"name": "Nonexistent Document"}, {"value": 999})
    assert updated_count == 0  # Expecting zero since no documents were updated

@patch('app.MultiDBLib.src.database.mongodb_client.MongoClient')
def test_delete_data_not_in_db_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_delete_many = MagicMock(return_value=MagicMock(deleted_count=0))  # No documents deleted
    mock_collection.delete_many = mock_delete_many
    mongo_client.connect()

    deleted_count = mongo_client.delete_data({"name": "Nonexistent Document"})
    assert deleted_count == 0  # Expecting zero since no documents were deleted




@patch('app.MultiDBLib.src.database.postgres_client.psycopg2')
def test_insert_data_postgres(mock_psycopg2):
    # Create the client directly in the test
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connect = MagicMock()
    mock_psycopg2.connect.return_value = mock_connect
    mock_cursor = MagicMock()
    mock_connect.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_connect.close = MagicMock()

    row_count = client.insert_data("INSERT INTO test_table (column) VALUES (%s)", ("value1",))
    assert row_count == 1
    mock_connect.close.assert_called()

@patch('app.MultiDBLib.src.database.postgres_client.psycopg2')
def test_fetch_data_postgres(mock_psycopg2):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connect = MagicMock()
    mock_psycopg2.connect.return_value = mock_connect
    mock_cursor = MagicMock()
    mock_connect.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("Test", 123)]
    mock_connect.close = MagicMock()

    rows = client.fetch_data("SELECT * FROM test_table")
    assert len(rows) == 1
    mock_connect.close.assert_called()

@patch('app.MultiDBLib.src.database.postgres_client.psycopg2')
def test_update_data_postgres(mock_psycopg2):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connect = MagicMock()
    mock_psycopg2.connect.return_value = mock_connect
    mock_cursor = MagicMock()
    mock_connect.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_connect.close = MagicMock()

    row_count = client.update_data("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "old_value"))
    assert row_count == 1
    mock_connect.close.assert_called()

@patch('app.MultiDBLib.src.database.postgres_client.psycopg2')
def test_delete_data_postgres(mock_psycopg2):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connect = MagicMock()
    mock_psycopg2.connect.return_value = mock_connect
    mock_cursor = MagicMock()
    mock_connect.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_connect.close = MagicMock()

    row_count = client.delete_data("DELETE FROM test_table WHERE column = %s", ("old_value",))
    assert row_count == 1
    mock_connect.close.assert_called()

@patch('app.MultiDBLib.src.database.postgres_client.psycopg2.connect')
def test_fetch_data_not_in_db_postgres(mock_connect):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []  # No data found

    rows = client.fetch_data("SELECT * FROM test_table WHERE column = %s", ("nonexistent_value",))
    assert len(rows) == 0  # Expecting empty list since no rows match
    mock_connect.assert_called_once()
    mock_connection.close.assert_called_once()

@patch('app.MultiDBLib.src.database.postgres_client.psycopg2.connect')
def test_update_data_not_in_db_postgres(mock_connect):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 0  # No rows updated

    row_count = client.update_data("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "nonexistent_value"))
    assert row_count == 0  # Expecting zero since no documents were updated
    mock_connect.assert_called_once()
    mock_connection.close.assert_called_once()

@patch('app.MultiDBLib.src.database.postgres_client.psycopg2.connect')
def test_delete_data_not_in_db_postgres(mock_connect):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 0  # No rows deleted

    row_count = client.delete_data("DELETE FROM test_table WHERE column = %s", ("nonexistent_value",))
    assert row_count == 0  # Expecting zero since no documents were deleted
    mock_connect.assert_called_once()
    mock_connection.close.assert_called_once()






@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_insert_data_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1

    row_count = client.insert_data("INSERT INTO test_table (column) VALUES (%s)", ("value1",))
    assert row_count == 1

@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_fetch_data_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("Test", 123)]

    rows = client.fetch_data("SELECT * FROM test_table")
    assert len(rows) == 1

@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_update_data_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 2

    row_count = client.update_data("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "old_value"))
    assert row_count == 2

@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_delete_data_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1

    row_count = client.delete_data("DELETE FROM test_table WHERE column = %s", ("old_value",))
    assert row_count == 1

@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_fetch_data_not_in_db_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []  # No data found

    rows = client.fetch_data("SELECT * FROM test_table WHERE column = %s", ("nonexistent_value",))
    assert len(rows) == 0  # Expecting an empty list since no rows match

@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_update_data_not_in_db_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 0  # No rows updated

    row_count = client.update_data("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "nonexistent_value"))
    assert row_count == 0  # Expecting zero since no documents were updated

@patch('app.MultiDBLib.src.database.mysql_client.mysql.connector.connect')
def test_delete_data_not_in_db_mysql(mock_connect):
    client = mysql_ops(host="localhost", port=3306, user="user", password="pass", database="test_db")
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 0  # No rows deleted

    row_count = client.delete_data("DELETE FROM test_table WHERE column = %s", ("nonexistent_value",))
    assert row_count == 0  # Expecting zero since no documents were deleted
