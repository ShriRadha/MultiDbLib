import pytest
from unittest.mock import patch, MagicMock
from MultiDBLib.src.databaseconnector.mongodb_client import MongoDBClient as mongodb_ops
from MultiDBLib.src.databaseconnector.postgres_client import PostgresClient as postgres_ops
from MultiDBLib.src.databaseconnector.mssql_client import MSSQLClient as mssql_ops

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

@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_insert_data_mongo(mock_mongo, mongo_client, test_document):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_insert_one = MagicMock(return_value=MagicMock(inserted_id='mock_id'))
    mock_collection.insert_one = mock_insert_one
    mongo_client.connect() 

    inserted_id = mongo_client.insert_data(test_document)

    assert inserted_id == 'mock_id'

@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_fetch_data_mongo(mock_mongo, mongo_client, test_document):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_collection.find.return_value = [test_document]
    mongo_client.connect()

    found_documents = mongo_client.fetch_data({"name": "Test Document"})
    assert found_documents == [test_document]

@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_update_data_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_update_many = MagicMock(return_value=MagicMock(modified_count=1))
    mock_collection.update_many = mock_update_many
    mongo_client.connect()

    updated_count = mongo_client.update_data({"name": "Test Document"}, {"value": 456})
    assert updated_count == 1

@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_delete_data_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_delete_many = MagicMock(return_value=MagicMock(deleted_count=1))
    mock_collection.delete_many = mock_delete_many
    mongo_client.connect()

    deleted_count = mongo_client.delete_data({"name": "Test Document"})
    assert deleted_count == 1


@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_fetch_data_not_in_db_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_collection.find.return_value = []  # No documents found
    mongo_client.connect()

    found_documents = mongo_client.fetch_data({"name": "Nonexistent Document"})
    assert found_documents == []  # Expecting an empty list since no documents match

@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_update_data_not_in_db_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_update_many = MagicMock(return_value=MagicMock(modified_count=0))  # No documents updated
    mock_collection.update_many = mock_update_many
    mongo_client.connect()

    updated_count = mongo_client.update_data({"name": "Nonexistent Document"}, {"value": 999})
    assert updated_count == 0  # Expecting zero since no documents were updated

@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_delete_data_not_in_db_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_delete_many = MagicMock(return_value=MagicMock(deleted_count=0))  # No documents deleted
    mock_collection.delete_many = mock_delete_many
    mongo_client.connect()

    deleted_count = mongo_client.delete_data({"name": "Nonexistent Document"})
    assert deleted_count == 0  # Expecting zero since no documents were deleted


@patch('MultiDBLib.src.databaseconnector.mongodb_client.MongoClient')
def test_delete_all_data_mongo(mock_mongo, mongo_client):
    mock_collection = MagicMock()
    mock_mongo.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_delete_many = MagicMock(return_value=MagicMock(deleted_count=1))
    mock_collection.delete_many = mock_delete_many
    mongo_client.connect()

    deleted_count = mongo_client.delete_all_data()
    assert deleted_count == 1




@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2')
def test_insert_data_postgres(mock_psycopg2):
    # Create the client directly in the test
    # Create the PostgresClient directly in the test
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")

        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_psycopg2.connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Run the insert_data method
        row_count = client.insert_data("INSERT INTO test_table (column) VALUES (%s)", ("value1",))

        # Assertions to check that the operations were performed as expected
        assert row_count == 1
        mock_cursor.execute.assert_called_once_with("INSERT INTO test_table (column) VALUES (%s)", ("value1",))
        mock_cursor.close.assert_called_once()
        mock_connection.commit.assert_called_once()

@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2')
def test_fetch_data_postgres(mock_psycopg2):
    # Create the PostgresClient directly in the test
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")

        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_psycopg2.connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("Test", 123)]

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Run the fetch_data method
        rows = client.fetch_data("SELECT * FROM test_table")

        # Assertions to check that the operations were performed as expected
        assert len(rows) == 1
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", None)
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()

@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2')
def test_update_data_postgres(mock_psycopg2):
    # Setup the PostgresClient
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
        
        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_psycopg2.connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Execute the update_data method
        row_count = client.update_data("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "old_value"))

        # Assertions to check that the operations were performed as expected
        assert row_count == 1
        mock_cursor.execute.assert_called_once_with("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "old_value"))
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_not_called()  # Verify that connection is not closed unless explicitly done so

@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2')
def test_delete_data_postgres(mock_psycopg2):
# Setup the PostgresClient
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
        
        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_psycopg2.connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Execute the delete_data method
        row_count = client.delete_data("DELETE FROM test_table WHERE column = %s", ("old_value",))

        # Assertions to check that the operations were performed as expected
        assert row_count == 1
        mock_cursor.execute.assert_called_once_with("DELETE FROM test_table WHERE column = %s", ("old_value",))
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_not_called()  # Verify that connection is not closed unless explicitly done so


@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2.connect')
def test_fetch_data_not_in_db_postgres(mock_psycopg2):
    # Setup the PostgresClient
        # Create the PostgresClient directly in the test
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")

        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_psycopg2.connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Run the fetch_data method
        rows = client.fetch_data("SELECT * FROM test_table")

        # Assertions to check that the operations were performed as expected
        assert len(rows) == 0
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", None)
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()

@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2.connect')
def test_update_data_not_in_db_postgres(mock_connect):
    client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
        
    # Setup the connection mocks
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 0  # No rows updated

    # Manually assign the mocked connection to simulate the connection being established
    client.connection = mock_connection

    # Execute the update_data method
    row_count = client.update_data("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "nonexistent_value"))

    # Assertions to check that the operations were performed as expected
    assert row_count == 0
    mock_cursor.execute.assert_called_once_with("UPDATE test_table SET column = %s WHERE column = %s", ("new_value", "nonexistent_value"))
    mock_connection.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_not_called()  # Verify that connection is not closed unless explicitly done so

@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2.connect')
def test_delete_data_not_in_db_postgres(mock_connect):
    # Setup the PostgresClient
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
        
        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 0  # No rows deleted

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Execute the delete_data method
        row_count = client.delete_data("DELETE FROM test_table WHERE column = %s", ("nonexistent_value",))

        # Assertions to check that the operations were performed as expected
        assert row_count == 0
        mock_cursor.execute.assert_called_once_with("DELETE FROM test_table WHERE column = %s", ("nonexistent_value",))
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_not_called()  # Verify that connection is not closed unless explicitly done so

@patch('MultiDBLib.src.databaseconnector.postgres_client.psycopg2.connect')
def test_delete_all_data_postgres(mock_connect):
    # Setup the PostgresClient
        client = postgres_ops(host="localhost", port=5432, user="user", password="pass", database="test_db")
        
        # Setup the connection mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1  # Simulate deletion of one row

        # Manually assign the mocked connection to simulate the connection being established
        client.connection = mock_connection

        # Execute the delete_all_data method
        row_count = client.delete_all_data("test_table")

        # Assertions to check that the operations were performed as expected
        assert row_count == 1
        mock_cursor.execute.assert_called_once_with("DELETE FROM test_table")  # Ensure the correct SQL is executed
        mock_connection.commit.assert_called_once()  # Ensure the transaction is committed
        mock_cursor.close.assert_called_once()  # Ensure the cursor is closed after operation




@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_insert_data_mssql(mock_pyodbc_connect):

# Mock the connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Setup cursor context management
        mock_cursor.__enter__.return_value = mock_cursor
        # Make sure __exit__ properly simulates closing the cursor
        mock_cursor.__exit__.return_value = None

        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')

        # Manually set the connection attribute to the mocked connection
        client.connection = mock_connection

        # Execute the insert_data method
        row_count = client.insert_data("INSERT INTO test_table (column) VALUES (?)", ("value1",))

        # Assertions to check that the operations were performed as expected
        assert row_count == 1
        mock_cursor.execute.assert_called_once_with("INSERT INTO test_table (column) VALUES (?)", ("value1",))
        mock_connection.commit.assert_called_once()


@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_fetch_data_mssql(mock_pyodbc_connect):

        # Mock the connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Set up fetchall return
        mock_cursor.fetchall.return_value = [("Test", 123)]

        # Setup cursor context management
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None  # Simulate the context manager closing the cursor

        # Initialize MSSQLClient
        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')
        
        # Manually set the connection attribute to the mocked connection
        client.connection = mock_connection
        
        # Execute the fetch_data method
        rows = client.fetch_data("SELECT * FROM test_table")
        
        # Assertions to check that the operations were performed as expected
        assert len(rows) == 1, "Expected one row to be returned from fetchall"
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table", ())
        mock_cursor.fetchall.assert_called_once()



@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_update_data_mssql(mock_pyodbc_connect):
    # Mock the connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Set up context management correctly
        mock_cursor.rowcount = 1  # Simulate updating one row
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None  # Simulate closing the cursor
        
        # Initialize MSSQLClient and perform update operation
        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')
        
        # Manually set the connection attribute to the mocked connection
        client.connection = mock_connection
        
        # Execute the update_data method
        row_count = client.update_data("UPDATE test_table SET column = ? WHERE column = ?", ("new_value", "old_value"))
        
        # Assertions to check that the operations were performed as expected
        assert row_count == 1, "Expected one row to be updated"
        mock_cursor.execute.assert_called_once_with("UPDATE test_table SET column = ? WHERE column = ?", ("new_value", "old_value"))
        mock_connection.commit.assert_called_once()
        assert mock_cursor.__exit__.called, "Cursor was not closed by context manager"


@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_delete_data_mssql(mock_pyodbc_connect):
# Mock the connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Simulate deleting one row
        mock_cursor.rowcount = 1
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None  # Assuming no exception handling is needed


        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')

        # Manually set the connection attribute to the mocked connection
        client.connection = mock_connection

        # Execute the delete_data method
        row_count = client.delete_data("DELETE FROM test_table WHERE column = ?", ("old_value",))

        # Assertions to check that the operations were performed as expected
        assert row_count == 1, "Expected one row to be deleted"
        mock_cursor.execute.assert_called_once_with("DELETE FROM test_table WHERE column = ?", ("old_value",))
        mock_connection.commit.assert_called_once()
        assert mock_cursor.__exit__.called, "Cursor was not closed by context manager"


@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_delete_all_data_mssql(mock_pyodbc_connect):
# Mock the connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Set up the cursor to correctly simulate row count and context management
        mock_cursor.rowcount = 1  # Simulate deleting rows
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None  # No exception handling

        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')

        # Set the connection attribute to the mocked connection
        client.connection = mock_connection

        # Execute the delete_all_data method
        row_count = client.delete_all_data("test_table")

        # Assertions to check that the operations were performed as expected
        assert row_count == 1, "Expected all rows to be deleted"
        mock_cursor.execute.assert_called_once_with("DELETE FROM test_table")
        assert mock_cursor.__exit__.called, "Cursor was not closed by context manager"



@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_delete_data_not_in_mssql(mock_pyodbc_connect):
 # Setup mock connection and cursor
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_pyodbc_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 0  # No rows affected

    # Mimic the context management behavior of a real cursor
    mock_cursor.__enter__.return_value = mock_cursor
    # Ensure close is called when exiting the context
    mock_cursor.__exit__.return_value = mock_cursor.close

    # Initialize MSSQLClient
    client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')

    # Manually set the connection attribute if not automatically set in initialization
    client.connection = mock_connection

    # Execute the delete_data method
    row_count = client.delete_data("DELETE FROM test_table WHERE column = ?", ("nonexistent_value",))

    # Assertions
    assert row_count == 0, "Expected no rows to be deleted since the value does not exist"
    mock_cursor.execute.assert_called_once_with("DELETE FROM test_table WHERE column = ?", ("nonexistent_value",))
    assert mock_cursor.__exit__.called, "Cursor was not closed by context manager"


@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_update_data_not_in_mssql( mock_pyodbc_connect):
# Setup mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        mock_cursor.rowcount = 0  # Simulate no rows updated
            # Mimic the context management behavior of a real cursor
        mock_cursor.__enter__.return_value = mock_cursor
        # Ensure close is called when exiting the context
        mock_cursor.__exit__.return_value = mock_cursor.close

        # Initialize MSSQLClient and simulate a connection
        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')
        client.connection = mock_connection  # Ensure the connection is set

        # Perform the update operation
        row_count = client.update_data("UPDATE test_table SET column = ? WHERE column = ?", ("new_value", "nonexistent_value"))

        # Assertions
        assert row_count == 0, "Expected no rows to be updated as the value does not exist"
        mock_cursor.execute.assert_called_once_with("UPDATE test_table SET column = ? WHERE column = ?", ("new_value", "nonexistent_value"))
        assert mock_cursor.__exit__.called, "Cursor was not closed by context manager"


@patch('MultiDBLib.src.databaseconnector.mssql_client.pyodbc.connect')
def test_fetch_data_not_in_mssql(mock_pyodbc_connect):
    # Setup mock connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_pyodbc_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []  # Simulate no rows fetched

        # Mimic the context management behavior of a real cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.__exit__.return_value = None

        # Initialize MSSQLClient
        client = mssql_ops('localhost', 1433, 'user', 'password', 'test_db', 'ODBC Driver 17 for SQL Server')
        client.connection = mock_connection  # Ensure the connection is set

        # Execute the fetch_data method
        rows = client.fetch_data("SELECT * FROM test_table WHERE column = ?", ("nonexistent_value",))

        # Assertions
        assert len(rows) == 0, "Expected no rows to be fetched as the value does not exist"
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table WHERE column = ?", ("nonexistent_value",))
        mock_cursor.fetchall.assert_called_once()
        assert mock_cursor.__exit__.called, "Cursor was not closed by context manager"
