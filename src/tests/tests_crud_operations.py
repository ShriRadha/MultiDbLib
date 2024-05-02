import unittest
from unittest.mock import patch, MagicMock
from src.operations.mongodb_operations import insert_document, find_document, update_document, delete_document
from src.operations.postgres_operations import insert_data, fetch_data, update_data, delete_data
from src.operations.mysql_operations import insert_data, fetch_data, update_data, delete_data



class TestMongoDBOperations(unittest.TestCase):

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_insert_document_mongo(self, mock_db_client):
        # Setup
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.insert_one.return_value = MagicMock(inserted_id='abc123')

        # Execution
        result = insert_document(mock_db_client_instance, 'collection', {'name': 'John'})

        # Assertion
        self.assertEqual(result, 'abc123')
        mock_db_client_instance.db.collection.insert_one.assert_called_with({'name': 'John'})

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_find_document_found_mongo(self, mock_db_client):
        # Setup
        expected_document = [{'name': 'John'}]
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.find.return_value = expected_document

        # Execution
        result = find_document(mock_db_client_instance, 'collection', {'name': 'John'})

        # Assertion
        self.assertEqual(result, expected_document)

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_find_document_not_found_mongo(self, mock_db_client):
        # Setup
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.find.return_value = []

        # Execution
        result = find_document(mock_db_client_instance, 'collection', {'name': 'Jane'})

        # Assertion
        self.assertEqual(result, [])

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_update_document_found_mongo(self, mock_db_client):
        # Setup
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.update_many.return_value = MagicMock(matched_count=1, modified_count=1)

        # Execution
        result = update_document(mock_db_client_instance, 'collection', {'name': 'John'}, {'$set': {'name': 'Jane'}})

        # Assertion
        self.assertEqual(result, 1)
        mock_db_client_instance.db.collection.update_many.assert_called_with({'name': 'John'}, {'$set': {'name': 'Jane'}})

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_update_document_not_found_mongo(self, mock_db_client):
        # Setup
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.update_many.return_value = MagicMock(matched_count=0, modified_count=0)

        # Execution
        result = update_document(mock_db_client_instance, 'collection', {'name': 'Unknown'}, {'$set': {'name': 'Jane'}})

        # Assertion
        self.assertEqual(result, 0)

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_delete_document_found_mongo(self, mock_db_client):
        # Setup
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.delete_many.return_value = MagicMock(deleted_count=1)

        # Execution
        result = delete_document(mock_db_client_instance, 'collection', {'name': 'John'})

        # Assertion
        self.assertEqual(result, 1)

    @patch('src.operations.mongodb_operations.MongoDBClient')
    def test_delete_document_not_found_mongo(self, mock_db_client):
        # Setup
        mock_db_client_instance = MagicMock()
        mock_db_client.return_value = mock_db_client_instance
        mock_db_client_instance.db.collection.delete_many.return_value = MagicMock(deleted_count=0)

        # Execution
        result = delete_document(mock_db_client_instance, 'collection', {'name': 'Unknown'})

        # Assertion
        self.assertEqual(result, 0)








    @patch('src.operations.postgres_operations.PostgresClient')
    def test_insert_data_postgres(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Execution
        result = insert_data(mock_db_client, "INSERT INTO table_name (column) VALUES (%s)", ('value',))

        # Assertion
        self.assertEqual(result, 1)
        mock_cursor.execute.assert_called_with("INSERT INTO table_name (column) VALUES (%s)", ('value',))
        mock_cursor.close.assert_called()

    @patch('src.operations.postgres_operations.PostgresClient')
    def test_fetch_data_postgres(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('data1', 'data2')]

        # Execution
        result = fetch_data(mock_db_client, "SELECT * FROM table_name WHERE column = %s", ('value',))

        # Assertion
        self.assertEqual(result, [('data1', 'data2')])
        mock_cursor.execute.assert_called_with("SELECT * FROM table_name WHERE column = %s", ('value',))
        mock_cursor.close.assert_called()

    @patch('src.operations.postgres_operations.PostgresClient')
    def test_update_data_postgres(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Execution
        result = update_data(mock_db_client, "UPDATE table_name SET column = %s WHERE condition = %s", ('new_value', 'old_value'))

        # Assertion
        self.assertEqual(result, 1)
        mock_cursor.execute.assert_called_with("UPDATE table_name SET column = %s WHERE condition = %s", ('new_value', 'old_value'))
        mock_cursor.close.assert_called()

    @patch('src.operations.postgres_operations.PostgresClient')
    def test_delete_data_postgres(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Execution
        result = delete_data(mock_db_client, "DELETE FROM table_name WHERE condition = %s", ('value',))

        # Assertion
        self.assertEqual(result, 1)
        mock_cursor.execute.assert_called_with("DELETE FROM table_name WHERE condition = %s", ('value',))
        mock_cursor.close.assert_called()









    @patch('src.operations.mysql_operations.MySQLClient')
    def test_insert_data_mysql(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Execution
        result = insert_data(mock_db_client, "INSERT INTO table_name (column) VALUES (%s)", ('value',))

        # Assertion
        self.assertEqual(result, 1)
        mock_cursor.execute.assert_called_with("INSERT INTO table_name (column) VALUES (%s)", ('value',))
        mock_cursor.close.assert_called()

    @patch('src.operations.mysql_operations.MySQLClient')
    def test_fetch_data_mysql(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('data1', 'data2')]

        # Execution
        result = fetch_data(mock_db_client, "SELECT * FROM table_name WHERE column = %s", ('value',))

        # Assertion
        self.assertEqual(result, [('data1', 'data2')])
        mock_cursor.execute.assert_called_with("SELECT * FROM table_name WHERE column = %s", ('value',))
        mock_cursor.close.assert_called()

    @patch('src.operations.mysql_operations.MySQLClient')
    def test_update_data_mysql(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Execution
        result = update_data(mock_db_client, "UPDATE table_name SET column = %s WHERE condition = %s", ('new_value', 'old_value'))

        # Assertion
        self.assertEqual(result, 1)
        mock_cursor.execute.assert_called_with("UPDATE table_name SET column = %s WHERE condition = %s", ('new_value', 'old_value'))
        mock_cursor.close.assert_called()

    @patch('src.operations.mysql_operations.MySQLClient')
    def test_delete_data_mysql(self, mock_db_client):
        # Setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_db_client.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 1

        # Execution
        result = delete_data(mock_db_client, "DELETE FROM table_name WHERE condition = %s", ('value',))

        # Assertion
        self.assertEqual(result, 1)
        mock_cursor.execute.assert_called_with("DELETE FROM table_name WHERE condition = %s", ('value',))
        mock_cursor.close.assert_called()
