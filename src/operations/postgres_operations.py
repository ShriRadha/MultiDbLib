# postgres_operations.py
from src.database.postgres_client import PostgresClient

def insert_data(db_client, query, params=None):
    """
    Inserts data into a PostgreSQL database.
    
    :param db_client: PostgresClient, an instance of PostgresClient for database interaction.
    :param query: str, the SQL query string to execute for inserting data.
    :param params: tuple or None, parameters for the SQL query to prevent SQL injection.
    :return: int, the number of rows affected.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        db_client.connection.commit()
        row_count = cursor.rowcount
        cursor.close()
        return row_count
    finally:
        db_client.close()

def fetch_data(db_client, query, params=None):
    """
    Fetches data from a PostgreSQL database.
    
    :param db_client: PostgresClient, an instance of PostgresClient for database interaction.
    :param query: str, the SQL query string to execute for fetching data.
    :param params: tuple or None, parameters for the SQL query to ensure safe queries.
    :return: list of tuple, the rows fetched from the database.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        db_client.close()

def update_data(db_client, query, params=None):
    """
    Updates data in a PostgreSQL database.
    
    :param db_client: PostgresClient, an instance of PostgresClient for database interaction.
    :param query: str, the SQL query string to execute for updating data.
    :param params: tuple or None, parameters for the SQL query to prevent SQL injection.
    :return: int, the number of rows affected.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        db_client.connection.commit()
        row_count = cursor.rowcount
        cursor.close()
        return row_count
    finally:
        db_client.close()

def delete_data(db_client, query, params=None):
    """
    Deletes data from a PostgreSQL database.
    
    :param db_client: PostgresClient, an instance of PostgresClient for database interaction.
    :param query: str, the SQL query string to execute for deleting data.
    :param params: tuple or None, parameters for the SQL query to ensure safe deletion.
    :return: int, the number of rows affected.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        db_client.connection.commit()
        row_count = cursor.rowcount
        cursor.close()
        return row_count
    finally:
        db_client.close()
