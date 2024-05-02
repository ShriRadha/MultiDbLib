from src.database.mysql_client import MySQLClient

def insert_data(db_client, query, params=None):
    """
    Inserts data into a MySQL database.
    
    :param db_client: MySQLClient, an instance of MySQLClient for database interaction.
    :param query: str, the SQL query string to execute for inserting data.
    :param params: tuple, optional parameters for the SQL query to prevent SQL injection.
    :return: The number of rows affected.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        db_client.connection.commit()
        inserted_rows = cursor.rowcount
        cursor.close()
        return inserted_rows
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_client.close()

def fetch_data(db_client, query, params=None):
    """
    Fetches data from a MySQL database.
    
    :param db_client: MySQLClient, an instance of MySQLClient for database interaction.
    :param query: str, the SQL query string to execute for fetching data.
    :param params: tuple, optional parameters for the SQL query to ensure safe queries.
    :return: A list of tuples representing the rows fetched.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_client.close()

def update_data(db_client, query, params=None):
    """
    Updates data in a MySQL database.
    
    :param db_client: MySQLClient, an instance of MySQLClient for database interaction.
    :param query: str, the SQL query string to execute for updating data.
    :param params: tuple, optional parameters for the SQL query to prevent SQL injection.
    :return: The number of rows affected.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        db_client.connection.commit()
        updated_rows = cursor.rowcount
        cursor.close()
        return updated_rows
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_client.close()

def delete_data(db_client, query, params=None):
    """
    Deletes data from a MySQL database.
    
    :param db_client: MySQLClient, an instance of MySQLClient for database interaction.
    :param query: str, the SQL query string to execute for deleting data.
    :param params: tuple, optional parameters for the SQL query to ensure safe deletion.
    :return: The number of rows affected.
    """
    try:
        db_client.connect()
        cursor = db_client.connection.cursor()
        cursor.execute(query, params)
        db_client.connection.commit()
        deleted_rows = cursor.rowcount
        cursor.close()
        return deleted_rows
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db_client.close()
