from .db import Database
class DBConnect:
    def __init__(self, db):
        if not isinstance(db, Database):
            raise ValueError("db must be an instance of a class that implements the DatabaseClient interface")
        self.db = db

    def connect(self):
        """
        Connect to the database.
        """
        self.db.connect()

    def close(self):
        """
        Close the database connection.
        """
        self.db.close()
    
    def insert_data(self, data):
        """
        Insert data into the database.
        param: data: dict, the data to insert.
        return: int, the number of rows inserted.
        """
        return self.db.insert_data(data)

    def fetch_data(self, query):
        """
        Fetch data from the database.
        param: query: str, query criteria.
        return: list, the fetched rows.
        """
        return self.db.fetch_data(query)
    
    def update_data(self, query, data=None):
        """
        Update data in the database.
        param: query: str, the query to match the documents that need updating.
        return: int, the number of rows updated.
        """

        return self.db.update_data(query, data)
    
    def delete_data(self, query):
        """
        Delete data from the database.
        param: query: str, the query to match the documents that need deleting.
        return: int, the number of rows deleted.
        """

        return self.db.delete_data(query)

    def delete_all_data(self, query):
        """
        Delete all data from the database.
        param: query: str, the query to match the documents that need deleting.
        return: int, the number of rows deleted.
        """
        return self.db.delete_all_data(query)