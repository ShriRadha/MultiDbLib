from .database.db import Database
class DBConnect:
    def __init__(self, db):
        if not isinstance(db, Database):
            raise ValueError("db must be an instance of a class that implements the DatabaseClient interface")
        self.db = db

    def connect(self):
        self.db.connect()

    def close(self):
        self.db.close()
    
    def insert_data(self, data):
        return self.db.insert_data(data)

    def fetch_data(self, query):
        return self.db.fetch_data(query)
    
    def update_data(self, query, data=None):
        return self.db.update_data(query, data)
    
    def delete_data(self, query):
        return self.db.delete_data(query)

    def delete_all_data(self, query):
        return self.db.delete_all_data(query)