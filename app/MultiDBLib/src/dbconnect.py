

class DBConnect():
    def __init__(self, db):
        self.db = db

    def connect(self):
        self.db.connect()

    def close(self):
        self.db.close()

    def execute_query(self, query):
        return self.db.execute_query(query)
    
    def insert_data(self, data):
        return self.db.insert_data(data)

    def fetch_data(self, query):
        return self.db.fetch_data(query)
    
    def update_data(self, query, data):
        return self.db.update_data(query, data)
    
    def delete_data(self, query):
        return self.db.delete_data(query)
    