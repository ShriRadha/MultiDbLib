from MultiDBLib import DBConnect, MongoDBClient, PostgresClient, MSSQLClient
from config import config


# mongo_client = MongoDBClient("localhost", 27017, "mydatabase", "mycollection")

# db_mongo = DBConnect(mongo_client)

# db_mongo.connect()

# db_mongo.delete_all_data({})

# db_mongo.insert_data({"name": "Shri", "age": 20})

# db_mongo.fetch_data({"name": "Shri"})

# db_mongo.update_data({"name": "Shri"}, {"age": 21})

# db_mongo.fetch_data({"name": "Shri"})

# db_mongo.delete_all_data({})

# db_mongo.fetch_data({})

# db_mongo.close()



# params = config()
# postgres_client = PostgresClient(**params, port=5433)
# db_postgres = DBConnect(postgres_client)

# db_postgres.connect()
# db_postgres.insert_data("INSERT INTO test (name, age) VALUES ('Shri', 20)")
# db_postgres.fetch_data("SELECT * FROM test")
# db_postgres.update_data("UPDATE test SET age = 21 WHERE name = 'Shri'")
# db_postgres.fetch_data("SELECT * FROM test")
# db_postgres.delete_all_data("test")
# db_postgres.fetch_data("SELECT * FROM test")
# db_postgres.close()



mssql_client = MSSQLClient("localhost", 1433, "SA", "testPWD123!", "testdbms", 'ODBC Driver 17 for SQL Server')    

db_mssql = DBConnect(mssql_client)

db_mssql.connect()

db_mssql.insert_data("INSERT INTO testtable (name, age) VALUES ('Shri', 20)")

db_mssql.fetch_data("SELECT * FROM testtable")

db_mssql.update_data("UPDATE testtable SET age = 21 WHERE name = 'Shri'")

db_mssql.fetch_data("SELECT * FROM testtable")

db_mssql.delete_all_data("testtable")

db_mssql.fetch_data("SELECT * FROM testtable")

db_mssql.close()
