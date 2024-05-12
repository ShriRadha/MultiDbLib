from MultiDBLib import DBConnect, MongoDBClient, PostgresClient, MySQLClient
from config import config


mongo_client = MongoDBClient("localhost", 27017, "mydatabase", "mycollection")

params = config()
postgres_client = PostgresClient(**params, port=5433)

mysql_client = MySQLClient("localhost", 3306, "myuser", "mypassword", "mydatabase")

db_mongo = DBConnect(mongo_client)
db_postgres = DBConnect(postgres_client)
db_mysql = DBConnect(mysql_client)

#db_mongo.connect()
#db_mongo.close()

#db_postgres.connect()
#db_postgres.insert_data("INSERT INTO test (name, age) VALUES ('Shri', 20)")
#db_postgres.close()


#db_mysql.connect()
#db_mysql.close()