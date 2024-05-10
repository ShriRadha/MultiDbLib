from MultiDBLib import DBConnect, MongoDBClient, PostgresClient, MySQLClient

mongo_client = MongoDBClient("localhost", 27017, "mydatabase", "mycollection")
postgres_client = PostgresClient("localhost", 5432, "myuser", "mypassword", "mydatabase")
mysql_client = MySQLClient("localhost", 3306, "myuser", "mypassword", "mydatabase")

db_mongo = DBConnect(mongo_client)
db_postgres = DBConnect(postgres_client)
db_mysql = DBConnect(mysql_client)

#db_mongo.connect()
#db_mongo.close()

#db_postgres.connect()
#db_postgres.close()


#db_mysql.connect()
#db_mysql.close()