from src.database.mongodb_client import MongoDBClient

def insert_document(db_client, collection, document):
    """
    Inserts a single document into a MongoDB collection.
    
    :param db_client: MongoDBClient, the MongoDB client instance.
    :param collection: str, the name of the collection where the document will be inserted.
    :param document: dict, the document to insert.
    :return: The ID of the inserted document.
    """
    try:
        result = db_client.db[collection].insert_one(document)
        print(f"Document inserted with id: {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        print(f"An error occurred: {e}")

def find_document(db_client, collection, query):
    """
    Finds documents in a MongoDB collection based on a query.
    
    :param db_client: MongoDBClient, the MongoDB client instance.
    :param collection: str, the name of the collection to search.
    :param query: dict, the query criteria.
    :return: A list of documents that match the query.
    """
    try:
        results = db_client.db[collection].find(query)
        documents = [doc for doc in results]
        print(f"Found {len(documents)} documents")
        return documents
    except Exception as e:
        print(f"An error occurred: {e}")

def update_document(db_client, collection, query, new_values):
    """
    Updates documents in a MongoDB collection based on a query.
    
    :param db_client: MongoDBClient, the MongoDB client instance.
    :param collection: str, the name of the collection where the update will occur.
    :param query: dict, the query to match the documents that need updating.
    :param new_values: dict, the new values to update in the matching documents.
    :return: The count of documents updated.
    """
    try:
        result = db_client.db[collection].update_many(query, {'$set': new_values})
        print(f"Documents updated: {result.modified_count}")
        return result.modified_count
    except Exception as e:
        print(f"An error occurred: {e}")

def delete_document(db_client, collection, query):
    """
    Deletes documents from a MongoDB collection based on a query.
    
    :param db_client: MongoDBClient, the MongoDB client instance.
    :param collection: str, the name of the collection where the deletion will occur.
    :param query: dict, the query to match the documents to be deleted.
    :return: The count of documents deleted.
    """
    try:
        result = db_client.db[collection].delete_many(query)
        print(f"Documents deleted: {result.deleted_count}")
        return result.deleted_count
    except Exception as e:
        print(f"An error occurred: {e}")

