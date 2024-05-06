class DatabaseError(Exception):
    """Base exception class for database-related errors."""
    pass

class ConnectionError(DatabaseError):
    """Exception raised when connection to the database could not be established."""
    pass

class QueryError(DatabaseError):
    """Exception raised for errors that occur during a query execution."""
    pass
