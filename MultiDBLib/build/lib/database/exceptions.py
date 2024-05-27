class DatabaseError(Exception):
    """Base exception class for database-related errors."""
    pass

class ConnectionError(DatabaseError):
    """Exception raised when connection to the database could not be established."""
    pass

class InsertionError(DatabaseError):
    """Exception raised for errors that occur during a query execution."""
    pass

class FetchError(DatabaseError):
    """Exception raised for errors that occur during a query execution."""
    pass

class UpdateError(DatabaseError):
    """Exception raised for errors that occur during a query execution."""
    pass

class DeletionError(DatabaseError):
    """Exception raised for errors that occur during a query execution."""
    pass

class OperationalError(DatabaseError):
    """Exception raised for errors that occur during a query execution."""
    pass

