from abc import ABC, abstractmethod
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database(ABC):
    """Abstract base class defining the interface for database operations."""

    @abstractmethod
    def connect(self):
        """Establish a connection to the database."""
        pass

    @abstractmethod
    def close(self):
        """Close the connection to the database."""
        pass

    @abstractmethod
    def execute_query(self, query):
        """
        Execute a database query.
        :param query: The query string to be executed.
        :return: The result of the query execution.
        """
        pass
