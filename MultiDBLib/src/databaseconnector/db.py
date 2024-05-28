from abc import ABC, abstractmethod
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    def insert_data(self, data):
        pass

    @abstractmethod
    def fetch_data(self, query):
        pass

    @abstractmethod
    def update_data(self, query, data):
        pass

    @abstractmethod
    def delete_data(self, query):
        pass

    @abstractmethod
    def delete_all_data(self):
        pass