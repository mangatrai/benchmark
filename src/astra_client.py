"""
DataStax AstraDB client for benchmarking operations.
Handles connection management, batch insertion, and error handling.
"""

import time
import logging
from typing import List, Dict, Any, Optional
from astrapy import DataAPIClient
from astrapy.data_types import DataAPIVector

logger = logging.getLogger(__name__)


class AstraClient:
    """Client for DataStax AstraDB operations."""
    
    def __init__(self, token: str, endpoint: str, namespace: str, collection: str, 
                 max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize AstraDB client.
        
        Args:
            token: AstraDB application token
            endpoint: AstraDB API endpoint
            namespace: Database namespace
            collection: Collection name
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.token = token
        self.endpoint = endpoint
        self.namespace = namespace
        self.collection_name = collection
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self.client = None
        self.collection = None
        
        logger.info(f"AstraClient initialized: namespace={namespace}, collection={collection}")
    
    def connect(self) -> None:
        """Establish connection to AstraDB."""
        try:
            self.client = DataAPIClient(token=self.token)
            # Use the full endpoint URL directly
            database = self.client.get_database_by_api_endpoint(self.endpoint)
            
            # Use collection API for vector operations
            try:
                self.collection = database.get_collection(self.collection_name)
                logger.info(f"Found existing collection: {self.collection_name}")
            except Exception:
                # Collection doesn't exist, create it with proper vector configuration
                logger.info(f"Creating new collection: {self.collection_name}")
                self.collection = database.create_collection(
                    name=self.collection_name,
                    options={
                        "vector": {
                            "dimension": 1536,
                            "metric": "cosine"
                        }
                    }
                )
                logger.info(f"Created collection with vector dimension 1536")
            
            logger.info("Successfully connected to AstraDB")
        except Exception as e:
            logger.error(f"Failed to connect to AstraDB: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close connection to AstraDB."""
        if self.client:
            # DataAPIClient doesn't have a close method in newer versions
            self.client = None
            logger.info("Disconnected from AstraDB")
    
    def insert_batch(self, documents: List[Dict[str, Any]]) -> Any:
        """Insert a batch of documents with retry logic.
        
        Args:
            documents: List of documents to insert
            
        Returns:
            Result from the insert operation
            
        Raises:
            Exception: If all retry attempts fail
        """
        if not self.collection:
            raise RuntimeError("Not connected to AstraDB. Call connect() first.")
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"Inserting batch of {len(documents)} documents (attempt {attempt + 1})")
                
                # Insert documents directly - the collection API handles vector fields automatically
                result = self.collection.insert_many(documents)
                logger.debug(f"Successfully inserted {len(documents)} documents")
                return result
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Insert attempt {attempt + 1} failed: {e}")
                
                if attempt < self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"All {self.max_retries + 1} insert attempts failed")
        
        raise last_exception
    
    def insert_many_batches(self, all_documents: List[Dict[str, Any]], 
                           batch_size: int = 100) -> List[Any]:
        """Insert multiple batches of documents.
        
        Args:
            all_documents: List of all documents to insert
            batch_size: Number of documents per batch
            
        Returns:
            List of insert result objects
        """
        if not self.collection:
            raise RuntimeError("Not connected to AstraDB. Call connect() first.")
        
        results = []
        total_documents = len(all_documents)
        num_batches = (total_documents + batch_size - 1) // batch_size
        
        logger.info(f"Inserting {total_documents} documents in {num_batches} batches of {batch_size}")
        
        for i in range(0, total_documents, batch_size):
            batch = all_documents[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.info(f"Processing batch {batch_num}/{num_batches} ({len(batch)} documents)")
            
            try:
                result = self.insert_batch(batch)
                results.append(result)
                logger.info(f"Batch {batch_num} completed successfully")
                
            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                raise
        
        logger.info(f"Successfully inserted all {total_documents} documents in {len(results)} batches")
        return results
    
    def get_collection_info(self) -> Dict[str, Any]:
        """Get information about the collection.
        
        Returns:
            Dictionary with collection information
        """
        if not self.collection:
            raise RuntimeError("Not connected to AstraDB. Call connect() first.")
        
        try:
            # Get collection statistics
            info = {
                "namespace": self.namespace,
                "collection": self.collection_name,
                "endpoint": self.endpoint
            }
            
            logger.info(f"Collection info: {info}")
            return info
            
        except Exception as e:
            logger.error(f"Failed to get collection info: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the connection to AstraDB.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.collection:
                return False
            
            # Try a simple operation to test connection
            self.get_collection_info()
            logger.info("Connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def create_astra_client(config) -> AstraClient:
    """Factory function to create an AstraClient with configuration.
    
    Args:
        config: Configuration object
        
    Returns:
        Configured AstraClient instance
    """
    return AstraClient(
        token=config.astra_token,
        endpoint=config.astra_endpoint,
        namespace=config.astra_namespace,
        collection=config.astra_collection,
        max_retries=config.max_retries,
        retry_delay=config.retry_delay
    )
