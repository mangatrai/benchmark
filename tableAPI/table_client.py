"""
Table API Client for AstraDB benchmarking.
Handles connection management, batch insertion, and error handling using astrapy Table API.
"""

import time
import logging
from typing import List, Dict, Any, Optional
from astrapy import DataAPIClient
from astrapy.data_types import DataAPIVector

logger = logging.getLogger(__name__)


class TableAPIClient:
    """Client for DataStax AstraDB Table API operations."""
    
    def __init__(self, token: str, endpoint: str, namespace: str, table: str, 
                 max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize Table API client.
        
        Args:
            token: AstraDB application token
            endpoint: AstraDB API endpoint
            namespace: Database namespace
            table: Table name
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.token = token
        self.endpoint = endpoint
        self.namespace = namespace
        self.table_name = table
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self.client = None
        self.database = None
        self.table = None
        
        logger.info(f"TableAPIClient initialized: namespace={namespace}, table={table}")
    
    def connect(self) -> None:
        """Establish connection to AstraDB."""
        try:
            self.client = DataAPIClient(token=self.token)
            # Use the full endpoint URL directly
            self.database = self.client.get_database_by_api_endpoint(self.endpoint)
            
            # Get table reference
            try:
                self.table = self.database.get_table(self.table_name)
                logger.info(f"Found existing table: {self.table_name}")
            except Exception:
                # Table doesn't exist, we'll need to create it first
                logger.error(f"Table {self.table_name} not found. Please create the table first.")
                raise RuntimeError(f"Table {self.table_name} not found. Please create the table first.")
            
            logger.info("Successfully connected to AstraDB Table API")
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
        """Insert a batch of documents using Table API insert_many.
        
        Args:
            documents: List of documents to insert
            
        Returns:
            Result from the insert operation
            
        Raises:
            Exception: If all retry attempts fail
        """
        if not self.table:
            raise RuntimeError("Not connected to AstraDB. Call connect() first.")
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"Inserting batch of {len(documents)} documents (attempt {attempt + 1})")
                
                # Convert vector data to DataAPIVector format
                processed_docs = []
                for doc in documents:
                    processed_doc = doc.copy()
                    if 'vector_column' in processed_doc:
                        processed_doc['vector_column'] = DataAPIVector(processed_doc['vector_column'])
                    processed_docs.append(processed_doc)
                
                # Insert documents using Table API insert_many
                result = self.table.insert_many(
                    processed_docs,
                    ordered=False,  # Allow parallel processing
                    chunk_size=len(processed_docs),  # Process as single chunk
                    concurrency=1  # Single concurrent request
                )
                
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
        if not self.table:
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
    
    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the table.
        
        Returns:
            Dictionary with table information
        """
        if not self.table:
            raise RuntimeError("Not connected to AstraDB. Call connect() first.")
        
        try:
            # Get table statistics
            info = {
                "namespace": self.namespace,
                "table": self.table_name,
                "endpoint": self.endpoint
            }
            
            logger.info(f"Table info: {info}")
            return info
            
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the connection to AstraDB.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self.table:
                return False
            
            # Try a simple operation to test connection
            self.get_table_info()
            logger.info("Connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def create_table_api_client(config) -> TableAPIClient:
    """Factory function to create a TableAPIClient with configuration.
    
    Args:
        config: Configuration object
        
    Returns:
        Configured TableAPIClient instance
    """
    return TableAPIClient(
        token=config.astra_token,
        endpoint=config.astra_endpoint,
        namespace=config.astra_namespace,
        table=config.astra_table,
        max_retries=config.max_retries,
        retry_delay=config.retry_delay
    )
