"""
Dual Table API Client for AstraDB benchmarking.
Handles dual database operations for vector and data tables using astrapy Table API.
"""

import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from astrapy import DataAPIClient
from astrapy.data_types import DataAPIVector

logger = logging.getLogger(__name__)


class DualTableAPIClient:
    """Client for dual database operations using Table API."""
    
    def __init__(self, vector_config: dict, data_config: dict, 
                 max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize dual Table API client.
        
        Args:
            vector_config: Configuration for vector database
            data_config: Configuration for data database
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.vector_config = vector_config
        self.data_config = data_config
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Vector database client
        self.vector_client = DataAPIClient(token=vector_config['token'])
        self.vector_database = None
        self.vector_table = None
        
        # Data database client
        self.data_client = DataAPIClient(token=data_config['token'])
        self.data_database = None
        self.data_table = None
        
        logger.info(f"DualTableAPIClient initialized:")
        logger.info(f"  - Vector DB: {vector_config['endpoint']} -> {vector_config['table']}")
        logger.info(f"  - Data DB: {data_config['endpoint']} -> {data_config['table']}")
    
    def connect(self) -> None:
        """Establish connection to both databases."""
        try:
            # Connect to vector database
            logger.info("Connecting to vector database...")
            self.vector_database = self.vector_client.get_database_by_api_endpoint(
                self.vector_config['endpoint']
            )
            self.vector_table = self.vector_database.get_table(self.vector_config['table'])
            logger.info(f"Connected to vector database: {self.vector_config['table']}")
            
            # Connect to data database
            logger.info("Connecting to data database...")
            self.data_database = self.data_client.get_database_by_api_endpoint(
                self.data_config['endpoint']
            )
            self.data_table = self.data_database.get_table(self.data_config['table'])
            logger.info(f"Connected to data database: {self.data_config['table']}")
            
            logger.info("Successfully connected to both databases")
            
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close connection to both databases."""
        try:
            if self.vector_client:
                self.vector_client = None
                logger.info("Disconnected from vector database")
            
            if self.data_client:
                self.data_client = None
                logger.info("Disconnected from data database")
                
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
    
    def insert_batch(self, vector_batch: List[Dict[str, Any]], data_batch: List[Dict[str, Any]]) -> Tuple[Any, Any]:
        """Insert batches into both tables with same UUID correlation.
        
        Args:
            vector_batch: List of vector documents (id + vector_column)
            data_batch: List of data documents (id + chunk + metadata)
            
        Returns:
            Tuple of (vector_result, data_result) from insert operations
            
        Raises:
            Exception: If insertion fails for either table
        """
        if not self.vector_table or not self.data_table:
            raise RuntimeError("Not connected to databases. Call connect() first.")
        
        if len(vector_batch) != len(data_batch):
            raise ValueError(f"Batch size mismatch: vector={len(vector_batch)}, data={len(data_batch)}")
        
        # Verify UUID correlation
        for i, (v_doc, d_doc) in enumerate(zip(vector_batch, data_batch)):
            if v_doc['id'] != d_doc['id']:
                raise ValueError(f"UUID mismatch at index {i}: vector={v_doc['id']}, data={d_doc['id']}")
        
        last_vector_exception = None
        last_data_exception = None
        
        # Retry logic for both insertions
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"Inserting batch of {len(vector_batch)} documents (attempt {attempt + 1})")
                
                # Prepare vector documents
                vector_docs = []
                for doc in vector_batch:
                    doc_copy = doc.copy()
                    if 'vector_column' in doc_copy and isinstance(doc_copy['vector_column'], list):
                        doc_copy['vector_column'] = DataAPIVector(doc_copy['vector_column'])
                    vector_docs.append(doc_copy)
                
                # Prepare data documents (no vector wrapping needed)
                data_docs = data_batch.copy()
                
                # Insert into vector table
                vector_result = self.vector_table.insert_many(
                    vector_docs,
                    ordered=True,
                    chunk_size=len(vector_docs),
                    concurrency=1  # Sequential for consistency
                )
                
                # Insert into data table
                data_result = self.data_table.insert_many(
                    data_docs,
                    ordered=True,
                    chunk_size=len(data_docs),
                    concurrency=1  # Sequential for consistency
                )
                
                logger.debug(f"Successfully inserted {len(vector_batch)} documents into both tables")
                return vector_result, data_result
                
            except Exception as e:
                last_vector_exception = e
                last_data_exception = e  # Same error for both
                logger.warning(f"Insert attempt {attempt + 1} failed: {e}")
                
                if attempt < self.max_retries:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"All {self.max_retries + 1} insert attempts failed")
        
        # If we get here, all retries failed
        raise Exception(f"Vector insertion failed: {last_vector_exception}, Data insertion failed: {last_data_exception}")
    
    def get_connection_status(self) -> Dict[str, bool]:
        """Get connection status for both databases.
        
        Returns:
            Dictionary with connection status for both databases
        """
        return {
            'vector_connected': self.vector_table is not None,
            'data_connected': self.data_table is not None,
            'both_connected': self.vector_table is not None and self.data_table is not None
        }
    
    def get_table_info(self) -> Dict[str, str]:
        """Get table information for both databases.
        
        Returns:
            Dictionary with table names and endpoints
        """
        return {
            'vector_table': self.vector_config['table'],
            'vector_endpoint': self.vector_config['endpoint'],
            'data_table': self.data_config['table'],
            'data_endpoint': self.data_config['endpoint']
        }
