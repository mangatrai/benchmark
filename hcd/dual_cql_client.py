"""
Dual CQL Client for HCD Cassandra benchmarking.
Handles dual database operations for vector and data tables using traditional Cassandra CQL.
"""

import time
import logging
from typing import List, Dict, Any, Optional, Tuple
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ProtocolVersion
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel

logger = logging.getLogger(__name__)


class DualCQLClient:
    """Client for dual database operations using traditional Cassandra CQL."""
    
    def __init__(self, vector_config: dict, data_config: dict, 
                 max_retries: int = 3, retry_delay: float = 1.0, 
                 write_consistency: str = 'LOCAL_QUORUM'):
        """Initialize dual CQL client.
        
        Args:
            vector_config: Configuration for vector database
            data_config: Configuration for data database
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            write_consistency: Write consistency level (e.g., 'ANY', 'LOCAL_QUORUM', 'QUORUM')
        """
        self.vector_config = vector_config
        self.data_config = data_config
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.write_consistency = write_consistency
        
        # Vector database connection
        self.vector_cluster = None
        self.vector_session = None
        
        # Data database connection
        self.data_cluster = None
        self.data_session = None
        
        logger.info(f"DualCQLClient initialized:")
        logger.info(f"  - Vector DB: {vector_config['host']} -> {vector_config['keyspace']}.{vector_config['table']}")
        logger.info(f"  - Data DB: {data_config['host']} -> {data_config['keyspace']}.{data_config['table']}")
    
    def connect(self) -> None:
        """Establish connection to both databases."""
        try:
            # Connect to vector database
            logger.info("Connecting to vector database...")
            vector_auth_provider = PlainTextAuthProvider(
                self.vector_config['user'], 
                self.vector_config['password']
            )
            self.vector_cluster = Cluster(
                [self.vector_config['host']],
                auth_provider=vector_auth_provider,
                protocol_version=ProtocolVersion.V4
            )
            self.vector_session = self.vector_cluster.connect()
            self.vector_session.set_keyspace(self.vector_config['keyspace'])
            logger.info(f"Connected to vector database: {self.vector_config['keyspace']}.{self.vector_config['table']}")
            
            # Connect to data database
            logger.info("Connecting to data database...")
            data_auth_provider = PlainTextAuthProvider(
                self.data_config['user'], 
                self.data_config['password']
            )
            self.data_cluster = Cluster(
                [self.data_config['host']],
                auth_provider=data_auth_provider,
                protocol_version=ProtocolVersion.V4
            )
            self.data_session = self.data_cluster.connect()
            self.data_session.set_keyspace(self.data_config['keyspace'])
            logger.info(f"Connected to data database: {self.data_config['keyspace']}.{self.data_config['table']}")
            
            logger.info("Successfully connected to both databases")
            
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close connection to both databases."""
        try:
            if self.vector_session:
                self.vector_session.shutdown()
                self.vector_session = None
                logger.info("Disconnected from vector database")
            
            if self.data_session:
                self.data_session.shutdown()
                self.data_session = None
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
        if not self.vector_session or not self.data_session:
            raise RuntimeError("Not connected to databases. Call connect() first.")
        
        if len(vector_batch) != len(data_batch):
            raise ValueError(f"Batch size mismatch: vector={len(vector_batch)}, data={len(data_batch)}")
        
        # Verify UUID correlation
        for i, (v_doc, d_doc) in enumerate(zip(vector_batch, data_batch)):
            if v_doc['id'] != d_doc['id']:
                raise ValueError(f"UUID mismatch at index {i}: vector={v_doc['id']}, data={d_doc['id']}")
        
        # If only one document, use individual insert for better performance
        if len(vector_batch) == 1:
            return self._insert_single_document_pair(vector_batch[0], data_batch[0])
        
        last_vector_exception = None
        last_data_exception = None
        
        # Retry logic for both insertions
        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(f"Inserting batch of {len(vector_batch)} documents (attempt {attempt + 1})")
                
                # Insert into vector table
                vector_result = self._insert_vector_batch(vector_batch)
                
                # Insert into data table
                data_result = self._insert_data_batch(data_batch)
                
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
    
    def _insert_vector_batch(self, vector_batch: List[Dict[str, Any]]) -> Any:
        """Insert vector batch using CQL."""
        try:
            # Create batch statement for vector table
            batch = BatchStatement(consistency_level=getattr(ConsistencyLevel, self.write_consistency))
            
            for doc in vector_batch:
                # Prepare vector insert statement
                vector_insert_cql = f"INSERT INTO {self.vector_config['table']} (id, vector_column) VALUES (?, ?)"
                prepared_stmt = self.vector_session.prepare(vector_insert_cql)
                
                # Add to batch
                batch.add(prepared_stmt, [doc['id'], doc['vector_column']])
            
            # Execute batch
            result = self.vector_session.execute(batch)
            logger.debug(f"Successfully inserted {len(vector_batch)} vector documents")
            return result
            
        except Exception as e:
            logger.error(f"Failed to insert vector batch: {e}")
            raise
    
    def _insert_data_batch(self, data_batch: List[Dict[str, Any]]) -> Any:
        """Insert data batch using CQL."""
        try:
            # Create batch statement for data table
            batch = BatchStatement(consistency_level=getattr(ConsistencyLevel, self.write_consistency))
            
            for doc in data_batch:
                # Prepare data insert statement with all fields
                columns = [
                    'id', 'chunk',
                    'meta_str_01', 'meta_str_02', 'meta_str_03', 'meta_str_04', 'meta_str_05',
                    'meta_str_06', 'meta_str_07', 'meta_str_08', 'meta_str_09', 'meta_str_10',
                    'meta_str_11', 'meta_str_12', 'meta_str_13', 'meta_str_14', 'meta_str_15',
                    'meta_str_16', 'meta_str_17', 'meta_str_18', 'meta_str_19', 'meta_str_20',
                    'meta_num_01', 'meta_num_02', 'meta_num_03', 'meta_num_04', 'meta_num_05',
                    'meta_num_06', 'meta_num_07', 'meta_num_08', 'meta_num_09', 'meta_num_10',
                    'meta_num_11', 'meta_num_12', 'meta_num_13', 'meta_num_14', 'meta_num_15',
                    'meta_num_16', 'meta_num_17', 'meta_num_18', 'meta_num_19', 'meta_num_20',
                    'meta_bool_01', 'meta_bool_02', 'meta_bool_03', 'meta_bool_04', 'meta_bool_05',
                    'meta_bool_06', 'meta_bool_07', 'meta_bool_08', 'meta_bool_09', 'meta_bool_10',
                    'meta_bool_11', 'meta_bool_12',
                    'meta_date_01', 'meta_date_02', 'meta_date_03', 'meta_date_04', 'meta_date_05',
                    'meta_date_06', 'meta_date_07', 'meta_date_08', 'meta_date_09', 'meta_date_10',
                    'meta_date_11', 'meta_date_12',
                    'created_at', 'updated_at'
                ]
                
                # Build placeholders and column list
                placeholders = ', '.join(['?' for _ in columns])
                column_list = ', '.join(columns)
                
                # Create INSERT statement
                data_insert_cql = f"INSERT INTO {self.data_config['table']} ({column_list}) VALUES ({placeholders})"
                prepared_stmt = self.data_session.prepare(data_insert_cql)
                
                # Build values list
                values = [
                    doc.get('id'),
                    doc.get('chunk'),
                    doc.get('meta_str_01'), doc.get('meta_str_02'), doc.get('meta_str_03'), doc.get('meta_str_04'), doc.get('meta_str_05'),
                    doc.get('meta_str_06'), doc.get('meta_str_07'), doc.get('meta_str_08'), doc.get('meta_str_09'), doc.get('meta_str_10'),
                    doc.get('meta_str_11'), doc.get('meta_str_12'), doc.get('meta_str_13'), doc.get('meta_str_14'), doc.get('meta_str_15'),
                    doc.get('meta_str_16'), doc.get('meta_str_17'), doc.get('meta_str_18'), doc.get('meta_str_19'), doc.get('meta_str_20'),
                    doc.get('meta_num_01'), doc.get('meta_num_02'), doc.get('meta_num_03'), doc.get('meta_num_04'), doc.get('meta_num_05'),
                    doc.get('meta_num_06'), doc.get('meta_num_07'), doc.get('meta_num_08'), doc.get('meta_num_09'), doc.get('meta_num_10'),
                    doc.get('meta_num_11'), doc.get('meta_num_12'), doc.get('meta_num_13'), doc.get('meta_num_14'), doc.get('meta_num_15'),
                    doc.get('meta_num_16'), doc.get('meta_num_17'), doc.get('meta_num_18'), doc.get('meta_num_19'), doc.get('meta_num_20'),
                    doc.get('meta_bool_01'), doc.get('meta_bool_02'), doc.get('meta_bool_03'), doc.get('meta_bool_04'), doc.get('meta_bool_05'),
                    doc.get('meta_bool_06'), doc.get('meta_bool_07'), doc.get('meta_bool_08'), doc.get('meta_bool_09'), doc.get('meta_bool_10'),
                    doc.get('meta_bool_11'), doc.get('meta_bool_12'),
                    doc.get('meta_date_01'), doc.get('meta_date_02'), doc.get('meta_date_03'), doc.get('meta_date_04'), doc.get('meta_date_05'),
                    doc.get('meta_date_06'), doc.get('meta_date_07'), doc.get('meta_date_08'), doc.get('meta_date_09'), doc.get('meta_date_10'),
                    doc.get('meta_date_11'), doc.get('meta_date_12'),
                    doc.get('created_at'), doc.get('updated_at')
                ]
                
                # Add to batch
                batch.add(prepared_stmt, values)
            
            # Execute batch
            result = self.data_session.execute(batch)
            logger.debug(f"Successfully inserted {len(data_batch)} data documents")
            return result
            
        except Exception as e:
            logger.error(f"Failed to insert data batch: {e}")
            raise
    
    def get_connection_status(self) -> Dict[str, bool]:
        """Get connection status for both databases.
        
        Returns:
            Dictionary with connection status for both databases
        """
        return {
            'vector_connected': self.vector_session is not None,
            'data_connected': self.data_session is not None,
            'both_connected': self.vector_session is not None and self.data_session is not None
        }
    
    def get_table_info(self) -> Dict[str, str]:
        """Get table information for both databases.
        
        Returns:
            Dictionary with table names and hosts
        """
        return {
            'vector_table': self.vector_config['table'],
            'vector_host': self.vector_config['host'],
            'data_table': self.data_config['table'],
            'data_host': self.data_config['host']
        }
    
    def _insert_single_document_pair(self, vector_doc: Dict[str, Any], data_doc: Dict[str, Any]) -> Tuple[Any, Any]:
        """Insert a single document pair using direct CQL execution for better performance.
        
        Args:
            vector_doc: Single vector document (id + vector_column)
            data_doc: Single data document (id + chunk + metadata)
            
        Returns:
            Tuple of (vector_result, data_result) from insert operations
            
        Raises:
            Exception: If insertion fails for either table
        """
        try:
            # Insert vector document
            vector_result = self._insert_single_vector_document(vector_doc)
            
            # Insert data document
            data_result = self._insert_single_data_document(data_doc)
            
            logger.debug("Successfully inserted single document pair into both tables")
            return vector_result, data_result
            
        except Exception as e:
            logger.error(f"Failed to insert single document pair: {e}")
            raise
    
    def _insert_single_vector_document(self, vector_doc: Dict[str, Any]) -> Any:
        """Insert a single vector document using direct CQL execution.
        
        Args:
            vector_doc: Single vector document (id + vector_column)
            
        Returns:
            Result from the insert operation
        """
        try:
            # Prepare vector insert statement
            vector_insert_cql = f"INSERT INTO {self.vector_config['table']} (id, vector_column) VALUES (?, ?)"
            prepared_stmt = self.vector_session.prepare(vector_insert_cql)
            
            # Execute single insert with configurable consistency
            result = self.vector_session.execute(
                prepared_stmt.bind([vector_doc['id'], vector_doc['vector_column']]),
                consistency_level=getattr(ConsistencyLevel, self.write_consistency)
            )
            logger.debug("Successfully inserted single vector document via CQL")
            return result
            
        except Exception as e:
            logger.error(f"Failed to insert single vector document: {e}")
            raise
    
    def _insert_single_data_document(self, data_doc: Dict[str, Any]) -> Any:
        """Insert a single data document using direct CQL execution.
        
        Args:
            data_doc: Single data document (id + chunk + metadata)
            
        Returns:
            Result from the insert operation
        """
        try:
            # Build the INSERT statement dynamically
            columns = [
                'id', 'chunk',
                'meta_str_01', 'meta_str_02', 'meta_str_03', 'meta_str_04', 'meta_str_05',
                'meta_str_06', 'meta_str_07', 'meta_str_08', 'meta_str_09', 'meta_str_10',
                'meta_str_11', 'meta_str_12', 'meta_str_13', 'meta_str_14', 'meta_str_15',
                'meta_str_16', 'meta_str_17', 'meta_str_18', 'meta_str_19', 'meta_str_20',
                'meta_num_01', 'meta_num_02', 'meta_num_03', 'meta_num_04', 'meta_num_05',
                'meta_num_06', 'meta_num_07', 'meta_num_08', 'meta_num_09', 'meta_num_10',
                'meta_num_11', 'meta_num_12', 'meta_num_13', 'meta_num_14', 'meta_num_15',
                'meta_num_16', 'meta_num_17', 'meta_num_18', 'meta_num_19', 'meta_num_20',
                'meta_bool_01', 'meta_bool_02', 'meta_bool_03', 'meta_bool_04', 'meta_bool_05',
                'meta_bool_06', 'meta_bool_07', 'meta_bool_08', 'meta_bool_09', 'meta_bool_10',
                'meta_bool_11', 'meta_bool_12',
                'meta_date_01', 'meta_date_02', 'meta_date_03', 'meta_date_04', 'meta_date_05',
                'meta_date_06', 'meta_date_07', 'meta_date_08', 'meta_date_09', 'meta_date_10',
                'meta_date_11', 'meta_date_12',
                'created_at', 'updated_at'
            ]
            
            # Build placeholders and column list
            placeholders = ', '.join(['?' for _ in columns])
            column_list = ', '.join(columns)
            
            # Create INSERT statement
            data_insert_cql = f"INSERT INTO {self.data_config['table']} ({column_list}) VALUES ({placeholders})"
            prepared_stmt = self.data_session.prepare(data_insert_cql)
            
            # Build values list
            values = [
                data_doc.get('id'),
                data_doc.get('chunk'),
                data_doc.get('meta_str_01'), data_doc.get('meta_str_02'), data_doc.get('meta_str_03'), data_doc.get('meta_str_04'), data_doc.get('meta_str_05'),
                data_doc.get('meta_str_06'), data_doc.get('meta_str_07'), data_doc.get('meta_str_08'), data_doc.get('meta_str_09'), data_doc.get('meta_str_10'),
                data_doc.get('meta_str_11'), data_doc.get('meta_str_12'), data_doc.get('meta_str_13'), data_doc.get('meta_str_14'), data_doc.get('meta_str_15'),
                data_doc.get('meta_str_16'), data_doc.get('meta_str_17'), data_doc.get('meta_str_18'), data_doc.get('meta_str_19'), data_doc.get('meta_str_20'),
                data_doc.get('meta_num_01'), data_doc.get('meta_num_02'), data_doc.get('meta_num_03'), data_doc.get('meta_num_04'), data_doc.get('meta_num_05'),
                data_doc.get('meta_num_06'), data_doc.get('meta_num_07'), data_doc.get('meta_num_08'), data_doc.get('meta_num_09'), data_doc.get('meta_num_10'),
                data_doc.get('meta_num_11'), data_doc.get('meta_num_12'), data_doc.get('meta_num_13'), data_doc.get('meta_num_14'), data_doc.get('meta_num_15'),
                data_doc.get('meta_num_16'), data_doc.get('meta_num_17'), data_doc.get('meta_num_18'), data_doc.get('meta_num_19'), data_doc.get('meta_num_20'),
                data_doc.get('meta_bool_01'), data_doc.get('meta_bool_02'), data_doc.get('meta_bool_03'), data_doc.get('meta_bool_04'), data_doc.get('meta_bool_05'),
                data_doc.get('meta_bool_06'), data_doc.get('meta_bool_07'), data_doc.get('meta_bool_08'), data_doc.get('meta_bool_09'), data_doc.get('meta_bool_10'),
                data_doc.get('meta_bool_11'), data_doc.get('meta_bool_12'),
                data_doc.get('meta_date_01'), data_doc.get('meta_date_02'), data_doc.get('meta_date_03'), data_doc.get('meta_date_04'), data_doc.get('meta_date_05'),
                data_doc.get('meta_date_06'), data_doc.get('meta_date_07'), data_doc.get('meta_date_08'), data_doc.get('meta_date_09'), data_doc.get('meta_date_10'),
                data_doc.get('meta_date_11'), data_doc.get('meta_date_12'),
                data_doc.get('created_at'), data_doc.get('updated_at')
            ]
            
            # Execute single insert with configurable consistency
            result = self.data_session.execute(
                prepared_stmt.bind(values),
                consistency_level=getattr(ConsistencyLevel, self.write_consistency)
            )
            logger.debug("Successfully inserted single data document via CQL")
            return result
            
        except Exception as e:
            logger.error(f"Failed to insert single data document: {e}")
            raise
