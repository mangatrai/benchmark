"""
CQL-based AstraDB client for high-performance vector operations.
Uses Cassandra driver for direct CQL operations instead of DataAPI.
"""

import os
import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ProtocolVersion
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, ConsistencyLevel
import json

logger = logging.getLogger(__name__)


class CQLClient:
    """CQL-based client for AstraDB operations."""
    
    def __init__(self, username: str, password: str, secure_connect_bundle: str, keyspace: str = "benchmark_keyspace", table_name: str = "sample_price_1"):
        """Initialize CQL client.
        
        Args:
            username: AstraDB username (usually 'token')
            password: AstraDB password/token
            secure_connect_bundle: Path to secure connect bundle
            keyspace: Keyspace name
            table_name: Table name for operations
        """
        self.username = username
        self.password = password
        self.secure_connect_bundle = secure_connect_bundle
        self.keyspace = keyspace
        self.table_name = table_name
        self.session = None
        
        logger.info(f"CQLClient initialized: keyspace={keyspace}, table={table_name}")
    
    def connect(self) -> None:
        """Establish connection to AstraDB using CQL."""
        try:
            # Configure cloud connection
            cloud_config = {
                'secure_connect_bundle': self.secure_connect_bundle,
                'connect_timeout': 30
            }
            
            # Configure authentication
            auth_provider = PlainTextAuthProvider(self.username, self.password)
            
            # Configure execution profile
            profile = ExecutionProfile(request_timeout=30)
            
            # Configure cluster with AstraDB settings
            cluster = Cluster(
                cloud=cloud_config,
                auth_provider=auth_provider,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                protocol_version=ProtocolVersion.V4
            )
            
            # Connect to cluster
            self.session = cluster.connect()
            
            # Use the specified keyspace
            self.session.set_keyspace(self.keyspace)
            
            logger.info("Successfully connected to AstraDB via CQL")
            
        except Exception as e:
            logger.error(f"Failed to connect to AstraDB: {e}")
            raise
    
    def disconnect(self) -> None:
        """Disconnect from AstraDB."""
        if self.session:
            self.session.shutdown()
            self.session = None
            logger.info("Disconnected from AstraDB")
    
    def insert_batch(self, documents: List[Dict[str, Any]], max_retries: int = 3, retry_delay: float = 1.0) -> None:
        """Insert a batch of documents using CQL batch statement or individual inserts.
        
        Args:
            documents: List of documents to insert
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        if not self.session:
            raise RuntimeError("Not connected to AstraDB. Call connect() first.")
        
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # If only one document, use individual insert for better performance
                if len(documents) == 1:
                    self._insert_single_document(documents[0])
                    return
                
                # Create batch statement for multiple documents
                batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            
                for doc in documents:
                    # Build the INSERT statement dynamically
                    columns = [
                        'id', 'chunk', 'vector_column',
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
                    
                    # Build placeholders
                    placeholders = ', '.join(['?' for _ in columns])
                    column_list = ', '.join(columns)
                    
                    # Create INSERT statement
                    insert_cql = f"INSERT INTO {self.table_name} ({column_list}) VALUES ({placeholders})"
                    
                    # Prepare statement
                    prepared_stmt = self.session.prepare(insert_cql)
                    
                    # Build values list
                    values = [
                        doc.get('id'),
                        doc.get('chunk'),
                        doc.get('vector_column'),
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
                self.session.execute(batch)
                logger.debug(f"Successfully inserted {len(documents)} documents via CQL batch")
                return  # Success, exit retry loop
                
            except Exception as e:
                last_exception = e
                logger.warning(f"Insert attempt {attempt + 1} failed: {e}")
                
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"All {max_retries + 1} insert attempts failed")
        
        # If we get here, all retries failed
        raise last_exception
    
    def _insert_single_document(self, document: Dict[str, Any]) -> None:
        """Insert a single document using direct CQL statement.
        
        Args:
            document: Single document to insert
        """
        try:
            # Build the INSERT statement dynamically
            columns = [
                'id', 'chunk', 'vector_column',
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
            
            # Build placeholders
            placeholders = ', '.join(['?' for _ in columns])
            column_list = ', '.join(columns)
            
            # Create INSERT statement
            insert_cql = f"INSERT INTO {self.table_name} ({column_list}) VALUES ({placeholders})"
            
            # Prepare statement
            prepared_stmt = self.session.prepare(insert_cql)
            
            # Build values list
            values = [
                document.get('id'),
                document.get('chunk'),
                document.get('vector_column'),
                document.get('meta_str_01'), document.get('meta_str_02'), document.get('meta_str_03'), document.get('meta_str_04'), document.get('meta_str_05'),
                document.get('meta_str_06'), document.get('meta_str_07'), document.get('meta_str_08'), document.get('meta_str_09'), document.get('meta_str_10'),
                document.get('meta_str_11'), document.get('meta_str_12'), document.get('meta_str_13'), document.get('meta_str_14'), document.get('meta_str_15'),
                document.get('meta_str_16'), document.get('meta_str_17'), document.get('meta_str_18'), document.get('meta_str_19'), document.get('meta_str_20'),
                document.get('meta_num_01'), document.get('meta_num_02'), document.get('meta_num_03'), document.get('meta_num_04'), document.get('meta_num_05'),
                document.get('meta_num_06'), document.get('meta_num_07'), document.get('meta_num_08'), document.get('meta_num_09'), document.get('meta_num_10'),
                document.get('meta_num_11'), document.get('meta_num_12'), document.get('meta_num_13'), document.get('meta_num_14'), document.get('meta_num_15'),
                document.get('meta_num_16'), document.get('meta_num_17'), document.get('meta_num_18'), document.get('meta_num_19'), document.get('meta_num_20'),
                document.get('meta_bool_01'), document.get('meta_bool_02'), document.get('meta_bool_03'), document.get('meta_bool_04'), document.get('meta_bool_05'),
                document.get('meta_bool_06'), document.get('meta_bool_07'), document.get('meta_bool_08'), document.get('meta_bool_09'), document.get('meta_bool_10'),
                document.get('meta_bool_11'), document.get('meta_bool_12'),
                document.get('meta_date_01'), document.get('meta_date_02'), document.get('meta_date_03'), document.get('meta_date_04'), document.get('meta_date_05'),
                document.get('meta_date_06'), document.get('meta_date_07'), document.get('meta_date_08'), document.get('meta_date_09'), document.get('meta_date_10'),
                document.get('meta_date_11'), document.get('meta_date_12'),
                document.get('created_at'), document.get('updated_at')
            ]
            
            # Execute single insert
            self.session.execute(prepared_stmt, values)
            logger.debug("Successfully inserted single document via CQL")
            
        except Exception as e:
            logger.error(f"Failed to insert single document: {e}")
            raise