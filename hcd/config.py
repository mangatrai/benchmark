"""
Configuration management for HCD Cassandra benchmark.
Loads settings from .env file in the hcd directory.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Config:
    """Configuration class for HCD Cassandra benchmark."""
    
    def __init__(self, env_file: str = None):
        """Initialize configuration.
        
        Args:
            env_file: Path to .env file (default: .env in hcd directory)
        """
        # Default to .env in hcd directory if no file specified
        if env_file is None:
            env_file = Path(__file__).parent / ".env"
        
        # Load environment variables from .env file
        if os.path.exists(env_file):
            load_dotenv(env_file)
            logger.info(f"Loaded configuration from {env_file}")
        else:
            logger.warning(f"Environment file {env_file} not found, using system environment variables")
        
        # Vector Database Configuration (Database 1) - HCD Cassandra
        self.vector_db_host = os.getenv('CASSANDRA_VECTOR_NODE_IP', '127.0.0.1')
        self.vector_db_user = os.getenv('CASSANDRA_VECTOR_USER', 'cassandra')
        self.vector_db_password = os.getenv('CASSANDRA_VECTOR_PASSWORD', 'cassandra')
        self.vector_db_keyspace = os.getenv('CASSANDRA_VECTOR_KS', 'price_vector')
        self.vector_db_table = os.getenv('VECTOR_TABLE', 'price_vector_1')
        
        # Data Database Configuration (Database 2) - HCD Cassandra
        self.data_db_host = os.getenv('CASSANDRA_DATA_NODE_IP', '127.0.0.1')
        self.data_db_user = os.getenv('CASSANDRA_DATA_USER', 'cassandra')
        self.data_db_password = os.getenv('CASSANDRA_DATA_PASSWORD', 'cassandra')
        self.data_db_keyspace = os.getenv('CASSANDRA_DATA_KS', 'price_data')
        self.data_db_table = os.getenv('DATA_TABLE', 'price_data_1')
        
        # Benchmarking Configuration
        self.total_records = int(os.getenv('TOTAL_RECORDS', '1000000'))
        self.num_threads = int(os.getenv('NUM_THREADS', '4'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        
        # Threading Configuration
        self.generator_threads = int(os.getenv('GENERATOR_THREADS', os.getenv('NUM_THREADS', '2')))
        self.insert_threads = int(os.getenv('INSERT_THREADS', os.getenv('NUM_THREADS', '4')))
        self.queue_size = int(os.getenv('QUEUE_SIZE', '1000'))
        
        # Retry Configuration
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('RETRY_DELAY', '1.0'))
        
        # Progress Tracking Configuration
        self.checkpoint_interval = int(os.getenv('CHECKPOINT_INTERVAL', '10000'))
        
        # Write Consistency Configuration
        self.write_consistency = os.getenv('WRITE_CONSISTENCY', 'LOCAL_QUORUM')
        
        # Chunk File Configuration
        self.chunk_file_enabled = os.getenv('CHUNK_FILE_ENABLED', 'false').lower() == 'true'
        self.chunk_file_directory = os.getenv('CHUNK_FILE_DIRECTORY', 'data')
        
        # Validate configuration
        self._validate_config()
        
        logger.info(f"HCD Cassandra Config initialized:")
        logger.info(f"  - Generator threads: {self.generator_threads}")
        logger.info(f"  - Insert threads: {self.insert_threads}")
        logger.info(f"  - Queue size: {self.queue_size}")
        logger.info(f"  - Batch size: {self.batch_size}")
        logger.info(f"  - Vector DB: {self.vector_db_host}:{self.vector_db_keyspace}.{self.vector_db_table}")
        logger.info(f"  - Data DB: {self.data_db_host}:{self.data_db_keyspace}.{self.data_db_table}")
        logger.info(f"  - Checkpoint interval: {self.checkpoint_interval}")
        logger.info(f"  - Chunk file enabled: {self.chunk_file_enabled}")
        logger.info(f"  - Chunk file directory: {self.chunk_file_directory}")
    
    def _validate_config(self) -> None:
        """Validate required configuration values."""
        required_vars = [
            ('CASSANDRA_VECTOR_NODE_IP', self.vector_db_host),
            ('CASSANDRA_VECTOR_USER', self.vector_db_user),
            ('CASSANDRA_VECTOR_PASSWORD', self.vector_db_password),
            ('CASSANDRA_DATA_NODE_IP', self.data_db_host),
            ('CASSANDRA_DATA_USER', self.data_db_user),
            ('CASSANDRA_DATA_PASSWORD', self.data_db_password),
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Validate numeric values
        if self.generator_threads <= 0:
            raise ValueError("GENERATOR_THREADS must be greater than 0")
        
        if self.insert_threads <= 0:
            raise ValueError("INSERT_THREADS must be greater than 0")
        
        if self.queue_size <= 0:
            raise ValueError("QUEUE_SIZE must be greater than 0")
        
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be greater than 0")
        
        logger.info("Configuration validation passed")
    
    def get_vector_db_credentials(self) -> dict:
        """Get Vector Database credentials as dictionary.
        
        Returns:
            Dictionary with Vector Database credentials
        """
        return {
            'host': self.vector_db_host,
            'user': self.vector_db_user,
            'password': self.vector_db_password,
            'keyspace': self.vector_db_keyspace,
            'table': self.vector_db_table
        }
    
    def get_data_db_credentials(self) -> dict:
        """Get Data Database credentials as dictionary.
        
        Returns:
            Dictionary with Data Database credentials
        """
        return {
            'host': self.data_db_host,
            'user': self.data_db_user,
            'password': self.data_db_password,
            'keyspace': self.data_db_keyspace,
            'table': self.data_db_table
        }
    
    def get_threading_config(self) -> dict:
        """Get threading configuration as dictionary.
        
        Returns:
            Dictionary with threading settings
        """
        return {
            'generator_threads': self.generator_threads,
            'insert_threads': self.insert_threads,
            'queue_size': self.queue_size,
            'batch_size': self.batch_size
        }
    
    def get_retry_config(self) -> dict:
        """Get retry configuration as dictionary.
        
        Returns:
            Dictionary with retry settings
        """
        return {
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay
        }
