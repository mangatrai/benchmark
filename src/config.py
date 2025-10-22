"""
Configuration management for the AstraDB benchmarking tool.
Loads settings from environment variables with sensible defaults.
"""

import os
import logging
from typing import Optional
from dotenv import load_dotenv


class Config:
    """Configuration class for AstraDB benchmarking tool."""
    
    def __init__(self, env_file: Optional[str] = None):
        """Initialize configuration from environment variables."""
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        # AstraDB Configuration
        self.astra_token = os.getenv('ASTRA_DB_APPLICATION_TOKEN')
        self.astra_endpoint = os.getenv('ASTRA_DB_API_ENDPOINT')
        self.astra_namespace = os.getenv('ASTRA_DB_NAMESPACE', 'default_keyspace')
        self.astra_collection = os.getenv('ASTRA_DB_COLLECTION', 'benchmark_collection')
        self.astra_secure_connect_bundle_path = os.getenv('ASTRA_SECURE_CONNECT_BUNDLE')
        
        # CQL-specific Configuration
        self.astra_keyspace = os.getenv('ASTRA_DB_KEYSPACE', 'default_keyspace')
        self.astra_table = os.getenv('ASTRA_DB_TABLE', 'sample_price_1')
        self.astra_username = os.getenv('ASTRA_USERNAME', 'token')
        self.astra_password = os.getenv('ASTRA_PASSWORD')
        
        # Benchmarking Configuration
        self.total_records = int(os.getenv('TOTAL_RECORDS', '1000000'))
        self.num_threads = int(os.getenv('NUM_THREADS', '4'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        
        # Threading Configuration
        self.generator_threads = int(os.getenv('GENERATOR_THREADS', os.getenv('GENERATOR_THREADS', '2')))
        self.insert_threads = int(os.getenv('INSERT_THREADS', os.getenv('INSERT_THREADS', '4')))  # Default to 4x insert threads
        self.queue_size = int(os.getenv('QUEUE_SIZE', '1000'))
        
        # Performance Tuning
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('RETRY_DELAY', '1.0'))
        
        # Progress Tracking Configuration
        self.checkpoint_interval = int(os.getenv('CHECKPOINT_INTERVAL', '10000'))
        
        # Data Generation Parameters
        self.chunk_size = 512  # Fixed chunk size
        self.embedding_dimension = 1536  # Fixed embedding dimension
        self.metadata_fields = 64  # Fixed metadata field count
        
        self._validate_config()
        self._setup_logging()
    
    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        if not self.astra_token:
            raise ValueError("ASTRA_DB_APPLICATION_TOKEN is required")
        if not self.astra_endpoint:
            raise ValueError("ASTRA_DB_API_ENDPOINT is required")
        if not self.astra_secure_connect_bundle_path:
            raise ValueError("ASTRA_SECURE_CONNECT_BUNDLE is required")
        if not self.astra_password:
            raise ValueError("ASTRA_PASSWORD is required for CQL connection")
        if self.total_records <= 0:
            raise ValueError("TOTAL_RECORDS must be positive")
        if self.num_threads <= 0:
            raise ValueError("NUM_THREADS must be positive")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        log_level = getattr(logging, self.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('benchmark.log')
            ]
        )
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return f"""Config(
            total_records={self.total_records:,},
            num_threads={self.num_threads},
            batch_size={self.batch_size},
            namespace={self.astra_namespace},
            collection={self.astra_collection}
        )"""
