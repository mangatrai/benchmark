"""
Configuration management for Table API benchmark.
Loads settings from .env file in the tableAPI directory.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Config:
    """Configuration class for Table API benchmark."""
    
    def __init__(self, env_file: str = None):
        """Initialize configuration.
        
        Args:
            env_file: Path to .env file (default: .env in tableAPI directory)
        """
        # Default to .env in tableAPI directory if no file specified
        if env_file is None:
            env_file = Path(__file__).parent / ".env"
        
        # Load environment variables from .env file
        if os.path.exists(env_file):
            load_dotenv(env_file)
            logger.info(f"Loaded configuration from {env_file}")
        else:
            logger.warning(f"Environment file {env_file} not found, using system environment variables")
        
        # AstraDB Configuration
        self.astra_token = os.getenv('ASTRA_DB_APPLICATION_TOKEN')
        self.astra_endpoint = os.getenv('ASTRA_DB_API_ENDPOINT')
        self.astra_namespace = os.getenv('ASTRA_DB_NAMESPACE', 'default_keyspace')
        self.astra_table = os.getenv('ASTRA_DB_TABLE', 'sample_price_1')
        
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
        
        # Validate configuration
        self._validate_config()
        
        logger.info(f"Table API Config initialized:")
        logger.info(f"  - Generator threads: {self.generator_threads}")
        logger.info(f"  - Insert threads: {self.insert_threads}")
        logger.info(f"  - Queue size: {self.queue_size}")
        logger.info(f"  - Batch size: {self.batch_size}")
        logger.info(f"  - Table: {self.astra_table}")
        logger.info(f"  - Checkpoint interval: {self.checkpoint_interval}")
    
    def _validate_config(self) -> None:
        """Validate required configuration values."""
        required_vars = [
            ('ASTRA_DB_APPLICATION_TOKEN', self.astra_token),
            ('ASTRA_DB_API_ENDPOINT', self.astra_endpoint),
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
    
    def get_astra_credentials(self) -> dict:
        """Get AstraDB credentials as dictionary.
        
        Returns:
            Dictionary with AstraDB credentials
        """
        return {
            'token': self.astra_token,
            'endpoint': self.astra_endpoint,
            'namespace': self.astra_namespace,
            'table': self.astra_table
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
