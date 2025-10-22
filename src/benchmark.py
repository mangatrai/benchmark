"""
Main benchmarking orchestrator with multi-threading support.
Coordinates data generation and database insertion across multiple threads.
"""

import time
import threading
import queue
import json
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from .config import Config
from .data_generator import DataGenerator, create_data_generator
from .astra_client import AstraClient, create_astra_client

logger = logging.getLogger(__name__)


class ThreadSafeCounter:
    """Thread-safe counter for tracking total bytes across multiple threads."""
    
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def add(self, value: int) -> None:
        """Add value to the counter in a thread-safe manner."""
        with self._lock:
            self._value += value
    
    def get_total(self) -> int:
        """Get the total value."""
        with self._lock:
            return self._value


class BenchmarkOrchestrator:
    """Orchestrates the benchmarking process with multi-threading support."""
    
    def __init__(self, config: Config):
        """Initialize the benchmark orchestrator.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.data_generator = create_data_generator(config)
        self.astra_client = create_astra_client(config)
        
        # Threading coordination
        self.document_queue = queue.Queue(maxsize=config.batch_size * 2)
        self.stop_event = threading.Event()
        self.total_bytes = ThreadSafeCounter()
        self.total_chunk_bytes = ThreadSafeCounter()
        self.total_vector_bytes = ThreadSafeCounter()
        self.total_metadata_bytes = ThreadSafeCounter()
        self.total_actual_data_bytes = ThreadSafeCounter()
        
        # Multiple encoding byte counters
        self.total_bytes_utf8 = ThreadSafeCounter()
        self.total_bytes_utf16le = ThreadSafeCounter()
        self.total_bytes_utf16_bom = ThreadSafeCounter()
        
        # File dumping for verification
        self.chunk_file_path = None
        self.chunk_file_handle = None
        self.chunk_file_lock = threading.Lock()  # Thread-safe file writing
        self.setup_chunk_file()
        self.stats = {
            'documents_generated': 0,
            'documents_inserted': 0,
            'batches_processed': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        
        logger.info(f"BenchmarkOrchestrator initialized with config: {config}")
    
    def generate_worker(self, worker_id: int, total_documents: int) -> None:
        """Worker thread for generating documents.
        
        Args:
            worker_id: Unique identifier for this worker
            total_documents: Total number of documents this worker should generate
        """
        logger.info(f"Generator worker {worker_id} started, generating {total_documents} documents")
        
        documents_generated = 0
        
        try:
            while documents_generated < total_documents and not self.stop_event.is_set():
                # Generate batch of documents
                batch_size = min(self.config.batch_size, total_documents - documents_generated)
                batch = self.data_generator.generate_batch(batch_size)
                
                # Track total bytes and actual data bytes for this batch
                batch_bytes = 0
                batch_chunk_bytes = 0
                batch_vector_bytes = 0
                batch_metadata_bytes = 0
                batch_actual_data_bytes = 0
                batch_utf8_bytes = 0
                batch_utf16le_bytes = 0
                batch_utf16_bom_bytes = 0
                
                for document in batch:
                    # Total document size (JSON overhead)
                    json_str = json.dumps(document)
                    batch_bytes += len(json_str.encode('utf-8'))
                    
                    # Actual data bytes (what gets stored in database)
                    chunk_bytes = len(document['chunk'].encode('utf-16le'))
                    vector_bytes = len(document['$vector']) * 8  # 8 bytes per float
                    metadata_bytes = sum(len(str(v).encode('utf-8')) for v in document['metadata'].values())
                    actual_data_bytes = chunk_bytes + vector_bytes + metadata_bytes
                    
                    # Calculate multiple encoding byte sizes
                    chunk_utf8_bytes = len(document['chunk'].encode('utf-8'))
                    chunk_utf16le_bytes = len(document['chunk'].encode('utf-16le'))
                    chunk_utf16_bom_bytes = len(document['chunk'].encode('utf-16'))
                    
                    batch_chunk_bytes += chunk_bytes
                    batch_vector_bytes += vector_bytes
                    batch_metadata_bytes += metadata_bytes
                    batch_actual_data_bytes += actual_data_bytes
                    batch_utf8_bytes += chunk_utf8_bytes
                    batch_utf16le_bytes += chunk_utf16le_bytes
                    batch_utf16_bom_bytes += chunk_utf16_bom_bytes
                    
                    # Write chunk to file for verification (memory-safe, immediate write)
                    self.write_chunk_to_file(document['chunk'])
                
                # Add to thread-safe counters
                self.total_bytes.add(batch_bytes)
                self.total_chunk_bytes.add(batch_chunk_bytes)
                self.total_vector_bytes.add(batch_vector_bytes)
                self.total_metadata_bytes.add(batch_metadata_bytes)
                self.total_actual_data_bytes.add(batch_actual_data_bytes)
                self.total_bytes_utf8.add(batch_utf8_bytes)
                self.total_bytes_utf16le.add(batch_utf16le_bytes)
                self.total_bytes_utf16_bom.add(batch_utf16_bom_bytes)
                
                # Put batch in queue
                self.document_queue.put(batch)
                documents_generated += len(batch)
                
                # Update stats
                with threading.Lock():
                    self.stats['documents_generated'] += len(batch)
                
                logger.debug(f"Worker {worker_id} generated {len(batch)} documents "
                           f"({documents_generated}/{total_documents})")
        
        except Exception as e:
            logger.error(f"Generator worker {worker_id} failed: {e}")
            with threading.Lock():
                self.stats['errors'].append(f"Generator worker {worker_id}: {e}")
        
        logger.info(f"Generator worker {worker_id} completed, generated {documents_generated} documents")
    
    def dry_run_worker(self, worker_id: int, total_documents: int) -> None:
        """Worker thread for dry-run data generation (no queue, no storage).
        
        Args:
            worker_id: Unique identifier for this worker
            total_documents: Total number of documents this worker should generate
        """
        logger.info(f"Dry-run worker {worker_id} started, generating {total_documents} documents")
        
        documents_generated = 0
        
        try:
            while documents_generated < total_documents and not self.stop_event.is_set():
                # Generate single document
                document = self.data_generator.generate_document()
                
                # Calculate bytes without storing document
                json_str = json.dumps(document)
                total_bytes = len(json_str.encode('utf-8'))
                
                # Calculate actual data bytes (what gets stored in database)
                chunk_bytes = len(document['chunk'].encode('utf-16le'))
                vector_bytes = len(document['$vector']) * 8  # 8 bytes per float
                metadata_bytes = sum(len(str(v).encode('utf-8')) for v in document['metadata'].values())
                actual_data_bytes = chunk_bytes + vector_bytes + metadata_bytes
                
                # Calculate multiple encoding byte sizes
                chunk_utf8_bytes = len(document['chunk'].encode('utf-8'))
                chunk_utf16le_bytes = len(document['chunk'].encode('utf-16le'))
                chunk_utf16_bom_bytes = len(document['chunk'].encode('utf-16'))
                
                # Add to thread-safe counters
                self.total_bytes.add(total_bytes)
                self.total_chunk_bytes.add(chunk_bytes)
                self.total_vector_bytes.add(vector_bytes)
                self.total_metadata_bytes.add(metadata_bytes)
                self.total_actual_data_bytes.add(actual_data_bytes)
                
                # Add multiple encoding counters
                self.total_bytes_utf8.add(chunk_utf8_bytes)
                self.total_bytes_utf16le.add(chunk_utf16le_bytes)
                self.total_bytes_utf16_bom.add(chunk_utf16_bom_bytes)
                
                # Write chunk to file for verification
                self.write_chunk_to_file(document['chunk'])
                
                documents_generated += 1
                
                # Update stats
                with threading.Lock():
                    self.stats['documents_generated'] += 1
                
                # Log progress every 1000 documents
                if documents_generated % 1000 == 0:
                    logger.debug(f"Dry-run worker {worker_id} generated {documents_generated}/{total_documents} documents")
        
        except Exception as e:
            logger.error(f"Dry-run worker {worker_id} failed: {e}")
            with threading.Lock():
                self.stats['errors'].append(f"Dry-run worker {worker_id}: {e}")
        
        logger.info(f"Dry-run worker {worker_id} completed, generated {documents_generated} documents")
    
    def insert_worker(self, worker_id: int) -> None:
        """Worker thread for inserting documents.
        
        Args:
            worker_id: Unique identifier for this worker
        """
        logger.info(f"Insert worker {worker_id} started")
        
        try:
            while not self.stop_event.is_set():
                try:
                    # Get batch from queue with timeout
                    batch = self.document_queue.get(timeout=5.0)
                    
                    if batch is None:  # Poison pill to stop worker
                        break
                    
                    # Insert batch
                    self.astra_client.insert_batch(batch)
                    
                    # Update stats
                    with threading.Lock():
                        self.stats['documents_inserted'] += len(batch)
                        self.stats['batches_processed'] += 1
                    
                    logger.debug(f"Insert worker {worker_id} processed batch of {len(batch)} documents")
                    
                except queue.Empty:
                    # Timeout waiting for batch, check if we should stop
                    if self.stats['documents_generated'] >= self.config.total_records:
                        break
                    continue
                
                except Exception as e:
                    logger.error(f"Insert worker {worker_id} failed: {e}")
                    with threading.Lock():
                        self.stats['errors'].append(f"Insert worker {worker_id}: {e}")
        
        except Exception as e:
            logger.error(f"Insert worker {worker_id} failed: {e}")
            with threading.Lock():
                self.stats['errors'].append(f"Insert worker {worker_id}: {e}")
        
        logger.info(f"Insert worker {worker_id} completed")
    
    def setup_chunk_file(self):
        """Setup chunk file for dumping generated chunks."""
        import os
        from datetime import datetime
        
        # Create data directory if it doesn't exist
        data_dir = "data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # Create timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.chunk_file_path = os.path.join(data_dir, f"chunks_{timestamp}.txt")
        
        # Open file for writing
        self.chunk_file_handle = open(self.chunk_file_path, 'w', encoding='utf-8')
        logger.info(f"Chunk file created: {self.chunk_file_path}")
    
    def write_chunk_to_file(self, chunk: str):
        """Write chunk to file for verification (thread-safe, memory-safe)."""
        if self.chunk_file_handle:
            with self.chunk_file_lock:  # Thread-safe file writing
                self.chunk_file_handle.write(chunk + "\n")
                self.chunk_file_handle.flush()  # Ensure data is written immediately
    
    def close_chunk_file(self):
        """Close chunk file and return file size."""
        if self.chunk_file_handle:
            self.chunk_file_handle.close()
            self.chunk_file_handle = None
            
            # Get file size
            import os
            if os.path.exists(self.chunk_file_path):
                file_size = os.path.getsize(self.chunk_file_path)
                logger.info(f"Chunk file size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
                return file_size
        return 0
    
    def run_benchmark(self) -> Dict[str, Any]:
        """Run the complete benchmarking process.
        
        Returns:
            Dictionary with benchmark results and statistics
        """
        logger.info(f"Starting benchmark: {self.config.total_records:,} documents, "
                   f"{self.config.num_threads} threads")
        
        self.stats['start_time'] = time.time()
        
        try:
            # Connect to AstraDB
            self.astra_client.connect()
            
            if not self.astra_client.test_connection():
                raise RuntimeError("Failed to connect to AstraDB")
            
            # Calculate documents per generator thread
            docs_per_generator = self.config.total_records // self.config.num_threads
            remaining_docs = self.config.total_records % self.config.num_threads
            
            # Start generator threads
            generator_threads = []
            for i in range(self.config.num_threads):
                docs_for_thread = docs_per_generator + (1 if i < remaining_docs else 0)
                if docs_for_thread > 0:
                    thread = threading.Thread(
                        target=self.generate_worker,
                        args=(i, docs_for_thread),
                        name=f"Generator-{i}"
                    )
                    thread.start()
                    generator_threads.append(thread)
            
            # Start insert threads
            insert_threads = []
            for i in range(self.config.num_threads):
                thread = threading.Thread(
                    target=self.insert_worker,
                    args=(i,),
                    name=f"Insert-{i}"
                )
                thread.start()
                insert_threads.append(thread)
            
            # Wait for generator threads to complete
            for thread in generator_threads:
                thread.join()
            
            # Signal insert threads to stop
            self.stop_event.set()
            
            # Wait for insert threads to complete
            for thread in insert_threads:
                thread.join()
            
            self.stats['end_time'] = time.time()
            
            # Calculate final statistics
            duration = self.stats['end_time'] - self.stats['start_time']
            throughput = self.stats['documents_inserted'] / duration if duration > 0 else 0
            
            # Close chunk file and get file size
            chunk_file_size = self.close_chunk_file()
            
            results = {
                'total_documents': self.config.total_records,
                'documents_generated': self.stats['documents_generated'],
                'documents_inserted': self.stats['documents_inserted'],
                'batches_processed': self.stats['batches_processed'],
                'duration_seconds': duration,
                'throughput_docs_per_second': throughput,
                'total_bytes_sent': self.total_bytes.get_total(),
                'total_chunk_bytes_sent': self.total_chunk_bytes.get_total(),
                'total_vector_bytes_sent': self.total_vector_bytes.get_total(),
                'total_metadata_bytes_sent': self.total_metadata_bytes.get_total(),
                'total_actual_data_bytes_sent': self.total_actual_data_bytes.get_total(),
                'total_chunk_bytes_utf8': self.total_bytes_utf8.get_total(),
                'total_chunk_bytes_utf16le': self.total_bytes_utf16le.get_total(),
                'total_chunk_bytes_utf16_bom': self.total_bytes_utf16_bom.get_total(),
                'chunk_file_size': chunk_file_size,
                'chunk_file_path': self.chunk_file_path,
                'errors': self.stats['errors'],
                'success': len(self.stats['errors']) == 0
            }
            
            logger.info(f"Benchmark completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Benchmark failed: {e}")
            self.stats['errors'].append(f"Benchmark orchestrator: {e}")
            raise
        
        finally:
            # Cleanup
            self.stop_event.set()
            self.astra_client.disconnect()
    
    def run_dry_run(self) -> Dict[str, Any]:
        """Run a dry-run benchmark that generates data but doesn't insert to database.
        
        Returns:
            Dictionary with benchmark results and statistics
        """
        logger.info(f"Starting dry-run benchmark: {self.config.total_records:,} documents, "
                   f"{self.config.num_threads} threads")
        
        self.stats['start_time'] = time.time()
        
        try:
            # Calculate documents per generator thread
            docs_per_generator = self.config.total_records // self.config.num_threads
            remaining_docs = self.config.total_records % self.config.num_threads
            
            # Start generator threads (no insert threads needed)
            generator_threads = []
            for i in range(self.config.num_threads):
                docs_for_thread = docs_per_generator + (1 if i < remaining_docs else 0)
                if docs_for_thread > 0:
                    thread = threading.Thread(
                        target=self.dry_run_worker,
                        args=(i, docs_for_thread),
                        name=f"Generator-{i}"
                    )
                    thread.start()
                    generator_threads.append(thread)
            
            # Wait for all generator threads to complete
            for thread in generator_threads:
                thread.join()
            
            # Set end time
            self.stats['end_time'] = time.time()
            
            # Calculate final statistics
            duration = self.stats['end_time'] - self.stats['start_time']
            throughput = self.stats['documents_generated'] / duration if duration > 0 else 0
            
            # Close chunk file and get file size
            chunk_file_size = self.close_chunk_file()
            
            results = {
                'total_documents': self.config.total_records,
                'documents_generated': self.stats['documents_generated'],
                'documents_inserted': 0,  # No insertions in dry-run
                'batches_processed': self.stats['documents_generated'],  # All documents processed
                'duration_seconds': duration,
                'throughput_docs_per_second': throughput,
                'total_bytes_sent': self.total_bytes.get_total(),
                'total_chunk_bytes_sent': self.total_chunk_bytes.get_total(),
                'total_vector_bytes_sent': self.total_vector_bytes.get_total(),
                'total_metadata_bytes_sent': self.total_metadata_bytes.get_total(),
                'total_actual_data_bytes_sent': self.total_actual_data_bytes.get_total(),
                'total_chunk_bytes_utf8': self.total_bytes_utf8.get_total(),
                'total_chunk_bytes_utf16le': self.total_bytes_utf16le.get_total(),
                'total_chunk_bytes_utf16_bom': self.total_bytes_utf16_bom.get_total(),
                'chunk_file_size': chunk_file_size,
                'chunk_file_path': self.chunk_file_path,
                'errors': self.stats['errors'],
                'success': len(self.stats['errors']) == 0,
                'dry_run': True
            }
            
            logger.info(f"Dry-run benchmark completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Dry-run benchmark failed: {e}")
            self.stats['errors'].append(f"Dry-run orchestrator: {e}")
            raise
    
    def run_simple_benchmark(self) -> Dict[str, Any]:
        """Run a simplified benchmark without complex threading.
        
        Returns:
            Dictionary with benchmark results and statistics
        """
        logger.info(f"Starting simple benchmark: {self.config.total_records:,} documents")
        
        self.stats['start_time'] = time.time()
        
        try:
            # Connect to AstraDB
            self.astra_client.connect()
            
            if not self.astra_client.test_connection():
                raise RuntimeError("Failed to connect to AstraDB")
            
            # Generate and insert documents in batches
            total_batches = (self.config.total_records + self.config.batch_size - 1) // self.config.batch_size
            
            for batch_num in range(total_batches):
                # Calculate batch size
                remaining_docs = self.config.total_records - self.stats['documents_generated']
                current_batch_size = min(self.config.batch_size, remaining_docs)
                
                if current_batch_size <= 0:
                    break
                
                # Generate batch
                batch = self.data_generator.generate_batch(current_batch_size)
                self.stats['documents_generated'] += len(batch)
                
                # Insert batch
                self.astra_client.insert_batch(batch)
                self.stats['documents_inserted'] += len(batch)
                self.stats['batches_processed'] += 1
                
                # Progress update
                if batch_num % 100 == 0 or batch_num == total_batches - 1:
                    progress = (batch_num + 1) / total_batches * 100
                    logger.info(f"Progress: {progress:.1f}% ({batch_num + 1}/{total_batches} batches)")
            
            self.stats['end_time'] = time.time()
            
            # Calculate final statistics
            duration = self.stats['end_time'] - self.stats['start_time']
            throughput = self.stats['documents_inserted'] / duration if duration > 0 else 0
            
            results = {
                'total_documents': self.config.total_records,
                'documents_generated': self.stats['documents_generated'],
                'documents_inserted': self.stats['documents_inserted'],
                'batches_processed': self.stats['batches_processed'],
                'duration_seconds': duration,
                'throughput_docs_per_second': throughput,
                'errors': self.stats['errors'],
                'success': len(self.stats['errors']) == 0
            }
            
            logger.info(f"Simple benchmark completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Simple benchmark failed: {e}")
            self.stats['errors'].append(f"Simple benchmark: {e}")
            raise
        
        finally:
            self.astra_client.disconnect()


def run_benchmark(config: Config, use_threading: bool = True) -> Dict[str, Any]:
    """Run the benchmark with the given configuration.
    
    Args:
        config: Configuration object
        use_threading: Whether to use multi-threading (default: True)
        
    Returns:
        Dictionary with benchmark results
    """
    orchestrator = BenchmarkOrchestrator(config)
    
    if use_threading and config.num_threads > 1:
        return orchestrator.run_benchmark()
    else:
        return orchestrator.run_simple_benchmark()


def run_dry_run(config: Config) -> Dict[str, Any]:
    """Run a dry-run benchmark that generates data but doesn't insert to database.
    
    Args:
        config: Configuration object
        
    Returns:
        Dictionary with benchmark results
    """
    orchestrator = BenchmarkOrchestrator(config)
    return orchestrator.run_dry_run()
