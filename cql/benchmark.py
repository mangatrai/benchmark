"""
CQL-based AstraDB benchmark using Cassandra driver.
High-performance vector operations with CQL instead of DataAPI.
"""

import os
import sys
import time
import logging
import threading
import queue
import csv
import psutil
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cql.cql_client import CQLClient
from cql.data_generator import CQLDataGenerator
from src.config import Config

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


class ThreadSafeCSVWriter:
    """Thread-safe CSV writer for progress tracking."""
    
    def __init__(self, filepath: str):
        """Initialize CSV writer.
        
        Args:
            filepath: Path to CSV file
        """
        self.filepath = filepath
        self._lock = threading.Lock()
        self._initialized = False
    
    def _ensure_header(self):
        """Ensure CSV file has proper header."""
        if not self._initialized:
            with self._lock:
                if not os.path.exists(self.filepath):
                    with open(self.filepath, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            'timestamp', 'documents_generated', 'documents_inserted', 'errors',
                            'chunk_bytes', 'vector_bytes', 'metadata_bytes', 'total_bytes',
                            'throughput_docs_per_sec', 'duration_seconds', 'chunk_file_size',
                            'memory_mb', 'status', 'error_message'
                        ])
                self._initialized = True
    
    def write_checkpoint(self, documents_generated: int, documents_inserted: int, 
                        errors: int, chunk_bytes: int, vector_bytes: int, 
                        metadata_bytes: int, total_bytes: int, throughput: float,
                        duration: float, chunk_file_size: int, memory_mb: int,
                        status: str, error_message: str = ""):
        """Write checkpoint to CSV file.
        
        Args:
            documents_generated: Number of documents generated
            documents_inserted: Number of documents inserted
            errors: Number of errors encountered
            chunk_bytes: Total chunk data bytes
            vector_bytes: Total vector data bytes
            metadata_bytes: Total metadata bytes
            total_bytes: Total JSON payload bytes
            throughput: Documents per second
            duration: Duration in seconds
            chunk_file_size: Chunk file size in bytes
            memory_mb: Current memory usage in MB
            status: Status (SUCCESS, ERROR, etc.)
            error_message: Error message if any
        """
        self._ensure_header()
        
        with self._lock:
            with open(self.filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.now().isoformat(),
                    documents_generated,
                    documents_inserted,
                    errors,
                    chunk_bytes,
                    vector_bytes,
                    metadata_bytes,
                    total_bytes,
                    round(throughput, 2),
                    round(duration, 2),
                    chunk_file_size,
                    memory_mb,
                    status,
                    error_message
                ])


class CQLBenchmark:
    """CQL-based benchmark for AstraDB vector operations."""
    
    def __init__(self, config: Config):
        """Initialize the CQL benchmark.
        
        Args:
            config: Configuration object with AstraDB settings
        """
        self.config = config
        self.cql_client = None
        self.data_generator = CQLDataGenerator(
            chunk_size=config.chunk_size,
            embedding_dim=config.embedding_dimension
        )
        
        # Threading and queue management
        self.document_queue = queue.Queue(maxsize=config.queue_size)
        self.stop_event = threading.Event()
        
        # Statistics tracking
        self.stats = {
            'documents_generated': 0,
            'documents_inserted': 0,
            'errors': [],
            'start_time': None,
            'end_time': None
        }
        
        # Byte size tracking with thread-safe counters
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
        self.chunk_file_lock = threading.Lock()
        
        # CSV progress tracking
        self.csv_writer = ThreadSafeCSVWriter('progress.csv')
        
        logger.info("CQLBenchmark initialized")
    
    def _write_checkpoint(self, status: str, error_message: str = ""):
        """Write checkpoint to CSV file.
        
        Args:
            status: Status (SUCCESS, ERROR, etc.)
            error_message: Error message if any
        """
        try:
            duration = time.time() - self.stats['start_time'] if self.stats['start_time'] else 0
            throughput = self.stats['documents_inserted'] / duration if duration > 0 else 0
            
            chunk_file_size = 0
            if self.chunk_file_path and os.path.exists(self.chunk_file_path):
                chunk_file_size = os.path.getsize(self.chunk_file_path)
            
            # Get current memory usage
            memory_mb = int(psutil.Process().memory_info().rss / 1024 / 1024)
            
            # Truncate error message to avoid massive stack traces in CSV
            if error_message:
                # Extract just the error type and first line, max 100 chars
                error_summary = error_message.split('\n')[0][:100]
                if len(error_message) > 100:
                    error_summary += "..."
            else:
                error_summary = ""

            self.csv_writer.write_checkpoint(
                documents_generated=self.stats['documents_generated'],
                documents_inserted=self.stats['documents_inserted'],
                errors=len(self.stats['errors']),
                chunk_bytes=self.total_chunk_bytes.get_total(),
                vector_bytes=self.total_vector_bytes.get_total(),
                metadata_bytes=self.total_metadata_bytes.get_total(),
                total_bytes=self.total_bytes.get_total(),
                throughput=throughput,
                duration=duration,
                chunk_file_size=chunk_file_size,
                memory_mb=memory_mb,
                status=status,
                error_message=error_summary
            )
        except Exception as e:
            logger.error(f"Failed to write checkpoint: {e}")
    
    def setup_chunk_file(self):
        """Setup chunk file for verification."""
        try:
            os.makedirs("data", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.chunk_file_path = f"data/chunks_{timestamp}.txt"
            self.chunk_file_handle = open(self.chunk_file_path, "w", encoding="utf-8")
            logger.info(f"Chunk file created: {self.chunk_file_path}")
        except Exception as e:
            logger.error(f"Failed to create chunk file: {e}")
    
    def write_chunk_to_file(self, chunk: str):
        """Write chunk to file for verification (thread-safe, memory-safe)."""
        if self.chunk_file_handle:
            with self.chunk_file_lock:
                self.chunk_file_handle.write(chunk)
                self.chunk_file_handle.flush()
    
    def close_chunk_file(self):
        """Close chunk file."""
        if self.chunk_file_handle:
            self.chunk_file_handle.close()
            self.chunk_file_handle = None
    
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
                # Generate batch of documents (or single document if batch_size=1)
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
                    json_str = json.dumps(document, default=str)
                    batch_bytes += len(json_str.encode('utf-8'))
                    
                    # Actual data bytes (what gets stored in database)
                    chunk_bytes = len(document['chunk'].encode('utf-16le'))
                    vector_bytes = len(document['vector_column']) * 8  # 8 bytes per float
                    metadata_bytes = sum(len(str(v).encode('utf-8')) for v in document.values() 
                                       if v not in ['id', 'chunk', 'vector_column', 'created_at', 'updated_at'])
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
                    total_generated = self.stats['documents_generated']
                    if total_generated % self.config.checkpoint_interval == 0:
                        logger.info(f"Total documents generated: {total_generated:,}")
                        # Write checkpoint at configured interval
                        self._write_checkpoint("SUCCESS")
                
                # Log progress every 1000 documents
                if documents_generated % 1000 == 0:
                    logger.info(f"Generator worker {worker_id} generated {documents_generated}/{total_documents} documents")
        
        except Exception as e:
            logger.error(f"Generator worker {worker_id} failed: {e}")
            with threading.Lock():
                self.stats['errors'].append(f"Generator worker {worker_id}: {e}")
                # Write error checkpoint
                self._write_checkpoint("ERROR", str(e))
        
        logger.info(f"Generator worker {worker_id} completed, generated {documents_generated} documents")
    
    def insert_worker(self, worker_id: int) -> None:
        """Worker thread for inserting documents into AstraDB.
        
        Args:
            worker_id: Unique identifier for this worker
        """
        logger.info(f"Insert worker {worker_id} started")
        
        documents_inserted = 0
        
        try:
            while not self.stop_event.is_set():
                try:
                    # Get batch from queue with timeout
                    batch = self.document_queue.get(timeout=5.0)
                    
                    # Insert batch into AstraDB with retry logic
                    self.cql_client.insert_batch(batch, max_retries=self.config.max_retries, retry_delay=self.config.retry_delay)
                    
                    documents_inserted += len(batch)
                    
                    # Update stats
                    with threading.Lock():
                        self.stats['documents_inserted'] += len(batch)
                        total_inserted = self.stats['documents_inserted']
                        if total_inserted % self.config.checkpoint_interval == 0:
                            logger.info(f"Total documents inserted: {total_inserted:,}")
                    
                    # Log progress every 1000 documents
                    if documents_inserted % 1000 == 0:
                        logger.info(f"Insert worker {worker_id} inserted {documents_inserted} documents")
                    
                    # Mark task as done
                    self.document_queue.task_done()
                    
                except queue.Empty:
                    # Check if we should stop
                    if self.stop_event.is_set():
                        break
                    # No more documents to process, continue waiting
                    continue
                except Exception as e:
                    logger.error(f"Insert worker {worker_id} failed: {e}")
                    with threading.Lock():
                        self.stats['errors'].append(f"Insert worker {worker_id}: {e}")
                        # Write error checkpoint
                        self._write_checkpoint("ERROR", str(e))
                    # Don't break - continue processing other batches
                    # Mark task as done even if it failed
                    self.document_queue.task_done()
                    continue
        
        except Exception as e:
            logger.error(f"Insert worker {worker_id} failed: {e}")
            with threading.Lock():
                self.stats['errors'].append(f"Insert worker {worker_id}: {e}")
        
        logger.info(f"Insert worker {worker_id} completed, inserted {documents_inserted} documents")
    
    def run_benchmark(self, total_documents: int) -> Dict[str, Any]:
        """Run the CQL benchmark.
        
        Args:
            total_documents: Total number of documents to generate and insert
            
        Returns:
            Dictionary with benchmark results
        """
        logger.info(f"Starting CQL benchmark for {total_documents} documents")
        
        try:
            # Setup chunk file
            self.setup_chunk_file()
            
            # Initialize CQL client
            self.cql_client = CQLClient(
                username=self.config.astra_username,
                password=self.config.astra_password,
                secure_connect_bundle=self.config.astra_secure_connect_bundle_path,
                keyspace=self.config.astra_keyspace,
                table_name=self.config.astra_table
            )
            
            # Connect to AstraDB
            self.cql_client.connect()
            
            # Start timing
            self.stats['start_time'] = time.time()
            
            # Calculate documents per generator worker with proper distribution
            base_docs_per_thread = total_documents // self.config.generator_threads
            extra_docs = total_documents % self.config.generator_threads
            
            # Start generator threads
            generator_threads = []
            for i in range(self.config.generator_threads):
                # Give extra documents to the first few threads
                docs_for_this_thread = base_docs_per_thread + (1 if i < extra_docs else 0)
                thread = threading.Thread(
                    target=self.generate_worker,
                    args=(i, docs_for_this_thread)
                )
                thread.start()
                generator_threads.append(thread)
            
            # Start insert threads
            insert_threads = []
            for i in range(self.config.insert_threads):
                thread = threading.Thread(
                    target=self.insert_worker,
                    args=(i,)
                )
                thread.start()
                insert_threads.append(thread)
            
            # Wait for all generator threads to complete
            for thread in generator_threads:
                thread.join()
            
            # Wait for queue to be empty before signaling stop
            while not self.document_queue.empty():
                time.sleep(0.1)
            
            # Signal insert workers that no more documents will be generated
            self.stop_event.set()
            
            # Wait for all insert threads to complete with timeout
            for thread in insert_threads:
                thread.join(timeout=30.0)  # 30 second timeout per thread
                if thread.is_alive():
                    logger.warning(f"Insert thread {thread.name} did not complete within timeout")
            
            # Stop timing
            self.stats['end_time'] = time.time()
            
            # Close chunk file
            self.close_chunk_file()
            
            # Calculate results
            duration = self.stats['end_time'] - self.stats['start_time']
            throughput = self.stats['documents_inserted'] / duration if duration > 0 else 0
            
            results = {
                'total_documents': total_documents,
                'documents_generated': self.stats['documents_generated'],
                'documents_inserted': self.stats['documents_inserted'],
                'duration_seconds': duration,
                'throughput_docs_per_second': throughput,
                'errors': self.stats['errors'],
                'total_bytes': self.total_bytes.get_total(),
                'total_chunk_bytes': self.total_chunk_bytes.get_total(),
                'total_vector_bytes': self.total_vector_bytes.get_total(),
                'total_metadata_bytes': self.total_metadata_bytes.get_total(),
                'total_actual_data_bytes': self.total_actual_data_bytes.get_total(),
                'total_bytes_utf8': self.total_bytes_utf8.get_total(),
                'total_bytes_utf16le': self.total_bytes_utf16le.get_total(),
                'total_bytes_utf16_bom': self.total_bytes_utf16_bom.get_total(),
                'chunk_file_path': self.chunk_file_path,
                'chunk_file_size': os.path.getsize(self.chunk_file_path) if self.chunk_file_path and os.path.exists(self.chunk_file_path) else 0
            }
            
            # Write final checkpoint
            self._write_checkpoint("COMPLETED")
            
            logger.info(f"CQL benchmark completed: {results['documents_inserted']} documents in {duration:.2f}s ({throughput:.2f} docs/s)")
            return results
            
        except Exception as e:
            logger.error(f"CQL benchmark failed: {e}")
            raise
        finally:
            # Cleanup
            if self.cql_client:
                self.cql_client.disconnect()
    
    def run_dry_run(self, total_documents: int) -> Dict[str, Any]:
        """Run a dry run to generate data and calculate sizes without database insertion.
        
        Args:
            total_documents: Total number of documents to generate
            
        Returns:
            Dictionary with dry run results
        """
        logger.info(f"Starting CQL dry run for {total_documents} documents")
        
        try:
            # Setup chunk file
            self.setup_chunk_file()
            
            # Start timing
            self.stats['start_time'] = time.time()
            
            # Calculate documents per generator worker with proper distribution
            base_docs_per_thread = total_documents // self.config.generator_threads
            extra_docs = total_documents % self.config.generator_threads
            
            # Start generator threads
            generator_threads = []
            for i in range(self.config.generator_threads):
                # Give extra documents to the first few threads
                docs_for_this_thread = base_docs_per_thread + (1 if i < extra_docs else 0)
                thread = threading.Thread(
                    target=self.generate_worker,
                    args=(i, docs_for_this_thread)
                )
                thread.start()
                generator_threads.append(thread)
            
            # Wait for all generator threads to complete
            for thread in generator_threads:
                thread.join()
            
            # Stop timing
            self.stats['end_time'] = time.time()
            
            # Close chunk file
            self.close_chunk_file()
            
            # Calculate results
            duration = self.stats['end_time'] - self.stats['start_time']
            throughput = self.stats['documents_generated'] / duration if duration > 0 else 0
            
            results = {
                'total_documents': total_documents,
                'documents_generated': self.stats['documents_generated'],
                'duration_seconds': duration,
                'throughput_docs_per_second': throughput,
                'total_bytes': self.total_bytes.get_total(),
                'total_chunk_bytes': self.total_chunk_bytes.get_total(),
                'total_vector_bytes': self.total_vector_bytes.get_total(),
                'total_metadata_bytes': self.total_metadata_bytes.get_total(),
                'total_actual_data_bytes': self.total_actual_data_bytes.get_total(),
                'total_bytes_utf8': self.total_bytes_utf8.get_total(),
                'total_bytes_utf16le': self.total_bytes_utf16le.get_total(),
                'total_bytes_utf16_bom': self.total_bytes_utf16_bom.get_total(),
                'chunk_file_path': self.chunk_file_path,
                'chunk_file_size': os.path.getsize(self.chunk_file_path) if self.chunk_file_path and os.path.exists(self.chunk_file_path) else 0
            }
            
            logger.info(f"CQL dry run completed: {results['documents_generated']} documents in {duration:.2f}s ({throughput:.2f} docs/s)")
            return results
            
        except Exception as e:
            logger.error(f"CQL dry run failed: {e}")
            raise
        finally:
            # Cleanup
            self.close_chunk_file()
