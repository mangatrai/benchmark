#!/usr/bin/env python3
"""
HCD Cassandra Benchmark Entry Point.
Main entry point for the HCD Cassandra benchmark application.
"""

import argparse
import logging
import sys
import os
from pathlib import Path

from config import Config
from benchmark import TableAPIBenchmark

# Configure logging (will be updated after config is loaded)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def print_results(results: dict, dry_run: bool = False) -> None:
    """Print benchmark results in a formatted way."""
    mode = "DRY RUN" if dry_run else "FULL BENCHMARK"
    
    print("=" * 80)
    print(f"HCD CASSANDRA BENCHMARK RESULTS")
    print("=" * 80)
    print(f"Mode: {mode} (Data Generation {'+ Database Insertion' if not dry_run else 'Only'})")
    print(f"Total Documents: {results['total_documents']:,}")
    print(f"Documents Generated: {results['documents_generated']:,}")
    if not dry_run:
        print(f"Documents Inserted: {results['documents_inserted']:,}")
    print(f"Duration: {results['duration_seconds']:.2f} seconds")
    print(f"Throughput: {results['throughput_docs_per_second']:.2f} docs/second")
    print()
    
    print("-" * 50)
    print("DATA SIZE ANALYSIS")
    print("-" * 50)
    print(f"Total JSON Payload Size: {format_bytes(results['total_bytes'])}")
    print(f"Actual Data Size: {format_bytes(results['total_actual_data_bytes'])}")
    print(f"  - Chunk Data: {format_bytes(results['total_chunk_bytes'])}")
    print(f"  - Vector Data: {format_bytes(results['total_vector_bytes'])}")
    print(f"  - Metadata: {format_bytes(results['total_metadata_bytes'])}")
    print()
    
    print("-" * 50)
    print("CHUNK ENCODING ANALYSIS")
    print("-" * 50)
    print(f"UTF-8 Encoding: {format_bytes(results['total_bytes_utf8'])}")
    print(f"UTF-16 LE Encoding: {format_bytes(results['total_bytes_utf16le'])}")
    print(f"UTF-16 BOM Encoding: {format_bytes(results['total_bytes_utf16_bom'])}")
    print()
    
    if results['chunk_file_path']:
        print("-" * 50)
        print("CHUNK FILE VERIFICATION")
        print("-" * 50)
        print(f"Chunk File: {results['chunk_file_path']}")
        print(f"File Size: {format_bytes(results['chunk_file_size'])}")
        print(f"Expected UTF-8 Size: {format_bytes(results['total_bytes_utf8'])}")
        size_match = "✓" if results['chunk_file_size'] == results['total_bytes_utf8'] else "✗"
        print(f"Size Match: {size_match}")
        print()
    
    if results['errors']:
        print("-" * 50)
        print("ERRORS")
        print("-" * 50)
        for error in results['errors']:
            print(f"- {error}")
        print()
    
    print("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='HCD Cassandra Benchmark')
    parser.add_argument('--records', type=int, default=None,
                       help='Number of records to generate and insert (overrides TOTAL_RECORDS from .env)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Run in dry-run mode (data generation only, no insertion)')
    parser.add_argument('--log-level', type=str, default=None,
                       help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) - overrides LOG_LEVEL from .env')
    parser.add_argument('--config', type=str, default='.env',
                       help='Path to environment configuration file (default: .env)')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = Config(env_file=args.config)
        
        # Configure logging based on config or command line override
        log_level_str = args.log_level if args.log_level is not None else config.log_level
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)
        logging.getLogger().setLevel(log_level)
        logger.info(f"Configuration loaded from {args.config}")
        logger.info(f"Log level set to: {log_level_str.upper()}")
        
        # Use command line records if provided, otherwise use config value
        total_records = args.records if args.records is not None else config.total_records
        
        # Create benchmark instance
        benchmark = TableAPIBenchmark(config)
        
        # Run benchmark
        if args.dry_run:
            logger.info(f"Starting HCD CQL dry run for {total_records} records")
            results = benchmark.run_dry_run(total_records)
        else:
            logger.info(f"Starting HCD CQL benchmark for {total_records} records")
            results = benchmark.run_benchmark(total_records)
        
        # Print results
        print_results(results, dry_run=args.dry_run)
        
        # Exit with error code if there were errors
        if results['errors']:
            logger.error(f"Benchmark completed with {len(results['errors'])} errors")
            sys.exit(1)
        else:
            logger.info("Benchmark completed successfully")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()