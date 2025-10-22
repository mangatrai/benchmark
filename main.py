#!/usr/bin/env python3
"""
DataStax AstraDB Benchmarking Tool

Main entry point for the benchmarking application.
Generates and inserts large volumes of test data into DataStax AstraDB collections.
"""

import sys
import argparse
import logging
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import Config
from src.benchmark import run_benchmark, run_dry_run


def setup_argument_parser() -> argparse.ArgumentParser:
    """Setup command line argument parser.
    
    Returns:
        Configured ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description="DataStax AstraDB Benchmarking Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default configuration
  python main.py
  
  # Run with custom record count
  python main.py --records 10000000
  
  # Run with custom thread count
  python main.py --threads 8
  
  # Run with custom configuration file
  python main.py --env-file custom.env
  
  # Run without threading
  python main.py --no-threading
        """
    )
    
    parser.add_argument(
        '--env-file',
        type=str,
        help='Path to environment file (default: .env)'
    )
    
    parser.add_argument(
        '--records',
        type=int,
        help='Total number of records to generate and insert'
    )
    
    parser.add_argument(
        '--threads',
        type=int,
        help='Number of threads to use for processing'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Number of documents per batch'
    )
    
    parser.add_argument(
        '--no-threading',
        action='store_true',
        help='Disable multi-threading (use single-threaded mode)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Generate data but do not insert into database'
    )
    
    return parser


def print_banner():
    """Print application banner."""
    print("=" * 60)
    print("DataStax AstraDB Benchmarking Tool")
    print("=" * 60)
    print()


def print_results(results: dict):
    """Print benchmark results in a formatted way.
    
    Args:
        results: Dictionary containing benchmark results
    """
    print("\n" + "=" * 60)
    if results.get('dry_run', False):
        print("DRY RUN RESULTS")
    else:
        print("BENCHMARK RESULTS")
    print("=" * 60)
    
    print(f"Total Documents: {results['total_documents']:,}")
    print(f"Documents Generated: {results['documents_generated']:,}")
    if not results.get('dry_run', False):
        print(f"Documents Inserted: {results['documents_inserted']:,}")
    print(f"Batches Processed: {results['batches_processed']:,}")
    print(f"Duration: {results['duration_seconds']:.2f} seconds")
    print(f"Throughput: {results['throughput_docs_per_second']:.2f} docs/second")
    print(f"Total Data Sent: {results['total_bytes_sent']:,} bytes ({results['total_bytes_sent']/1024/1024:.2f} MB)")
    print(f"Total Chunk Data Sent: {results['total_chunk_bytes_sent']:,} bytes ({results['total_chunk_bytes_sent']/1024/1024:.2f} MB)")
    print(f"Total Vector Data Sent: {results['total_vector_bytes_sent']:,} bytes ({results['total_vector_bytes_sent']/1024/1024:.2f} MB)")
    print(f"Total Metadata Data Sent: {results['total_metadata_bytes_sent']:,} bytes ({results['total_metadata_bytes_sent']/1024/1024:.2f} MB)")
    print(f"Total Actual Data Sent: {results['total_actual_data_bytes_sent']:,} bytes ({results['total_actual_data_bytes_sent']/1024/1024:.2f} MB)")
    
    # Multiple encoding byte sizes
    print(f"\nChunk Encoding Analysis:")
    print(f"  UTF-8: {results['total_chunk_bytes_utf8']:,} bytes ({results['total_chunk_bytes_utf8']/1024/1024:.2f} MB)")
    print(f"  UTF-16 LE: {results['total_chunk_bytes_utf16le']:,} bytes ({results['total_chunk_bytes_utf16le']/1024/1024:.2f} MB)")
    print(f"  UTF-16 BOM: {results['total_chunk_bytes_utf16_bom']:,} bytes ({results['total_chunk_bytes_utf16_bom']/1024/1024:.2f} MB)")
    
    # File verification
    if 'chunk_file_size' in results and results['chunk_file_size'] > 0:
        print(f"\nFile Verification:")
        print(f"  Chunk file: {results['chunk_file_path']}")
        print(f"  File size: {results['chunk_file_size']:,} bytes ({results['chunk_file_size']/1024/1024:.2f} MB)")
        print(f"  Expected UTF-8: {results['total_chunk_bytes_utf8']:,} bytes")
        print(f"  Difference: {results['chunk_file_size'] - results['total_chunk_bytes_utf8']:,} bytes")
    
    if results['errors']:
        print(f"\nErrors ({len(results['errors'])}):")
        for error in results['errors']:
            print(f"  - {error}")
    else:
        print("\nStatus: SUCCESS - No errors encountered")
    
    print("=" * 60)


def main():
    """Main entry point for the application."""
    print_banner()
    
    # Parse command line arguments
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = Config(env_file=args.env_file)
        
        # Override configuration with command line arguments
        if args.records:
            config.total_records = args.records
        if args.threads:
            config.num_threads = args.threads
        if args.batch_size:
            config.batch_size = args.batch_size
        if args.log_level:
            config.log_level = args.log_level
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, config.log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('benchmark.log')
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info(f"Starting benchmark with configuration: {config}")
        
        # Print configuration
        print(f"Configuration:")
        print(f"  Total Records: {config.total_records:,}")
        print(f"  Threads: {config.num_threads}")
        print(f"  Batch Size: {config.batch_size}")
        print(f"  Namespace: {config.astra_namespace}")
        print(f"  Collection: {config.astra_collection}")
        print(f"  Use Threading: {not args.no_threading}")
        print()
        
        if args.dry_run:
            print("DRY RUN MODE - Data will be generated but not inserted")
            print("=" * 60)
            results = run_dry_run(config)
        else:
            # Run benchmark
            use_threading = not args.no_threading
            results = run_benchmark(config, use_threading=use_threading)
        
        # Print results
        print_results(results)
        
        # Exit with appropriate code
        sys.exit(0 if results['success'] else 1)
        
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        logging.error(f"Benchmark failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
