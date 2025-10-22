"""
CQL-based AstraDB benchmark main script.
High-performance vector operations using Cassandra driver.
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cql.benchmark import CQLBenchmark
from src.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_results(results: dict, is_dry_run: bool = False):
    """Print benchmark results in a formatted way."""
    print("\n" + "="*80)
    print("CQL ASTRA DB BENCHMARK RESULTS")
    print("="*80)
    
    if is_dry_run:
        print(f"Mode: DRY RUN (Data Generation Only)")
    else:
        print(f"Mode: FULL BENCHMARK (Data Generation + Database Insertion)")
    
    print(f"Total Documents: {results['total_documents']:,}")
    print(f"Documents Generated: {results['documents_generated']:,}")
    
    if not is_dry_run:
        print(f"Documents Inserted: {results['documents_inserted']:,}")
    
    print(f"Duration: {results['duration_seconds']:.2f} seconds")
    print(f"Throughput: {results['throughput_docs_per_second']:.2f} docs/second")
    
    print("\n" + "-"*50)
    print("DATA SIZE ANALYSIS")
    print("-"*50)
    print(f"Total JSON Payload Size: {results['total_bytes']:,} bytes ({results['total_bytes']/1024/1024:.2f} MB)")
    print(f"Actual Data Size: {results['total_actual_data_bytes']:,} bytes ({results['total_actual_data_bytes']/1024/1024:.2f} MB)")
    print(f"  - Chunk Data: {results['total_chunk_bytes']:,} bytes ({results['total_chunk_bytes']/1024/1024:.2f} MB)")
    print(f"  - Vector Data: {results['total_vector_bytes']:,} bytes ({results['total_vector_bytes']/1024/1024:.2f} MB)")
    print(f"  - Metadata: {results['total_metadata_bytes']:,} bytes ({results['total_metadata_bytes']/1024/1024:.2f} MB)")
    
    print("\n" + "-"*50)
    print("CHUNK ENCODING ANALYSIS")
    print("-"*50)
    print(f"UTF-8 Encoding: {results['total_bytes_utf8']:,} bytes ({results['total_bytes_utf8']/1024/1024:.2f} MB)")
    print(f"UTF-16 LE Encoding: {results['total_bytes_utf16le']:,} bytes ({results['total_bytes_utf16le']/1024/1024:.2f} MB)")
    print(f"UTF-16 BOM Encoding: {results['total_bytes_utf16_bom']:,} bytes ({results['total_bytes_utf16_bom']/1024/1024:.2f} MB)")
    
    if results.get('chunk_file_path'):
        print("\n" + "-"*50)
        print("CHUNK FILE VERIFICATION")
        print("-"*50)
        print(f"Chunk File: {results['chunk_file_path']}")
        print(f"File Size: {results['chunk_file_size']:,} bytes ({results['chunk_file_size']/1024/1024:.2f} MB)")
        print(f"Expected UTF-8 Size: {results['total_bytes_utf8']:,} bytes")
        print(f"Size Match: {'✓' if results['chunk_file_size'] == results['total_bytes_utf8'] else '✗'}")
    
    if results.get('errors'):
        print("\n" + "-"*50)
        print("ERRORS")
        print("-"*50)
        for error in results['errors']:
            print(f"  - {error}")
    
    print("\n" + "="*80)


def main():
    """Main function for CQL benchmark."""
    parser = argparse.ArgumentParser(description='CQL-based AstraDB Vector Benchmark')
    parser.add_argument('--records', type=int, default=None, help='Total number of records to generate (overrides TOTAL_RECORDS from .env)')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no database insertion)')
    parser.add_argument('--config', type=str, default='.env', help='Configuration file path')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = Config(env_file=args.config)
        
        # Use command line records if provided, otherwise use config value
        total_records = args.records if args.records is not None else config.total_records
        
        # Create benchmark instance
        benchmark = CQLBenchmark(config)
        
        # Run benchmark
        if args.dry_run:
            logger.info(f"Starting CQL dry run for {total_records} records")
            results = benchmark.run_dry_run(total_records)
        else:
            logger.info(f"Starting CQL benchmark for {total_records} records")
            results = benchmark.run_benchmark(total_records)
        
        # Print results
        print_results(results, is_dry_run=args.dry_run)
        
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
