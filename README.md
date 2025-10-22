# DataStax AstraDB Benchmarking Tool

A high-performance tool for generating and inserting large volumes of test data into DataStax AstraDB collections with configurable threading and batch processing.

## Features

- **Scalable Data Generation**: Generate millions of documents with realistic data structures
- **Multi-threaded Processing**: Configurable thread count for parallel data generation and insertion
- **Batch Processing**: Efficient batch insertion with configurable batch sizes
- **Flexible Configuration**: Environment-based configuration with command-line overrides
- **Performance Metrics**: Detailed throughput and timing statistics
- **Error Handling**: Robust error handling with retry logic

## Data Structure

The tool generates documents matching this structure:

```json
{
  "_id": "uuid-string",
  "chunk": "512-character-base64-like-string",
  "embedding": [1536 float values],
  "metadata": {
    "meta_str_01": "random-string",
    "meta_str_02": "random-string",
    // ... 38 string fields
    "meta_num_01": 1234567,
    "meta_num_02": 2345678,
    // ... 19 numeric fields
    "meta_date_01": "2023-10-25T20:12:49",
    "meta_date_02": "2021-01-17T20:12:49",
    // ... 7 date fields
  }
}
```

## Installation

1. Clone or download the project
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file from the template:
   ```bash
   cp env.template .env
   ```

4. Configure your AstraDB credentials in `.env`:
   ```env
   ASTRA_DB_APPLICATION_TOKEN=your_token_here
   ASTRA_DB_API_ENDPOINT=https://your-database-id-your-region.apps.astra.datastax.com
   ASTRA_DB_NAMESPACE=your_namespace
   ASTRA_DB_COLLECTION=your_collection
   ```

## Usage

### Basic Usage

```bash
# Run with default configuration (1M records, 4 threads)
python main.py

# Run with custom record count
python main.py --records 10000000

# Run with custom thread count
python main.py --threads 8

# Run without threading (single-threaded)
python main.py --no-threading
```

### Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `TOTAL_RECORDS` | 1000000 | Total number of records to generate |
| `NUM_THREADS` | 4 | Number of threads for processing |
| `BATCH_SIZE` | 100 | Documents per batch |
| `LOG_LEVEL` | INFO | Logging level |
| `MAX_RETRIES` | 3 | Maximum retry attempts |
| `RETRY_DELAY` | 1.0 | Delay between retries (seconds) |

### Command Line Options

```bash
python main.py --help
```

Available options:
- `--env-file`: Path to environment file
- `--records`: Total number of records
- `--threads`: Number of threads
- `--batch-size`: Batch size
- `--no-threading`: Disable multi-threading
- `--log-level`: Set logging level
- `--dry-run`: Generate data but don't insert

## Performance Tuning

### Thread Count
- Start with 4-8 threads for most use cases
- Monitor CPU and memory usage
- More threads don't always mean better performance

### Batch Size
- Default 100 documents per batch works well
- Larger batches reduce API calls but use more memory
- Smaller batches provide better progress feedback

### Memory Considerations
- Each document is approximately 8-10KB
- 100M documents ≈ 800GB-1TB of data
- Monitor memory usage during large runs

## Example Output

```
============================================================
DataStax AstraDB Benchmarking Tool
============================================================

Configuration:
  Total Records: 1,000,000
  Threads: 4
  Batch Size: 100
  Namespace: default_keyspace
  Collection: benchmark_collection
  Use Threading: True

[2024-01-15 10:30:15] INFO - Starting benchmark: 1,000,000 documents, 4 threads
[2024-01-15 10:30:20] INFO - Progress: 10.0% (1000/10000 batches)
[2024-01-15 10:30:25] INFO - Progress: 20.0% (2000/10000 batches)
...
[2024-01-15 10:35:30] INFO - Benchmark completed

============================================================
BENCHMARK RESULTS
============================================================
Total Documents: 1,000,000
Documents Generated: 1,000,000
Documents Inserted: 1,000,000
Batches Processed: 10,000
Duration: 315.45 seconds
Throughput: 3,169.23 docs/second

Status: SUCCESS - No errors encountered
============================================================
```

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify AstraDB credentials
   - Check network connectivity
   - Ensure database is accessible

2. **Memory Issues**
   - Reduce batch size
   - Use fewer threads
   - Monitor system resources

3. **Performance Issues**
   - Adjust thread count
   - Optimize batch size
   - Check database performance

### Logs

The application creates detailed logs in `benchmark.log` for troubleshooting.

## Architecture

```
src/
├── __init__.py          # Package initialization
├── config.py            # Configuration management
├── data_generator.py    # Random data generation
├── astra_client.py      # AstraDB operations
└── benchmark.py         # Main orchestration

main.py                  # Entry point
requirements.txt         # Dependencies
env.template            # Configuration template
README.md               # This file
```

## License

This tool is provided as-is for benchmarking and testing purposes.
