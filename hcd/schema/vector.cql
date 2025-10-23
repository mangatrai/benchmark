-- CQL Schema for AstraDB Vector Benchmarking
-- Table: sample_price_50 with balanced metadata fields

-- Main table for vector documents
CREATE TABLE IF NOT EXISTS price_vector.price_vector_50 (
    -- Primary key
    id UUID PRIMARY KEY,
    
    vector_column VECTOR<FLOAT, 1536>,
) WITH compaction = {
    'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy',
    'num_shards': '32',
    'static_scaling_factors': '4,2,-8',
    'min_sstable_size_in_mb': '100',
    'max_space_overhead': '0.20'
};

CREATE CUSTOM INDEX price_vector_10_vector_idx ON price_vector.price_vector_10 (vector_column) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'similarity_function': 'cosine'};

-- ALTER STATEMENT
ALTER TABLE price_vector.price_vector_10
WITH compaction = {
    'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy',
    'num_shards': '32',
    'static_scaling_factors': '4,2,-8',
    'min_sstable_size_in_mb': '100',
    'max_space_overhead': '0.20'
};