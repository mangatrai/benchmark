-- CQL Schema for AstraDB Vector Benchmarking
-- Table: sample_price_50 with balanced metadata fields

-- Main table for data documents
CREATE TABLE IF NOT EXISTS price_data.price_data_50 (
    -- Primary key
    id UUID PRIMARY KEY,
    
    -- Core content fields
    chunk TEXT,
    
    -- String metadata fields (20 fields)
    meta_str_01 TEXT,
    meta_str_02 TEXT,
    meta_str_03 TEXT,
    meta_str_04 TEXT,
    meta_str_05 TEXT,
    meta_str_06 TEXT,
    meta_str_07 TEXT,
    meta_str_08 TEXT,
    meta_str_09 TEXT,
    meta_str_10 TEXT,
    meta_str_11 TEXT,
    meta_str_12 TEXT,
    meta_str_13 TEXT,
    meta_str_14 TEXT,
    meta_str_15 TEXT,
    meta_str_16 TEXT,
    meta_str_17 TEXT,
    meta_str_18 TEXT,
    meta_str_19 TEXT,
    meta_str_20 TEXT,
    
    -- Numeric metadata fields (20 fields) - mix of INT and DOUBLE
    meta_num_01 INT,
    meta_num_02 INT,
    meta_num_03 DOUBLE,
    meta_num_04 INT,
    meta_num_05 DOUBLE,
    meta_num_06 INT,
    meta_num_07 DOUBLE,
    meta_num_08 INT,
    meta_num_09 DOUBLE,
    meta_num_10 INT,
    meta_num_11 DOUBLE,
    meta_num_12 INT,
    meta_num_13 DOUBLE,
    meta_num_14 INT,
    meta_num_15 DOUBLE,
    meta_num_16 INT,
    meta_num_17 DOUBLE,
    meta_num_18 INT,
    meta_num_19 DOUBLE,
    meta_num_20 INT,
    
    -- Boolean metadata fields (12 fields)
    meta_bool_01 BOOLEAN,
    meta_bool_02 BOOLEAN,
    meta_bool_03 BOOLEAN,
    meta_bool_04 BOOLEAN,
    meta_bool_05 BOOLEAN,
    meta_bool_06 BOOLEAN,
    meta_bool_07 BOOLEAN,
    meta_bool_08 BOOLEAN,
    meta_bool_09 BOOLEAN,
    meta_bool_10 BOOLEAN,
    meta_bool_11 BOOLEAN,
    meta_bool_12 BOOLEAN,
    
    -- Date metadata fields (12 fields)
    meta_date_01 TIMESTAMP,
    meta_date_02 TIMESTAMP,
    meta_date_03 TIMESTAMP,
    meta_date_04 TIMESTAMP,
    meta_date_05 TIMESTAMP,
    meta_date_06 TIMESTAMP,
    meta_date_07 TIMESTAMP,
    meta_date_08 TIMESTAMP,
    meta_date_09 TIMESTAMP,
    meta_date_10 TIMESTAMP,
    meta_date_11 TIMESTAMP,
    meta_date_12 TIMESTAMP,
    
    -- Additional fields for enhanced indexing
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH compaction = {
    'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy',
    'num_shards': '16',
    'static_scaling_factors': '2,-8',
    'min_sstable_size_in_mb': '100',
    'max_space_overhead': '0.20'
};

-- ALTER STATEMENT
ALTER TABLE price_data.price_data_10
WITH compaction = {
    'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy',
    'num_shards': '16',
    'static_scaling_factors': '2,-8',
    'min_sstable_size_in_mb': '100',
    'max_space_overhead': '0.20'
};