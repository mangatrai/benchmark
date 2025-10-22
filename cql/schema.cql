-- CQL Schema for AstraDB Vector Benchmarking
-- Table: sample_price_50 with balanced metadata fields

-- Main table for vector documents
CREATE TABLE IF NOT EXISTS sample_price_50 (
    -- Primary key
    id UUID PRIMARY KEY,
    
    -- Core content fields
    chunk TEXT,
    vector_column VECTOR<FLOAT, 1536>,
    
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
);

-- SAI Indexes for Vector Search and Metadata Filtering (Total: 10 indexes)
-- 1. Vector similarity index (cosine similarity)
CREATE CUSTOM INDEX sample_price_50_vector_idx ON sample_price_50 (vector_column) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'similarity_function': 'cosine'};

-- 2. Text search indexes (3 string fields)
CREATE CUSTOM INDEX sample_price_50_str_01_idx ON sample_price_50 (meta_str_01) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'index_analyzer': 'standard'};

CREATE CUSTOM INDEX sample_price_50_str_02_idx ON sample_price_50 (meta_str_02) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'index_analyzer': 'standard'};

CREATE CUSTOM INDEX sample_price_50_str_03_idx ON sample_price_50 (meta_str_03) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'index_analyzer': 'standard'};

-- 3. Numeric range indexes (2 numeric fields - mix of INT and DOUBLE)
CREATE CUSTOM INDEX sample_price_50_num_01_idx ON sample_price_50 (meta_num_01) 
USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX sample_price_50_num_02_idx ON sample_price_50 (meta_num_03) 
USING 'StorageAttachedIndex';

-- 4. Boolean indexes (2 boolean fields)
CREATE CUSTOM INDEX sample_price_50_bool_01_idx ON sample_price_50 (meta_bool_01) 
USING 'StorageAttachedIndex';

CREATE CUSTOM INDEX sample_price_50_bool_02_idx ON sample_price_50 (meta_bool_02) 
USING 'StorageAttachedIndex';

-- 5. Date range indexes (1 date field)
CREATE CUSTOM INDEX sample_price_50_date_01_idx ON sample_price_50 (meta_date_01) 
USING 'StorageAttachedIndex';

-- 6. Chunk text search index
CREATE CUSTOM INDEX sample_price_50_chunk_idx ON sample_price_50 (chunk) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'index_analyzer': 'standard'};
