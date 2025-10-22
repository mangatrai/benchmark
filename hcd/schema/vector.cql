-- CQL Schema for AstraDB Vector Benchmarking
-- Table: sample_price_50 with balanced metadata fields

-- Main table for vector documents
CREATE TABLE IF NOT EXISTS price_vector_50 (
    -- Primary key
    id UUID PRIMARY KEY,
    
    vector_column VECTOR<FLOAT, 1536>,
);

-- SAI Indexes for Vector Search and Metadata Filtering (Total: 10 indexes)
-- 1. Vector similarity index (cosine similarity)
CREATE CUSTOM INDEX price_vector_50_vector_idx ON price_vector_50 (vector_column) 
USING 'StorageAttachedIndex' 
WITH OPTIONS = {'similarity_function': 'cosine'};
