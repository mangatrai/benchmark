"""
Table API Data Generator for AstraDB benchmarking.
Generates data matching the sample_price_1 table structure for Table API.
"""

import uuid
import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import logging
from faker import Faker
from langchain_core.embeddings.fake import FakeEmbeddings

logger = logging.getLogger(__name__)


class TableAPIDataGenerator:
    """Generates random data matching the sample_price_1 table structure."""
    
    def __init__(self, chunk_size: int = 512, embedding_dim: int = 1536):
        """Initialize the Table API data generator.
        
        Args:
            chunk_size: Length of the chunk string (default: 512)
            embedding_dim: Dimension of the embedding vector (default: 1536)
        """
        self.chunk_size = chunk_size
        self.embedding_dim = embedding_dim
        
        # Initialize Faker for diverse text generation
        self.fake = Faker()
        
        # Initialize LangChain FakeEmbeddings for realistic vector generation
        self.fake_embeddings = FakeEmbeddings(size=self.embedding_dim)
        
        logger.info(f"TableAPIDataGenerator initialized: chunk_size={chunk_size}, embedding_dim={embedding_dim}")
    
    def generate_uuid(self) -> str:
        """Generate a random UUID string for Table API."""
        return str(uuid.uuid4())
    
    def generate_chunk(self) -> str:
        """Generate human-readable text of exact length with 20–30% specials/emojis."""
        # Single-codepoint pools to avoid ZWJ/grapheme slicing drama
        currency = "€£¥$¢₹₽₩₪₫₱₡₨₴₸₺₼₾₿"
        mathsy = "∑π∆±×÷≈≠≤≥∞∂∇∫∮∝"
        emojis = "😀🚀💻📊💰🎯🔥💡⭐🌟🎉📈💼🔧⚡🎨📱"
        punct = "—–•·"  # en/em dash, bullets
        specials_pool = currency + mathsy + emojis + punct

        # 1) Build a varied Faker base
        text_sources = [
            lambda: self.fake.paragraph(nb_sentences=random.randint(3, 8)),
            lambda: self.fake.text(max_nb_chars=self.chunk_size * 2),
            lambda: self.fake.sentence(nb_words=random.randint(15, 30)),
            lambda: self.fake.catch_phrase() + ". " + self.fake.bs() + ". " + self.fake.company(),
            lambda: f"{self.fake.name()} works at {self.fake.company()} as a {self.fake.job()}. {self.fake.sentence()}",
            lambda: f"Product: {self.fake.catch_phrase()}. Description: {self.fake.text(max_nb_chars=200)}",
            lambda: f"Event: {self.fake.sentence()}. Location: {self.fake.address()}. Time: {self.fake.date_time()}",
            lambda: f"Article: {self.fake.sentence()}. Author: {self.fake.name()}. Content: {self.fake.paragraph()}",
            lambda: f"Review: {self.fake.sentence()}. Rating: {random.randint(1, 5)}/5. Comment: {self.fake.text(max_nb_chars=150)}",
            lambda: f"News: {self.fake.sentence()}. Source: {self.fake.company()}. {self.fake.paragraph()}",
        ]

        buf, run_len = [], 0
        # Slight overshoot so clipping won't produce under-length output
        target_chars = max(self.chunk_size, int(self.chunk_size * 1.2))
        while run_len < target_chars:
            s = str(random.choice(text_sources)()).replace("\n", " ")
            buf.append(s)
            run_len += len(s) + 1

        # Normalize whitespace once, then clip
        plain = " ".join(" ".join(buf).split())[:self.chunk_size]
        if len(plain) < self.chunk_size:
            # Pad with extra Faker text if we accidentally under-shot
            pad = self.fake.text(max_nb_chars=(self.chunk_size - len(plain) + 32)).replace("\n", " ")
            plain = (" ".join((plain + " " + pad).split()))[:self.chunk_size]

        # 2) Choose a true 20–30% of positions for specials
        k_special = random.randint(int(self.chunk_size * 0.20), int(self.chunk_size * 0.30))
        k_special = min(k_special, max(0, self.chunk_size - 1))  # keep at least 1 plain char when size >= 2
        markers = [0] * self.chunk_size
        for pos in random.sample(range(self.chunk_size), k_special):
            markers[pos] = 1

        # 3) Build output by replacing the marked positions
        out = list(plain)
        for idx, mark in enumerate(markers):
            if mark:
                out[idx] = random.choice(specials_pool)

        return "".join(out)  # exact length, Unicode text
    
    def generate_embedding(self) -> List[float]:
        """Generate realistic embedding vector using LangChain FakeEmbeddings."""
        # Generate a random text for embedding
        text = self.fake.sentence()
        embedding = self.fake_embeddings.embed_query(text)
        return embedding
    
    def generate_metadata_string(self, field_name: str) -> str:
        """Generate realistic string data for metadata field using Faker."""
        string_sources = [
            lambda: self.fake.word()[:20],
            lambda: self.fake.name()[:20],
            lambda: self.fake.company()[:20],
            lambda: self.fake.job()[:20],
            lambda: self.fake.catch_phrase()[:20],
            lambda: self.fake.sentence()[:20],
            lambda: self.fake.word() + " " + self.fake.word()[:20],
            lambda: self.fake.color_name()[:20],
            lambda: self.fake.city()[:20],
            lambda: self.fake.country()[:20]
        ]
        
        return random.choice(string_sources)()
    
    def generate_metadata_number(self, field_name: str) -> float:
        """Generate realistic numeric data for metadata field."""
        # Generate either INT or DOUBLE based on field name
        if 'meta_num_03' in field_name or 'meta_num_05' in field_name or 'meta_num_07' in field_name or 'meta_num_09' in field_name or 'meta_num_11' in field_name or 'meta_num_13' in field_name or 'meta_num_15' in field_name or 'meta_num_17' in field_name or 'meta_num_19' in field_name:
            # DOUBLE fields
            return round(random.uniform(0.0, 1000.0), 2)
        else:
            # INT fields
            return random.randint(1000, 999999)
    
    def generate_metadata_boolean(self, field_name: str) -> bool:
        """Generate realistic boolean data for metadata field."""
        return random.choice([True, False])
    
    def generate_metadata_date(self, field_name: str) -> str:
        """Generate realistic date data for metadata field as timestamp string."""
        # Table API expects timestamps in ISO format with timezone
        dt = self.fake.date_time_between(start_date='-2y', end_date='+1y')
        return dt.isoformat() + 'Z'
    
    def generate_document(self) -> Dict[str, Any]:
        """Generate a complete document matching the sample_price_1 table structure."""
        now = datetime.now()
        
        document = {
            'id': self.generate_uuid(),
            'chunk': self.generate_chunk(),
            'vector_column': self.generate_embedding(),
            'created_at': now.isoformat() + 'Z',
            'updated_at': now.isoformat() + 'Z'
        }
        
        # Generate string metadata fields (20 fields)
        for i in range(1, 21):
            field_name = f"meta_str_{i:02d}"
            document[field_name] = self.generate_metadata_string(field_name)
        
        # Generate numeric metadata fields (20 fields)
        for i in range(1, 21):
            field_name = f"meta_num_{i:02d}"
            document[field_name] = self.generate_metadata_number(field_name)
        
        # Generate boolean metadata fields (12 fields)
        for i in range(1, 13):
            field_name = f"meta_bool_{i:02d}"
            document[field_name] = self.generate_metadata_boolean(field_name)
        
        # Generate date metadata fields (12 fields)
        for i in range(1, 13):
            field_name = f"meta_date_{i:02d}"
            document[field_name] = self.generate_metadata_date(field_name)
        
        return document
    
    def generate_vector_document(self, base_document: Dict[str, Any]) -> Dict[str, Any]:
        """Generate vector table document (id + vector_column only).
        
        Args:
            base_document: Complete document with all fields
            
        Returns:
            Document with only id and vector_column for vector table
        """
        return {
            'id': base_document['id'],  # Same UUID
            'vector_column': base_document['vector_column']
        }
    
    def generate_data_document(self, base_document: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data table document (id + chunk + metadata).
        
        Args:
            base_document: Complete document with all fields
            
        Returns:
            Document with id, chunk, and all metadata fields for data table
        """
        # Start with id, chunk, and timestamps
        data_document = {
            'id': base_document['id'],  # Same UUID
            'chunk': base_document['chunk'],
            'created_at': base_document['created_at'],
            'updated_at': base_document['updated_at']
        }
        
        # Add all metadata fields
        # String metadata fields (20 fields)
        for i in range(1, 21):
            field_name = f"meta_str_{i:02d}"
            data_document[field_name] = base_document[field_name]
        
        # Numeric metadata fields (20 fields)
        for i in range(1, 21):
            field_name = f"meta_num_{i:02d}"
            data_document[field_name] = base_document[field_name]
        
        # Boolean metadata fields (12 fields)
        for i in range(1, 13):
            field_name = f"meta_bool_{i:02d}"
            data_document[field_name] = base_document[field_name]
        
        # Date metadata fields (12 fields)
        for i in range(1, 13):
            field_name = f"meta_date_{i:02d}"
            data_document[field_name] = base_document[field_name]
        
        return data_document
    
    def generate_batch(self, batch_size: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Generate batches for both tables.
        
        Args:
            batch_size: Number of documents to generate
            
        Returns:
            Tuple of (vector_batch, data_batch) for dual database insertion
        """
        vector_batch = []
        data_batch = []
        
        for _ in range(batch_size):
            # Generate complete document first
            base_document = self.generate_document()
            
            # Split into vector and data documents
            vector_batch.append(self.generate_vector_document(base_document))
            data_batch.append(self.generate_data_document(base_document))
        
        logger.debug(f"Generated batch of {len(vector_batch)} vector documents and {len(data_batch)} data documents")
        return vector_batch, data_batch
