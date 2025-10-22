"""
Random data generator for AstraDB benchmarking.
Generates JSON documents matching the sample structure with configurable parameters.
"""

import uuid
import random
import string
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
from astrapy.data_types import DataAPIVector
from faker import Faker
from langchain_core.embeddings.fake import FakeEmbeddings

logger = logging.getLogger(__name__)


class DataGenerator:
    """Generates random data matching the sample JSON structure."""
    
    def __init__(self, chunk_size: int = 512, embedding_dim: int = 1536, metadata_fields: int = 64):
        """Initialize the data generator.
        
        Args:
            chunk_size: Length of the chunk string (default: 512)
            embedding_dim: Dimension of the embedding vector (default: 1536)
            metadata_fields: Number of metadata fields to generate (default: 64)
        """
        self.chunk_size = chunk_size
        self.embedding_dim = embedding_dim
        self.metadata_fields = metadata_fields
        
        # Initialize Faker for diverse text generation
        self.fake = Faker()
        
        # Initialize LangChain FakeEmbeddings for realistic vector generation
        self.fake_embeddings = FakeEmbeddings(size=self.embedding_dim)
        
        # Character sets for different data types
        self.base64_chars = string.ascii_letters + string.digits + '+/-_='
        self.alphanumeric_chars = string.ascii_letters + string.digits
        self.special_chars = '!@#$%^&*()_+-=[]{}|;:,.<>?'
        
        logger.info(f"DataGenerator initialized: chunk_size={chunk_size}, "
                   f"embedding_dim={embedding_dim}, metadata_fields={metadata_fields}")
    
    def generate_uuid(self) -> str:
        """Generate a random UUID string."""
        return str(uuid.uuid4())
    
    def generate_chunk(self) -> str:
        """
        Faker-based, exact-length chunk with 20–30% specials/emojis.
        Returns a Unicode str of length self.chunk_size.
        """
        # Single-codepoint pools to avoid ZWJ/grapheme slicing drama
        currency = "€£¥$¢₹₽₩₪₫₱₡₨₴₸₺₼₾₿"
        mathsy   = "∑π∆±×÷≈≠≤≥∞∂∇∫∮∝"
        emojis   = "😀🚀💻📊💰🎯🔥💡⭐🌟🎉📈💼🔧⚡🎨📱"
        punct    = "—–•·"  # en/em dash, bullets
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
        """Generate a realistic embedding vector using LangChain FakeEmbeddings.

        Returns:
            List of float values representing the embedding vector
        """
        # Generate a random text to create embeddings from
        text = self.fake.sentence()
        
        # Use LangChain FakeEmbeddings to generate realistic vector
        embedding = self.fake_embeddings.embed_query(text)
        
        return embedding
    
    def generate_metadata_string(self, field_name: str) -> str:
        """Generate realistic string data for metadata field using Faker.
        
        Args:
            field_name: Name of the metadata field
            
        Returns:
            Realistic string data
        """
        # Use different Faker methods for variety (limited to 20 characters)
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
    
    def generate_metadata_number(self, field_name: str) -> int:
        """Generate realistic numeric data for metadata field using Faker.
        
        Args:
            field_name: Name of the metadata field
            
        Returns:
            Realistic numeric data
        """
        # Use different Faker methods for variety
        number_sources = [
            lambda: self.fake.random_int(min=1000, max=999999),
            lambda: self.fake.random_int(min=10000, max=9999999),
            lambda: self.fake.random_int(min=1, max=100),
            lambda: self.fake.random_int(min=100000, max=9999999),
            lambda: self.fake.random_int(min=1, max=1000),
            lambda: self.fake.random_int(min=1000000, max=99999999),
            lambda: self.fake.random_int(min=1, max=10000),
            lambda: self.fake.random_int(min=100, max=9999),
            lambda: self.fake.random_int(min=1000, max=99999),
            lambda: self.fake.random_int(min=1, max=1000000)
        ]
        
        return random.choice(number_sources)()
    
    def generate_metadata_date(self, field_name: str) -> str:
        """Generate realistic date data for metadata field using Faker.
        
        Args:
            field_name: Name of the metadata field
            
        Returns:
            Realistic date string in ISO format
        """
        # Use different Faker methods for variety
        date_sources = [
            lambda: self.fake.date_time_between(start_date='-2y', end_date='now').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-1y', end_date='+1y').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-5y', end_date='-1y').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-1m', end_date='now').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='now', end_date='+1y').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-3y', end_date='-1y').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-1y', end_date='+2y').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time().strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-6m', end_date='+6m').strftime('%Y-%m-%dT%H:%M:%S'),
            lambda: self.fake.date_time_between(start_date='-10y', end_date='now').strftime('%Y-%m-%dT%H:%M:%S')
        ]
        
        return random.choice(date_sources)()
    
    def generate_metadata(self) -> Dict[str, Any]:
        """Generate metadata object with the specified number of fields.
        
        Returns:
            Dictionary containing metadata fields
        """
        metadata = {}
        
        # Generate string fields (38 fields)
        for i in range(1, 39):
            field_name = f"meta_str_{i:02d}"
            metadata[field_name] = self.generate_metadata_string(field_name)
        
        # Generate numeric fields (19 fields)
        for i in range(1, 20):
            field_name = f"meta_num_{i:02d}"
            metadata[field_name] = self.generate_metadata_number(field_name)
        
        # Generate date fields (7 fields)
        for i in range(1, 8):
            field_name = f"meta_date_{i:02d}"
            metadata[field_name] = self.generate_metadata_date(field_name)
        
        return metadata
    
    def generate_document(self) -> Dict[str, Any]:
        """Generate a complete document matching the sample structure.
        
        Returns:
            Dictionary representing a complete document
        """
        document = {
            "chunk": self.generate_chunk(),
            "$vector": self.generate_embedding(),  # Use $vector field name for vector search
            "metadata": self.generate_metadata()
        }
        
        return document
    
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a batch of documents.
        
        Args:
            batch_size: Number of documents to generate
            
        Returns:
            List of document dictionaries
        """
        batch = []
        for _ in range(batch_size):
            batch.append(self.generate_document())
        
        logger.debug(f"Generated batch of {len(batch)} documents")
        return batch
    
    def get_document_size_estimate(self) -> int:
        """Estimate the size of a single document in bytes.
        
        Returns:
            Estimated document size in bytes
        """
        # Rough estimation based on field sizes
        uuid_size = 36  # UUID string
        chunk_size = self.chunk_size
        embedding_size = self.embedding_dim * 8  # 8 bytes per float
        metadata_size = 64 * 50  # 64 fields * ~50 chars each
        
        return uuid_size + chunk_size + embedding_size + metadata_size


def create_data_generator(config) -> DataGenerator:
    """Factory function to create a DataGenerator with configuration.
    
    Args:
        config: Configuration object
        
    Returns:
        Configured DataGenerator instance
    """
    return DataGenerator(
        chunk_size=config.chunk_size,
        embedding_dim=config.embedding_dimension,
        metadata_fields=config.metadata_fields
    )
