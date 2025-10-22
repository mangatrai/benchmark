#!/usr/bin/env python3
"""
Collection Cleaner for DataStax AstraDB
Deletes all documents from a specified collection.
"""

import os
import logging
from dotenv import load_dotenv
from astrapy import DataAPIClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_collection():
    """Delete all documents from the collection."""
    
    # Load environment variables
    load_dotenv()
    
    # Get configuration from environment
    token = os.getenv('ASTRA_DB_APPLICATION_TOKEN')
    endpoint = os.getenv('ASTRA_DB_API_ENDPOINT')
    namespace = os.getenv('ASTRA_DB_NAMESPACE', 'default_keyspace')
    collection_name = os.getenv('ASTRA_DB_COLLECTION', 'sample_pricing')
    
    if not token or not endpoint:
        logger.error("Missing required environment variables: ASTRA_DB_APPLICATION_TOKEN and ASTRA_DB_API_ENDPOINT")
        return False
    
    try:
        # Initialize client and get collection
        logger.info(f"Connecting to AstraDB...")
        client = DataAPIClient(token=token)
        database = client.get_database_by_api_endpoint(endpoint)
        collection = database.get_collection(collection_name)
        
        logger.info(f"Connected to collection: {collection_name}")
        
        # Count documents before deletion
        logger.info("Counting documents before deletion...")
        count_before = collection.count_documents({}, upper_bound=10000)
        logger.info(f"Documents in collection before deletion: {count_before}")
        
        if count_before == 0:
            logger.info("Collection is already empty. Nothing to delete.")
            return True
        
        # Delete all documents
        logger.info("Deleting all documents...")
        result = collection.delete_many({})
        
        # Log results
        logger.info(f"Deletion completed:")
        logger.info(f"  - Deleted count: {result.deleted_count}")
        
        # Count documents after deletion
        logger.info("Counting documents after deletion...")
        count_after = collection.count_documents({}, upper_bound=10000)
        logger.info(f"Documents in collection after deletion: {count_after}")
        
        if count_after == 0:
            logger.info("✅ Collection successfully cleaned!")
            return True
        else:
            logger.warning(f"⚠️  Some documents may still remain: {count_after}")
            return False
            
    except Exception as e:
        logger.error(f"Error cleaning collection: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("DataStax AstraDB Collection Cleaner")
    print("=" * 60)
    
    success = clean_collection()
    
    if success:
        print("\n✅ Collection cleaning completed successfully!")
    else:
        print("\n❌ Collection cleaning failed!")
        exit(1)
