#!/usr/bin/env python3
import os
import psycopg2
import logging
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from scraper import Outscraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fetch_all_reviews.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fetch_all_reviews")

def get_db_connection():
    """Create a database connection using environment variables."""
    load_dotenv()
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Validate config
    if not all(db_config.values()):
        missing = [k for k, v in db_config.items() if not v]
        error_msg = f"Missing database configuration: {', '.join(missing)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        cursor_factory=DictCursor
    )
    
    return conn

def fetch_active_subscription_business_ids():
    """Fetch all business_place_ids from the user_business table that have active subscriptions."""
    place_ids = []
    conn = None
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT business_place_id 
                FROM user_business 
                WHERE subscription_status = 'active' 
                AND (expires_at IS NULL OR expires_at > NOW())
            """)
            for row in cursor:
                place_ids.append(row['business_place_id'])
        
        logger.info(f"Found {len(place_ids)} businesses with active subscriptions")
        return place_ids
    
    except Exception as e:
        logger.error(f"Error fetching businesses with active subscriptions: {str(e)}")
        raise
    
    finally:
        if conn:
            conn.close()

def main():
    try:
        # Initialize Outscraper
        scraper = Outscraper()
        
        # Get all business place_ids with active subscriptions
        place_ids = fetch_active_subscription_business_ids()
        
        if not place_ids:
            logger.warning("No businesses with active subscriptions found in the database")
            return
        
        # Process each business
        total = len(place_ids)
        for idx, place_id in enumerate(place_ids, 1):
            logger.info(f"Processing business {idx}/{total}: {place_id}")
            
            try:
                # Fetch and save reviews from the last 24 hours for this business
                scraper.get_all_reviews_last_24(place_id)
                logger.info(f"Successfully processed business: {place_id}")
            
            except Exception as e:
                logger.error(f"Error processing business {place_id}: {str(e)}")
                # Continue with the next business
                continue
        
        logger.info(f"Completed processing all {total} businesses with active subscriptions")
    
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}")

if __name__ == "__main__":
    main() 