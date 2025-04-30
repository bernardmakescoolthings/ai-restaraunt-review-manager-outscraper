#!/usr/bin/env python3
import sys
import logging
from scraper import Outscraper
import os
from dotenv import load_dotenv

load_dotenv()

REVIEW_LIMIT = os.getenv('REVIEW_LIMIT'), #0 means all reviews
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fetch_reviews.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fetch_reviews")

def main():
    if len(sys.argv) != 2:
        print("Usage: python fetch_reviews_by_id.py <place_id>")
        sys.exit(1)
    
    place_id = sys.argv[1]
    logger.info(f"Scraping reviews for place_id: {place_id}")
    
    try:
        # Initialize Outscraper
        scraper = Outscraper()
        
        # Fetch and save reviews from the last 24 hours
        scraper.get_all_reviews([place_id], REVIEW_LIMIT)
        
        logger.info(f"Successfully Scraped reviews for place_id: {place_id}")
    except Exception as e:
        logger.error(f"Error scraping reviews: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 