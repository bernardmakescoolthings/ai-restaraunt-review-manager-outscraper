import os
import json
import psycopg2
import logging
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from datetime import datetime
from outscraper.api_client import ApiClient
from datetime import datetime, timedelta
import time


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("outscraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("outscraper")

class Outscraper:
    """
    A class to handle Google Maps business and review data storage.
    """
    
    def __init__(self):
        """Initialize the Outscraper with database connection from environment variables."""
        load_dotenv()
        self.api_key = os.getenv('OUTSCRAPER_API_KEY')
        
        if not self.api_key:
            logger.error("OUTSCRAPER_API_KEY not found in environment variables")
            raise ValueError("OUTSCRAPER_API_KEY not found in environment variables")
            
        self.client = ApiClient(api_key=self.api_key)
        logger.info("Initialized Outscraper client with API key")
        
        # Get database credentials from environment variables
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        # Initialize connection objects
        self.conn = None
        self.cursor = None
        
        # Results tracking
        self.results = {
            'businesses_inserted': 0,
            'reviews_inserted': 0,
            'businesses_errors': [],
            'reviews_errors': []
        }
    
    def _validate_db_config(self):
        """Validate the database configuration."""
        if not all(self.db_config.values()):
            missing = [k for k, v in self.db_config.items() if not v]
            return False, f"Missing database configuration: {', '.join(missing)}"
        return True, ""
    
    def connect_to_db(self):
        """Establish database connection."""
        is_valid, message = self._validate_db_config()
        if not is_valid:
            return False, message
        
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                cursor_factory=DictCursor
            )
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            return True, "Connection established"
        except Exception as e:
            return False, str(e)
    
    def close_connection(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def save_business(self, business_data):
        """
        Save a single business record to the database.
        
        Args:
            business_data (dict): Business data to save
            
        Returns:
            tuple: (success, message_or_business_id)
        """
        try:
            # Extract business data
            business = {
                'query': business_data.get('query'),
                'name': business_data.get('name'),
                'name_for_emails': business_data.get('name_for_emails'),
                'place_id': business_data.get('place_id'),
                'google_id': business_data.get('google_id'),
                'kgmid': business_data.get('kgmid'),
                'full_address': business_data.get('full_address'),
                'borough': business_data.get('borough'),
                'street': business_data.get('street'),
                'postal_code': business_data.get('postal_code'),
                'area_service': business_data.get('area_service'),
                'country_code': business_data.get('country_code'),
                'country': business_data.get('country'),
                'city': business_data.get('city'),
                'us_state': business_data.get('us_state'),
                'state': business_data.get('state'),
                'plus_code': business_data.get('plus_code'),
                'latitude': business_data.get('latitude'),
                'longitude': business_data.get('longitude'),
                'h3': business_data.get('h3'),
                'time_zone': business_data.get('time_zone'),
                'popular_times': json.dumps(business_data.get('popular_times')) if business_data.get('popular_times') else None,
                'site': business_data.get('site'),
                'phone': business_data.get('phone'),
                'type': business_data.get('type'),
                'logo': business_data.get('logo'),
                'description': business_data.get('description'),
                'typical_time_spent': business_data.get('typical_time_spent'),
                'located_in': business_data.get('located_in'),
                'located_google_id': business_data.get('located_google_id'),
                'category': business_data.get('category'),
                'subtypes': business_data.get('subtypes'),
                'posts': json.dumps(business_data.get('posts')) if business_data.get('posts') else None,
                'reviews_tags': json.dumps(business_data.get('reviews_tags')) if business_data.get('reviews_tags') else None,
                'rating': business_data.get('rating'),
                'reviews_count': business_data.get('reviews'),
                'photos_count': business_data.get('photos_count'),
                'cid': business_data.get('cid'),
                'reviews_link': business_data.get('reviews_link'),
                'reviews_id': business_data.get('reviews_id'),
                'photo': business_data.get('photo'),
                'street_view': business_data.get('street_view'),
                'working_hours_old_format': business_data.get('working_hours_old_format'),
                'working_hours': json.dumps(business_data.get('working_hours')) if business_data.get('working_hours') else None,
                'other_hours': json.dumps(business_data.get('other_hours')) if business_data.get('other_hours') else None,
                'business_status': business_data.get('business_status'),
                'about': json.dumps(business_data.get('about')) if business_data.get('about') else None,
                'range': business_data.get('range'),
                'reviews_per_score': business_data.get('reviews_per_score'),
                'reviews_per_score_1': business_data.get('reviews_per_score_1'),
                'reviews_per_score_2': business_data.get('reviews_per_score_2'),
                'reviews_per_score_3': business_data.get('reviews_per_score_3'),
                'reviews_per_score_4': business_data.get('reviews_per_score_4'),
                'reviews_per_score_5': business_data.get('reviews_per_score_5'),
                'reservation_links': json.dumps(business_data.get('reservation_links')) if business_data.get('reservation_links') else None,
                'booking_appointment_link': business_data.get('booking_appointment_link'),
                'menu_link': business_data.get('menu_link'),
                'order_links': json.dumps(business_data.get('order_links')) if business_data.get('order_links') else None,
                'owner_id': business_data.get('owner_id'),
                'verified': business_data.get('verified'),
                'owner_title': business_data.get('owner_title'),
                'owner_link': business_data.get('owner_link'),
                'location_link': business_data.get('location_link'),
                'location_reviews_link': business_data.get('location_reviews_link')
            }
            
            # Remove None values to avoid conflicts with NOT NULL constraints
            business = {k: v for k, v in business.items() if v is not None}
            
            # Insert or update business data
            columns = ', '.join(business.keys())
            placeholders = ', '.join(['%s'] * len(business))
            values = list(business.values())
            
            # Check if business exists by place_id
            self.cursor.execute("SELECT 1 FROM businesses WHERE place_id = %s", (business['place_id'],))
            if self.cursor.fetchone():
                # Update existing business
                update_parts = [f"{key} = %s" for key in business.keys()]
                update_query = f"UPDATE businesses SET {', '.join(update_parts)} WHERE place_id = %s"
                self.cursor.execute(update_query, values + [business['place_id']])
                logger.info(f"Updated existing business: {business['name']} ({business['place_id']})")
            else:
                # Insert new business
                insert_query = f"INSERT INTO businesses ({columns}) VALUES ({placeholders})"
                self.cursor.execute(insert_query, values)
                self.results['businesses_inserted'] += 1
                logger.info(f"Inserted new business: {business['name']} ({business['place_id']})")
            
            return True, business['place_id']
            
        except Exception as e:
            error_msg = f"Error saving business {business_data.get('place_id')}: {str(e)}"
            logger.error(error_msg)
            self.results['businesses_errors'].append({
                'place_id': business_data.get('place_id'),
                'error': str(e)
            })
            return False, str(e)
    
    def save_review(self, review, business_place_id):
        """
        Save a single review record to the database.
        
        Args:
            review (dict): Review data to save
            business_place_id (str): The place_id of the associated business
            
        Returns:
            tuple: (success, message_or_review_id)
        """
        try:
            # Convert the datetime string to a proper timestamp
            review_datetime_utc = None
            if review.get('review_datetime_utc'):
                try:
                    review_datetime_utc = datetime.strptime(
                        review['review_datetime_utc'], 
                        '%m/%d/%Y %H:%M:%S'
                    )
                except ValueError:
                    # Handle potential different format
                    pass
            
            # Extract review data
            review_record = {
                'business_place_id': business_place_id,
                'google_id': review.get('google_id'),
                'review_id': review.get('review_id'),
                'review_pagination_id': review.get('review_pagination_id'),
                'author_link': review.get('author_link'),
                'author_title': review.get('author_title'),
                'author_id': review.get('author_id'),
                'author_image': review.get('author_image'),
                'author_reviews_count': review.get('author_reviews_count'),
                'author_ratings_count': review.get('author_ratings_count'),
                'review_text': review.get('review_text'),
                'review_img_urls': json.dumps(review.get('review_img_urls')) if review.get('review_img_urls') else None,
                'review_img_url': review.get('review_img_url'),
                'review_questions': json.dumps(review.get('review_questions')) if review.get('review_questions') else None,
                'review_photo_ids': json.dumps(review.get('review_photo_ids')) if review.get('review_photo_ids') else None,
                'owner_answer': review.get('owner_answer'),
                'owner_answer_timestamp': review.get('owner_answer_timestamp'),
                'owner_answer_timestamp_datetime_utc': review.get('owner_answer_timestamp_datetime_utc'),
                'review_link': review.get('review_link'),
                'review_rating': review.get('review_rating'),
                'review_timestamp': review.get('review_timestamp'),
                'review_datetime_utc': review_datetime_utc,
                'review_likes': review.get('review_likes'),
                'reviews_id': review.get('reviews_id'),
                'replies': json.dumps(review.get('replies')) if review.get('replies') else None
            }
            
            # Remove None values to avoid conflicts with NOT NULL constraints
            review_record = {k: v for k, v in review_record.items() if v is not None}
            
            # Insert or update review data
            review_columns = ', '.join(review_record.keys())
            review_placeholders = ', '.join(['%s'] * len(review_record))
            review_values = list(review_record.values())
            
            # Check if review exists by review_id
            self.cursor.execute("SELECT 1 FROM reviews WHERE review_id = %s", (review_record['review_id'],))
            if self.cursor.fetchone():
                # Update existing review
                rev_update_parts = [f"{key} = %s" for key in review_record.keys()]
                rev_update_query = f"UPDATE reviews SET {', '.join(rev_update_parts)} WHERE review_id = %s"
                self.cursor.execute(rev_update_query, review_values + [review_record['review_id']])
                logger.debug(f"Updated existing review: {review_record['review_id']}")
            else:
                # Insert new review
                rev_insert_query = f"INSERT INTO reviews ({review_columns}) VALUES ({review_placeholders})"
                self.cursor.execute(rev_insert_query, review_values)
                self.results['reviews_inserted'] += 1
                logger.debug(f"Inserted new review: {review_record['review_id']}")
            
            return True, review_record['review_id']
            
        except Exception as e:
            error_msg = f"Error saving review {review.get('review_id')}: {str(e)}"
            logger.error(error_msg)
            self.results['reviews_errors'].append({
                'review_id': review.get('review_id'),
                'error': str(e)
            })
            return False, str(e)
    
    def save_data(self, data):
        """
        Save Google Maps data to the database.
        
        Args:
            data (list): A list of dictionaries containing Google Maps business and review data.
            
        Returns:
            dict: A dictionary with status and details of the operation.
        """
        try:
            # Connect to database
            connected, message = self.connect_to_db()
            if not connected:
                logger.error(f"Database connection failed: {message}")
                return {
                    'status': 'error',
                    'message': message
                }
            
            logger.info(f"Processing {len(data)} businesses")
            
            # Reset results
            self.results = {
                'businesses_inserted': 0,
                'reviews_inserted': 0,
                'businesses_errors': [],
                'reviews_errors': []
            }
            
            # Process each business and its reviews
            for business_data in data:
                # Try to get place_id directly or from saving the business
                place_id = business_data.get('place_id')
                
                if place_id:
                    # Save business (will update if exists)
                    business_success, business_place_id = self.save_business(business_data)
                    
                    # Always process reviews if we have a valid place_id, even if business update had an issue
                    # Process reviews if available
                    reviews_data = business_data.get('reviews_data', [])
                    if reviews_data:
                        logger.info(f"Processing {len(reviews_data)} reviews for {business_data.get('name', 'Unknown')} ({place_id})")
                        for review in reviews_data:
                            self.save_review(review, place_id)
                else:
                    # Log error if no place_id is available
                    logger.error("No place_id provided for business")
                    self.results['businesses_errors'].append({
                        'place_id': 'unknown',
                        'error': 'No place_id provided for business'
                    })
            
            # Commit the transaction
            self.conn.commit()
            
            # Log summary
            logger.info(f"Results summary - Businesses inserted: {self.results['businesses_inserted']}, Reviews inserted: {self.results['reviews_inserted']}")
            if self.results['businesses_errors']:
                logger.warning(f"Business errors: {len(self.results['businesses_errors'])}")
            if self.results['reviews_errors']:
                logger.warning(f"Review errors: {len(self.results['reviews_errors'])}")
            
            return {
                'status': 'success',
                'results': self.results
            }
        
        except Exception as e:
            # Rollback in case of error
            if self.conn:
                self.conn.rollback()
            
            logger.error(f"Error saving data: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }
        
        finally:
            # Close connection
            self.close_connection()

    def get_all_reviews(self, place_ids: list, reviews_limit: int = 10):
        """
        Fetches reviews for a given place ID and saves them to the database.
        
        Args:
            place_id (str): The Google Maps place ID to fetch reviews for
            
        Returns:
            list: The API result
        """
        running_request_ids = set()
        results = []
        for place_id in place_ids:
            for attempt in range(3):
                try:
                    #print(f"Fetching reviews for {place_id} with limit {reviews_limit}")
                    response  = self.client.google_maps_reviews(place_id, sort='newest', reviews_limit=reviews_limit, language='en', async_request=True)
                    running_request_ids.add(response['id'])
                except Exception as e:
                    logger.error(f"Error in get_all_reviews for {place_id}: {str(e)}")
                    raise
                else:
                    break
            else:
                logger.error(f"Failed to get reviews for {place_id} after 3 attempts")
        

        #Grab the results
        attempts = 10 # retry 5 times
        while attempts and running_request_ids: # stop when no more attempts are left or when no more running request ids
            attempts -= 1
            time.sleep(30)

            for request_id in list(running_request_ids): # we don't want to change the set while iterating, so cloning it to list
                result = self.client.get_request_archive(request_id)

                if result['status'] == 'Success':
                    results.append(result['data'])
                    running_request_ids.remove(request_id)
        
        for result in results:
            self.save_data(result)
    
    def get_all_reviews_last_24(self, place_id: str):
        yesterday_timestamp = int((datetime.now() - timedelta(1)).timestamp())

        running_request_ids = set()
        results = []
        try:
            for attempt in range(3):
                try:
                    response = self.client.google_maps_reviews(place_id, sort='newest', reviews_limit=50, async_request=True, cutoff=yesterday_timestamp, language='en')
                    running_request_ids.add(response['id'])
                    break
                except Exception as e:
                    logger.error(f"Error in get_all_reviews_last_24 for {place_id}: {str(e)}")
                    if attempt == 2:  # Last attempt failed
                        raise
            else:
                logger.error(f"Failed to get reviews for {place_id} after 3 attempts")

            # Grab the results
            attempts = 20  # retry up to 20 times
            while attempts and running_request_ids:  # stop when no more attempts are left or when no more running request ids
                attempts -= 1
                time.sleep(30)

                for request_id in list(running_request_ids):  # we don't want to change the set while iterating, so cloning it to list
                    result = self.client.get_request_archive(request_id)

                    if result['status'] == 'Success':
                        results.append(result['data'])
                        running_request_ids.remove(request_id)
            
            for result in results:
                self.save_data(result)
                
        except Exception as e:
            logger.error(f"Error in get_all_reviews_last_24 for {place_id}: {str(e)}")
            raise
