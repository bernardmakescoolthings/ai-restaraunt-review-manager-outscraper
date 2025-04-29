import pytest
from unittest.mock import patch, MagicMock, call
import os
import psycopg2
from datetime import datetime, timedelta

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Mock environment variables for all tests."""
    env_vars = {
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test_db',
        'DB_USER': 'test_user',
        'DB_PASSWORD': 'test_password',
        'OUTSCRAPER_API_KEY': 'test_api_key'
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars

@pytest.fixture
def mock_db_connection():
    """Mock database connection and cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor

@pytest.fixture
def mock_api_client():
    """Mock the Outscraper API client."""
    with patch('outscraper.api_client.ApiClient') as mock:
        mock_instance = mock.return_value
        yield mock_instance

def test_outscraper_init(mock_api_client):
    """Test Outscraper initialization with environment variables."""
    with patch('scraper.psycopg2.connect') as mock_connect, \
         patch('scraper.load_dotenv'), \
         patch('scraper.ApiClient', mock_api_client):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Import and instantiate after all mocks are in place
        from scraper import Outscraper
        scraper = Outscraper()
        
        assert scraper.api_key == 'test_api_key'
        # Verify ApiClient was instantiated with the correct API key
        mock_api_client.assert_called_once_with(api_key='test_api_key')
        assert scraper.db_config['host'] == 'localhost'
        assert scraper.db_config['port'] == '5432'
        assert scraper.db_config['database'] == 'test_db'
        assert scraper.db_config['user'] == 'test_user'
        assert scraper.db_config['password'] == 'test_password'

def test_outscraper_init_missing_api_key(monkeypatch):
    """Test Outscraper initialization with missing API key."""
    # Remove the API key from environment
    monkeypatch.delenv('OUTSCRAPER_API_KEY', raising=False)
    
    # Import and instantiate after removing the API key
    with patch('scraper.load_dotenv'):
        from scraper import Outscraper
        with pytest.raises(ValueError) as exc_info:
            Outscraper()
        
        assert "OUTSCRAPER_API_KEY not found in environment variables" in str(exc_info.value)

def test_connect_to_db(mock_api_client):
    """Test database connection."""
    with patch('scraper.psycopg2.connect') as mock_connect, \
         patch('scraper.load_dotenv'), \
         patch('scraper.ApiClient', mock_api_client):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Import and instantiate after all mocks are in place
        from scraper import Outscraper
        scraper = Outscraper()
        
        # Mock the validation method
        with patch.object(scraper, '_validate_db_config', return_value=(True, "Valid config")):
            success, message = scraper.connect_to_db()
            
            assert success is True
            assert message == "Connection established"
            mock_connect.assert_called_once()
            assert scraper.conn is mock_conn
            assert scraper.cursor is mock_conn.cursor.return_value

def test_save_business(mock_api_client, mock_db_connection, sample_business_data):
    """Test saving business data to database."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = None  # Simulate new business
    
    with patch('scraper.psycopg2.connect') as mock_connect, \
         patch('scraper.load_dotenv'), \
         patch('scraper.ApiClient', mock_api_client):
        mock_connect.return_value = mock_conn
        
        # Import and instantiate after all mocks are in place
        from scraper import Outscraper
        scraper = Outscraper()
        
        # Mock the validation method
        with patch.object(scraper, '_validate_db_config', return_value=(True, "Valid config")):
            # Set up the connection
            scraper.connect_to_db()
            
            # Now save the business
            success, result = scraper.save_business(sample_business_data)
            
            # Verify the execute was called twice (check and insert)
            assert mock_cursor.execute.call_count == 2
            mock_conn.commit.assert_called_once()
            assert success is True
            assert result == sample_business_data['place_id']

def test_save_business_update(mock_api_client, mock_db_connection, sample_business_data):
    """Test updating existing business data."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = (1,)  # Simulate existing business
    
    with patch('scraper.psycopg2.connect') as mock_connect, \
         patch('scraper.load_dotenv'), \
         patch('scraper.ApiClient', mock_api_client):
        mock_connect.return_value = mock_conn
        
        # Import and instantiate after all mocks are in place
        from scraper import Outscraper
        scraper = Outscraper()
        
        # Mock the validation method
        with patch.object(scraper, '_validate_db_config', return_value=(True, "Valid config")):
            # Set up the connection
            scraper.connect_to_db()
            
            # Now save the business
            success, result = scraper.save_business(sample_business_data)
            
            # Verify the execute was called twice (check and update)
            assert mock_cursor.execute.call_count == 2
            mock_conn.commit.assert_called_once()
            assert success is True
            assert result == sample_business_data['place_id']

def test_save_review(mock_api_client, mock_db_connection, sample_review_data):
    """Test saving review data to database."""
    mock_conn, mock_cursor = mock_db_connection
    mock_cursor.fetchone.return_value = None  # Simulate new review
    
    with patch('scraper.psycopg2.connect') as mock_connect, \
         patch('scraper.load_dotenv'), \
         patch('scraper.ApiClient', mock_api_client):
        mock_connect.return_value = mock_conn
        
        # Import and instantiate after all mocks are in place
        from scraper import Outscraper
        scraper = Outscraper()
        
        # Mock the validation method
        with patch.object(scraper, '_validate_db_config', return_value=(True, "Valid config")):
            # Set up the connection
            scraper.connect_to_db()
            
            # Now save the review
            success, result = scraper.save_review(sample_review_data, 'test_place_id')
            
            # Verify the execute was called twice (check and insert/update)
            assert mock_cursor.execute.call_count == 2
            mock_conn.commit.assert_called_once()
            assert success is True
            assert result == sample_review_data['review_id']