import pytest
from unittest.mock import MagicMock, patch
import os
from datetime import datetime, timedelta

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
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
    """Mock database connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn, mock_cursor

@pytest.fixture
def mock_outscraper():
    """Mock Outscraper class."""
    with patch('scraper.Outscraper') as mock:
        instance = mock.return_value
        instance.get_all_reviews_last_24.return_value = {
            'status': 'Success',
            'data': [
                {
                    'place_id': 'test_place_id',
                    'reviews': [
                        {
                            'review_id': '1',
                            'text': 'Great service!',
                            'rating': 5,
                            'time': datetime.now().isoformat()
                        }
                    ]
                }
            ]
        }
        yield instance

@pytest.fixture
def sample_business_data():
    """Sample business data for testing."""
    return {
        'place_id': 'test_place_id',
        'name': 'Test Restaurant',
        'rating': 4.5,
        'reviews_count': 100,
        'address': '123 Test St'
    }

@pytest.fixture
def sample_review_data():
    """Sample review data for testing."""
    return {
        'review_id': '1',
        'text': 'Great food!',
        'rating': 5,
        'time': datetime.now().isoformat(),
        'author_name': 'Test User'
    } 