"""
Base scraper class that other scrapers will inherit from.
Provides common functionality for all scrapers.
"""

import requests
from bs4 import BeautifulSoup
import time
import logging
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class BaseScraper:
    """
    Base class for all scrapers.
    
    Attributes:
        base_url (str): The base URL for scraping
        headers (dict): HTTP headers for requests
        timeout (int): Request timeout in seconds
        logger: Logging instance
    """
    
    def __init__(self, base_url: str, timeout: int = 10):
        """
        Initialize the base scraper.
        
        Args:
            base_url: The base URL to scrape from
            timeout: Request timeout in seconds (default: 10)
        """
        self.base_url = base_url
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Common headers to avoid being blocked
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a web page and return BeautifulSoup object.
        
        Args:
            url: URL to fetch
            
        Returns:
            BeautifulSoup object if successful, None otherwise
        """
        try:
            self.logger.info(f"Fetching: {url}")
            response = requests.get(
                url, 
                headers=self.headers, 
                timeout=self.timeout
            )
            response.raise_for_status()  # Raise exception for bad status codes
            
            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None
    
    def rate_limit(self, seconds: int = 1):
        """
        Sleep for specified seconds to avoid overloading servers.
        
        Args:
            seconds: Number of seconds to sleep (default: 1)
        """
        self.logger.debug(f"Rate limiting: sleeping for {seconds}s")
        time.sleep(seconds)
    
    def scrape(self) -> Dict[str, Any]:
        """
        Main scraping method. To be implemented by subclasses.
        
        Returns:
            Dictionary containing scraped data
        """
        raise NotImplementedError("Subclasses must implement scrape() method")