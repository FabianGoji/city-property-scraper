"""
Main entry point for City Property & Environment Data Scraper.
"""

import argparse
import logging
from src.environment_scraper import EnvironmentScraper
from src.property_scraper import PropertyScraper
from src.data_manager import DataManager
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main application function."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Scrape property and environmental data for a city'
    )
    parser.add_argument('--city', type=str, required=True, help='City name')
    parser.add_argument('--state', type=str, required=True, help='State code (e.g., TX)')
    parser.add_argument('--properties', type=int, default=10, help='Number of properties to scrape')
    parser.add_argument('--api-key', type=str, help='OpenWeatherMap API key (or set OPENWEATHER_API_KEY env var)')
    
    args = parser.parse_args()
    
    # Get API key
    api_key = args.api_key or os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        logger.error("API key required. Set --api-key or OPENWEATHER_API_KEY environment variable")
        logger.info("Get free API key at: https://openweathermap.org/api")
        return
    
    logger.info(f"Starting scraper for {args.city}, {args.state}")
    
    # Initialize scrapers
    env_scraper = EnvironmentScraper(api_key=api_key)
    prop_scraper = PropertyScraper()
    data_manager = DataManager()
    
    # Scrape environmental data
    logger.info("Scraping environmental data...")
    env_data = env_scraper.scrape(args.city, args.state)
    
    # Scrape property data
    logger.info("Scraping property data...")
    prop_data = prop_scraper.scrape(args.city, args.state, limit=args.properties)
    
    # Save data
    logger.info("Saving data...")
    env_file = data_manager.save_environment_data(env_data, args.city, args.state)
    prop_file = data_manager.save_property_data(prop_data, args.city, args.state)
    combined_file = data_manager.save_combined_data(env_data, prop_data, args.city, args.state)
    # Save to database
    data_manager.save_properties_to_db(prop_data)
    data_manager.save_environment_to_db(env_data)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("SCRAPING COMPLETE")
    logger.info("="*60)
    logger.info(f"City: {args.city}, {args.state}")
    logger.info(f"Properties scraped: {len(prop_data.get('properties', []))}")
    logger.info(f"\nFiles saved:")
    logger.info(f"  - Environment: {env_file}")
    logger.info(f"  - Properties: {prop_file}")
    logger.info(f"  - Combined: {combined_file}")
    logger.info("="*60)


if __name__ == "__main__":
    main()