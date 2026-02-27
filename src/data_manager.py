"""
Handles data storage and export functionality.
"""

import pandas as pd
import os
from typing import Dict, Any
from datetime import datetime
import logging
from .database import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataManager:
    """
    Manages data storage and export to CSV.
    """
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize data manager.
        
        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Data manager initialized with output dir: {output_dir}")

    def __init__(self, output_dir: str = "data", use_database: bool = True):
        """
        Initialize data manager.
        
        Args:
            output_dir: Directory to save output files
            use_database: Whether to also save to database
        """
        self.output_dir = output_dir
        self.use_database = use_database
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Data manager initialized with output dir: {output_dir}")
        
        # Initialize database if enabled
        if self.use_database:
            self.db = DatabaseManager()
            logger.info("Database manager initialized")
    
    def save_environment_data(self, data: Dict[str, Any], city: str, state: str) -> str:
        """
        Save environmental data to CSV.
        
        Args:
            data: Environmental data dictionary
            city: City name
            state: State code
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"environment_{city}_{state}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Flatten the nested dictionary
        flattened = {}
        
        if data.get('weather'):
            for key, value in data['weather'].items():
                flattened[f'weather_{key}'] = value
        
        if data.get('air_quality'):
            for key, value in data['air_quality'].items():
                flattened[f'air_quality_{key}'] = value
        
        # Add timestamp
        flattened['scrape_timestamp'] = timestamp
        
        # Convert to DataFrame and save
        df = pd.DataFrame([flattened])
        df.to_csv(filepath, index=False)
        
        logger.info(f"Environment data saved to {filepath}")
        return filepath
    
    def save_property_data(self, data: Dict[str, Any], city: str, state: str) -> str:
        """
        Save property data to CSV.
        
        Args:
            data: Property data dictionary
            city: City name
            state: State code
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"properties_{city}_{state}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Extract properties list
        properties = data.get('properties', [])
        
        if not properties:
            logger.warning("No property data to save")
            return ""
        
        # Add metadata
        for prop in properties:
            prop['scrape_timestamp'] = timestamp
        
        # Convert to DataFrame and save
        df = pd.DataFrame(properties)
        df.to_csv(filepath, index=False)
        
        logger.info(f"Property data saved to {filepath} ({len(properties)} properties)")
        return filepath
    
    def save_properties_to_db(self, data: Dict[str, Any]):
        """
        Save properties to database.
        
        Args:
            data: Property data dictionary
        """
        if not self.use_database:
            return
        
        properties = data.get('properties', [])
        for prop in properties:
            try:
                # Remove scrape_timestamp if exists (DB adds it automatically)
                prop_copy = prop.copy()
                if 'scrape_timestamp' in prop_copy:
                    del prop_copy['scrape_timestamp']
                
                self.db.save_property(prop_copy)
                logger.info(f"Saved property {prop_copy['property_id']} to database")
            except Exception as e:
                logger.error(f"Error saving property to database: {e}")
    
    def save_environment_to_db(self, data: Dict[str, Any]):
        """
        Save environmental data to database.
        
        Args:
            data: Environmental data dictionary
        """
        if not self.use_database:
            return
        
        try:
            # Flatten weather and air quality data
            env_data = {}
            
            if data.get('weather'):
                weather = data['weather']
                env_data['city'] = weather.get('city')
                env_data['state'] = weather.get('state')
                env_data['temperature'] = weather.get('temperature')
                env_data['feels_like'] = weather.get('feels_like')
                env_data['humidity'] = weather.get('humidity')
                env_data['pressure'] = weather.get('pressure')
                env_data['weather_description'] = weather.get('weather_description')
                env_data['wind_speed'] = weather.get('wind_speed')
                env_data['visibility'] = weather.get('visibility')
            
            if data.get('air_quality'):
                air = data['air_quality']
                env_data['aqi'] = air.get('aqi')
                env_data['co'] = air.get('co')
                env_data['no2'] = air.get('no2')
                env_data['o3'] = air.get('o3')
                env_data['pm2_5'] = air.get('pm2_5')
                env_data['pm10'] = air.get('pm10')
            
            self.db.save_environment(env_data)
            logger.info(f"Saved environment data for {env_data.get('city')} to database")
        except Exception as e:
            logger.error(f"Error saving environment to database: {e}")

    def save_combined_data(self, env_data: Dict[str, Any], 
                          prop_data: Dict[str, Any], 
                          city: str, state: str) -> str:
        """
        Save combined environmental and property data.
        
        Args:
            env_data: Environmental data
            prop_data: Property data
            city: City name
            state: State code
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"combined_{city}_{state}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # Get properties DataFrame
        properties = prop_data.get('properties', [])
        if not properties:
            logger.warning("No properties to combine with environment data")
            return ""
        
        df_props = pd.DataFrame(properties)
        
        # Add environmental data as columns to each property
        if env_data.get('weather'):
            for key, value in env_data['weather'].items():
                df_props[f'weather_{key}'] = value
        
        if env_data.get('air_quality'):
            for key, value in env_data['air_quality'].items():
                df_props[f'air_quality_{key}'] = value
        
        # Add timestamp
        df_props['scrape_timestamp'] = timestamp
        
        # Save
        df_props.to_csv(filepath, index=False)
        
        logger.info(f"Combined data saved to {filepath}")
        return filepath