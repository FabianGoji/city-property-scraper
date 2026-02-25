"""
Handles data storage and export functionality.
"""

import pandas as pd
import os
from typing import Dict, Any
from datetime import datetime
import logging

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