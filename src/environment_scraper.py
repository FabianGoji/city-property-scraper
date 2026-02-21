"""
Scraper for environmental data (weather, air quality).
Uses OpenWeatherMap API (free tier).
"""

import requests
from typing import Dict, Any, Optional
from .scraper import BaseScraper


class EnvironmentScraper(BaseScraper):
    """
    Scrapes environmental data for a given city.
    
    Uses OpenWeatherMap API for weather and air quality data.
    """
    
    def __init__(self, api_key: str):
        """
        Initialize environment scraper.
        
        Args:
            api_key: OpenWeatherMap API key
        """
        super().__init__(base_url="https://api.openweathermap.org/data/2.5")
        self.api_key = api_key
    
    def get_weather_data(self, city: str, state: str) -> Optional[Dict[str, Any]]:
        """
        Get current weather data for a city.
        
        Args:
            city: City name
            state: State code (e.g., "TX")
            
        Returns:
            Dictionary with weather data or None if failed
        """
        try:
            # Build query string
            location = f"{city},{state},US"
            url = f"{self.base_url}/weather"
            
            params = {
                'q': location,
                'appid': self.api_key,
                'units': 'imperial'  # Fahrenheit
            }
            
            self.logger.info(f"Fetching weather for {location}")
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract relevant information
            weather_data = {
                'city': city,
                'state': state,
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'weather_description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'visibility': data.get('visibility', 'N/A')
            }
            
            self.logger.info(f"Successfully fetched weather for {location}")
            return weather_data
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching weather data: {e}")
            return None
        except (KeyError, IndexError) as e:
            self.logger.error(f"Error parsing weather data: {e}")
            return None
    
    def get_air_quality(self, city: str, state: str) -> Optional[Dict[str, Any]]:
        """
        Get air quality data for a city.
        
        Args:
            city: City name
            state: State code
            
        Returns:
            Dictionary with air quality data or None if failed
        """
        try:
            # First get coordinates
            location = f"{city},{state},US"
            geo_url = "http://api.openweathermap.org/geo/1.0/direct"
            
            geo_params = {
                'q': location,
                'limit': 1,
                'appid': self.api_key
            }
            
            geo_response = requests.get(geo_url, params=geo_params, timeout=self.timeout)
            geo_response.raise_for_status()
            geo_data = geo_response.json()
            
            if not geo_data:
                self.logger.error(f"Could not find coordinates for {location}")
                return None
            
            lat = geo_data[0]['lat']
            lon = geo_data[0]['lon']
            
            # Get air quality
            aqi_url = f"{self.base_url}/air_pollution"
            aqi_params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key
            }
            
            self.logger.info(f"Fetching air quality for {location}")
            aqi_response = requests.get(aqi_url, params=aqi_params, timeout=self.timeout)
            aqi_response.raise_for_status()
            
            aqi_data = aqi_response.json()
            
            # Extract air quality info
            components = aqi_data['list'][0]['components']
            air_quality = {
                'city': city,
                'state': state,
                'aqi': aqi_data['list'][0]['main']['aqi'],  # 1-5 scale
                'co': components['co'],      # Carbon monoxide
                'no2': components['no2'],    # Nitrogen dioxide
                'o3': components['o3'],      # Ozone
                'pm2_5': components['pm2_5'], # Fine particulate matter
                'pm10': components['pm10']    # Coarse particulate matter
            }
            
            self.logger.info(f"Successfully fetched air quality for {location}")
            return air_quality
            
        except requests.RequestException as e:
            self.logger.error(f"Error fetching air quality data: {e}")
            return None
        except (KeyError, IndexError) as e:
            self.logger.error(f"Error parsing air quality data: {e}")
            return None
    
    def scrape(self, city: str, state: str) -> Dict[str, Any]:
        """
        Scrape all environmental data for a city.
        
        Args:
            city: City name
            state: State code
            
        Returns:
            Dictionary containing all environmental data
        """
        self.logger.info(f"Starting environmental data scrape for {city}, {state}")
        
        weather = self.get_weather_data(city, state)
        air_quality = self.get_air_quality(city, state)
        
        # Combine data
        result = {
            'weather': weather,
            'air_quality': air_quality
        }
        
        self.logger.info("Environmental data scrape completed")
        return result