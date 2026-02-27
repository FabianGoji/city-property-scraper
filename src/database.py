"""
Database module for storing scraped data in SQLite.
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()


class Property(Base):
    """Property listing model."""
    __tablename__ = 'properties'
    
    id = Column(Integer, primary_key=True)
    property_id = Column(String, unique=True, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    address = Column(String)
    neighborhood = Column(String)
    property_type = Column(String)
    price = Column(Integer)
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    sqft = Column(Integer)
    year_built = Column(Integer)
    lot_size = Column(Integer)
    scrape_timestamp = Column(DateTime, default=datetime.utcnow)


class Environment(Base):
    """Environmental data model."""
    __tablename__ = 'environment'
    
    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    temperature = Column(Float)
    feels_like = Column(Float)
    humidity = Column(Integer)
    pressure = Column(Integer)
    weather_description = Column(String)
    wind_speed = Column(Float)
    visibility = Column(Integer)
    aqi = Column(Integer)
    co = Column(Float)
    no2 = Column(Float)
    o3 = Column(Float)
    pm2_5 = Column(Float)
    pm10 = Column(Float)
    scrape_timestamp = Column(DateTime, default=datetime.utcnow)


class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, db_path='data/scraper.db'):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        # Ensure data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Create engine
        self.engine = create_engine(f'sqlite:///{db_path}')
        
        # Create tables
        Base.metadata.create_all(self.engine)
        
        # Create session factory
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def save_property(self, property_data):
        """
        Save property to database.
        
        Args:
            property_data: Dictionary with property information
        """
        prop = Property(**property_data)
        self.session.add(prop)
        self.session.commit()
    
    def save_environment(self, env_data):
        """
        Save environmental data to database.
        
        Args:
            env_data: Dictionary with environmental information
        """
        env = Environment(**env_data)
        self.session.add(env)
        self.session.commit()
    
    def get_properties_by_city(self, city, state):
        """
        Get all properties for a city.
        
        Args:
            city: City name
            state: State code
            
        Returns:
            List of Property objects
        """
        return self.session.query(Property).filter_by(
            city=city, 
            state=state
        ).all()
    
    def get_latest_environment(self, city, state):
        """
        Get latest environmental data for a city.
        
        Args:
            city: City name
            state: State code
            
        Returns:
            Environment object or None
        """
        return self.session.query(Environment).filter_by(
            city=city,
            state=state
        ).order_by(Environment.scrape_timestamp.desc()).first()
    
    def close(self):
        """Close database connection."""
        self.session.close()