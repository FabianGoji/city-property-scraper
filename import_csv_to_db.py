"""
Import existing CSV files into the database.
"""

import pandas as pd
from src.database import DatabaseManager
import sys

def import_properties_csv(csv_path, db):
    """Import properties from CSV to database."""
    df = pd.read_csv(csv_path)
    
    for _, row in df.iterrows():
        property_data = {
            'property_id': row['property_id'],
            'city': row['city'],
            'state': row['state'],
            'address': row['address'],
            'neighborhood': row['neighborhood'],
            'property_type': row['property_type'],
            'price': int(row['price']),
            'bedrooms': int(row['bedrooms']),
            'bathrooms': int(row['bathrooms']),
            'sqft': int(row['sqft']),
            'year_built': int(row['year_built']),
            'lot_size': int(row['lot_size'])
        }
        
        try:
            db.save_property(property_data)
            print(f"✓ Imported {property_data['property_id']}")
        except Exception as e:
            if "UNIQUE constraint" in str(e):
                print(f"⊘ Skipped {property_data['property_id']} (already exists)")
            else:
                print(f"✗ Error importing {property_data['property_id']}: {e}")
            # Rollback and continue
            db.session.rollback()

def import_environment_csv(csv_path, db):
    """Import environment data from CSV to database."""
    df = pd.read_csv(csv_path)
    
    # Get first row (environment data is same for all rows)
    row = df.iloc[0]
    
    env_data = {
        'city': row['weather_city'],
        'state': row['weather_state'],
        'temperature': float(row['weather_temperature']),
        'feels_like': float(row['weather_feels_like']),
        'humidity': int(row['weather_humidity']),
        'pressure': int(row['weather_pressure']),
        'weather_description': row['weather_weather_description'],
        'wind_speed': float(row['weather_wind_speed']),
        'visibility': int(row['weather_visibility']),
        'aqi': int(row['air_quality_aqi']),
        'co': float(row['air_quality_co']),
        'no2': float(row['air_quality_no2']),
        'o3': float(row['air_quality_o3']),
        'pm2_5': float(row['air_quality_pm2_5']),
        'pm10': float(row['air_quality_pm10'])
    }
    
    try:
        db.save_environment(env_data)
        print(f"✓ Imported environment data for {env_data['city']}, {env_data['state']}")
    except Exception as e:
        print(f"✗ Error importing environment data: {e}")
        db.session.rollback()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_csv_to_db.py data/combined_City_ST_timestamp.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    print(f"Importing from {csv_path}...")
    
    db = DatabaseManager()
    
    # Import both properties and environment from combined CSV
    import_properties_csv(csv_path, db)
    import_environment_csv(csv_path, db)
    
    db.close()
    print("Import complete!")