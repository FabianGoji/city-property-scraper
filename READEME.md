# City Property & Environment Data Scraper

A Python-based web scraper that collects property and environmental data for a specified city.

## Features
- Scrapes property listings from public sources
- Collects environmental data (air quality, weather)
- Exports data to CSV for analysis
- Object-oriented design for extensibility

## Tech Stack
- Python 3.x
- BeautifulSoup4 / Requests for web scraping
- Pandas for data handling
- CSV for data storage

## Setup
```bash
# Clone repository
git clone <your-repo-url>
cd city-property-scraper

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage
```bash
python main.py --city "Austin" --state "TX"
```

## Project Structure
```
city-property-scraper/
├── src/
│   ├── __init__.py
│   ├── scraper.py              # Base scraper class
│   ├── property_scraper.py     # Property data scraper
│   ├── environment_scraper.py  # Environmental data scraper
│   └── data_manager.py         # Data storage handler
├── data/                        # Output directory
├── tests/                       # Unit tests
├── main.py                     # Entry point
├── requirements.txt
└── README.md
```

## Roadmap
- [ ] Base scraper class
- [ ] Property data scraper
- [ ] Environmental data scraper
- [ ] Data export to CSV
- [ ] Error handling and logging
- [ ] Unit tests
- [ ] Command-line interface

## Author
Richard Camarillo

## License
MIT
```

### **Create requirements.txt**
```
requests==2.31.0
beautifulsoup4==4.12.2
pandas==2.1.3
python-dotenv==1.0.0
lxml==4.9.3