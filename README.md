# NYC Taxi Trip Flask API

A Flask-based REST API for querying and analyzing NYC taxi trip data.

## Prerequisites

- Python 3.12 or higher
- MySQL server running
- Required Python packages:
  - Flask
  - pandas
  - numpy
  - mysql-connector-python
  - python-dotenv

## Setup Instructions

### 1. Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate 
 or on windows
venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install Flask pandas numpy mysql-connector-python python-dotenv
```

### 3. Configure Environment Variables

Create a `.env` file in the project root with your database configuration:

```env
DB_HOST=localhost
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=taxi_trips
SCHEMA_FILE=data_processing/nyc_trip.sql
DATA_FILE=data_processing/train.csv
```

### 4. First-Time Initialization: Process Data

**Important:** Before running the API for the first time, you need to process and load the data into the database:

```bash
# Make sure your virtual environment is activated
source .venv/bin/activate

# Set Flask app and environment
export FLASK_APP=app.py
export FLASK_ENV=development
export FLASK_DEBUG=0

# Run the data processing command
python -m flask process-data
```

This command will:
- Create the database schema
- Load data from `train.csv`
- Process and transform the data
- Insert records into the MySQL database

## Running the Application

After completing the initial data processing, run the Flask application:

```bash
# Activate virtual environment
source .venv/bin/activate

# Set Flask environment variables (similar to PyCharm configuration)
export FLASK_APP=app.py
export FLASK_ENV=development
export FLASK_DEBUG=0

# Run the Flask app
python -m flask run
```

The application will start on `http://localhost:5000`

## API Endpoints

### 1. Get Trips

Retrieve taxi trip records with optional filtering.

**Endpoint:** `GET /api/trips`

**Query Parameters:**
- `start_date` (optional): Filter by start date (e.g., `2024-01-01`)
- `end_date` (optional): Filter by end date (e.g., `2024-01-31`)
- `hour_of_day` (optional): Filter by hour (0-23)
- `day_of_week` (optional): Filter by day of week (0-6)
- `is_weekend` (optional): Filter by weekend status (`true`/`false`)
- `distance_category` (optional): Filter by distance category (`short`, `medium`, `long`)
- `min_speed` (optional): Minimum trip speed in km/h
- `max_speed` (optional): Maximum trip speed in km/h
- `passenger_count` (optional): Number of passengers
- `limit` (optional): Number of results to return (default: 100)
- `offset` (optional): Offset for pagination (default: 0)

**Example Requests:**

```bash
# Get 10 trips
curl "http://localhost:5000/api/trips?limit=10"

# Get trips for a specific date range
curl "http://localhost:5000/api/trips?start_date=2024-01-01&end_date=2024-01-31&limit=50"

# Get medium-distance trips during rush hour
curl "http://localhost:5000/api/trips?hour_of_day=17&distance_category=medium&limit=20"
```

**Python Example:**

```python
import requests

response = requests.get('http://localhost:5000/api/trips', params={
    'start_date': '2024-01-01',
    'end_date': '2024-01-31',
    'distance_category': 'medium',
    'limit': 100
})

trips = response.json()
print(trips)
```

### 2. Get Trip Statistics

Retrieve aggregated statistics about taxi trips.

**Endpoint:** `GET /api/trips/statistics`

**Query Parameters:**
- `group_by` (required): Group results by `hour_of_day`, `day_of_week`, `month`, or `distance_category`
- `metrics` (optional, multiple): Metrics to calculate - `avg_speed`, `avg_duration`, `avg_distance`, `trip_count`
- `start_date` (optional): Filter by start date
- `end_date` (optional): Filter by end date

**Example Requests:**

```bash
# Get statistics grouped by hour of day
curl "http://localhost:5000/api/trips/statistics?group_by=hour_of_day&metrics=avg_speed&metrics=trip_count"

# Get statistics grouped by distance category with multiple metrics
curl "http://localhost:5000/api/trips/statistics?group_by=distance_category&metrics=avg_speed&metrics=avg_duration&metrics=trip_count"

# Get statistics for a specific date range
curl "http://localhost:5000/api/trips/statistics?start_date=2024-01-01&end_date=2024-01-31&group_by=day_of_week&metrics=trip_count"
```

**Python Example:**

```python
import requests

response = requests.get('http://localhost:5000/api/trips/statistics', params={
    'group_by': 'hour_of_day',
    'metrics': ['avg_speed', 'avg_duration', 'trip_count'],
    'start_date': '2024-01-01',
    'end_date': '2024-01-31'
})

statistics = response.json()
print(statistics)
```

## Project Structure

```
FlaskProject/
├── app.py                      # Main Flask application
├── trip_api.py                 # API route definitions
├── data_processing/
│   ├── data_processor.py       # Data processing logic
│   ├── taxi_trip_db.py         # Database operations
│   ├── spatial_index.py        # Spatial indexing utilities
│   ├── quick_select.py         # Quick select algorithm
│   ├── nyc_trip.sql            # Database schema
├── static/                     # Static files
├── templates/                  # HTML templates
├── .env                        # Environment configuration
└── README.md                   # This file
```

## Troubleshooting

### MySQL Connection Issues

If you encounter "MySQL Connection not available" errors:

1. Make sure MySQL server is running
2. Verify your `.env` file has correct database credentials
3. Check that the database exists and is accessible
4. Restart the Flask application

### Data Processing Issues

If data processing fails:

1. Ensure `train.csv` exists and you provided the correct path in `.env`
2. Check MySQL user has sufficient permissions to create tables
3. Verify the schema file path in `.env` is correct
4. Check logs in `data_processing.log` for detailed error messages