import logging
import math
from datetime import datetime
from typing import Dict, Any
from data_processing.taxi_trip_db import TaxiTripDatabase
from data_processing.spatial_index import SpatialGridIndex
from data_processing.quick_select import QuickSelect
import pandas as pd
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NYCTaxiDataProcessor:
    NYC_BOUNDS = {
        'lat_min': 40.493325,
        'lat_max': 40.916046,
        'lon_min': -74.269855,
        'lon_max': -73.687826
    }

    def __init__(self):
        self.raw_data = None
        self.clean_data = None
        self.excluded_records = []
        self.spatial_index = SpatialGridIndex()
        self.processing_stats = {
            'total_records': 0,
            'excluded_records': 0,
            'exclusion_reasons': {}
        }

    def load_data(self, filepath: str) -> pd.DataFrame:
        logger.info(f"Loading data from {filepath}")
        try:
            self.raw_data = pd.read_csv(filepath)
            self.processing_stats['total_records'] = len(self.raw_data)
            logger.info(f"Loaded {len(self.raw_data)} records")
            return self.raw_data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def _log_exclusion(self, index: int, reason: str, details: Dict = None):
        """Log excluded records for transparency"""
        self.excluded_records.append({
            'index': index,
            'reason': reason,
            'details': details or {},
            'timestamp': datetime.now().isoformat()
        })

        # Update statistics
        if reason not in self.processing_stats['exclusion_reasons']:
            self.processing_stats['exclusion_reasons'][reason] = 0
        self.processing_stats['exclusion_reasons'][reason] += 1
        self.processing_stats['excluded_records'] += 1

    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        return (self.NYC_BOUNDS['lat_min'] <= lat <= self.NYC_BOUNDS['lat_max'] and
                self.NYC_BOUNDS['lon_min'] <= lon <= self.NYC_BOUNDS['lon_max'])

    @staticmethod
    def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371  # Earth's radius in kilometers

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) *
             math.sin(delta_lon / 2) ** 2)

        c = 2 * math.asin(math.sqrt(a))

        return R * c

    def clean_dataset(self) -> pd.DataFrame:
        """Comprehensive data cleaning pipeline"""
        logger.info("Starting data cleaning process")

        if self.raw_data is None:
            raise ValueError("No data loaded. Call load_data() first.")

        df = self.raw_data.copy()
        initial_count = len(df)

        # 1. Handle missing values
        logger.info("Handling missing values...")
        missing_mask = df.isnull().any(axis=1)
        for idx in df[missing_mask].index:
            self._log_exclusion(idx, "missing_values",
                                {'missing_columns': df.loc[idx].isnull().sum()})
        df = df.dropna()
        logger.info(f"Removed {missing_mask.sum()} records with missing values")

        # 2. Convert datetime columns
        logger.info("Processing datetime fields...")
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce')

        invalid_datetime_mask = df['pickup_datetime'].isnull() | df['dropoff_datetime'].isnull()
        for idx in df[invalid_datetime_mask].index:
            self._log_exclusion(idx, "invalid_datetime")
        df = df[~invalid_datetime_mask]

        # 3. Validate trip duration
        logger.info("Validating trip durations...")
        df['calculated_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds()

        # Remove negative durations
        negative_duration_mask = df['trip_duration'] <= 0
        for idx in df[negative_duration_mask].index:
            self._log_exclusion(idx, "negative_duration",
                                {'duration': float(df.loc[idx, 'trip_duration'])})
        df = df[~negative_duration_mask]

        # Detect outliers using custom QuickSelect algorithm
        durations = df['trip_duration'].tolist()
        p99 = QuickSelect.find_percentile(durations, 0.99)
        p01 = QuickSelect.find_percentile(durations, 0.01)

        logger.info(f"Duration outlier thresholds: P1={p01:.2f}s, P99={p99:.2f}s")

        duration_outlier_mask = (df['trip_duration'] < p01) | (df['trip_duration'] > p99)
        for idx in df[duration_outlier_mask].index:
            self._log_exclusion(idx, "duration_outlier",
                                {'duration': float(df.loc[idx, 'trip_duration']),
                                 'p01': p01, 'p99': p99})
        df = df[~duration_outlier_mask]

        # 4. Validate coordinates
        logger.info("Validating geographical coordinates...")
        coordinate_columns = ['pickup_latitude', 'pickup_longitude',
                              'dropoff_latitude', 'dropoff_longitude']

        for col in coordinate_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        invalid_coords_mask = df[coordinate_columns].isnull().any(axis=1)
        for idx in df[invalid_coords_mask].index:
            self._log_exclusion(idx, "invalid_coordinates")
        df = df[~invalid_coords_mask]

        # Validate NYC boundaries
        valid_pickup = df.apply(
            lambda row: self._validate_coordinates(
                row['pickup_latitude'], row['pickup_longitude']
            ), axis=1
        )
        valid_dropoff = df.apply(
            lambda row: self._validate_coordinates(
                row['dropoff_latitude'], row['dropoff_longitude']
            ), axis=1
        )

        out_of_bounds_mask = ~(valid_pickup & valid_dropoff)
        for idx in df[out_of_bounds_mask].index:
            self._log_exclusion(idx, "out_of_nyc_bounds", {
                'pickup_lat': float(df.loc[idx, 'pickup_latitude']),
                'pickup_lon': float(df.loc[idx, 'pickup_longitude'])
            })
        df = df[valid_pickup & valid_dropoff]

        # 5. Validate passenger count
        logger.info("Validating passenger counts...")
        df['passenger_count'] = pd.to_numeric(df['passenger_count'], errors='coerce')
        invalid_passengers_mask = (df['passenger_count'] < 1) | (df['passenger_count'] > 6)
        for idx in df[invalid_passengers_mask].index:
            self._log_exclusion(idx, "invalid_passenger_count",
                                {'count': float(df.loc[idx, 'passenger_count'])})
        df = df[~invalid_passengers_mask]

        # 6. Remove duplicates
        logger.info("Removing duplicate records...")
        duplicate_mask = df.duplicated(subset=['pickup_datetime', 'pickup_latitude',
                                               'pickup_longitude', 'trip_duration'])
        for idx in df[duplicate_mask].index:
            self._log_exclusion(idx, "duplicate_record")
        df = df[~duplicate_mask]

        self.clean_data = df.reset_index(drop=True)

        logger.info(f"Cleaning complete: {initial_count} -> {len(self.clean_data)} records")
        logger.info(f"Excluded: {self.processing_stats['excluded_records']} records")
        logger.info(f"Exclusion breakdown: {self.processing_stats['exclusion_reasons']}")

        return self.clean_data

    def derived_features(self) -> pd.DataFrame:
        logger.info("derived features...")

        if self.clean_data is None:
            raise ValueError("No cleaned data. Call clean_data() first.")

        df = self.clean_data.copy()

        # Feature 1: Trip Distance (using custom Haversine implementation)
        logger.info("Calculating trip distances using Haversine formula...")
        df['trip_distance_km'] = df.apply(
            lambda row: self._haversine_distance(
                row['pickup_latitude'], row['pickup_longitude'],
                row['dropoff_latitude'], row['dropoff_longitude']
            ), axis=1
        )

        # Feature 2: Trip Speed (km/h)
        logger.info("Calculating average trip speeds...")
        df['trip_speed_kmh'] = (df['trip_distance_km'] / df['trip_duration']) * 3600

        # Remove impossible speeds (over 120 km/h in NYC traffic or under 1 km/h)
        speed_outliers = (df['trip_speed_kmh'] > 120) | (df['trip_speed_kmh'] < 1)
        for idx in df[speed_outliers].index:
            self._log_exclusion(idx, "impossible_speed",
                                {'speed_kmh': float(df.loc[idx, 'trip_speed_kmh'])})
        df = df[~speed_outliers]

        # Feature 3: Temporal Features
        logger.info("Extracting temporal features...")
        df['hour_of_day'] = df['pickup_datetime'].dt.hour
        df['day_of_week'] = df['pickup_datetime'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        df['month'] = df['pickup_datetime'].dt.month

        # Feature 4: Distance Efficiency (actual duration vs expected from distance)
        # Assuming average city speed of 20 km/h
        df['expected_duration_min'] = (df['trip_distance_km'] / 20) * 60
        df['actual_duration_min'] = df['trip_duration'] / 60
        df['efficiency_ratio'] = df['expected_duration_min'] / df['actual_duration_min']

        # Feature 5: Trip distance categories
        df['distance_category'] = pd.cut(
            df['trip_distance_km'],
            bins=[0, 2, 5, 10, float('inf')],
            labels=['short', 'medium', 'long', 'very_long']
        )

        # Build spatial index for pickup locations
        logger.info("Building spatial index for pickup locations...")
        for idx, row in df.iterrows():
            self.spatial_index.insert(
                row['pickup_latitude'],
                row['pickup_longitude'],
                {
                    'id': row['id'],
                    'datetime': row['pickup_datetime'],
                    'passengers': row['passenger_count']
                }
            )

        spatial_stats = self.spatial_index.get_statistics()
        logger.info(f"Spatial index statistics: {spatial_stats}")

        self.clean_data = df

        logger.info(f"Feature engineering complete. Added columns: {df.columns.tolist()}")

        return self.clean_data

    def get_data_summary(self) -> Dict[str, Any]:

        if self.clean_data is None:
            return {}

        df = self.clean_data

        summary = {
            'record_count': len(df),
            'date_range': {
                'start': df['pickup_datetime'].min().isoformat(),
                'end': df['pickup_datetime'].max().isoformat()
            },
            'trip_duration': {
                'mean': float(df['trip_duration'].mean()),
                'median': float(df['trip_duration'].median()),
                'std': float(df['trip_duration'].std())
            },
            'trip_distance': {
                'mean': float(df['trip_distance_km'].mean()),
                'median': float(df['trip_distance_km'].median()),
                'std': float(df['trip_distance_km'].std())
            },
            'trip_speed': {
                'mean': float(df['trip_speed_kmh'].mean()),
                'median': float(df['trip_speed_kmh'].median()),
                'std': float(df['trip_speed_kmh'].std())
            },
            'passenger_distribution': df['passenger_count'].value_counts().to_dict(),
            'spatial_index': self.spatial_index.get_statistics(),
            'processing_stats': self.processing_stats
        }

        return summary

    def save_excluded_records(self, filepath: str = 'excluded_records.json'):
        """Save excluded records log for transparency"""
        logger.info(f"Saving excluded records to {filepath}")
        with open(filepath, 'w') as f:
            json.dump({
                'summary': self.processing_stats,
                'records': self.excluded_records
            }, f, indent=2)

    def process(self, db: TaxiTripDatabase = None, filepath: str = 'train.csv'):
        print("=" * 80)
        print("NYC TAXI TRIP DATA PROCESSING PIPELINE")
        print("=" * 80)

        print("\n[1/5] Loading raw data...")
        try:
            self.load_data(filepath)
        except FileNotFoundError:
            print(f"ERROR: {filepath} not found. Please place the dataset in the same directory.")
            print("Download from: NYC Taxi Trip Dataset")
            return

        print("\n[2/5] Cleaning data...")
        self.clean_dataset()

        print("\n[3/5] Derived features...")
        self.derived_features()

        print("\n[4/5] Saving excluded records log...")
        self.save_excluded_records()

        print("\n[5/5] Inserting data into database...")
        db.create_schema(db.schema_file)
        db.insert_data(self.clean_data, self)

        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE!")
        print("=" * 80)
        print(f"✓ Cleaned data: {len(self.clean_data)} records")
        print(f"✓ Excluded records log: excluded_records.json")
        print(f"✓ Processing log: data_processing.log")