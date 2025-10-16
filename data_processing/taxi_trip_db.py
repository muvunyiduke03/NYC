import logging
from datetime import datetime
from typing import Dict, Any

import mysql.connector
import pandas as pd
from mysql.connector import Error

logger = logging.getLogger(__name__)


class TaxiTripDatabase:

    def __init__(self, host: str = 'localhost', user: str = 'root', password: str = '', database: str = 'nyc_trip', schema_file='nyc_trip.sql'):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.schema_file = schema_file
        self.connection = None
        self.cursor = None

    def connect(self) -> bool | None:
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                use_unicode=True
            )

            if self.connection.is_connected():
                self.cursor = self.connection.cursor()
                db_info = self.connection.get_server_info()
                logger.info(f"Connected to MySQL Server version {db_info}")
                logger.info(f"Connected to database: {self.database}")
                return True

        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return False

    def create_schema(self, schema_file: str = 'nyc_trip.sql'):
        try:
            logger.info("Creating database schema...")

            with open(schema_file, 'r') as f:
                sql_script = f.read()

            statements = sql_script.split(';')
            for statement in statements:
                statement = statement.strip()
                if statement and not statement.startswith('--') and not statement.startswith('/*'):
                    try:
                        self.cursor.execute(statement)
                    except Error as e:
                        if 'Unknown table' not in str(e):
                            logger.warning(f"Statement execution warning: {e}")

            self.connection.commit()
            logger.info("Database schema created successfully")

        except FileNotFoundError:
            logger.error("Schema file 'nyc_trip.sql' not found")
            raise
        except Error as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def insert_trips_batch(self, df: pd.DataFrame, batch_size: int = 50000) -> int:
        trips = self.get_stats()

        if trips.get('total_trips') > 0:
            logger.warning("Trips table is not empty. Skipping trip insertion to avoid duplicates.")
            return 0

        insert_query = """
                       INSERT INTO trips (id, vendor_id, pickup_datetime, dropoff_datetime, \
                                          hour_of_day, day_of_week, is_weekend, month, \
                                          passenger_count, pickup_latitude, pickup_longitude, \
                                          dropoff_latitude, dropoff_longitude, \
                                          trip_duration, calculated_duration, trip_distance_km, \
                                          trip_speed_kmh, distance_category, \
                                          expected_duration_min, actual_duration_min, efficiency_ratio, \
                                          store_and_fwd_flag) \
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                       """

        total_inserted = 0
        total_rows = len(df)

        try:
            logger.info(f"Inserting {total_rows} trip records in batches of {batch_size}...")

            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]

                batch_data = []
                for _, row in batch_df.iterrows():
                    record = (
                        str(row['id']),
                        int(row['vendor_id']),
                        row['pickup_datetime'].to_pydatetime(),
                        row['dropoff_datetime'].to_pydatetime(),
                        int(row['hour_of_day']),
                        int(row['day_of_week']),
                        int(row['is_weekend']),
                        int(row['month']),
                        int(row['passenger_count']),
                        float(row['pickup_latitude']),
                        float(row['pickup_longitude']),
                        float(row['dropoff_latitude']),
                        float(row['dropoff_longitude']),
                        int(row['trip_duration']),
                        int(row['calculated_duration']) if pd.notna(row['calculated_duration']) else None,
                        float(row['trip_distance_km']),
                        float(row['trip_speed_kmh']),
                        str(row['distance_category']),
                        float(row['expected_duration_min']) if pd.notna(row['expected_duration_min']) else None,
                        float(row['actual_duration_min']) if pd.notna(row['actual_duration_min']) else None,
                        float(row['efficiency_ratio']) if pd.notna(row['efficiency_ratio']) else None,
                        str(row.get('store_and_fwd_flag', 'N'))
                    )
                    batch_data.append(record)

                self.cursor.executemany(insert_query, batch_data)
                self.connection.commit()

                total_inserted += len(batch_data)
                logger.info(f"Progress: {total_inserted}/{total_rows} records inserted "
                            f"({(total_inserted / total_rows) * 100:.1f}%)")

            logger.info(f"Successfully inserted {total_inserted} trip records")
            return total_inserted

        except Error as e:
            logger.error(f"Error inserting trip records: {e}")
            self.connection.rollback()
            raise

    def insert_spatial_grid(self, df: pd.DataFrame, spatial_index) -> int:

        cells = self.get_stats()

        if cells.get('total_grid_cells') > 0:
            logger.warning("Spatial grid cells table is not empty. Skipping spatial grid insertion to avoid duplicates.")
            return 0

        insert_query = """
                       INSERT INTO spatial_grid_cells (cell_x, cell_y, lat_min, lat_max, lon_min, lon_max,
                                                       total_passengers,
                                                       avg_trip_duration, avg_trip_distance, peak_hour, weekend_ratio)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY \
                       UPDATE \
                        total_passengers = \
                       VALUES (total_passengers), avg_trip_duration = \
                       VALUES (avg_trip_duration), avg_trip_distance = \
                       VALUES (avg_trip_distance), peak_hour = \
                       VALUES (peak_hour), weekend_ratio = \
                       VALUES (weekend_ratio)
                       """

        try:
            logger.info("Calculating spatial grid statistics...")

            trip_lookup = df.set_index('id').to_dict('index')

            grid_data = []
            for cell_key, points in spatial_index.grid.items():
                cell_x, cell_y = cell_key
                bounds = spatial_index.get_cell_bounds(cell_key)
                min_lat, max_lat, min_lon, max_lon = bounds

                cell_trips = []
                for point in points:
                    trip_id = point['id']
                    if trip_id in trip_lookup:
                        cell_trips.append(trip_lookup[trip_id])

                if not cell_trips:
                    continue

                total_passengers = sum(trip.get('passenger_count', 0) for trip in cell_trips)

                durations = [trip.get('trip_duration', 0) / 60 for trip in cell_trips if trip.get('trip_duration')]
                avg_trip_duration = sum(durations) / len(durations) if durations else None

                distances = [trip.get('trip_distance_km', 0) for trip in cell_trips if trip.get('trip_distance_km')]
                avg_trip_distance = sum(distances) / len(distances) if distances else None

                hours = [trip.get('hour_of_day') for trip in cell_trips if trip.get('hour_of_day') is not None]
                if hours:
                    peak_hour = max(set(hours), key=hours.count)
                else:
                    peak_hour = None

                weekend_trips = sum(1 for trip in cell_trips if trip.get('is_weekend', 0) == 1)
                weekend_ratio = weekend_trips / len(cell_trips) if cell_trips else None

                record = (
                    cell_x,
                    cell_y,
                    float(min_lat),
                    float(max_lat),
                    float(min_lon),
                    float(max_lon),
                    total_passengers,
                    float(avg_trip_duration) if avg_trip_duration is not None else None,
                    float(avg_trip_distance) if avg_trip_distance is not None else None,
                    int(peak_hour) if peak_hour is not None else None,
                    float(weekend_ratio) if weekend_ratio is not None else None
                )
                grid_data.append(record)

            if grid_data:
                logger.info(f"Inserting {len(grid_data)} spatial grid cells...")
                self.cursor.executemany(insert_query, grid_data)
                self.connection.commit()
                logger.info(f"Successfully inserted {len(grid_data)} spatial grid cells")
                return len(grid_data)
            else:
                logger.warning("No spatial grid data to insert")
                return 0

        except Error as e:
            logger.error(f"Error inserting spatial grid: {e}")
            self.connection.rollback()
            raise

    def insert_excluded_records(self, excluded_records: list) -> int:

        records = self.get_stats()
        if records.get('total_excluded') > 0:
            logger.warning("Excluded records table is not empty. Skipping excluded records insertion to avoid duplicates.")
            return 0

        insert_query = """
                       INSERT INTO excluded_records (original_index, exclusion_reason, exclusion_timestamp, details) \
                       VALUES (%s, %s, %s, %s) \
                       """

        try:
            logger.info(f"Inserting {len(excluded_records)} excluded records...")

            batch_data = []
            for record in excluded_records:
                import json
                data = (
                    int(record['index']),
                    str(record['reason']),
                    datetime.fromisoformat(record['timestamp']),
                    json.dumps(record['details'])
                )
                batch_data.append(data)

            if batch_data:
                self.cursor.executemany(insert_query, batch_data)
                self.connection.commit()
                logger.info(f"Successfully inserted {len(batch_data)} excluded records")
                return len(batch_data)
            else:
                return 0

        except Error as e:
            logger.error(f"Error inserting excluded records: {e}")
            self.connection.rollback()
            raise

    def insert_data(self, df: pd.DataFrame, processor) -> Dict[str, int]:
        summary = {}

        try:
            trips_inserted = self.insert_trips_batch(df)
            summary['trips'] = trips_inserted

            spatial_inserted = self.insert_spatial_grid(df, processor.spatial_index)
            summary['spatial_grid_cells'] = spatial_inserted

            excluded_inserted = self.insert_excluded_records(processor.excluded_records)
            summary['excluded_records'] = excluded_inserted

            logger.info(f"Database insertion complete: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Error during data insertion: {e}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        try:
            stats = {}

            self.cursor.execute("SELECT COUNT(*) FROM trips")
            stats['total_trips'] = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(*) FROM excluded_records")
            stats['total_excluded'] = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(*) FROM spatial_grid_cells")
            stats['total_grid_cells'] = self.cursor.fetchone()[0]

            self.cursor.execute("""
                                SELECT MIN(pickup_datetime), MAX(pickup_datetime)
                                FROM trips
                                """)
            min_date, max_date = self.cursor.fetchone()
            stats['date_range'] = {
                'start': min_date.isoformat() if min_date else None,
                'end': max_date.isoformat() if max_date else None
            }

            return stats

        except Error as e:
            logger.error(f"Error getting database stats: {e}")
            return {}

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection and self.connection.is_connected():
                self.connection.close()
                logger.info("MySQL connection closed")
        except Error as e:
            logger.error(f"Error closing connection: {e}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()