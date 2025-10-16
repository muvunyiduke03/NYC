import os
import io
from flask import Flask, g, jsonify, request, send_file
from dotenv import load_dotenv
from data_processing.data_processor import NYCTaxiDataProcessor
from data_processing.taxi_trip_db import TaxiTripDatabase
from trip_api import trip_api
import pandas as pd
from flask_cors import CORS

load_dotenv()

def create_app():
    app = Flask(__name__)
    CORS(app)

    app.config['db_config'] = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME'),
        'schema_file': os.getenv('SCHEMA_FILE')
    }

    app.config['data_file'] = os.getenv('DATA_FILE', 'train.csv')

    app.register_blueprint(trip_api)

    @app.before_request
    def get_db():
        if 'db' not in g:
            g.db = TaxiTripDatabase(**app.config['db_config'])
            g.db.connect()

    @app.teardown_appcontext
    def close_db_connection(exception=None):
        db = g.pop('db', None)
        if db is not None:
            db.close()

    @app.route('/api/metrics')
    def metrics():
        """Return key performances indicators for dashboard."""
        args = request.args
        db = g.db

        query = "SELECT * FROM trips"
        df = db.query_to_df(query)

        #filtering
        if args.get("start"):
            df = df[df["pickup_datetime"] >= args.get("start")]
        if args.get("end"):
            df = df[df["pickup_datetime"] <= args.get("end")]
        if args.get("vendor_id"):
            df = df[df["vendorID"] == int(args.get("vendor_id"))]
        
        total_trips = len(df)
        total_distance = round(df["trip_distance"].sum(), 2)
        avg_fare = round(df["fare_amount"].mean(), 2)
        avg_trip_time = 20

        # Time
        ts = (
            df.groupby(df["pickup_datetime"].dt.date)["pickup_datetime"]
            .count()
            .reset_index(name="trips")
        )
        time_series = [
            {"date": str(row["pickup_datetime"]), "trips": int(row["trips"])}
            for _, row in ts.iterrows()
        ]

        # Borough aggregate
        by_borough = (
            df.groupby("pickup_borough")["pickup_borough"]
            .count()
            .reset_index(name="trips")
        ).to_dict(orient="records")

        return jsonify({
            "totalTrips": total_trips,
            "totalDistanceKm": total_distance,
            "avgFare": avg_fare,
            "avgTripTimeMin": avg_trip_time,
            "timeSeries": time_series,
            "byBorough": by_borough
        })
    
    @app.route('/api/trips')
    def trips():
        """Paging trip table."""
        db = g.db
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 50))
        query = f"SELECT * FROM trips ORDER BY pickup_datetime DESC LIMIT{limit} OFFSET {offset}"
        df = db.query_to_df(query)
        total = db.query_to_df("SELECT COUNT(*) AS total FROM trips")["total"][0]
        return jsonify({"rows": df.to_dict(orient='records'), "total": int(total)})
    
    @app.route('/api/geo/heatmap')
    def heatmap():
        """Return pickup coordinates for leaflet."""
        db = g.db
        query = "SELECT pickup_latitude AS lat, pickup_longitude AS lng FROM trips LIMIT 8000"
        df = db.query_to_df(query)
        heat = [[row.lat, row.lng, 1] for row in df.itertuples()]
        return jsonify(heat)
    
    @app.route('/api/export.csv')
    def export_csv():
        """Export all trips as CSV"""
        db = g.db
        query = "SELECT * FROM trips"
        df = db.query_to_df(query)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            io.BytesIO(buf.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="export.csv"
        )
    
    @app.route('/')
    def hello_world():
        return 'NYC Mobility Dashboard API is running'

    @app.cli.command('process-data')
    def process_data_command():
        print("Starting data processing pipeline...")
        data_pipeline = NYCTaxiDataProcessor()
        db = TaxiTripDatabase(**app.config['db_config'])
        db.connect()
        try:
            data_pipeline.process(db, app.config['data_file'])
        finally:
            db.close()
        print("Data processing complete!")

    return app


app = create_app()

if __name__ == '__main__':
    app.run(debug=True)