import os
from flask import Flask, g
from dotenv import load_dotenv
from data_processing.data_processor import NYCTaxiDataProcessor
from data_processing.taxi_trip_db import TaxiTripDatabase
from trip_api import trip_api

load_dotenv()

def create_app():
    app = Flask(__name__)

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

    @app.route('/')
    def hello_world():
        return 'Hello World!'

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
    app.run()