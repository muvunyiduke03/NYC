import os
from flask import Flask
from dotenv import load_dotenv
from data_processing.data_processor import NYCTaxiDataProcessor
from data_processing.taxi_trip_db import TaxiTripDatabase

load_dotenv()

def create_app():
    app = Flask(__name__)

    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME'),
        'schema_file': os.getenv('SCHEMA_FILE')
    }

    data_file = os.getenv('DATA_FILE', 'train.csv')

    data_pipeline = NYCTaxiDataProcessor()
    db = TaxiTripDatabase(**db_config)

    with app.app_context():
        db.connect()
        data_pipeline.process(db, data_file)

    @app.route('/')
    def hello_world():
        return 'Hello World!'

    @app.teardown_appcontext
    def close_db_connection(exception=None):
        if db is not None:
            db.close()

    return app


app = create_app()

if __name__ == '__main__':
    app.run()