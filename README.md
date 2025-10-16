# NYC
# urban-mobility-data-explorer

## Running the Flask App

1. **Create and activate a virtual environment (recommended):**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies** (if not already):
   ```bash
   pip install flask python-dotenv pandas numpy mysql-connector-python
   ```

3. **Set environment variables**
   - Create a `.env` file in the project root with the following variables:
     ```env
     DB_HOST=your_db_host
     DB_USER=your_db_user
     DB_PASSWORD=your_db_password
     DB_NAME=your_db_name
     SCHEMA_FILE=path/to/nyc_trip.sql
     DATA_FILE=path/to/train.csv  # Optional, defaults to train.csv
     ```

4. **Run the app (PyCharm style)**
   - Set the following environment variables:
     ```bash
     export FLASK_APP=app.py
     export FLASK_ENV=development
     export FLASK_DEBUG=0
     ```
   - If using a virtual environment, ensure it is activated:
     ```bash
     source .venv/bin/activate
     ```
   - Then run:
     ```bash
     python -m flask run
     ```
   - Or, if you are in PyCharm, set these environment variables in your run configuration and use the IDE's run button.

5. **Access the app**
   - Open your browser and go to: [http://127.0.0.1:5000/](http://127.0.0.1:5000/)

## Notes
- Ensure your database is running and accessible with the credentials provided.
- The app will process the data file and load it into the database on startup.