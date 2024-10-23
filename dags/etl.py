from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import requests
import pandas as pd
from datetime import timedelta
from psycopg2.extras import execute_values

# Define Constants
POSTGRES_CONN_ID = "postgres_default"
API_URL = "https://faaliat.sa/ar/events_api/year-events"

# Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id="api_event_etl_dag",
    default_args=default_args,
    description="ETL for Data From API",
    schedule_interval="@daily",
    catchup=False,
) as dag:


    @task()
    def extract_data():
        """Extract Data from API."""
        try:
            response = requests.get(url=API_URL, verify=False)
            response.raise_for_status()
            data = response.json()
            if "contents" not in data or "events" not in data["contents"]:
                raise ValueError("Unexpected response format: missing 'contents' or 'events'")
            return data
        except requests.RequestException as e:
            raise Exception(f"Failed to extract data from API: {str(e)}")
        except ValueError as e:
            raise Exception(f"Failed to parse JSON: {str(e)}")
        

    @task()
    def transform_data(data):
        """Transform Event Data."""
        events = data["contents"].get("events", [])
        if not events:
            raise ValueError("No events found in data.")

        extracted_events = [
            {
                "id": event["id"],
                "title": event["title"],
                "owner_id": event["ownerid"],
                "owner_name": event["ownername"],
                "link": event["link"],
                "language": event["lang"],
                "image": event["image"],
                "city": event["city"]["name"],
                "event_start_date": event["event_date"]["start_date"],
                "event_start_time": event["event_date"]["start_time"],
                "event_end_date": event["event_date"]["end_date"],
                "event_end_time": event["event_date"]["end_time"],
                "age_group": event["age_group"]["name"],
                "event_period": event["event_period"]["name"],
                "attendance_type": event["attendance_type"]["name"],
                "event_price": event["event_price"]["name"],
                "type_of_event": event["type_of_event"]["name"],
            }
            for event in events
        ]

        df = pd.DataFrame(extracted_events).drop_duplicates(subset="id")
        
        # Convert columns to appropriate types
        df['event_start_date'] = pd.to_datetime(df['event_start_date'], errors="coerce").dt.strftime('%Y-%m-%d')
        df['event_end_date'] = pd.to_datetime(df['event_end_date'], errors="coerce").dt.strftime('%Y-%m-%d')
        
        # Clean and convert time columns
        df['event_start_time'] = df['event_start_time'].str.extract('(\d{1,2}:\d{2})')[0]
        df['event_end_time'] = df['event_end_time'].str.extract('(\d{1,2}:\d{2})')[0]

        # Convert cleaned time strings to time objects
        df['event_start_time'] = pd.to_datetime(df['event_start_time'], format='%I:%M', errors='coerce').dt.time
        df['event_end_time'] = pd.to_datetime(df['event_end_time'], format='%I:%M', errors='coerce').dt.time

        # Convert times to strings for serialization
        df['event_start_time'] = df['event_start_time'].astype(str)
        df['event_end_time'] = df['event_end_time'].astype(str)

        # Drop rows with any null values
        df.dropna(how="any", inplace=True)

        if df.empty:
            raise ValueError("Transformed data is empty.")
        
        return df.to_dict("records")
    
    @task()
    def load_data(events):
        """Load Event Data into Postgres."""
        if not events:
            raise ValueError("No data found to load.")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        id VARCHAR(50) PRIMARY KEY,
                        title TEXT,
                        owner_id VARCHAR(50),
                        owner_name VARCHAR(255),
                        link TEXT,
                        language VARCHAR(10),
                        image TEXT,
                        city VARCHAR(255),
                        event_start_date DATE,
                        event_start_time TIME,
                        event_end_date DATE,
                        event_end_time TIME,
                        age_group VARCHAR(50),
                        event_period VARCHAR(100),
                        attendance_type VARCHAR(100),
                        event_price VARCHAR(100),
                        type_of_event VARCHAR(255)
                    );
                """)

                insert_query = """
                    INSERT INTO events (
                        id, title, owner_id, owner_name, link, language, image, city,
                        event_start_date, event_start_time, event_end_date, event_end_time,
                        age_group, event_period, attendance_type, event_price, type_of_event
                    ) VALUES %s
                    ON CONFLICT (id) DO NOTHING;
                """
                events_tuples = [
                    (
                        event["id"],
                        event["title"],
                        event["owner_id"],
                        event["owner_name"],
                        event["link"],
                        event["language"],
                        event["image"],
                        event["city"],
                        event["event_start_date"],
                        event["event_start_time"],
                        event["event_end_date"],
                        event["event_end_time"],
                        event["age_group"],
                        event["event_period"],
                        event["attendance_type"],
                        event["event_price"],
                        event["type_of_event"],
                    ) for event in events
                ]

                execute_values(cursor, insert_query, events_tuples)

    # Task Dependencies
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)

