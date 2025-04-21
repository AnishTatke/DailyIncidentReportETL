import os
import io
import pandas as pd
import logging

from datetime import datetime, timedelta
import numpy as np
from airflow import DAG
from airflow.decorators import task
from plugins.utils import save_csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


API_CONN_ID = "norman_api"
POSTGRES_CONN_ID = "airflow_postgres_conn_id"

default_args = {
    "owner": "airflow",
    "start_date": datetime.today(),
}

with DAG(
    dag_id="normanpd_dag",
    default_args=default_args,
    description="Fetch Norman PD daily incident reports",
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    @task(
        task_id="check_latest_date"
    )
    def check_latest_date():
        from plugins.db_utils import fetch_latest_data, create_connection
        from airflow.hooks.base import BaseHook

        try:
            conn = BaseHook.get_connection(POSTGRES_CONN_ID)
            conn_str = f"host={conn.host} dbname={conn.schema} user={conn.login} password={conn.password} port={conn.port}"
            logging.info(f"Connecting to database: {conn_str}")

            with create_connection(conn_str) as connection:
                cursor = connection.cursor()
                latest_date = fetch_latest_data(cursor)
                logging.info(f"Latest date in DB: {latest_date[1]}")
                return latest_date[1] if latest_date else None

        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            raise e
        
    @task.branch(
        task_id="should_fetch_new_report"
    )
    def should_fetch_new_report(latest_date, **kwargs):
        execution_date = kwargs['execution_date'].date()
        print(f"Execution date: {execution_date}, type: {type(execution_date)}")
        
        if latest_date:
            db_date = latest_date.date()
            if db_date >= execution_date:
                return "skip_pipeline"
            
        return "fetch_latest_report"
    
    @task(
        task_id="skip_pipeline",
    )
    def skip_pipeline():
        logging.info("No new report to fetch. Skipping pipeline.")


    @task(
        task_id="fetch_latest_report",
    )
    def fetch_latest_report(**kwargs):
        from pathlib import Path
        from airflow.providers.http.hooks.http import HttpHook

        execution_date = kwargs['execution_date']

        logging.info(f"Fetching latest report for {execution_date}")
        date_to_check  = execution_date
        max_retries = 7
        retries = 0

        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
        save_dir = Path('/tmp/incidents')
        os.makedirs(save_dir, exist_ok=True)
        logging.info("Start fetching")
        while retries < max_retries:
            endpoint = "{}/{}_daily_incident_summary.pdf".format(
                date_to_check.strftime("%Y-%m"), date_to_check.strftime("%Y-%m-%d")
            )
            save_path = "{}/{}_daily_incident_summary.pdf".format(
                save_dir, date_to_check.strftime("%Y-%m-%d")
            )

            try:
                logging.info(f"Attempt {retries + 1}: Fetching from {endpoint}")
                response = http_hook.run(endpoint)
                content_type = response.headers.get("Content-Type", "")
                logging.info(f"Response Status: {response.status_code}, Content-Type: {content_type}")
    
                if response.status_code == 200 and response.headers['Content-Type'] == 'application/pdf':
                    with open(save_path, 'wb') as f:
                        f.write(response.content)
                    logging.info(f"Downloaded {save_path}")
                    return save_path 
            except Exception as e:
                logging.warning(f"Request failed for {endpoint}")

            logging.info(f"Error: HTTPError\n Trying for previous day")
            date_to_check -= timedelta(days=1)
            retries += 1

        raise ValueError(f"No incidents found in the last {max_retries} days")

    @task(
        task_id="extract_incidents",
    )
    def extract_incidents(file_path: str):
        from pypdf import PdfReader
        import re

        with open(file_path, 'rb') as f:
            reader = PdfReader(io.BytesIO(f.read()))

        incidents_list = []
        skipped_incidents = 0

        def getincidents(reader: PdfReader):
            incidents = []
            for idx, page in enumerate(reader.pages):
                text = page.extract_text(extraction_mode='layout')
                # Remove the header from the first page
                if idx == 0:
                    text = text.replace('Date / Time', '')
                    text = text.replace('Incident Number', '')
                    text = text.replace('Location', '')
                    text = text.replace('Nature', '')
                    text = text.replace('Incident ORI', '')
                    text = text.replace('NORMAN POLICE DEPARTMENT', '')
                    text = text.replace('Daily Incident Summary (Public)', '')

                incidents.extend([incident.replace('\n', '').strip() for incident in text.split('\n\n\n')])
            return incidents
        
        incidents = getincidents(reader)
        incidents.pop() # Remove the last element which irrelevant
        pattern = re.compile(r'\s{2,}')
        for idx, incident in enumerate(incidents):    
            incident_data = pattern.split(incident)
            if len(incident_data) == 5:
                incidents_list.append({
                    'Date / Time': incident_data[0],
                    'Incident Number': incident_data[1],
                    'Location': incident_data[2],
                    'Incident Type': incident_data[3],
                    'Incident ORI': incident_data[4]
                })
            elif len(incident_data) == 6:
                incidents_list.append({
                    'Date / Time': incident_data[0],
                    'Incident Number': incident_data[1],
                    'Location': incident_data[2] + " " + incident_data[-1],
                    'Incident Type': incident_data[3],
                    'Incident ORI': incident_data[4]
                })
            elif len(incident_data) < 5:
                skipped_incidents += 1
                continue

        logging.info(f"Extracted {len(incidents_list)} incidents")
        logging.info(f"Skipped {skipped_incidents} incidents")
        columns = ['Date / Time', 'Incident Number', 'Location', 'Incident Type', 'Incident ORI']
        return save_csv(incidents_list, columns, file_path, ('.pdf', '.csv'))
         
    
    @task(
        task_id="clean_data",
    )
    def clean_data(file_path: str):
        df = pd.read_csv(file_path)
        df['Date / Time'].apply(lambda x: pd.to_datetime(x))
        df['Incident Year'] = df['Date / Time'].apply(lambda x: pd.to_datetime(x).year)
        df['Incident Hour'] = df['Date / Time'].apply(lambda x: pd.to_datetime(x).hour)
        df['Incident Month'] = df['Date / Time'].apply(lambda x: pd.to_datetime(x).month_name())
        df['Incident Day'] = df['Date / Time'].apply(lambda x: pd.to_datetime(x).day)
        df['Incident Day of Week'] = df['Date / Time'].apply(lambda x: pd.to_datetime(x).day_name())

        file_path = save_csv(df, df.columns, file_path, ('.csv', '_cleaned.csv'))
        return file_path
    
    @task(
        task_id="geocode_locations",
    )
    def geocode_locations(file_path: str):
        import asyncio
        from plugins.utils import get_lat_long

        df = pd.read_csv(file_path)
        df['Latitude'] = None
        df['Longitude'] = None
        location_coords = asyncio.run(get_lat_long(df))
        logging.info(f"Geocoded {len(location_coords)} locations")

        df['Latitude'] = df['Location'].map(lambda loc: location_coords.get(loc, (np.nan, np.nan))[0])
        df['Longitude'] = df['Location'].map(lambda loc: location_coords.get(loc, (np.nan, np.nan))[1])

        df['Latitude'].fillna(df['Latitude'].median(), inplace=True)
        df['Longitude'].fillna(df['Longitude'].median(), inplace=True)
        logging.info(f"Filled missing coordinates with median values")

        file_path = save_csv(df, df.columns, file_path, ('.csv', '_geocoded.csv'))
        return file_path


    @task(
        task_id="cluster_incidents",
    )
    def cluster_incidents(file_path: str):
        from plugins.cluster_utils import create_pipeline

        df = pd.read_csv(file_path)
        pipeline, num_features, cat_features = create_pipeline(df)
        cluster_labels = pipeline.fit_predict(df)
        df['Cluster'] = cluster_labels
        file_path = save_csv(df, df.columns, file_path, ('.csv', '_clustered.csv'))
        return file_path
    
    @task(
        task_id="save_to_db",
    )
    def save_to_db(file_path: str, table_name = 'incident_reports'):
        from plugins.db_utils import create_connection, create_table_if_not_exists, insert_data
        from airflow.hooks.base import BaseHook

        df = pd.read_csv(file_path)
        df['Date / Time'] = pd.to_datetime(df['Date / Time'])

        try:
            conn = BaseHook.get_connection(POSTGRES_CONN_ID)
            conn_str = f"host={conn.host} dbname={conn.schema} user={conn.login} password={conn.password} port={conn.port}"
            logging.info(f"Connecting to database: {conn_str}")

            with create_connection(conn_str) as connection:
                cursor = connection.cursor()
                create_table_if_not_exists(cursor, table_name)
                insert_data(cursor, df, table_name)
                connection.commit()
                cursor.close()
                logging.info("Data inserted successfully")

        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            raise e

    latest_date = check_latest_date()
    branch_decision = should_fetch_new_report(latest_date)

    # Skipped Pipeline
    skip = skip_pipeline()
    
    # Real Flow
    file_path = fetch_latest_report()
    extracted_file = extract_incidents(file_path)
    cleaned_file = clean_data(extracted_file)
    geocoded_file = geocode_locations(cleaned_file)
    clustered_file = cluster_incidents(geocoded_file)
    db_write = save_to_db(clustered_file)

    latest_date >> branch_decision
    branch_decision >> skip
    branch_decision >> file_path
    file_path >> extracted_file >> cleaned_file >> geocoded_file >> clustered_file >> db_write