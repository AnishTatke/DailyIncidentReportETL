import psycopg2
from psycopg2 import sql, OperationalError, DatabaseError, IntegrityError
import logging

def create_connection(conn_str):
    try:
        conn = psycopg2.connect(conn_str)
        logging.info("Database connection established.")
        return conn
    except OperationalError as e:
        logging.error(f"Could not connect to PostgreSQL: {e}")
        raise

def create_table_if_not_exists(cursor, table_name = 'incident_reports'):
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP NOT NULL,
            incident_number TEXT UNIQUE NOT NULL,
            location TEXT NOT NULL,
            incident_type TEXT NOT NULL,
            incident_ori TEXT,
            latitude FLOAT,
            longitude FLOAT,
            incident_year INT,
            incident_month TEXT,
            incident_day INT,
            incident_hour INT,
            incident_day_of_week TEXT,
            cluster INT
        );
    """
    try:
        cursor.execute(create_table_query)
        logging.info("Table created successfully or already exists.")
    except DatabaseError as e:
        logging.error(f"Error creating table: {e}")
        raise
    
def insert_data(cursor, df, table_name = 'incident_reports'):
    insert_data_query = f"""
        INSERT INTO {table_name} (
            datetime, incident_number, location, incident_type, incident_ori, latitude, longitude, incident_year, incident_month, incident_day, incident_hour, incident_day_of_week, cluster
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (incident_number) DO NOTHING;
    """
    try:
        for _, row in df.iterrows():
            cursor.execute(insert_data_query, (
                row['Date / Time'],
                row['Incident Number'],
                row['Location'],
                row['Incident Type'],
                row['Incident ORI'],
                row['Latitude'],
                row['Longitude'],
                row['Incident Year'],
                row['Incident Month'],
                row['Incident Day'],
                row['Incident Hour'],
                row['Incident Day of Week'],
                row['Cluster']
            ))
        logging.info("Data inserted successfully.")
    except IntegrityError as e:
        logging.warning(f"Integrity error: {e}")
        raise
    except DatabaseError as e:
        logging.error(f"Error inserting data: {e}")
        raise

def check_table_exists(cursor, table_name = 'incident_reports'):
    check_table_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
    """
    try:
        cursor.execute(check_table_query)
        exists = cursor.fetchone()[0]
        logging.info(f"Table {table_name} exists: {exists}")
        return exists
    except DatabaseError as e:
        logging.error(f"Error checking table existence: {e}")
        raise

def fetch_latest_data(cursor, table_name = 'incident_reports'):
    fetch_query = f"SELECT * FROM {table_name} ORDER BY datetime DESC LIMIT 1;"

    if not check_table_exists(cursor, table_name):
        logging.warning(f"Table {table_name} does not exist.")
        return None
    
    try:
        cursor.execute(fetch_query)
        latest_data = cursor.fetchone()
        logging.info("Latest data fetched successfully.")
        return latest_data
    except DatabaseError as e:
        logging.error(f"Error fetching latest data: {e}")
        raise

def drop_table(cursor, table_name = 'incident_reports'):
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    try:
        cursor.execute(drop_table_query)
        logging.info("Table dropped successfully.")
    except DatabaseError as e:
        logging.error(f"Error dropping table: {e}")
        raise
    