from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
import requests
import json
from kafka import KafkaProducer
import os

# Default arguments for the DAG
default_args = {
    'owner': 'transport-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'tfl_data_pipeline',
    default_args=default_args,
    description='TfL Real-time Data Pipeline',
    schedule_interval=timedelta(minutes=2),  # Run every 2 minutes
    catchup=False,
    max_active_runs=1,
)

def fetch_and_send_bus_arrivals(**context):
    """Fetch bus arrivals from TfL API and send to Kafka"""
    
    # TfL API configuration
    api_key = os.getenv('TFL_API_KEY')
    base_url = os.getenv('TFL_BASE_URL', 'https://api.tfl.gov.uk')
    
    # Major bus stops in London
    bus_stops = [
        '490000001E',  # Oxford Circus
        '490000148S',  # King's Cross
        '490000021N',  # Victoria
        '490000138S',  # Liverpool Street
        '490000254S',  # Waterloo
    ]
    
    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for stop_id in bus_stops:
        try:
            # Fetch bus arrivals
            url = f"{base_url}/StopPoint/{stop_id}/Arrivals"
            params = {'app_key': api_key} if api_key else {}
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            arrivals = response.json()
            
            # Process and send each arrival to Kafka
            for arrival in arrivals:
                message = {
                    'type': 'bus_arrival',
                    'station_id': arrival.get('naptanId'),
                    'station_name': arrival.get('stationName'),
                    'line_id': arrival.get('lineId'),
                    'line_name': arrival.get('lineName'),
                    'destination_name': arrival.get('destinationName'),
                    'vehicle_id': arrival.get('vehicleId'),
                    'expected_arrival': arrival.get('expectedArrival'),
                    'time_to_station': arrival.get('timeToStation'),
                    'current_location': arrival.get('currentLocation'),
                    'towards': arrival.get('towards'),
                    'direction': arrival.get('direction'),
                    'bearing': arrival.get('bearing'),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                producer.send('bus-arrivals', value=message)
                
        except Exception as e:
            print(f"Error fetching bus arrivals for stop {stop_id}: {str(e)}")
    
    producer.flush()
    producer.close()

def fetch_and_send_tube_arrivals(**context):
    """Fetch tube arrivals from TfL API and send to Kafka"""
    
    api_key = os.getenv('TFL_API_KEY')
    base_url = os.getenv('TFL_BASE_URL', 'https://api.tfl.gov.uk')
    
    # Major tube stations
    tube_stations = [
        '940GZZLUOXC',  # Oxford Circus
        '940GZZLUKSX',  # King's Cross St. Pancras
        '940GZZLUVIC',  # Victoria
        '940GZZLULVT',  # Liverpool Street
        '940GZZLUWLO',  # Waterloo
    ]
    
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    for station_id in tube_stations:
        try:
            url = f"{base_url}/StopPoint/{station_id}/Arrivals"
            params = {'app_key': api_key} if api_key else {}
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            arrivals = response.json()
            
            for arrival in arrivals:
                message = {
                    'type': 'tube_arrival',
                    'station_id': arrival.get('naptanId'),
                    'station_name': arrival.get('stationName'),
                    'line_id': arrival.get('lineId'),
                    'line_name': arrival.get('lineName'),
                    'platform_name': arrival.get('platformName'),
                    'destination_name': arrival.get('destinationName'),
                    'vehicle_id': arrival.get('vehicleId'),
                    'expected_arrival': arrival.get('expectedArrival'),
                    'time_to_station': arrival.get('timeToStation'),
                    'current_location': arrival.get('currentLocation'),
                    'towards': arrival.get('towards'),
                    'direction': arrival.get('direction'),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                producer.send('tube-arrivals', value=message)
                
        except Exception as e:
            print(f"Error fetching tube arrivals for station {station_id}: {str(e)}")
    
    producer.flush()
    producer.close()

def fetch_and_send_line_status(**context):
    """Fetch line status from TfL API and send to Kafka"""
    
    api_key = os.getenv('TFL_API_KEY')
    base_url = os.getenv('TFL_BASE_URL', 'https://api.tfl.gov.uk')
    
    producer = KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        # Fetch tube line status
        url = f"{base_url}/Line/Mode/tube/Status"
        params = {'app_key': api_key} if api_key else {}
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        lines = response.json()
        
        for line in lines:
            for status in line.get('lineStatuses', []):
                message = {
                    'type': 'line_status',
                    'line_id': line.get('id'),
                    'line_name': line.get('name'),
                    'transport_mode': line.get('modeName'),
                    'status_severity': status.get('statusSeverity'),
                    'status_severity_description': status.get('statusSeverityDescription'),
                    'reason': status.get('reason'),
                    'disruption_category': status.get('disruption', {}).get('category'),
                    'created': status.get('created'),
                    'modified': status.get('modified'),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                producer.send('line-status', value=message)
                
    except Exception as e:
        print(f"Error fetching line status: {str(e)}")
    
    producer.flush()
    producer.close()

# Define tasks
check_tfl_api = HttpSensor(
    task_id='check_tfl_api',
    http_conn_id='tfl_api',
    endpoint='/',
    timeout=20,
    poke_interval=30,
    dag=dag,
)

fetch_bus_arrivals_task = PythonOperator(
    task_id='fetch_bus_arrivals',
    python_callable=fetch_and_send_bus_arrivals,
    dag=dag,
)

fetch_tube_arrivals_task = PythonOperator(
    task_id='fetch_tube_arrivals',
    python_callable=fetch_and_send_tube_arrivals,
    dag=dag,
)

fetch_line_status_task = PythonOperator(
    task_id='fetch_line_status',
    python_callable=fetch_and_send_line_status,
    dag=dag,
)

# Set task dependencies
check_tfl_api >> [fetch_bus_arrivals_task, fetch_tube_arrivals_task, fetch_line_status_task]
