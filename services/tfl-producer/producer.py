import os
import json
import time
import logging
import requests
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import schedule
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TfLDataProducer:
    def __init__(self):
        self.api_key = os.getenv('TFL_API_KEY')
        self.base_url = os.getenv('TFL_BASE_URL', 'https://api.tfl.gov.uk')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            retry_backoff_ms=1000
        )
        
        # Major London transport hubs
        self.bus_stops = [
            '490000001E',  # Oxford Circus
            '490000148S',  # King's Cross
            '490000021N',  # Victoria
            '490000138S',  # Liverpool Street
            '490000254S',  # Waterloo
            '490000057S',  # London Bridge
            '490000076B',  # Piccadilly Circus
            '490000139S',  # Bank
        ]
        
        self.tube_stations = [
            '940GZZLUOXC',  # Oxford Circus
            '940GZZLUKSX',  # King's Cross St. Pancras
            '940GZZLUVIC',  # Victoria
            '940GZZLULVT',  # Liverpool Street
            '940GZZLUWLO',  # Waterloo
            '940GZZLULNB',  # London Bridge
            '940GZZLUPCC',  # Piccadilly Circus
            '940GZZLUBNK',  # Bank
        ]

    def fetch_bus_arrivals(self):
        """Fetch and send bus arrival data to Kafka"""
        logger.info("Fetching bus arrivals...")
        
        for stop_id in self.bus_stops:
            try:
                url = f"{self.base_url}/StopPoint/{stop_id}/Arrivals"
                params = {'app_key': self.api_key} if self.api_key else {}
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                arrivals = response.json()
                logger.info(f"Fetched {len(arrivals)} bus arrivals for stop {stop_id}")
                
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
                        'naptan_id': arrival.get('naptanId'),
                        'timestamp': datetime.utcnow().isoformat(),
                        'ingestion_time': datetime.utcnow().isoformat()
                    }
                    
                    # Send to Kafka with station_id as key for partitioning
                    self.producer.send(
                        'bus-arrivals',
                        key=arrival.get('naptanId'),
                        value=message
                    )
                
            except Exception as e:
                logger.error(f"Error fetching bus arrivals for stop {stop_id}: {str(e)}")
        
        self.producer.flush()

    def fetch_tube_arrivals(self):
        """Fetch and send tube arrival data to Kafka"""
        logger.info("Fetching tube arrivals...")
        
        for station_id in self.tube_stations:
            try:
                url = f"{self.base_url}/StopPoint/{station_id}/Arrivals"
                params = {'app_key': self.api_key} if self.api_key else {}
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                arrivals = response.json()
                logger.info(f"Fetched {len(arrivals)} tube arrivals for station {station_id}")
                
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
                        'naptan_id': arrival.get('naptanId'),
                        'timestamp': datetime.utcnow().isoformat(),
                        'ingestion_time': datetime.utcnow().isoformat()
                    }
                    
                    self.producer.send(
                        'tube-arrivals',
                        key=arrival.get('naptanId'),
                        value=message
                    )
                
            except Exception as e:
                logger.error(f"Error fetching tube arrivals for station {station_id}: {str(e)}")
        
        self.producer.flush()

    def fetch_line_status(self):
        """Fetch and send line status data to Kafka"""
        logger.info("Fetching line status...")
        
        try:
            # Fetch tube line status
            url = f"{self.base_url}/Line/Mode/tube/Status"
            params = {'app_key': self.api_key} if self.api_key else {}
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            lines = response.json()
            logger.info(f"Fetched status for {len(lines)} tube lines")
            
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
                        'disruption_category': status.get('disruption', {}).get('category') if status.get('disruption') else None,
                        'created': status.get('created'),
                        'modified': status.get('modified'),
                        'timestamp': datetime.utcnow().isoformat(),
                        'ingestion_time': datetime.utcnow().isoformat()
                    }
                    
                    self.producer.send(
                        'line-status',
                        key=line.get('id'),
                        value=message
                    )
            
            # Fetch bus line status
            url = f"{self.base_url}/Line/Mode/bus/Status"
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            bus_lines = response.json()
            logger.info(f"Fetched status for {len(bus_lines)} bus lines")
            
            for line in bus_lines:
                for status in line.get('lineStatuses', []):
                    message = {
                        'type': 'line_status',
                        'line_id': line.get('id'),
                        'line_name': line.get('name'),
                        'transport_mode': line.get('modeName'),
                        'status_severity': status.get('statusSeverity'),
                        'status_severity_description': status.get('statusSeverityDescription'),
                        'reason': status.get('reason'),
                        'disruption_category': status.get('disruption', {}).get('category') if status.get('disruption') else None,
                        'created': status.get('created'),
                        'modified': status.get('modified'),
                        'timestamp': datetime.utcnow().isoformat(),
                        'ingestion_time': datetime.utcnow().isoformat()
                    }
                    
                    self.producer.send(
                        'line-status',
                        key=line.get('id'),
                        value=message
                    )
                
        except Exception as e:
            logger.error(f"Error fetching line status: {str(e)}")
        
        self.producer.flush()

    def fetch_bike_points(self):
        """Fetch and send bike point data to Kafka"""
        logger.info("Fetching bike points...")
        
        try:
            url = f"{self.base_url}/BikePoint"
            params = {'app_key': self.api_key} if self.api_key else {}
            
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            bike_points = response.json()
            logger.info(f"Fetched {len(bike_points)} bike points")
            
            for point in bike_points:
                # Extract additional properties
                properties = {prop['key']: prop['value'] for prop in point.get('additionalProperties', [])}
                
                message = {
                    'type': 'bike_point',
                    'bike_point_id': point.get('id'),
                    'common_name': point.get('commonName'),
                    'place_type': point.get('placeType'),
                    'lat': point.get('lat'),
                    'lon': point.get('lon'),
                    'bikes_available': int(properties.get('NbBikes', 0)),
                    'docks_available': int(properties.get('NbEmptyDocks', 0)),
                    'total_docks': int(properties.get('NbDocks', 0)),
                    'installed': properties.get('Installed') == 'true',
                    'locked': properties.get('Locked') == 'true',
                    'temporary': properties.get('Temporary') == 'true',
                    'timestamp': datetime.utcnow().isoformat(),
                    'ingestion_time': datetime.utcnow().isoformat()
                }
                
                self.producer.send(
                    'bike-points',
                    key=point.get('id'),
                    value=message
                )
            
        except Exception as e:
            logger.error(f"Error fetching bike points: {str(e)}")
        
        self.producer.flush()

    def run_data_collection(self):
        """Run all data collection methods"""
        logger.info("Starting data collection cycle...")
        
        try:
            self.fetch_bus_arrivals()
            self.fetch_tube_arrivals()
            self.fetch_line_status()
            self.fetch_bike_points()
            logger.info("Data collection cycle completed successfully")
        except Exception as e:
            logger.error(f"Error in data collection cycle: {str(e)}")

    def start_scheduler(self):
        """Start the scheduled data collection"""
        logger.info("Starting TfL Data Producer scheduler...")
        
        # Schedule different data types at different intervals
        schedule.every(1).minutes.do(self.fetch_bus_arrivals)
        schedule.every(1).minutes.do(self.fetch_tube_arrivals)
        schedule.every(5).minutes.do(self.fetch_line_status)
        schedule.every(10).minutes.do(self.fetch_bike_points)
        
        # Run initial collection
        self.run_data_collection()
        
        # Keep running
        while True:
            try:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
            except KeyboardInterrupt:
                logger.info("Shutting down producer...")
                break
            except Exception as e:
                logger.error(f"Scheduler error: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying

    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            self.producer.close()

if __name__ == "__main__":
    producer = TfLDataProducer()
    
    try:
        producer.start_scheduler()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        producer.close()
        logger.info("Producer shut down complete")
