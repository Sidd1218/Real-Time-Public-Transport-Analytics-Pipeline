import os
import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import threading
import time
from collections import defaultdict
from database import db_manager
from models import (
    BusArrival, TubeArrival, LineStatus, BikePoint,
    StagingArrival, StagingServiceStatus,
    HourlyArrival, DailyServicePerformance
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TfLDataConsumer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        
        # Initialize database
        try:
            db_manager.create_tables()
            logger.info("Database tables initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
        
        # Kafka topics to consume
        self.topics = ['bus-arrivals', 'tube-arrivals', 'line-status', 'bike-points']
        
        # Analytics processing intervals
        self.last_staging_process = datetime.utcnow()
        self.last_analytics_process = datetime.utcnow()
        self.staging_interval = timedelta(minutes=5)  # Process staging every 5 minutes
        self.analytics_interval = timedelta(minutes=15)  # Process analytics every 15 minutes

    def process_bus_arrival(self, message):
        """Process bus arrival message and insert into database"""
        session = None
        try:
            session = db_manager.get_session()
            
            # Convert timestamp strings to datetime objects
            expected_arrival = None
            if message.get('expected_arrival'):
                try:
                    expected_arrival = datetime.fromisoformat(message['expected_arrival'].replace('Z', '+00:00'))
                except:
                    pass
            
            timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            
            # Create BusArrival object
            bus_arrival = BusArrival(
                station_id=message.get('station_id'),
                station_name=message.get('station_name'),
                line_id=message.get('line_id'),
                line_name=message.get('line_name'),
                destination_name=message.get('destination_name'),
                vehicle_id=message.get('vehicle_id'),
                expected_arrival=expected_arrival,
                time_to_station=message.get('time_to_station'),
                current_location=message.get('current_location'),
                towards=message.get('towards'),
                direction=message.get('direction'),
                bearing=message.get('bearing'),
                naptan_id=message.get('naptan_id'),
                timestamp=timestamp
            )
            
            session.add(bus_arrival)
            session.commit()
            
            # Trigger real-time transformations if needed
            self.check_and_process_transformations()
            
        except Exception as e:
            logger.error(f"Error processing bus arrival: {str(e)}")
            if session:
                session.rollback()
        finally:
            if session:
                db_manager.close_session(session)

    def process_tube_arrival(self, message):
        """Process tube arrival message and insert into database"""
        session = None
        try:
            session = db_manager.get_session()
            
            # Convert timestamp strings to datetime objects
            expected_arrival = None
            if message.get('expected_arrival'):
                try:
                    expected_arrival = datetime.fromisoformat(message['expected_arrival'].replace('Z', '+00:00'))
                except:
                    pass
            
            timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            
            # Create TubeArrival object
            tube_arrival = TubeArrival(
                station_id=message.get('station_id'),
                station_name=message.get('station_name'),
                line_id=message.get('line_id'),
                line_name=message.get('line_name'),
                platform_name=message.get('platform_name'),
                destination_name=message.get('destination_name'),
                vehicle_id=message.get('vehicle_id'),
                expected_arrival=expected_arrival,
                time_to_station=message.get('time_to_station'),
                current_location=message.get('current_location'),
                towards=message.get('towards'),
                direction=message.get('direction'),
                naptan_id=message.get('naptan_id'),
                timestamp=timestamp
            )
            
            session.add(tube_arrival)
            session.commit()
            
            # Trigger real-time transformations if needed
            self.check_and_process_transformations()
            
        except Exception as e:
            logger.error(f"Error processing tube arrival: {str(e)}")
            if session:
                session.rollback()
        finally:
            if session:
                db_manager.close_session(session)

    def process_line_status(self, message):
        """Process line status message and insert into database"""
        session = None
        try:
            session = db_manager.get_session()
            
            # Convert timestamp strings to datetime objects
            created = None
            modified = None
            if message.get('created'):
                try:
                    created = datetime.fromisoformat(message['created'].replace('Z', '+00:00'))
                except:
                    pass
            
            if message.get('modified'):
                try:
                    modified = datetime.fromisoformat(message['modified'].replace('Z', '+00:00'))
                except:
                    pass
            
            timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            
            # Create LineStatus object
            line_status = LineStatus(
                line_id=message.get('line_id'),
                line_name=message.get('line_name'),
                status_severity=message.get('status_severity'),
                status_severity_description=message.get('status_severity_description'),
                reason=message.get('reason'),
                disruption_category=message.get('disruption_category'),
                created=created,
                modified=modified,
                timestamp=timestamp
            )
            
            session.add(line_status)
            session.commit()
            
        except Exception as e:
            logger.error(f"Error processing line status: {str(e)}")
            if session:
                session.rollback()
        finally:
            if session:
                db_manager.close_session(session)

    def process_bike_point(self, message):
        """Process bike point message and insert into database"""
        session = None
        try:
            session = db_manager.get_session()
            
            timestamp = datetime.fromisoformat(message['timestamp'].replace('Z', '+00:00'))
            
            # Create BikePoint object
            bike_point = BikePoint(
                bike_point_id=message.get('bike_point_id'),
                common_name=message.get('common_name'),
                place_type=message.get('place_type'),
                lat=message.get('lat'),
                lon=message.get('lon'),
                bikes_available=message.get('bikes_available'),
                docks_available=message.get('docks_available'),
                total_docks=message.get('total_docks'),
                installed=message.get('installed'),
                locked=message.get('locked'),
                temporary=message.get('temporary'),
                timestamp=timestamp
            )
            
            session.add(bike_point)
            session.commit()
            
        except Exception as e:
            logger.error(f"Error processing bike point: {str(e)}")
            if session:
                session.rollback()
        finally:
            if session:
                db_manager.close_session(session)

    def process_message(self, message):
        """Route message to appropriate processor based on type"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            message_type = data.get('type')
            
            if message_type == 'bus_arrival':
                self.process_bus_arrival(data)
            elif message_type == 'tube_arrival':
                self.process_tube_arrival(data)
            elif message_type == 'line_status':
                self.process_line_status(data)
            elif message_type == 'bike_point':
                self.process_bike_point(data)
            else:
                logger.warning(f"Unknown message type: {message_type}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON message: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def consume_topic(self, topic):
        """Consume messages from a specific topic"""
        logger.info(f"Starting consumer for topic: {topic}")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'transport-analytics-{topic}',
            value_deserializer=lambda m: m,
            consumer_timeout_ms=1000
        )
        
        try:
            for message in consumer:
                self.process_message(message)
                
        except Exception as e:
            logger.error(f"Error in consumer for topic {topic}: {str(e)}")
        finally:
            consumer.close()

    def start_consumers(self):
        """Start consumers for all topics in separate threads"""
        logger.info("Starting TfL Data Consumer...")
        
        threads = []
        for topic in self.topics:
            thread = threading.Thread(target=self.consume_topic, args=(topic,))
            thread.daemon = True
            thread.start()
            threads.append(thread)
            logger.info(f"Started consumer thread for topic: {topic}")
        
        try:
            # Keep main thread alive
            while True:
                for thread in threads:
                    if not thread.is_alive():
                        logger.warning(f"Consumer thread died, restarting...")
                        # Restart dead threads
                        topic_index = threads.index(thread)
                        topic = self.topics[topic_index]
                        new_thread = threading.Thread(target=self.consume_topic, args=(topic,))
                        new_thread.daemon = True
                        new_thread.start()
                        threads[topic_index] = new_thread
                
                # Sleep for a bit before checking again
                import time
                time.sleep(30)
                
        except KeyboardInterrupt:
            logger.info("Shutting down consumers...")

    def check_and_process_transformations(self):
        """Check if it's time to run staging or analytics transformations"""
        now = datetime.utcnow()
        
        # Process staging data every 5 minutes
        if now - self.last_staging_process >= self.staging_interval:
            self.process_staging_data()
            self.last_staging_process = now
        
        # Process analytics data every 15 minutes
        if now - self.last_analytics_process >= self.analytics_interval:
            self.process_analytics_data()
            self.last_analytics_process = now

    def process_staging_data(self):
        """Transform raw data into staging tables"""
        logger.info("Processing staging data transformations...")
        
        session = None
        try:
            session = db_manager.get_session()
            
            # Get recent bus arrivals
            five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
            
            bus_arrivals = session.query(BusArrival).filter(
                BusArrival.timestamp >= five_minutes_ago,
                BusArrival.expected_arrival.isnot(None)
            ).all()
            
            # Transform bus arrivals to staging
            for arrival in bus_arrivals:
                staging_arrival = StagingArrival(
                    transport_mode='bus',
                    station_id=arrival.station_id,
                    station_name=arrival.station_name,
                    line_id=arrival.line_id,
                    line_name=arrival.line_name,
                    destination_name=arrival.destination_name,
                    expected_arrival=arrival.expected_arrival,
                    time_to_station=arrival.time_to_station,
                    direction=arrival.direction,
                    date_partition=arrival.timestamp.date(),
                    hour_partition=arrival.timestamp.hour
                )
                session.merge(staging_arrival)  # Use merge to handle duplicates
            
            # Get recent tube arrivals
            tube_arrivals = session.query(TubeArrival).filter(
                TubeArrival.timestamp >= five_minutes_ago,
                TubeArrival.expected_arrival.isnot(None)
            ).all()
            
            # Transform tube arrivals to staging
            for arrival in tube_arrivals:
                staging_arrival = StagingArrival(
                    transport_mode='tube',
                    station_id=arrival.station_id,
                    station_name=arrival.station_name,
                    line_id=arrival.line_id,
                    line_name=arrival.line_name,
                    destination_name=arrival.destination_name,
                    expected_arrival=arrival.expected_arrival,
                    time_to_station=arrival.time_to_station,
                    direction=arrival.direction,
                    date_partition=arrival.timestamp.date(),
                    hour_partition=arrival.timestamp.hour
                )
                session.merge(staging_arrival)
            
            # Get recent line status data
            line_statuses = session.query(LineStatus).filter(
                LineStatus.timestamp >= five_minutes_ago
            ).all()
            
            # Transform line status to staging
            for status in line_statuses:
                # Determine transport mode
                transport_mode = 'bus' if (status.line_name and 
                    ('bus' in status.line_name.lower() or status.line_name.isdigit())) else 'tube'
                
                staging_status = StagingServiceStatus(
                    line_id=status.line_id,
                    line_name=status.line_name,
                    transport_mode=transport_mode,
                    status_severity=status.status_severity,
                    status_description=status.status_severity_description,
                    disruption_reason=status.reason,
                    date_partition=status.timestamp.date()
                )
                session.merge(staging_status)
            
            session.commit()
            logger.info("Staging data processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing staging data: {str(e)}")
            if session:
                session.rollback()
        finally:
            if session:
                db_manager.close_session(session)

    def process_analytics_data(self):
        """Generate analytics aggregations"""
        logger.info("Processing analytics data transformations...")
        
        session = None
        try:
            session = db_manager.get_session()
            
            # Generate hourly arrival analytics
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            
            # Query staging arrivals for hourly aggregation
            hourly_data = session.query(
                func.date_trunc('hour', StagingArrival.expected_arrival).label('date_hour'),
                StagingArrival.transport_mode,
                StagingArrival.line_name,
                StagingArrival.station_name,
                func.count().label('total_arrivals'),
                func.round(func.avg(StagingArrival.time_to_station), 2).label('avg_time_to_station'),
                func.min(StagingArrival.time_to_station).label('min_time_to_station'),
                func.max(StagingArrival.time_to_station).label('max_time_to_station')
            ).filter(
                StagingArrival.created_at >= one_hour_ago,
                StagingArrival.time_to_station > 0
            ).group_by(
                func.date_trunc('hour', StagingArrival.expected_arrival),
                StagingArrival.transport_mode,
                StagingArrival.line_name,
                StagingArrival.station_name
            ).all()
            
            # Create or update hourly arrival records
            for data in hourly_data:
                hourly_arrival = HourlyArrival(
                    date_hour=data.date_hour,
                    transport_mode=data.transport_mode,
                    line_name=data.line_name,
                    station_name=data.station_name,
                    total_arrivals=data.total_arrivals,
                    avg_time_to_station=data.avg_time_to_station,
                    min_time_to_station=data.min_time_to_station,
                    max_time_to_station=data.max_time_to_station
                )
                session.merge(hourly_arrival)
            
            # Generate daily service performance analytics
            today = datetime.utcnow().date()
            
            # Query staging service status for daily aggregation
            daily_data = session.query(
                StagingServiceStatus.date_partition,
                StagingServiceStatus.transport_mode,
                StagingServiceStatus.line_name,
                func.count().label('total_disruptions'),
                func.round(func.avg(StagingServiceStatus.status_severity), 2).label('avg_severity')
            ).filter(
                StagingServiceStatus.created_at >= today,
                StagingServiceStatus.status_severity > 10  # Only count actual disruptions
            ).group_by(
                StagingServiceStatus.date_partition,
                StagingServiceStatus.transport_mode,
                StagingServiceStatus.line_name
            ).all()
            
            # Create or update daily performance records
            for data in daily_data:
                # Calculate uptime percentage based on severity
                if data.avg_severity <= 10:
                    uptime_percentage = 99.0
                elif data.avg_severity <= 15:
                    uptime_percentage = 95.0
                else:
                    uptime_percentage = 85.0
                
                daily_performance = DailyServicePerformance(
                    date_partition=data.date_partition,
                    transport_mode=data.transport_mode,
                    line_name=data.line_name,
                    total_disruptions=data.total_disruptions,
                    avg_severity=data.avg_severity,
                    uptime_percentage=uptime_percentage
                )
                session.merge(daily_performance)
            
            session.commit()
            logger.info("Analytics data processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error processing analytics data: {str(e)}")
            if session:
                session.rollback()
        finally:
            if session:
                db_manager.close_session(session)

if __name__ == "__main__":
    consumer = TfLDataConsumer()
    
    try:
        consumer.start_consumers()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        logger.info("Consumer shut down complete")
