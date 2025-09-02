"""
SQLAlchemy models for the transport analytics pipeline
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, UniqueConstraint, Index
from sqlalchemy.types import Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

# Raw data models
class BusArrival(Base):
    __tablename__ = 'bus_arrivals'
    __table_args__ = (
        Index('idx_bus_arrivals_station_time', 'station_id', 'timestamp'),
    )
    
    id = Column(Integer, primary_key=True)
    station_id = Column(String(50))
    station_name = Column(String(255))
    line_id = Column(String(50))
    line_name = Column(String(255))
    destination_name = Column(String(255))
    vehicle_id = Column(String(50))
    expected_arrival = Column(DateTime)
    time_to_station = Column(Integer)
    current_location = Column(String(255))
    towards = Column(String(255))
    direction = Column(String(10))
    bearing = Column(Integer)
    naptan_id = Column(String(50))
    timestamp = Column(DateTime, default=func.current_timestamp())
    created_at = Column(DateTime, default=func.current_timestamp())

class TubeArrival(Base):
    __tablename__ = 'tube_arrivals'
    __table_args__ = (
        Index('idx_tube_arrivals_station_time', 'station_id', 'timestamp'),
    )
    
    id = Column(Integer, primary_key=True)
    station_id = Column(String(50))
    station_name = Column(String(255))
    line_id = Column(String(50))
    line_name = Column(String(255))
    platform_name = Column(String(255))
    destination_name = Column(String(255))
    vehicle_id = Column(String(50))
    expected_arrival = Column(DateTime)
    time_to_station = Column(Integer)
    current_location = Column(String(255))
    towards = Column(String(255))
    direction = Column(String(10))
    naptan_id = Column(String(50))
    timestamp = Column(DateTime, default=func.current_timestamp())
    created_at = Column(DateTime, default=func.current_timestamp())

class LineStatus(Base):
    __tablename__ = 'line_status'
    __table_args__ = (
        Index('idx_line_status_time', 'timestamp'),
    )
    
    id = Column(Integer, primary_key=True)
    line_id = Column(String(50))
    line_name = Column(String(255))
    status_severity = Column(Integer)
    status_severity_description = Column(String(255))
    reason = Column(String(500))
    disruption_category = Column(String(100))
    created = Column(DateTime)
    modified = Column(DateTime)
    timestamp = Column(DateTime, default=func.current_timestamp())

class BikePoint(Base):
    __tablename__ = 'bike_points'
    
    id = Column(Integer, primary_key=True)
    bike_point_id = Column(String(50))
    common_name = Column(String(255))
    place_type = Column(String(50))
    lat = Column(Numeric(10, 8))
    lon = Column(Numeric(11, 8))
    bikes_available = Column(Integer)
    docks_available = Column(Integer)
    total_docks = Column(Integer)
    installed = Column(Boolean)
    locked = Column(Boolean)
    temporary = Column(Boolean)
    timestamp = Column(DateTime, default=func.current_timestamp())

# Staging data models
class StagingArrival(Base):
    __tablename__ = 'arrivals'
    __table_args__ = (
        Index('idx_staging_arrivals_partition', 'date_partition', 'hour_partition'),
        Index('idx_staging_arrivals_created', 'created_at'),
    )
    
    id = Column(Integer, primary_key=True)
    transport_mode = Column(String(20))  # 'bus' or 'tube'
    station_id = Column(String(50))
    station_name = Column(String(255))
    line_id = Column(String(50))
    line_name = Column(String(255))
    destination_name = Column(String(255))
    expected_arrival = Column(DateTime)
    time_to_station = Column(Integer)
    direction = Column(String(10))
    date_partition = Column(DateTime)  # DATE type
    hour_partition = Column(Integer)
    created_at = Column(DateTime, default=func.current_timestamp())

class StagingServiceStatus(Base):
    __tablename__ = 'service_status'
    __table_args__ = (
        Index('idx_staging_status_created', 'created_at'),
    )
    
    id = Column(Integer, primary_key=True)
    line_id = Column(String(50))
    line_name = Column(String(255))
    transport_mode = Column(String(20))
    status_severity = Column(Integer)
    status_description = Column(String(255))
    disruption_reason = Column(String(500))
    date_partition = Column(DateTime)  # DATE type
    created_at = Column(DateTime, default=func.current_timestamp())

# Analytics data models
class HourlyArrival(Base):
    __tablename__ = 'hourly_arrivals'
    __table_args__ = (
        UniqueConstraint('date_hour', 'transport_mode', 'line_name', 'station_name', 
                        name='unique_hourly_arrivals'),
        Index('idx_analytics_hourly_date', 'date_hour'),
    )
    
    id = Column(Integer, primary_key=True)
    date_hour = Column(DateTime)
    transport_mode = Column(String(20))
    line_name = Column(String(255))
    station_name = Column(String(255))
    total_arrivals = Column(Integer)
    avg_time_to_station = Column(Numeric(8, 2))
    min_time_to_station = Column(Integer)
    max_time_to_station = Column(Integer)
    created_at = Column(DateTime, default=func.current_timestamp())

class DailyServicePerformance(Base):
    __tablename__ = 'daily_service_performance'
    __table_args__ = (
        UniqueConstraint('date_partition', 'transport_mode', 'line_name', 
                        name='unique_daily_performance'),
        Index('idx_analytics_daily_date', 'date_partition'),
    )
    
    id = Column(Integer, primary_key=True)
    date_partition = Column(DateTime)  # DATE type
    transport_mode = Column(String(20))
    line_name = Column(String(255))
    total_disruptions = Column(Integer)
    avg_severity = Column(Numeric(4, 2))
    uptime_percentage = Column(Numeric(5, 2))
    created_at = Column(DateTime, default=func.current_timestamp())
