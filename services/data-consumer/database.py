"""
Database configuration and utilities for the transport analytics pipeline
"""
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
from dotenv import load_dotenv
from models import Base
import logging

load_dotenv()
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        # Database connection parameters
        db_host = os.getenv('POSTGRES_HOST', 'postgres')
        db_name = os.getenv('POSTGRES_DB', 'transport_analytics')
        db_user = os.getenv('POSTGRES_USER', 'postgres')
        db_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
        
        self.db_url = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
        self.engine = create_engine(self.db_url, pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    def create_schemas(self):
        """Create database schemas if they don't exist"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
            logger.info("Database schemas created successfully")
        except Exception as e:
            logger.error(f"Error creating schemas: {str(e)}")
            raise
    
    def create_tables(self):
        """Create all tables defined in models"""
        try:
            # First create schemas
            self.create_schemas()
            
            # Then create all tables
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise
    
    def get_session(self):
        """Get a database session"""
        return self.SessionLocal()
    
    def close_session(self, session):
        """Close a database session"""
        if session:
            session.close()

# Global database manager instance
db_manager = DatabaseManager()
