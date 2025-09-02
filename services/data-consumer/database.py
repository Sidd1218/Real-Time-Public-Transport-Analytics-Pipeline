"""
Database configuration and utilities for the transport analytics pipeline
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
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
    
    def create_tables(self):
        """Create all tables defined in models"""
        try:
            # Create all tables in public schema
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
