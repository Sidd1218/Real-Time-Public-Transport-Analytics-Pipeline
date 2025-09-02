#!/usr/bin/env python3
"""
Database migration script using Alembic
"""
import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def run_migrations():
    """Run Alembic migrations"""
    try:
        # Change to the data-consumer directory
        os.chdir(Path(__file__).parent)
        
        print("Running Alembic migrations...")
        
        # Run alembic upgrade to apply migrations
        result = subprocess.run(['alembic', 'upgrade', 'head'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database migrations completed successfully")
            print(result.stdout)
        else:
            print("❌ Migration failed:")
            print(result.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error running migrations: {str(e)}")
        sys.exit(1)

def create_initial_migration():
    """Create initial migration from models"""
    try:
        os.chdir(Path(__file__).parent)
        
        print("Creating initial migration...")
        
        # Create initial migration
        result = subprocess.run([
            'alembic', 'revision', '--autogenerate', 
            '-m', 'Initial migration with all tables'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Initial migration created successfully")
            print(result.stdout)
        else:
            print("❌ Failed to create migration:")
            print(result.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error creating migration: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "create":
        create_initial_migration()
    else:
        run_migrations()
