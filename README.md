# Real-Time Public Transport Analytics Pipeline

A comprehensive real-time analytics engine for London's public transport system using TfL (Transport for London) API data.

## Architecture Overview

This project implements a modern data pipeline with the following components:

- **Data Source**: London TfL API (real-time transport data)
- **Streaming**: Apache Kafka for real-time data ingestion
- **Orchestration**: Apache Airflow for scheduling and workflow management
- **Data Models**: SQLAlchemy + Alembic for database management
- **Analytics**: Real-time data transformations with SQLAlchemy
- **Visualization**: Grafana dashboards
- **Infrastructure**: Docker containers for local development
- **Deployment**: GitHub Actions for EC2 deployment

## Data Pipeline Flow

```
TfL API → Airflow (Scheduler) → Kafka Producer → Kafka Topics → 
Kafka Consumer → PostgreSQL (Real-time Transformations) → Grafana
```

## Quick Start

### Option 1: Automated Setup
```bash
./scripts/setup.sh
```

### Option 2: Manual Setup
```bash
# 1. Setup environment
make setup

# 2. Edit .env file and add your TfL API key
# Get your free API key from: https://api-portal.tfl.gov.uk/

# 3. Start all services
make dev-setup
```

### Access Services
- **Grafana**: http://localhost:3000 (admin/admin)
- **Airflow**: http://localhost:8080 (admin/admin)

### Useful Commands
```bash
make help          # Show all available commands
make logs          # View logs from all services
make stop          # Stop all services
make clean         # Clean up containers and volumes
```

## Services

- **Kafka**: Message streaming (ports 9092, 2181)
- **PostgreSQL**: Data storage (port 5432)
- **Airflow**: Workflow orchestration (port 8080)
- **Grafana**: Visualization (port 3000)
- **API Service**: TfL data ingestion service

## Development

### Project Structure
```
├── requirements.txt              # Global Python dependencies
├── docker/                       # Shared Docker configurations
│   ├── Dockerfile.python        # Python services container
│   └── Dockerfile.airflow        # Airflow container
├── services/
│   ├── tfl-producer/            # TfL API data producer
│   └── data-consumer/           # Kafka consumer & database writer
│       ├── models.py            # SQLAlchemy data models
│       ├── database.py          # Database configuration
│       ├── migrate.py           # Migration script
│       ├── alembic.ini          # Alembic configuration
│       └── alembic/             # Database migrations
├── airflow/
│   ├── dags/                    # Airflow DAGs
│   └── airflow.cfg             # Airflow configuration
├── grafana/
│   ├── dashboards/             # Pre-built dashboards
│   └── provisioning/           # Grafana configuration
└── scripts/
    └── setup.sh              # Automated setup script
```

### Data Flow
1. **Database Migration** - Alembic creates schemas and tables from SQLAlchemy models
2. **Airflow** schedules TfL API calls every 2 minutes
3. **TfL Producer** fetches real-time data and sends to Kafka topics
4. **Data Consumer** processes Kafka messages using SQLAlchemy ORM
5. **Real-time Transformations** - Automatic staging (5min) and analytics (15min) processing
6. **Grafana** visualizes the data with real-time dashboards

### Database Architecture
- **Raw Schema**: Direct TfL API data storage using SQLAlchemy models
- **Staging Schema**: Cleaned and unified transport data
- **Analytics Schema**: Aggregated metrics and performance indicators
- **Migrations**: Version-controlled schema changes with Alembic

### Troubleshooting
- Check service logs: `make logs`
- Test TfL API: `make test-api`
- View Kafka topics: `make kafka-topics`
- Database shell: `make shell-postgres`

## API Key Setup

Get your free TfL API key from [TfL API Portal](https://api-portal.tfl.gov.uk/) and add it to your `.env` file:

```bash
TFL_API_KEY=your_actual_api_key_here
```