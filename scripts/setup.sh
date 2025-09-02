#!/bin/bash

# Real-Time Public Transport Analytics Pipeline Setup Script

set -e

echo "🚀 Setting up Real-Time Public Transport Analytics Pipeline..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    echo "Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    echo "Visit: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✅ Docker and Docker Compose are installed"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✅ Created .env file from .env.example"
    echo ""
    echo "⚠️  IMPORTANT: Please edit the .env file and add your TfL API key"
    echo "   You can get a free API key from: https://api-portal.tfl.gov.uk/"
    echo ""
    read -p "Press Enter to continue after updating your .env file..."
else
    echo "✅ .env file already exists"
fi

# Generate Airflow Fernet key if not set
if grep -q "your_fernet_key_here" .env; then
    echo "🔑 Generating Airflow Fernet key..."
    # Try to generate with Python, fallback to a default key if cryptography is not available
    if command -v python3 &> /dev/null && python3 -c "import cryptography" 2>/dev/null; then
        FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    else
        echo "⚠️  cryptography package not found, using a default key for development"
        FERNET_KEY="YlCImzjge_TeZc7jPJ7Jz7-7dFTIaFm0cNJpaE7jnxo="
    fi
    sed -i.bak "s/your_fernet_key_here/$FERNET_KEY/" .env
    echo "✅ Generated Airflow Fernet key"
fi

# Generate Airflow secret key if not set
if grep -q "your_secret_key_here" .env; then
    echo "🔑 Generating Airflow secret key..."
    SECRET_KEY=$(openssl rand -hex 32)
    sed -i.bak "s/your_secret_key_here/$SECRET_KEY/" .env
    echo "✅ Generated Airflow secret key"
fi

echo "🔨 Building Docker images..."
docker-compose build

echo "🔧 Setting up script permissions..."
chmod +x services/data-consumer/migrate.py
chmod +x scripts/health-check.sh

# Function to wait for service health
wait_for_service() {
    local service=$1
    local max_attempts=$2
    local attempt=1
    
    echo "⏳ Waiting for $service to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps $service | grep -q "(healthy)"; then
            echo "✅ $service is healthy"
            return 0
        fi
        
        echo "   Attempt $attempt/$max_attempts - $service not ready yet..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "❌ $service failed to become healthy after $max_attempts attempts"
    echo "📋 Service logs:"
    docker-compose logs --tail=20 $service
    return 1
}

# Function to check if all containers are running
check_all_services() {
    echo "🔍 Checking service status..."
    
    local failed_services=()
    local services=("zookeeper" "kafka" "postgres" "redis" "airflow-init" "airflow-webserver" "airflow-scheduler" "tfl-producer" "data-consumer" "grafana")
    
    for service in "${services[@]}"; do
        if ! docker-compose ps $service | grep -q "Up"; then
            failed_services+=("$service")
        fi
    done
    
    if [ ${#failed_services[@]} -eq 0 ]; then
        echo "✅ All services are running"
        return 0
    else
        echo "❌ Failed services: ${failed_services[*]}"
        return 1
    fi
}

echo "🧹 Cleaning up any existing containers..."
docker-compose down -v

echo "🚀 Starting core infrastructure services..."
docker-compose up -d zookeeper postgres redis

# Wait for core services
wait_for_service "zookeeper" 12
wait_for_service "postgres" 12
wait_for_service "redis" 12

echo "🚀 Starting Kafka..."
docker-compose up -d kafka
wait_for_service "kafka" 15

echo "🚀 Starting Airflow initialization..."
docker-compose up -d airflow-init

# Wait for airflow-init to complete
echo "⏳ Waiting for Airflow initialization to complete..."
while docker-compose ps airflow-init | grep -q "Up"; do
    sleep 5
done

if docker-compose ps airflow-init | grep -q "Exited (0)"; then
    echo "✅ Airflow initialization completed successfully"
else
    echo "❌ Airflow initialization failed"
    docker-compose logs airflow-init
    exit 1
fi

echo "🚀 Starting remaining services..."
docker-compose up -d

echo "⏳ Waiting for all services to be ready..."
sleep 30

# Final health check
if check_all_services; then
    echo "🗄️ Setting up database migrations..."
    
    # Wait a bit more for data-consumer to be fully ready
    sleep 15
    
    # Ensure alembic versions directory exists
    docker-compose exec -T data-consumer mkdir -p services/data-consumer/alembic/versions
    
    # Check if migration files exist, create initial migration if not
    MIGRATION_COUNT=$(docker-compose exec -T data-consumer find services/data-consumer/alembic/versions -name "*.py" | wc -l)
    if [ "$MIGRATION_COUNT" -eq 0 ]; then
        echo "📝 Creating initial migration from models..."
        docker-compose exec -T data-consumer python services/data-consumer/migrate.py create
    fi
    
    echo "🏃 Running database migrations..."
    docker-compose exec -T data-consumer python services/data-consumer/migrate.py
else
    echo "❌ Some services failed to start. Check logs with: docker-compose logs"
    exit 1
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📊 Access your services:"
echo "   Grafana Dashboard: http://localhost:3000 (admin/admin)"
echo "   Airflow Web UI: http://localhost:8080 (admin/admin)"
echo ""
echo "🔧 Useful commands:"
echo "   View logs: make logs"
echo "   Stop services: make stop"
echo "   Restart services: make restart"
echo "   Clean up: make clean"
echo "   Health check: ./scripts/health-check.sh"
echo ""
echo "📖 For more commands, run: make help"
echo ""
echo "🔍 Verifying data flow..."
./scripts/health-check.sh
