98129496b325   confluentinc/cp-kafka:7.4.0   "/etc/confluent/dock…"   3 days ago   Up 41 minutes   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp, 0.0.0.0:9101->9101/tcp, [::]:9101->9101/tcp   kafka
98129496b325   confluentinc/cp-kafka:7.4.0   "/etc/confluent/dock…"   3 days ago   Up 41 minutes   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp, 0.0.0.0:9101->9101/tcp, [::]:9101->9101/tcp   kafka
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

echo "🔧 Setting up migration permissions..."
chmod +x services/data-consumer/migrate.py

echo "🚀 Starting services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 90

echo "🗄️ Setting up database migrations..."
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
echo ""
echo "📖 For more commands, run: make help"
