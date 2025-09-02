# Real-Time Public Transport Analytics Pipeline
# Local Development Commands

.PHONY: help setup start stop logs clean test

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initial setup - copy environment file
	@echo "Setting up local development environment..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ Created .env file from .env.example"; \
		echo "⚠️  Please edit .env and add your TfL API key"; \
	else \
		echo "✅ .env file already exists"; \
	fi

start: ## Start all services
	@echo "🚀 Starting all services..."
	docker-compose up -d
	@echo "✅ Services started!"
	@echo "📊 Grafana: http://localhost:3000 (admin/admin)"
	@echo "🔄 Airflow: http://localhost:8080 (admin/admin)"
	@echo "📈 Kafka UI: http://localhost:9092"

stop: ## Stop all services
	@echo "🛑 Stopping all services..."
	docker-compose down

restart: ## Restart all services
	@echo "🔄 Restarting all services..."
	docker-compose down
	docker-compose up -d

logs: ## Show logs for all services
	docker-compose logs -f

logs-producer: ## Show logs for TfL producer
	docker-compose logs -f tfl-producer

logs-consumer: ## Show logs for data consumer
	docker-compose logs -f data-consumer

logs-airflow: ## Show logs for Airflow
	docker-compose logs -f airflow-webserver airflow-scheduler

build: ## Build all Docker images
	@echo "🔨 Building Docker images..."
	docker-compose build

clean: ## Clean up containers, networks, and volumes
	@echo "🧹 Cleaning up..."
	docker-compose down -v --remove-orphans
	docker system prune -f

status: ## Show status of all services
	@echo "📊 Service Status:"
	docker-compose ps

shell-producer: ## Open shell in producer container
	docker-compose exec tfl-producer bash

shell-consumer: ## Open shell in consumer container
	docker-compose exec data-consumer bash

shell-postgres: ## Open PostgreSQL shell
	docker-compose exec postgres psql -U postgres -d transport_analytics

test-api: ## Test TfL API connection
	@echo "🧪 Testing TfL API connection..."
	curl -s "https://api.tfl.gov.uk/Line/Mode/tube/Status" | jq '.[0].name' || echo "❌ API test failed - check your connection"

kafka-topics: ## List Kafka topics
	docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consumer-test: ## Test Kafka consumer for bus arrivals
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic bus-arrivals --from-beginning --max-messages 5

db-migrate: ## Run database migrations
	docker-compose exec data-consumer alembic upgrade head

db-reset: ## Reset database (WARNING: This will delete all data)
	@echo "⚠️  This will delete all data. Are you sure? [y/N]" && read ans && [ $${ans:-N} = y ]
	docker-compose exec postgres psql -U postgres -c "DROP DATABASE IF EXISTS transport_analytics;"
	docker-compose exec postgres psql -U postgres -c "CREATE DATABASE transport_analytics;"
	docker-compose exec data-consumer python migrate.py

dev-setup: setup build start ## Complete development setup
	@echo "🎉 Development environment is ready!"
	@echo ""
	@echo "Next steps:"
	@echo "1. Edit .env file and add your TfL API key"
	@echo "2. Run 'make restart' to apply the API key"
	@echo "3. Visit http://localhost:3000 for Grafana dashboards"
	@echo "4. Visit http://localhost:8080 for Airflow"
