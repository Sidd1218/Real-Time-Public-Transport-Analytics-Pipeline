#!/bin/bash

# Health Check Script for Real-Time Public Transport Analytics Pipeline

set -e

echo "🔍 Running comprehensive health check..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service health
check_service_health() {
    local services=("$@")
    local failed_services=()
    
    for service in "${services[@]}"; do
        if [[ "$service" == "airflow-init" ]]; then
            # airflow-init should be exited with code 0
            if ! docker-compose ps $service | grep -q "Exited (0)"; then
                failed_services+=("$service")
            fi
        else
            if ! docker-compose ps $service | grep -q "Up"; then
                failed_services+=("$service")
            fi
        fi
    done
    
    for service in "${services[@]}"; do
        if [[ " ${failed_services[@]} " =~ " $service " ]]; then
            echo -e "❌ ${RED}$service${NC}: Not in expected state"
        else
            echo -e "✅ ${GREEN}$service${NC}: Up"
        fi
    done
}

# Function to check database connectivity and data
check_database_data() {
    echo "🗄️ Checking database connectivity and data..."
    
    # Check if we can connect to PostgreSQL
    if docker-compose exec -T postgres pg_isready -U ${POSTGRES_USER:-transport_user} -d ${POSTGRES_DB:-transport_analytics} > /dev/null 2>&1; then
        echo -e "✅ ${GREEN}PostgreSQL${NC}: Connection successful"
        
        # Check for recent data (last 10 minutes)
        local bus_count=$(docker-compose exec -T postgres psql -U ${POSTGRES_USER:-transport_user} -d ${POSTGRES_DB:-transport_analytics} -t -c "SELECT COUNT(*) FROM bus_arrivals WHERE timestamp > NOW() - INTERVAL '10 minutes';" 2>/dev/null | tr -d ' ')
        local tube_count=$(docker-compose exec -T postgres psql -U ${POSTGRES_USER:-transport_user} -d ${POSTGRES_DB:-transport_analytics} -t -c "SELECT COUNT(*) FROM tube_arrivals WHERE timestamp > NOW() - INTERVAL '10 minutes';" 2>/dev/null | tr -d ' ')
        
        if [[ "$bus_count" =~ ^[0-9]+$ ]] && [ "$bus_count" -gt 0 ]; then
            echo -e "✅ ${GREEN}Bus arrivals${NC}: $bus_count records in last 10 minutes"
        else
            echo -e "⚠️ ${YELLOW}Bus arrivals${NC}: No recent data ($bus_count records)"
        fi
        
        if [[ "$tube_count" =~ ^[0-9]+$ ]] && [ "$tube_count" -gt 0 ]; then
            echo -e "✅ ${GREEN}Tube arrivals${NC}: $tube_count records in last 10 minutes"
        else
            echo -e "⚠️ ${YELLOW}Tube arrivals${NC}: No recent data ($tube_count records)"
        fi
        
        return 0
    else
        echo -e "❌ ${RED}PostgreSQL${NC}: Connection failed"
        return 1
    fi
}

# Function to check Kafka topics
check_kafka_topics() {
    echo "📡 Checking Kafka topics..."
    
    local topics=("bus-arrivals" "tube-arrivals" "line-status" "bike-points")
    local healthy_topics=0
    
    for topic in "${topics[@]}"; do
        if docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^$topic$"; then
            # Get message count in topic
            local offset=$(docker-compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic --time -1 2>/dev/null | cut -d: -f3 | head -1)
            if [[ "$offset" =~ ^[0-9]+$ ]] && [ "$offset" -gt 0 ]; then
                echo -e "✅ ${GREEN}$topic${NC}: $offset messages"
                healthy_topics=$((healthy_topics + 1))
            else
                echo -e "⚠️ ${YELLOW}$topic${NC}: No messages yet"
            fi
        else
            echo -e "❌ ${RED}$topic${NC}: Topic not found"
        fi
    done
    
    if [ $healthy_topics -ge 2 ]; then
        return 0
    else
        return 1
    fi
}

# Function to check web interfaces
check_web_interfaces() {
    echo "🌐 Checking web interfaces..."
    
    # Check Grafana
    if curl -s -f http://localhost:3000/api/health > /dev/null 2>&1; then
        echo -e "✅ ${GREEN}Grafana${NC}: http://localhost:3000 (admin/admin)"
    else
        echo -e "❌ ${RED}Grafana${NC}: Not accessible at http://localhost:3000"
    fi
    
    # Check Airflow
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "✅ ${GREEN}Airflow${NC}: http://localhost:8080 (admin/admin)"
    else
        echo -e "⚠️ ${YELLOW}Airflow${NC}: Not accessible at http://localhost:8080 (may still be starting)"
    fi
}

# Main health check
echo "🐳 Docker Container Status:"
echo "================================"

# Check core infrastructure
check_service_health "zookeeper" "Up"
check_service_health "kafka" "Up"
check_service_health "postgres" "Up"
check_service_health "redis" "Up"

# Check Airflow services
if check_service_health "airflow-init" "Exited (0)"; then
    check_service_health "airflow-webserver" "Up"
    check_service_health "airflow-scheduler" "Up"
fi

# Check data services
check_service_health "tfl-producer" "Up"
check_service_health "data-consumer" "Up"
check_service_health "grafana" "Up"

echo ""
echo "📊 Data Flow Status:"
echo "================================"

# Check database and data flow
check_database_data

echo ""
# Check Kafka topics
check_kafka_topics

echo ""
# Check web interfaces
check_web_interfaces

echo ""
echo "📋 Quick Commands:"
echo "================================"
echo "View logs:           docker-compose logs -f [service-name]"
echo "Restart service:     docker-compose restart [service-name]"
echo "Check service:       docker-compose ps [service-name]"
echo "Database shell:      docker-compose exec postgres psql -U \${POSTGRES_USER} -d \${POSTGRES_DB}"
echo "Kafka topics:        docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list"

echo ""
echo "🔍 Health check completed!"
