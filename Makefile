# Agentics AGStream Makefile
# Complete environment setup and management

.PHONY: help install install-dev install-agentics install-agstream setup-env check-docker start-basic start-full stop clean test flink-sql status logs

# Default target
.DEFAULT_GOAL := help

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m

help: ## Show this help message
	@echo "$(GREEN)Agentics AGStream - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Installation Options:$(NC)"
	@echo "  make install-agentics    # Install core Agentics package only"
	@echo "  make install-agstream    # Install Agentics + AGStream Manager"
	@echo ""
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  make install-agentics    # Install core package"
	@echo "  make install-agstream    # Install with AGStream"
	@echo "  make start-full          # Start all services (with Flink)"
	@echo "  make status              # Check service status"
	@echo ""

check-python: ## Check Python version
	@echo "$(GREEN)Checking Python version...$(NC)"
	@python3 --version || (echo "$(RED)Python 3 not found!$(NC)" && exit 1)
	@python3 -c "import sys; exit(0 if sys.version_info >= (3, 9) else 1)" || \
		(echo "$(RED)Python 3.9+ required!$(NC)" && exit 1)
	@echo "$(GREEN)✓ Python version OK$(NC)"

check-docker: ## Check Docker availability
	@echo "$(GREEN)Checking Docker...$(NC)"
	@docker --version > /dev/null 2>&1 || (echo "$(RED)Docker not found!$(NC)" && exit 1)
	@if ! docker ps > /dev/null 2>&1; then \
		if [ "$$(uname)" = "Darwin" ] && command -v colima > /dev/null 2>&1; then \
			echo "$(YELLOW)Docker daemon not accessible. Restarting Colima...$(NC)"; \
			colima restart; \
			echo "$(YELLOW)Waiting for Docker to be ready (30 seconds)...$(NC)"; \
			sleep 30; \
			if ! docker ps > /dev/null 2>&1; then \
				echo "$(YELLOW)Docker still initializing, waiting 10 more seconds...$(NC)"; \
				sleep 10; \
				if ! docker ps > /dev/null 2>&1; then \
					echo "$(RED)Docker daemon still not accessible after restart!$(NC)"; \
					exit 1; \
				fi; \
			fi; \
		else \
			echo "$(RED)Docker daemon not running!$(NC)"; \
			exit 1; \
		fi \
	fi
	@echo "$(GREEN)✓ Docker is running$(NC)"

check-colima: ## Check if Colima is available (macOS)
	@if [ "$$(uname)" = "Darwin" ]; then \
		if command -v colima > /dev/null 2>&1; then \
			echo "$(GREEN)✓ Colima is available$(NC)"; \
		else \
			echo "$(YELLOW)⚠ Colima not found. Install with: brew install colima$(NC)"; \
		fi \
	fi

setup-env: ## Set up environment file
	@echo "$(GREEN)Setting up environment...$(NC)"
	@if [ ! -f .env ]; then \
		cp .env_sample .env; \
		echo "$(GREEN)✓ Created .env file from .env_sample$(NC)"; \
	else \
		echo "$(YELLOW)⚠ .env file already exists$(NC)"; \
	fi

install-agentics: check-python setup-env ## Install core Agentics package only
	@echo "$(GREEN)Installing core Agentics package with uv...$(NC)"
	uv sync --no-dev
	@echo "$(GREEN)✓ Core Agentics installation complete$(NC)"
	@echo ""
	@echo "$(YELLOW)Core package installed. To install AGStream Manager, run:$(NC)"
	@echo "  make install-agstream"

install-agstream: check-python setup-env ## Install Agentics with AGStream Manager
	@echo "$(GREEN)Installing Agentics with AGStream Manager using uv...$(NC)"
	uv sync --no-dev --group agstream
	@echo "$(GREEN)✓ Agentics with AGStream Manager installation complete$(NC)"
	@echo ""
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. Start services: make start-full"
	@echo "  2. Open UI:        make open-ui"
	@echo "  3. Start Flink:    make flink-sql"

install: install-agentics ## Alias for install-agentics (core package only)

install-dev: check-python setup-env ## Install with development dependencies
	@echo "$(GREEN)Installing Agentics (dev mode) with uv...$(NC)"
	uv sync --group dev
	@echo "$(GREEN)✓ Development installation complete$(NC)"

start-docker: ## Start Docker/Colima if not running
	@echo "$(GREEN)Starting Docker...$(NC)"
	@if [ "$$(uname)" = "Darwin" ] && command -v colima > /dev/null 2>&1; then \
		if ! colima status > /dev/null 2>&1; then \
			echo "$(YELLOW)Starting Colima...$(NC)"; \
			colima start; \
		fi \
	fi
	@$(MAKE) check-docker

start-basic: check-docker ## Start basic stack (Kafka + Karapace + AGStream Manager)
	@echo "$(GREEN)Starting basic stack...$(NC)"
	cd tools/agstream_manager && ./scripts/manage_services.sh start
	@echo ""
	@echo "$(GREEN)✓ Services started!$(NC)"
	@echo "$(YELLOW)Access Points:$(NC)"
	@echo "  AGStream Manager: http://localhost:5003"
	@echo "  Kafka UI:         http://localhost:8080"
	@echo "  Schema Registry:  http://localhost:8081"

start-full: check-docker ## Start full stack (Kafka + Karapace + Flink + AGStream Manager)
	@echo "$(GREEN)Starting full stack with Flink...$(NC)"
	cd tools/agstream_manager && ./scripts/manage_services_full.sh start
	@echo ""
	@echo "$(GREEN)✓ Services started!$(NC)"
	@echo "$(YELLOW)Access Points:$(NC)"
	@echo "  AGStream Manager: http://localhost:5003"
	@echo "  Flink Web UI:     http://localhost:8085"
	@echo "  Kafka UI:         http://localhost:8080"
	@echo "  Schema Registry:  http://localhost:8081"

stop: ## Stop all services
	@echo "$(GREEN)Stopping services...$(NC)"
	@if [ -f tools/agstream_manager/scripts/manage_services_full.sh ]; then \
		cd tools/agstream_manager && ./scripts/manage_services_full.sh stop 2>/dev/null || ./scripts/manage_services.sh stop; \
	else \
		cd tools/agstream_manager && ./scripts/manage_services.sh stop; \
	fi
	@echo "$(GREEN)✓ Services stopped$(NC)"

restart-basic: ## Restart basic stack
	@echo "$(GREEN)Restarting basic stack...$(NC)"
	cd tools/agstream_manager && ./scripts/manage_services.sh restart

restart-full: ## Restart full stack
	@echo "$(GREEN)Restarting full stack...$(NC)"
	cd tools/agstream_manager && ./scripts/manage_services_full.sh restart

clean: stop ## Stop services and clean Kafka data
	@echo "$(GREEN)Cleaning Kafka data...$(NC)"
	cd tools/agstream_manager && (./scripts/manage_services.sh clean-restart || ./scripts/manage_services_full.sh clean-restart)
	@echo "$(GREEN)✓ Clean restart complete$(NC)"

status: ## Show service status
	@echo "$(GREEN)Service Status:$(NC)"
	@cd tools/agstream_manager && (./scripts/manage_services.sh status 2>/dev/null || ./scripts/manage_services_full.sh status 2>/dev/null || echo "$(RED)No services running$(NC)")

logs: ## Show AGStream Manager logs
	@echo "$(GREEN)AGStream Manager Logs:$(NC)"
	@tail -f /tmp/agstream_manager.log

logs-kafka: ## Show Kafka logs
	@echo "$(GREEN)Kafka Logs:$(NC)"
	@cd tools/agstream_manager && (docker compose -f docker-compose-karapace.yml logs -f kafka 2>/dev/null || \
		docker compose -f docker-compose-karapace-flink.yml logs -f kafka)

logs-flink: ## Show Flink JobManager logs
	@echo "$(GREEN)Flink JobManager Logs:$(NC)"
	@cd tools/agstream_manager && docker compose -f docker-compose-karapace-flink.yml logs -f flink-jobmanager

flink-sql: ## Start Flink SQL client with auto-loaded tables
	@echo "$(GREEN)Starting Flink SQL Client...$(NC)"
	@cd tools/agstream_manager && ./scripts/flink

test: ## Run tests
	@echo "$(GREEN)Running tests...$(NC)"
	pytest tests/ -v

test-examples: ## Run example scripts
	@echo "$(GREEN)Running examples...$(NC)"
	python examples/hello_world.py
	@echo "$(GREEN)✓ Examples completed$(NC)"

open-ui: ## Open AGStream Manager in browser
	@echo "$(GREEN)Opening AGStream Manager...$(NC)"
	@open http://localhost:5003 2>/dev/null || xdg-open http://localhost:5003 2>/dev/null || \
		echo "$(YELLOW)Please open http://localhost:5003 in your browser$(NC)"

open-kafka-ui: ## Open Kafka UI in browser
	@echo "$(GREEN)Opening Kafka UI...$(NC)"
	@open http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080 2>/dev/null || \
		echo "$(YELLOW)Please open http://localhost:8080 in your browser$(NC)"

open-flink-ui: ## Open Flink Web UI in browser
	@echo "$(GREEN)Opening Flink Web UI...$(NC)"
	@open http://localhost:8085 2>/dev/null || xdg-open http://localhost:8085 2>/dev/null || \
		echo "$(YELLOW)Please open http://localhost:8085 in your browser$(NC)"

verify: check-python check-docker ## Verify installation
	@echo "$(GREEN)Verifying installation...$(NC)"
	@python3 -c "import agentics; print('✓ Agentics package installed')" || \
		echo "$(RED)✗ Agentics not installed$(NC)"
	@python3 -c "import flask_sock; print('✓ flask-sock installed')" || \
		echo "$(RED)✗ flask-sock not installed$(NC)"
	@curl -s http://localhost:5003 > /dev/null && \
		echo "$(GREEN)✓ AGStream Manager is running$(NC)" || \
		echo "$(YELLOW)⚠ AGStream Manager not running$(NC)"
	@echo "$(GREEN)Verification complete$(NC)"

# Complete setup from scratch
setup: check-python check-colima install start-docker start-full verify ## Complete setup from scratch
	@echo ""
	@echo "$(GREEN)═══════════════════════════════════════════════════════$(NC)"
	@echo "$(GREEN)✓ Setup Complete!$(NC)"
	@echo "$(GREEN)═══════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(YELLOW)Next Steps:$(NC)"
	@echo "  1. Open AGStream Manager: make open-ui"
	@echo "  2. Start Flink SQL:       make flink-sql"
	@echo "  3. Run examples:          make test-examples"
	@echo "  4. View logs:             make logs"
	@echo ""
	@echo "$(YELLOW)Useful Commands:$(NC)"
	@echo "  make status      - Check service status"
	@echo "  make stop        - Stop all services"
	@echo "  make restart-full - Restart all services"
	@echo "  make help        - Show all commands"
	@echo ""

# Quick development setup
dev-setup: install-dev start-full ## Quick development setup
	@echo "$(GREEN)✓ Development environment ready!$(NC)"

# Cleanup everything
purge: stop ## Remove all data and containers
	@echo "$(YELLOW)⚠ This will remove all data!$(NC)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		cd tools/agstream_manager && docker compose -f docker-compose-karapace.yml down -v 2>/dev/null || true; \
		cd tools/agstream_manager && docker compose -f docker-compose-karapace-flink.yml down -v 2>/dev/null || true; \
		rm -rf agstream-backends/; \
		echo "$(GREEN)✓ Purge complete$(NC)"; \
	fi

# Show all access points
urls: ## Show all service URLs
	@echo "$(GREEN)Service URLs:$(NC)"
	@echo "  AGStream Manager:     http://localhost:5003"
	@echo "  Kafka UI:             http://localhost:8080"
	@echo "  Schema Registry UI:   http://localhost:8000"
	@echo "  Schema Registry API:  http://localhost:8081"
	@echo "  Flink Web UI:         http://localhost:8085"
	@echo "  Persistence API:      http://localhost:8083"
