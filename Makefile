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

install-agentics: check-python setup-env ## Install core Agentics package
	@echo "$(GREEN)Installing core Agentics package with uv...$(NC)"
	uv sync --no-dev
	@echo "$(GREEN)✓ Core Agentics installation complete$(NC)"

install: install-agentics ## Install core Agentics package

install-dev: check-python setup-env ## Install with development dependencies
	@echo "$(GREEN)Installing Agentics (dev mode) with uv...$(NC)"
	uv sync --group dev
	@echo "$(GREEN)✓ Development installation complete$(NC)"


test: ## Run tests
	@echo "$(GREEN)Running tests...$(NC)"
	pytest tests/ -v

test-examples: ## Run example scripts
	@echo "$(GREEN)Running examples...$(NC)"
	python examples/hello_world.py
	@echo "$(GREEN)✓ Examples completed$(NC)"

verify: check-python ## Verify installation
	@echo "$(GREEN)Verifying installation...$(NC)"
	@python3 -c "import agentics; print('✓ Agentics package installed')" || \
		echo "$(RED)✗ Agentics not installed$(NC)"
	@echo "$(GREEN)Verification complete$(NC)"

# Quick development setup
dev-setup: install-dev ## Quick development setup
	@echo "$(GREEN)✓ Development environment ready!$(NC)"
