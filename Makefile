.PHONY: help dev dev-backend dev-frontend install test clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Run both backend and frontend
	@echo "Starting backend and frontend..."
	@make -j2 dev-backend dev-frontend

dev-backend: ## Start backend server
	@echo "Starting backend on http://localhost:8000..."
	@cd backend && PYTHONPATH=$$PYTHONPATH:$(pwd) ./venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000

dev-frontend: ## Start frontend server
	@echo "Starting frontend on http://localhost:8001..."
	@cd frontend && python3 -m http.server 8001

install: ## Install all dependencies
	@echo "Installing npm dependencies..."
	@npm install
	@echo "Installing backend dependencies..."
	@cd backend && ./venv/bin/pip install -r requirements.txt

install-backend: ## Install backend dependencies only
	@echo "Installing backend dependencies..."
	@cd backend && ./venv/bin/pip install -r requirements.txt

test: ## Run backend tests
	@echo "Running tests..."
	@cd backend && ./venv/bin/pytest

clean: ## Clean up generated files
	@echo "Cleaning up..."
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete
	@rm -rf backend/*.db 2>/dev/null || true

status: ## Show server status
	@echo "Backend health:"
	@curl -s http://localhost:8000/api/health 2>/dev/null | jq '.' || echo "  Backend: Not running"
	@echo "Frontend:"
	@curl -s http://localhost:8001/ 2>/dev/null | head -1 | grep -q "Fantasy Baseball Dashboard" && echo "  Frontend: Running on http://localhost:8001" || echo "  Frontend: Not running"
