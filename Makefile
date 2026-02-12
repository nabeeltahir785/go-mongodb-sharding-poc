.PHONY: setup up init start status down clean logs demo help

# Default target
help: ## Show this help
	@echo ""
	@echo "MongoDB Sharding POC - Makefile"
	@echo "================================"
	@echo ""
	@echo "Usage:"
	@echo "  make start    - Full lifecycle: setup + up + init"
	@echo "  make down     - Stop all containers"
	@echo "  make clean    - Stop containers, remove volumes and keyfile"
	@echo "  make status   - Show cluster status"
	@echo "  make logs     - Tail container logs"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Generate keyfile for internal authentication
	@echo "Generating MongoDB keyfile..."
	@chmod +x scripts/setup-keyfile.sh
	@bash scripts/setup-keyfile.sh
	@echo "Keyfile setup complete."

up: ## Start all 14 Docker containers
	@echo "Starting MongoDB sharded cluster (14 containers)..."
	docker compose up -d
	@echo ""
	@echo "Waiting for containers to become healthy..."
	@sleep 15
	docker compose ps
	@echo ""
	@echo "Containers started. Run 'make init' to initialize the cluster."

init: ## Initialize replica sets, create users, add shards (requires 'up' first)
	@echo "Initializing MongoDB sharded cluster..."
	go run ./cmd/sharding-poc/

start: setup up init ## Full lifecycle: generate keyfile, start containers, initialize cluster

demo: ## Run sharding strategy demos (requires running cluster)
	@echo "Running sharding strategy demos..."
	go run ./cmd/sharding-demo/

status: ## Print cluster status report
	@docker compose ps
	@echo ""
	@echo "Shard status:"
	@docker exec mongos-1 mongosh --port 27017 -u clusterAdmin -p admin123 --authenticationDatabase admin --quiet --eval "sh.status()" 2>/dev/null || echo "  [cluster not initialized yet]"

down: ## Stop all containers (preserves data volumes)
	docker compose down

clean: ## Stop containers AND remove all data volumes + keyfile
	docker compose down -v
	rm -rf keyfile/
	@echo "All data volumes and keyfile removed."

logs: ## Tail logs from all containers
	docker compose logs -f --tail=50

logs-mongos: ## Tail logs from mongos routers only
	docker compose logs -f --tail=50 mongos-1 mongos-2

logs-config: ## Tail logs from config servers only
	docker compose logs -f --tail=50 cfg-1 cfg-2 cfg-3

logs-shard1: ## Tail logs from shard 1 nodes
	docker compose logs -f --tail=50 shard1-1 shard1-2 shard1-3

logs-shard2: ## Tail logs from shard 2 nodes
	docker compose logs -f --tail=50 shard2-1 shard2-2 shard2-3

logs-shard3: ## Tail logs from shard 3 nodes
	docker compose logs -f --tail=50 shard3-1 shard3-2 shard3-3
