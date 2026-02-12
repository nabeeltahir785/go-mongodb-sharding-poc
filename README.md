# MongoDB Sharding POC

A production-grade MongoDB Sharding Proof of Concept using **Go** and **Docker Compose**. Simulates real-world data distribution, high availability, load balancing, failover, and security.

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                      Go Application                            │
│                  (Multi-mongos failover)                       │
└──────────┬───────────────────────────────┬─────────────────────┘
           │                               │
    ┌──────▼──────┐                 ┌──────▼──────┐
    │  mongos-1   │                 │  mongos-2   │
    │  :27017     │                 │  :27018     │
    └──────┬──────┘                 └──────┬──────┘
           │                               │
    ┌──────▼───────────────────────────────▼──────┐
    │         Config Server RS (configrs)          │
    │   cfg-1:27019  cfg-2:27020  cfg-3:27021     │
    └─────────────────────────────────────────────┘
           │              │              │
    ┌──────▼──────┐┌──────▼──────┐┌──────▼──────┐
    │  Shard 1 RS ││  Shard 2 RS ││  Shard 3 RS │
    │  (shard1rs) ││  (shard2rs) ││  (shard3rs) │
    │             ││             ││             │
    │ P :27022    ││ P :27025    ││ P :27028    │
    │ S :27023    ││ S :27026    ││ S :27029    │
    │ S :27024    ││ S :27027    ││ S :27030    │
    └─────────────┘└─────────────┘└─────────────┘
```

**Total: 14 containers** (3 config servers + 9 shard nodes + 2 mongos routers)

## Prerequisites

- **Docker Desktop** (with at least 8GB RAM allocated)
- **Go 1.21+**
- **Make**

## Quick Start

```bash
# One command to set up everything
make start
```

This will:
1. Generate the authentication keyfile
2. Start all 14 Docker containers
3. Initialize all 4 replica sets (1 config + 3 shards)
4. Create cluster admin users
5. Register all 3 shards through mongos
6. Enable sharding on the application database
7. Create RBAC users (readWrite + read-only)
8. Verify cluster health and RBAC

## Available Commands

| Command | Description |
|---|---|
| `make start` | Full lifecycle: setup → up → init |
| `make setup` | Generate keyfile only |
| `make up` | Start Docker containers only |
| `make init` | Initialize cluster (requires containers running) |
| `make status` | Print cluster status |
| `make down` | Stop containers (preserves data) |
| `make clean` | Stop containers + remove all data volumes |
| `make logs` | Tail all container logs |
| `make logs-mongos` | Tail mongos router logs only |
| `make logs-shard1` | Tail shard 1 logs only |

## Connection Strings

After `make start` completes:

```bash
# Admin access via mongos-1
mongodb://clusterAdmin:admin123@localhost:27017/?authSource=admin

# Admin access via mongos-2
mongodb://clusterAdmin:admin123@localhost:27018/?authSource=admin

# App user (readWrite)
mongodb://appUser:app123@localhost:27017/?authSource=sharding_poc

# Read-only user
mongodb://readOnlyUser:read123@localhost:27017/?authSource=sharding_poc

# Multi-mongos failover
mongodb://clusterAdmin:admin123@localhost:27017,localhost:27018/?authSource=admin
```

## Security

- **Internal Authentication**: Keyfile shared across all `mongod` and `mongos` instances
- **RBAC Users**:
  - `clusterAdmin` — `root` role on `admin` (cluster management)
  - `appUser` — `readWrite` role on `sharding_poc` (application access)
  - `readOnlyUser` — `read` role on `sharding_poc` (analytics)

## Manual Verification

```bash
# Check all containers
docker compose ps

# Shard status
docker exec mongos-1 mongosh --port 27017 \
  -u clusterAdmin -p admin123 --authenticationDatabase admin \
  --eval "sh.status()"

# Replica set status for shard 1
docker exec shard1-1 mongosh --port 27022 \
  -u clusterAdmin -p admin123 --authenticationDatabase admin \
  --eval "rs.status()"

# Test RBAC (should succeed - readWrite user)
docker exec mongos-1 mongosh --port 27017 \
  -u appUser -p app123 --authenticationDatabase sharding_poc \
  --eval "db.test.insertOne({x:1})"

# Test RBAC (should FAIL - read-only user)
docker exec mongos-1 mongosh --port 27017 \
  -u readOnlyUser -p read123 --authenticationDatabase sharding_poc \
  --eval "db.test.insertOne({x:1})"
```

## Project Structure

```
├── cmd/sharding-poc/main.go     # Entrypoint (11-step cluster setup)
├── internal/
│   ├── cluster/
│   │   ├── init.go              # RS init, shard management, mongos connection
│   │   └── status.go            # Cluster status & verification
│   ├── config/config.go         # Configuration loader
│   └── security/rbac.go         # RBAC user management
├── scripts/
│   ├── setup-keyfile.sh         # Keyfile generation
│   └── init-*.js                # RS init scripts (reference)
├── docker-compose.yml           # 14-container topology
├── Makefile                     # Automation
├── .env                         # Default credentials
└── README.md
```

## Troubleshooting

| Issue | Solution |
|---|---|
| Containers not starting | Increase Docker Desktop RAM to 8GB+ |
| `make init` fails | Run `make logs` to check container health |
| Auth errors | Ensure keyfile exists: `ls keyfile/mongo-keyfile` |
| Port conflicts | Check if ports 27017-27030 are available |
| Cluster already initialized | Run `make clean` then `make start` for a fresh setup |
