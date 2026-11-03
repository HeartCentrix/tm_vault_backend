# TM Vault Backend - Microservices Architecture

Python FastAPI backend built with a microservices architecture for the TM Vault M365/Azure backup platform.

## Architecture

```
┌─────────────────────────────────────────────┐
│              API Gateway (8000)             │
└──────────────┬──────────────────────────────┘
               │ Routes requests to:
    ┌──────────┼──────────┬─────────┬──────────┐
    │          │          │         │          │
┌───▼───┐ ┌──▼───┐ ┌───▼───┐ ┌──▼───┐ ┌──▼───┐
│ Auth  │ │Tenant│ │Resource│ │ Job  │ │Snapshot│
│ :8001 │ │:8002 │ │ :8003 │ │:8004 │ │ :8005 │
└───────┘ └──────┘ └───────┘ └──────┘ └───────┘
                                             │
    ┌────────────────────────────────────────┘
    │
┌───▼────────┐  ┌────────────┐
│ Dashboard  │  │  Alert     │
│  :8006     │  │  :8007     │
└────────────┘  └────────────┘
```

### Services

| Service | Port | Responsibility |
|---------|------|----------------|
| **API Gateway** | 8000 | Request routing, CORS, health checks |
| **Auth Service** | 8001 | OAuth2, JWT, user management |
| **Tenant Service** | 8002 | Tenants, organizations, discovery |
| **Resource Service** | 8003 | Resources, SLA policies |
| **Job Service** | 8004 | Jobs, backup triggers, restore, export |
| **Snapshot Service** | 8005 | Snapshots, snapshot items browsing |
| **Dashboard Service** | 8006 | Metrics, aggregated statistics |
| **Alert Service** | 8007 | Alerts, notifications, access groups |

### Infrastructure

| Component | Purpose |
|-----------|---------|
| **PostgreSQL 16** | Shared database for all services |
| **RabbitMQ** | Async job queue (backup, restore, discovery) |
| **Redis** | Caching (optional) |

### Databases / SQL Connections

Every SQL server and database the application connects to:

| Server (host) | Database | Driver / access | Purpose |
|---|---|---|---|
| `tmvault-pooler-rw.databases.svc.cluster.local:5432` (pgbouncer, transaction pooling → shared CNPG `postgres-ha` cluster) | `tmvault_prod` | `asyncpg` via SQLAlchemy (`DB_HOST`) | Primary application database for **all** services (tenants, resources, snapshots, jobs, `audit_events`, settings). |
| `postgres-ha-rw.databases.svc.cluster.local:5432` (CNPG primary, direct — bypasses the pooler) | `tmvault_prod` | `asyncpg` (`DB_DIRECT_HOST`) | LISTEN/NOTIFY only — the pgbouncer transaction pooler does not support LISTEN/NOTIFY, so the notify path connects directly. |
| Per-tenant **Azure SQL** databases (customer-owned; server + database resolved dynamically per backup workload from the tenant's Azure configuration) | (customer databases) | `pyodbc` (`workers/azure-workload-worker/handlers/sql_handler.py` for backup, `sql_restore_handler.py` for restore), `Encrypt=yes` + `TrustServerCertificate=no` | Backing up and restoring customer Azure SQL workloads. Not a fixed connection — one per protected Azure SQL resource. |

Local development uses a single-container `postgres` (see the Configuration block below); production is the shared HA cluster above.

## Quick Start

### Docker Compose (Recommended)

```bash
cd tm_backend

# Copy environment file
cp .env.example .env

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f api-gateway

# Stop
docker-compose down
```

### Manual Development

```bash
# Start PostgreSQL first
# Then run each service:
cd services/auth-service
pip install -r requirements.txt
PYTHONPATH=../.. uvicorn main:app --reload --port 8001
```

## API Endpoints

All endpoints are accessible through the API Gateway at `http://localhost:8000/api/v1/`

### Authentication
- `GET /auth/microsoft/url` - Get Microsoft OAuth2 URL
- `POST /auth/callback` - Exchange code for tokens
- `POST /auth/refresh` - Refresh access token
- `POST /auth/logout` - Logout
- `GET /auth/me` - Get current user

### Dashboard
- `GET /dashboard/overview` - Overview metrics
- `GET /dashboard/status/24hour` - 24h backup status
- `GET /dashboard/status/7day` - 7-day trends
- `GET /dashboard/protection/status` - Protection coverage
- `GET /dashboard/backup/size` - Storage consumption

### Tenants
- `GET /tenants` - List tenants
- `POST /tenants` - Create tenant
- `PUT /tenants/{id}` - Update tenant
- `DELETE /tenants/{id}` - Delete tenant
- `POST /tenants/{id}/discover-m365` - Trigger M365 discovery
- `POST /tenants/{id}/discover-azure` - Trigger Azure discovery

### Resources
- `GET /resources` - List resources (paginated)
- `GET /resources/users` - Get users with workloads
- `POST /resources/{id}/assign-policy` - Assign SLA
- `DELETE /resources/{id}` - Delete resource

### Jobs
- `GET /jobs` - List jobs
- `GET /jobs/{id}/progress` - SSE progress stream
- `POST /jobs/{id}/cancel` - Cancel job
- `POST /backups/trigger` - Trigger backup

### Snapshots
- `GET /resources/{id}/snapshots` - List snapshots
- `GET /resources/snapshots/{id}/items` - List items

### SLA Policies
- `GET /policies` - List policies
- `POST /policies` - Create policy
- `PUT /policies/{id}` - Update policy
- `DELETE /policies/{id}` - Delete policy

### Alerts
- `GET /alerts` - List alerts
- `POST /alerts/{id}/resolve` - Resolve alert

## Configuration

Copy `.env.example` to `.env` and configure:

```env
# Database
DB_HOST=postgres
DB_PORT=5432
DB_NAME=tm_vault_db
DB_USERNAME=tm_vault_admin
DB_PASSWORD=<set-a-strong-password>

# JWT — generate with: python -c "import secrets; print(secrets.token_hex(32))"
JWT_SECRET=<set-a-strong-random-secret>

# Microsoft OAuth
MICROSOFT_CLIENT_ID=your-client-id
MICROSOFT_CLIENT_SECRET=your-client-secret
MICROSOFT_TENANT_ID=common

# RabbitMQ (optional)
RABBITMQ_ENABLED=false

# Redis (optional)
REDIS_ENABLED=false
```

## Project Structure

```
tm_backend/
├── shared/                    # Shared library
│   ├── config.py             # Configuration
│   ├── database.py           # Database connection
│   ├── models.py             # SQLAlchemy models
│   ├── schemas.py            # Pydantic schemas
│   ├── security.py           # Auth utilities
│   └── message_bus.py        # RabbitMQ client
│
├── api-gateway/              # API Gateway
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── services/
│   ├── auth-service/         # Authentication
│   ├── tenant-service/       # Tenant management
│   ├── resource-service/     # Resources & SLA
│   ├── job-service/          # Jobs & triggers
│   ├── snapshot-service/     # Snapshots
│   ├── dashboard-service/    # Metrics
│   └── alert-service/        # Alerts & access
│
├── workers/
│   └── backup-worker/        # Async backup processor
│
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## Inter-Service Communication

- **Synchronous**: HTTP calls via API Gateway routing
- **Asynchronous**: RabbitMQ for job processing
  - `backup.urgent` / `backup.normal` - Backup jobs
  - `restore.urgent` - Restore jobs
  - `discovery.m365` / `discovery.azure` - Discovery jobs
  - `notification` - Email/webhook alerts
  - `delete.low` - Delayed deletion

## Scaling

Each service runs independently and can be scaled horizontally:

```bash
# Scale a specific service
docker-compose up -d --scale resource-service=3
```

## Development

### Adding a New Service

1. Create directory: `services/new-service/`
2. Add `main.py`, `Dockerfile`, `requirements.txt`
3. Import shared models: `from shared.models import ...`
4. Add to `docker-compose.yml`
5. Add routes to `api-gateway/main.py`

### Health Checks

Each service exposes `/health` endpoint:
```bash
curl http://localhost:8001/health  # Auth
curl http://localhost:8002/health  # Tenant
curl http://localhost:8000/health  # Gateway
```

## Azure AD App Setup

TM Vault uses **application-only (app-only) permissions** with Microsoft Graph API to perform enterprise backup operations. This requires a multi-tenant Azure AD app registration.

### Required Application Permissions (not delegated)

The following **Application** permissions must be granted on the Azure AD app:

| Permission | API | Purpose |
|-----------|-----|---------|
| `Directory.Read.All` | Microsoft Graph | Enumerate users, groups, devices |
| `User.Read.All` | Microsoft Graph | Read all user profiles |
| `Group.Read.All` | Microsoft Graph | Read all group objects |
| `Mail.Read` | Microsoft Graph | Read all mailboxes (backup emails) |
| `Files.Read.All` | Microsoft Graph | Read all OneDrive files |
| `Sites.Read.All` | Microsoft Graph | Read all SharePoint sites |
| `Channel.ReadBasic.All` | Microsoft Graph | List Teams channels |
| `ChannelMessage.Read.All` | Microsoft Graph | Read Teams channel messages |
| `Chat.Read.All` | Microsoft Graph | Read Teams chat messages |
| `Contacts.Read` | Microsoft Graph | Read user contacts |
| `Calendars.Read` | Microsoft Graph | Read user calendars |
| `Notes.Read.All` | Microsoft Graph | Read OneNote notebooks |
| `Tasks.Read.All` | Microsoft Graph | Read Planner/Todo tasks |
| `Application.Read.All` | Microsoft Graph | Read app registrations |
| `Policy.Read.All` | Microsoft Graph | Read Conditional Access policies (future) |

### Admin Consent Flow

After registering the app with the above permissions:

1. Go to **Azure Portal → App registrations → Your App → API permissions → Grant admin consent**
2. Or use the **admin consent URL** which TM Vault provides:
   ```
   https://login.microsoftonline.com/organizations/adminconsent?client_id={APP_CLIENT_ID}&redirect_uri={FRONTEND_URL}/datasource-callback&state={csrf_token}
   ```
3. An admin signs in and grants consent — this gives the app tenant-wide access
4. TM Vault then authenticates using **client credentials flow** (`grant_type=client_credentials`) with the app's own `client_secret`

### Environment Variables

```env
# Microsoft Graph app-only credentials (from Azure AD app registration)
MICROSOFT_CLIENT_ID=your-app-client-id
MICROSOFT_CLIENT_SECRET=your-app-client-secret

# Encryption key for storing secrets (generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
ENCRYPTION_KEY=your-fernet-key
```

> **Important**: `MICROSOFT_CLIENT_ID` and `MICROSOFT_CLIENT_SECRET` belong to a **multi-tenant app** with the application permissions listed above. Do **not** use delegated (user-delegated) permissions — they cannot enumerate 10,000 tenant users or access all mailboxes.

## Frontend Integration

Frontend connects to API Gateway at `http://localhost:8000/api/v1/`

Update frontend environment:
```typescript
// environment.ts
export const environment = {
  apiUrl: 'http://localhost:8000/api/v1'
};
```


## Customer code integration (October 2026)

The customer backend snapshot `98e1398e9bf52a7be87cac905f326bf5c62b1c52`
was reconciled with the existing HeartCentrix code. Customer file comparisons
were review aids; the export does not supply verified Git ancestry.

The integration retains customer scheduled drain waves, overlap/outbox fences,
Graph paging and cursor repairs, bounded database writes, and honest partial
failure accounting. It also retains local envelope encryption and KEK rotation,
archive enrichment/export, licensing, sparse chat paths, and the default-off
retention deletion switch. A committed inner FAILED content backup now raises
through the outer single-resource wrapper instead of being reported COMPLETED.

Compatibility decisions:

- Archive writes retain decoded opaque bytes and existing local metadata,
  deduplication, enriched attachment rows, and forced-full behavior, while using
  the customer traversal/coverage/stall safeguards. Existing customer-only
  base64-text archive blobs are not converted by this integration.
- Archive pilot UPN restrictions are optional configuration, with no
  customer-specific account in the default. Whole-user chat skipping remains
  opt-in; per-chat sparse writes retain the prior-inventory gate.
- Full mail baselines may use date buckets. Saved incremental cursors remain
  authoritative; large mailbox growth does not replace them with a fresh census.
- Customer deployment manifests are reference configuration, not an instruction
  to apply them. Existing local deployment settings are retained.

Run the backend tests with `./scripts/run-tests.sh`. For the cross-repository
frontend coverage assertion, set `TM_VAULT_FRONTEND_PATH` to the frontend checkout.
Production Graph/SSO/backup validation and diagnosis of the live copy-slot holder
remain deployment checks; offline tests cannot establish those outcomes.
