# Zero-Cache Package - Distributed Server Architecture

## Overview

Zero-cache implements the **server-side coordination and caching layer** for the Zero database system. It provides a sophisticated multi-process architecture that bridges PostgreSQL with real-time client synchronization through SQLite replicas and WebSocket communication.

## Architecture

### Multi-Process Design
```
Main Process (Dispatcher)
├── Change Streamer Worker (optional dedicated mode)
├── Replicator Worker (backup mode, optional)
├── Replicator Worker (serving mode)
└── Syncer Workers (1 per CPU core)
    ├── ViewSyncer instances (1 per client group)
    ├── Mutagen instances (handles mutations)
    └── Pusher instances (custom mutations)
```

### Database Architecture
- **PostgreSQL**: Upstream authoritative database with logical replication
- **SQLite Replicas**: Local replicas for serving client requests
- **CVR Database**: Client View Records for sync tracking
- **Change Database**: Replication log coordination between syncers

### Process Communication
- **Node.js IPC**: Inter-process communication between workers
- **WebSocket Handoff**: Connections routed between processes
- **Notification System**: Replicator notifies syncers of state changes

## Key Components

### Main Server (`src/server/main.ts`)
- **Process Orchestration**: Manages worker lifecycle and coordination
- **Service Management**: Implements service-oriented architecture pattern
- **Graceful Shutdown**: Coordinates shutdown across all processes
- **Health Monitoring**: Built-in health checks and observability

### Worker Dispatcher (`src/server/worker-dispatcher.ts`)
- **URL Routing**: Routes WebSocket connections by URL pattern
  - `/sync/v*/start` → Syncer workers
  - `/mutate` → Mutator workers (optional)
  - `/replication` → Replicator workers
- **Load Balancing**: Hash-based distribution of client groups
- **Connection Handoff**: Transfers WebSocket connections to appropriate workers

## Core Workers

### Syncer Worker (`src/workers/syncer.ts`)
**Responsibilities:**
- Handles client WebSocket connections via Zero protocol
- Manages `ViewSyncer` instances for client groups
- Processes queries using ZQL incremental view maintenance
- Handles mutations and custom operations

**Key Features:**
- **Connection Management**: JWT authentication and authorization
- **Query Processing**: Real-time reactive queries via ZQL
- **Mutation Handling**: CRUD operations and custom mutators
- **Poke Protocol**: Multi-part data synchronization messages

### Replicator Worker (`src/workers/replicator.ts`)
**Responsibilities:**
- Manages SQLite replica files and WAL modes
- Coordinates with Litestream for backup operations
- Handles replica reset on schema changes
- Monitors upstream database changes

**Replica Modes:**
- **`serving`**: Main replica for client requests
- **`serving-copy`**: Copy during backup operations
- **`backup`**: Used exclusively by Litestream

### Change Streamer (`src/server/change-streamer.ts`)
**Responsibilities:**
- Streams changes from PostgreSQL logical replication
- Maintains watermark-based change ordering
- Handles transaction boundaries and ACID properties
- Supports custom change sources

## Database Integration

### PostgreSQL Components
```typescript
interface PostgreSQLIntegration {
  upstream: PostgreSQLPool;    // Main authoritative database
  cvr: PostgreSQLPool;         // Client View Records
  change: PostgreSQLPool;      // Change log coordination
  logicalReplication: LogicalReplicationSlot;
}
```

### SQLite Replica Management
- **WAL2 Mode**: Serving replicas use WAL2 for performance
- **WAL Mode**: Backup replicas use standard WAL
- **Atomic Swaps**: Replica updates with atomic file operations
- **Schema Tracking**: Automatic reset on schema version changes

## Protocol Implementation

### WebSocket Protocol Support
- **Protocol Versions**: Supports multiple Zero protocol versions
- **Message Types**: Complete implementation of upstream/downstream messages
- **Connection Lifecycle**: Handshake, authentication, sync, graceful close
- **Error Handling**: Structured error responses with backoff strategies

### Authentication & Authorization
```typescript
interface AuthSystem {
  jwtVerification: JWTVerifier;
  permissionLoader: PermissionLoader;
  readAuthorizer: ReadAuthorizer;
  writeAuthorizer: WriteAuthorizer;
}
```

**Features:**
- **JWT-based Authentication**: Multiple key sources (JWK, JWKS, symmetric)
- **Row-level Security**: Fine-grained permission system
- **Permission Caching**: Dynamic loading and caching from database
- **Token Refresh**: Handles invalid token scenarios gracefully

## Key Files

### Server Infrastructure
- `src/server/main.ts` - Main entry point and process orchestration
- `src/server/worker-dispatcher.ts` - WebSocket connection routing
- `src/server/syncer.ts` - Syncer worker factory
- `src/server/replicator.ts` - Replicator worker factory

### Worker Implementation
- `src/workers/syncer.ts` - Client connection handling
- `src/workers/replicator.ts` - SQLite replica management
- `src/workers/connection.ts` - Individual WebSocket connections

### Database Layer
- `src/db/create.ts` - Database initialization and setup
- `src/db/migration.ts` - PostgreSQL schema migrations
- `src/db/migration-lite.ts` - SQLite replica migrations
- `src/db/pg-to-lite.ts` - PostgreSQL to SQLite replication
- `src/db/transaction-pool.ts` - Database connection pooling

### Authentication
- `src/auth/jwt.ts` - JWT token verification
- `src/auth/read-authorizer.ts` - Query authorization
- `src/auth/write-authorizer.ts` - Mutation authorization
- `src/auth/load-permissions.ts` - Permission loading system

## Configuration

### Environment Variables (prefix with `ZERO_`)
```typescript
interface ZeroConfig {
  // Database connections
  ZERO_UPSTREAM_DB: string;
  ZERO_CVR_DB?: string;
  ZERO_CHANGE_DB?: string;
  
  // Server configuration
  ZERO_PORT: number;
  ZERO_REPLICA_FILE: string;
  
  // Authentication (optional)
  ZERO_AUTH_SECRET?: string;
  ZERO_AUTH_JWK?: string;
  ZERO_AUTH_JWKS_URL?: string;
  
  // Performance tuning
  ZERO_MAX_CONNECTIONS?: number;
  ZERO_WORKER_COUNT?: number;
}
```

### Advanced Configuration
- **Custom Change Sources**: Support for external change streams
- **Replica Management**: Configurable WAL modes and backup strategies
- **Performance Tuning**: Connection pools, worker counts, cache sizes
- **Observability**: OpenTelemetry integration with metrics and tracing

## Testing

### Multi-Database Testing
```bash
# Test against specific PostgreSQL versions
npm run test                    # All tests with PG 17
npm run test:pg-15             # PostgreSQL 15
npm run test:pg-16             # PostgreSQL 16  
npm run test:pg-17             # PostgreSQL 17
npm run test:no-pg             # Tests without database
```

### Test Infrastructure
- **Testcontainers**: Isolated PostgreSQL instances per test
- **Integration Tests**: End-to-end WebSocket client testing
- **Custom PG Support**: Environment variable for custom PostgreSQL
- **Mock Components**: Comprehensive mocking for unit tests

### Key Test Files
- `src/integration/integration.pg-test.ts` - Full system integration
- `src/workers/syncer.test.ts` - Syncer worker functionality
- `src/workers/replicator.test.ts` - Replica management
- `src/db/*.pg-test.ts` - Database layer tests

## Deployment

### Production Features
- **Litestream Integration**: Automatic SQLite backup and restore
- **Graceful Shutdown**: Coordinated worker termination
- **Health Checks**: Built-in monitoring endpoints
- **Rate Limiting**: Per-user mutation rate limiting
- **Resource Management**: Automatic cleanup and garbage collection

### Container Support
- **Docker Ready**: Containerized deployment support
- **Multi-Environment**: Development, staging, production configs
- **Secret Management**: Secure handling of JWT keys and database credentials

## Development Workflows

### Local Development
```bash
npm run dev                    # Development mode with hot reload
npm run build                 # Production build
npm run test                  # Run test suite
npm run check-types           # TypeScript validation
```

### Debugging
- **Structured Logging**: Comprehensive logging with log levels
- **OpenTelemetry**: Distributed tracing and metrics
- **Worker Inspection**: Process state and connection monitoring
- **Database Profiling**: Query performance analysis

### Performance Monitoring
- **Connection Metrics**: WebSocket connection health
- **Query Performance**: ZQL query execution timing
- **Replica Lag**: SQLite replica synchronization delays
- **Memory Usage**: Worker memory consumption tracking

## Integration Points

### Package Dependencies
- **`zero-protocol`**: WebSocket communication protocol
- **`zql`**: Incremental view maintenance engine
- **`zqlite`**: SQLite integration and table sources
- **`zero-schema`**: Schema definition and validation
- **`shared`**: Common utilities and types

### External Dependencies
- **PostgreSQL**: Primary database with logical replication
- **SQLite**: Local replica storage
- **Litestream**: SQLite backup and restore
- **Fastify**: HTTP server framework
- **OpenTelemetry**: Observability and monitoring

## Common Issues

### Replica Sync Issues
Monitor replica lag and ensure logical replication slots don't fall behind. Use `auto_reset` for schema changes.

### Connection Scaling
Adjust worker count and connection pool sizes based on client load. Monitor memory usage per worker.

### Authentication Errors
Verify JWT configuration and ensure permission tables are properly populated in the upstream database.

### Database Migrations
Run migrations on upstream PostgreSQL first, then restart zero-cache to trigger replica updates.

## Performance Tuning

### Worker Configuration
- **Worker Count**: Typically 1 per CPU core for optimal performance
- **Connection Pools**: Size based on expected concurrent clients
- **Memory Limits**: Monitor and adjust worker memory limits

### Database Optimization
- **Connection Pooling**: Shared pools across workers
- **Query Optimization**: Index usage in PostgreSQL
- **WAL Mode Selection**: WAL2 for serving, WAL for backup replicas

Zero-cache represents a sophisticated distributed database server that successfully bridges PostgreSQL's reliability with real-time client synchronization while maintaining ACID guarantees and horizontal scalability.