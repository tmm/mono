# Rocicorp Monorepo - CLAUDE.md

## Overview

This is the monorepo for **Rocicorp** containing two main products: **Zero** (distributed database with incremental view maintenance) and **Replicache** (client-side sync library). The repository implements a sophisticated distributed system with real-time synchronization, reactive queries, and incremental view maintenance.

## Repository Structure

```
mono/
├── apps/
│   ├── otel-proxy/         # OpenTelemetry proxy service
│   └── zbugs/              # Bug tracker demo app showcasing Zero
├── packages/
│   ├── replicache/         # Client-side sync library
│   ├── zero-client/        # Zero client library (uses Replicache)
│   ├── zero-cache/         # Zero server-side cache/replica (replica of user's upstream database)
│   ├── zero-protocol/      # Communication protocol between client/replica
│   ├── zero-schema/        # Schema definition
│   ├── zql/               # Incremental View Maintenance (IVM) engine
│   ├── zqlite/            # SQLite integration for ZQL
│   ├── shared/            # Common utilities and types
│   └── [other packages]
├── tools/                  # Development and testing tools
└── prod/                   # Production deployment configuration
```

## Key Technologies & Dependencies

- **TypeScript 5.8.2** - Primary language
- **Vitest 3.2.4** - Testing framework
- **Turbo** - Monorepo build system
- **PostgreSQL** - Primary database for server
- **SQLite** - Database used in Zero-Cache and provides `sources` for IVM on the server
- **WebSockets** - Real-time communication
- **OpenTelemetry** - Observability and monitoring
- **Fastify** - HTTP server framework

## Core Architecture

### 1. Zero Database System

Zero is a **distributed database** that spans client and server with **incremental view maintenance** for reactive queries.

**Key Components:**

- **zero-client**: Browser/client library built on Replicache
- **zero-cache**: Server-side coordination and caching layer. Is a replica of the user's upstream database.
- **zql**: Incremental View Maintenance (IVM) engine
- **zero-protocol**: WebSocket-based communication protocol

### 2. Replicache Sync Engine

Replicache provides **offline-first synchronization** with optimistic updates and conflict resolution.

**Key Features:**

- Client-side persistence via IndexedDB
- Optimistic mutations with rollback capability
- Delta synchronization for bandwidth efficiency
- Offline-first architecture

### 3. Incremental View Maintenance (ZQL)

ZQL implements a sophisticated IVM system for **real-time reactive queries**.

**Architecture:**

- **Pipeline Pattern**: Operators chained in query execution pipeline
- **Change Propagation**: Incremental updates flow through materialized views
- **Reference Counting**: Efficient handling of duplicate rows
- **Binary Search**: O(log n) insertions/deletions in sorted results

## Client-Server Communication

### Protocol Overview

The system uses a **WebSocket-based protocol** for real-time synchronization:

**Connection Flow:**

1. Client opens WebSocket to `/sync` endpoint
2. Server sends `connected` message with session ID
3. Client sends `initConnection` with desired queries and schema
4. Bidirectional message flow for queries, mutations, and pokes

**Message Types:**

- **Upstream (Client → Server)**: `initConnection`, `ping`, `pull`, `push`, `changeDesiredQueries`
- **Downstream (Server → Client)**: `connected`, `poke*`, `pong`, `pullResponse`, `pushResponse`, `error`

### Workers & Scaling

**Multi-Process Architecture:**

- **Dispatcher**: Main process handling WebSocket handoffs
- **Syncer Workers**: Handle client connections and query processing
- **Replicator Worker**: Manages database replication and change streaming
- **Change Streamer**: Processes PostgreSQL logical replication

## Development Workflows

### Build System

```bash
npm run build          # Build all packages (uses Turbo)
npm run dev           # Development mode
npm run test          # Run all tests
npm run lint          # Lint all packages
npm run check-types   # TypeScript type checking
```

### Testing Strategy

- **Vitest** for unit and integration tests
- **Multi-database testing**: PostgreSQL versions 15, 16, 17
- **Playwright** for end-to-end testing (in zbugs app)
- **Benchmarking**: Performance tests for critical paths

### Package Scripts

- `test`: `vitest run`
- `test:watch`: `vitest` (watch mode)
- `check-types`: `tsc` (TypeScript checking)
- `format`: `prettier --write .`
- `lint`: `eslint --ext .ts,.tsx,.js,.jsx src/`

## Key Files & Locations

### Core Implementations

- `packages/zero-client/src/client/zero.ts` - Main Zero client implementation
- `packages/zero-cache/src/server/main.ts` - Server entry point
- `packages/replicache/src/replicache.ts` - Replicache client library
- `packages/zql/src/ivm/` - Incremental View Maintenance engine
- `packages/zero-protocol/src/` - Communication protocol definitions

### Configuration Files

- `turbo.json` - Monorepo build configuration
- `vitest.config.ts` - Root test configuration
- `package.json` - Workspace configuration
- `tsconfig.json` - TypeScript configuration

### Testing Configuration

- Multiple `vitest.config.*.ts` files for different PostgreSQL versions
- Workspace-level test orchestration via root `vitest.config.ts`
- Package-specific test configurations

## Development Guidelines

### Code Style

- **ESLint**: `@rocicorp/eslint-config`
- **Prettier**: `@rocicorp/prettier-config`
- **TypeScript**: Strict mode enabled
- **No comments** unless specifically requested

### Testing Requirements

- Always run relevant test commands (`npm run test`, `npm run check-types`)
- Test against multiple PostgreSQL versions for database-related changes
- Include both unit tests and integration tests

### Database Migrations

- Server uses PostgreSQL with logical replication
- Schema changes require migration scripts
- Backup/restore via Litestream for SQLite replicas

### Performance Considerations

- **Lazy Evaluation**: Streams and iterators are lazy
- **Binary Search**: Used extensively for sorted data
- **Reference Counting**: Efficient duplicate handling
- **Connection Pooling**: Configurable database connection limits

## Common Tasks

### Adding New Features

1. Identify affected packages (client, server, protocol, schema)
2. Update protocol definitions if new messages needed
3. Implement server-side changes in zero-cache
4. Implement client-side changes in zero-client
5. Add comprehensive tests
6. Update schema if database changes required

### Debugging

- **OpenTelemetry**: Distributed tracing and metrics
- **Structured Logging**: via `@rocicorp/logger`
- **WebSocket Inspector**: Built-in debugging tools
- **Query Analysis**: Built-in query performance analysis

### Deployment

- Production configuration in `prod/` directory
- SST (Serverless Stack) for AWS deployments
- Multi-environment support (dev, staging, prod)
- Litestream for SQLite backup/restore

## Important Notes

- **Offline-First**: Design assumes intermittent connectivity
- **Optimistic Updates**: Client mutations are applied immediately, then synced
- **Conflict Resolution**: Automatic conflict resolution via timestamps and client IDs
- **Security**: JWT-based authentication with configurable permissions
- **Scalability**: Horizontal scaling via worker processes and connection pooling

This system represents a sophisticated distributed database with real-time synchronization, designed for modern applications requiring offline capability and reactive data updates.
