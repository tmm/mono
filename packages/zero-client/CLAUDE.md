# Zero-Client Package - Distributed Database Client

## Overview

Zero-client provides a high-level **reactive database interface** built on top of Replicache for sync and persistence. It acts as a distributed database client that spans client and server with incremental view maintenance, WebSocket connectivity, and real-time reactive queries.

## Architecture

### Core Integration Stack
```
Zero Client API (zero.ts)
    ↓
ZQL Integration (Incremental View Maintenance)
    ↓
Replicache (Sync Engine + Persistence)
    ↓
IndexedDB/Memory Storage
```

### Key Design Patterns
- **Reactive Database**: Real-time query results that update automatically
- **Offline-First**: Built on Replicache's offline-first architecture
- **Server Integration**: WebSocket-based protocol with zero-cache server
- **Type Safety**: Full TypeScript integration with schema-driven types

## Core Components

### Main Zero Class (`src/client/zero.ts`)
**Primary Interface**: `Zero<S, MD, TWrappedTransaction>`
- **S**: Schema type for type safety
- **MD**: Mutator definitions for custom mutations  
- **TWrappedTransaction**: Transaction wrapper type

**Key Responsibilities:**
- Connection management and WebSocket lifecycle
- Query orchestration and deduplication
- Mutation handling and optimistic updates
- Integration with Replicache for persistence and sync

### Context Bridge (`src/client/context.ts`)
**Purpose**: `ZeroContext` bridges ZQL and Replicache
```typescript
class ZeroContext implements QueryDelegate {
  // Bridges data changes between Replicache and ZQL IVM
  // Handles view updates and change propagation
  // Manages transaction isolation and consistency
}
```

### Query Management (`src/client/query-manager.ts`)
**Features:**
- Query deduplication by hash
- Reference counting and TTL management
- Server synchronization via `changeDesiredQueries`
- Recent queries caching with configurable limits

## Reactive Query System

### Query Interface
```typescript
// Type-safe queries generated from schema
export type MakeEntityQueriesFromSchema<S extends Schema> = {
  readonly [K in keyof S['tables'] & string]: Query<S, K>;
};

// Usage example
const users = await zero.query.user.findMany();
const user = await zero.query.user.findUnique({where: {id}});
```

### Integration with ZQL
- **QueryManager**: Maintains active query registry with deduplication
- **IVMSourceBranch**: Provides in-memory sources for incremental updates
- **Batch Updates**: Synchronous view updates within batch boundaries
- **Change Propagation**: Replicache → ZeroContext → IVM → Query views

### Real-time Updates
```typescript
// Reactive subscriptions that update automatically
const subscription = zero.query.user.findMany().subscribe(users => {
  updateUI(users);
});
```

## Connection Architecture

### WebSocket Management
```typescript
enum ConnectionState {
  Disconnected,
  Connecting,
  Connected
}
```

**Features:**
- **Zero Protocol v1**: Structured upstream/downstream messages
- **Handshake Optimization**: `sec-protocol` header for connection params
- **Health Monitoring**: Ping/pong every 5 seconds
- **Automatic Reconnection**: Exponential backoff with jitter

### Connection Lifecycle
1. **WebSocket Creation**: Connect to zero-cache `/sync` endpoint
2. **Protocol Handshake**: Negotiate protocol version and features
3. **Authentication**: JWT-based auth with token refresh support
4. **Query Registration**: Send desired queries to server
5. **Real-time Sync**: Bidirectional message flow for updates

### Hidden Tab Optimization
- **Auto-disconnect**: Disconnect after 5 seconds when tab hidden
- **Resource Conservation**: Reduces server load and client battery usage
- **Automatic Reconnection**: Reconnect when tab becomes visible

## CRUD Operations (`src/client/crud.ts`)

### Table-Level Operations
```typescript
interface TableCRUD<T> {
  insert(values: T[]): Promise<void>;
  upsert(values: T[], options?: UpsertOptions): Promise<void>;
  update(where: Condition, values: Partial<T>): Promise<void>;
  delete(where: Condition): Promise<void>;
}
```

**Features:**
- **Batch Operations**: Multiple rows processed in single transaction
- **Type Safety**: Schema-driven type checking for all operations
- **Optimistic Updates**: Immediate local updates with server sync
- **Conflict Resolution**: Server-authoritative conflict handling

### Mutation System
```typescript
// Built-in CRUD mutations
await zero.mutate.user.insert([{name: 'John', email: 'john@example.com'}]);

// Custom mutations
const zero = new Zero({
  mutators: {
    promoteUser: async (tx, userID: string) => {
      const user = await tx.get(['user', userID]);
      await tx.set(['user', userID], {...user, role: 'admin'});
    }
  }
});
await zero.mutate.promoteUser('user123');
```

## Authentication & Security

### JWT Integration
```typescript
interface ZeroAuth {
  auth?: string | ((error?: 'invalid-token') => MaybePromise<string | undefined>);
}
```

**Features:**
- **Static Tokens**: Simple string-based authentication
- **Dynamic Tokens**: Function-based providers for token refresh
- **Error Handling**: Automatic token refresh on `invalid-token` errors
- **Secure Storage**: No token storage, always requested fresh

### Authorization
- **Server-Side**: Row-level security handled by zero-cache
- **Client Validation**: Early validation for better user experience
- **Permission Caching**: Permissions cached for performance

## Metrics & Analytics

### Client Metrics (`src/client/metrics.ts`)
```typescript
interface MetricCollection {
  connectionMetrics: ConnectionHealthMetrics;
  queryMetrics: QueryPerformanceMetrics;
  mutationMetrics: MutationTrackingMetrics;
}
```

**Current Status**: 
- **Remote Reporting**: Disabled (`enableAnalytics: false`)
- **Console Logging**: Active for development debugging
- **Custom Tags**: Support for environment and host identification

### Performance Monitoring
- **Connection Health**: Time to connect, error rates, disconnect reasons
- **Query Performance**: Materialization timing and warning thresholds
- **Mutation Tracking**: Success rates and error categorization

## Testing Infrastructure

### Test Environment
```typescript
interface TestZero<S extends Schema> {
  zero: Zero<S>;
  testingContext: TestingContext;
}

interface TestingContext {
  puller: Puller;
  pusher: Pusher;
  socketResolver: () => Resolver<WebSocket>;
  connectionState: () => ConnectionState;
}
```

### Testing Patterns
- **Mock WebSockets**: Simulated server responses for testing
- **Connection States**: Controllable connection state changes
- **Deterministic Behavior**: Background processes disabled in tests
- **Comprehensive Coverage**: 23+ test files covering all functionality

### Key Test Files
- `src/client/zero.test.ts` - Core functionality tests
- `src/client/context.test.ts` - ZQL integration tests
- `src/client/query-manager.test.ts` - Query management tests
- `src/client/mutation-tracker.test.ts` - Mutation lifecycle tests

## Performance Optimizations

### Query Optimization
```typescript
interface QueryOptimizations {
  deduplication: boolean;    // Hash-based query deduplication
  referenceCounting: boolean; // Automatic query lifecycle
  ttlManagement: boolean;    // Time-to-live for unused queries
  batchUpdates: boolean;     // Batch query changes with throttling
}
```

### Memory Management
- **Recent Queries Cache**: Configurable size with LRU eviction
- **IVM Source Lifecycle**: Lazy creation and automatic cleanup
- **Weak References**: Automatic cleanup of unused subscriptions
- **Connection Pooling**: Efficient WebSocket resource usage

### Network Optimization
- **Frame-based Debouncing**: Uses `requestAnimationFrame` for batching
- **Protocol Compression**: Minimal overhead message format
- **Incremental Updates**: Only changed data transmitted
- **Connection Parameter Optimization**: Header-based optimization hints

## Key Files Reference

### Core Implementation
- `src/client/zero.ts` - Main Zero client class (2,120 lines)
- `src/client/context.ts` - ZQL-Replicache bridge
- `src/client/zero-rep.ts` - Replicache integration layer

### Query System
- `src/client/query-manager.ts` - Query lifecycle and deduplication
- `src/client/ivm-branch.ts` - IVM source management with forking
- `src/client/crud.ts` - Table-level CRUD operations

### Connection Management
- `src/client/zero-poke-handler.ts` - Multi-part server message handling
- `src/client/mutation-tracker.ts` - Mutation state tracking
- `src/client/server-option.ts` - Server configuration and endpoints

### Support Infrastructure
- `src/client/metrics.ts` - Performance monitoring and analytics
- `src/client/options.ts` - Configuration interfaces and types
- `src/util/` - Utility functions and helpers

## Development Workflows

### Basic Usage
```typescript
import {Zero} from '@rocicorp/zero';

const zero = new Zero({
  server: 'ws://localhost:4848',
  schema: mySchema,
  auth: () => getAuthToken(),
});

// Reactive queries
const users = zero.query.user.findMany().subscribe(users => {
  console.log('Current users:', users);
});

// Mutations
await zero.mutate.user.insert([{name: 'Alice', email: 'alice@example.com'}]);
```

### Development Commands
```bash
npm run build          # Production build
npm run test           # Run test suite
npm run test:watch     # Watch mode testing  
npm run check-types    # TypeScript validation
```

### Debugging Tools
```typescript
// Inspector interface for runtime debugging
const inspector = await zero.inspect();
console.log('Active queries:', inspector.queries);

// Global debugging (development only)
globalThis.__zero = zero;
```

## Integration Points

### Package Dependencies
- **`zero-protocol`**: Message schemas, AST definitions, error types
- **`zero-schema`**: Schema building, name mapping, permissions
- **`zql`**: Query implementation and IVM engine
- **`replicache`**: Core sync engine and persistence
- **`shared`**: Common utilities and browser environment handling

### Framework Integration
- **React**: Use with `@rocicorp/zero/react` 
- **Solid**: Use with `@rocicorp/zero/solid`
- **Vanilla**: Direct integration via Zero client API

### Server Integration
- **zero-cache**: WebSocket server with distributed architecture
- **PostgreSQL**: Upstream database with logical replication
- **Authentication**: JWT-based with configurable providers

## Common Issues

### Connection Problems
- Verify WebSocket endpoint URL format
- Check authentication token validity and refresh logic
- Monitor network connectivity and retry behavior

### Query Performance
- Use query deduplication to avoid duplicate network requests
- Monitor materialization warnings for slow queries
- Consider pagination for large result sets

### Memory Usage
- Clean up subscriptions when components unmount
- Monitor recent queries cache size
- Use proper TypeScript types to catch issues early

### Schema Mismatches
- Ensure client and server schema versions match
- Handle `onUpdateNeeded` callbacks for version updates
- Test schema migrations thoroughly

Zero-client represents a sophisticated distributed database client that seamlessly integrates real-time synchronization, offline-first capabilities, and reactive query management while providing excellent developer experience through comprehensive TypeScript integration.