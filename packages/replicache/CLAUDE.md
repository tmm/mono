# Replicache Package - Offline-First Sync Library

## Overview

Replicache is a sophisticated **offline-first synchronization library** that provides client-side data persistence with optimistic updates, conflict resolution, and seamless server integration. It implements a distributed database architecture with real-time reactive queries and B-tree based persistence.

## Architecture

### Core Design Principles
- **Offline-First**: All operations work locally first, sync happens asynchronously
- **Optimistic Updates**: Mutations applied immediately to local state
- **Server-Authoritative**: Server state wins in conflict resolution
- **ACID Transactions**: Full transaction support with durability guarantees

### Layered Architecture
```
Public API (replicache.ts)
    ↓
Core Implementation (replicache-impl.ts)
    ↓
Storage Layer (B-tree + IndexedDB/Memory)
    ↓
Sync Layer (Pull/Push Protocols)
    ↓
Persistence (IndexedDB with Memory Fallback)
```

## Key Components

### Main Entry Points
- `src/replicache.ts` - Public API wrapper class
- `src/replicache-impl.ts` - Core implementation (1,683 lines)
- `src/mod.ts` - Module exports and type definitions

### Storage Architecture
```typescript
interface Store {
  read(): Promise<Read>;
  write(): Promise<Write>;  
  close(): Promise<void>;
}
```

**Storage Implementations:**
- **`IDBStore`**: IndexedDB persistence for production
- **`MemStore`**: In-memory storage for testing/fallback
- **`IDBStoreWithMemFallback`**: Automatic fallback mechanism

### B-Tree Implementation (`src/btree/`)
- **`node.ts`**: B+ tree node implementation with copy-on-write
- **`read.ts`**: Read operations with lazy loading
- **`write.ts`**: Write operations with immutable updates
- **Features**: O(log n) operations, prefix scans, range queries

## Sync Protocols

### Pull Protocol (`src/sync/pull.ts`)
**Purpose**: Synchronize server state to client

**Flow:**
1. **Request**: Send current cookie to server
2. **Receive**: Server responds with incremental patch
3. **Apply**: Patch applied to local B-tree
4. **Replay**: Local mutations replayed on top

```typescript
export async function beginPullV1(
  profileID: string,
  clientID: ClientID,
  clientGroupID: ClientGroupID,
  schemaVersion: string,
  puller: Puller,
  requestID: string,
  store: Store,
  formatVersion: FormatVersion,
  lc: LogContext,
): Promise<BeginPullResponseV1>
```

### Push Protocol (`src/sync/push.ts`)
**Purpose**: Send local mutations to server

**Flow:**
1. **Gather**: Collect pending local mutations
2. **Send**: POST mutations to push endpoint
3. **Process**: Handle server response (success/failure/conflicts)
4. **Update**: Mark mutations as committed or handle replays

### Conflict Resolution
- **Server Wins**: Server state is always authoritative
- **Deterministic Replay**: Mutations ordered by timestamp + client ID
- **Automatic Recovery**: Failed mutations trigger recovery process
- **Mutation Recovery**: Cross-client instance mutation persistence

## Data Persistence

### B-Tree Features
```typescript
interface PersistentBTree {
  copyOnWrite: boolean;    // Immutable nodes for transaction safety
  lazyLoading: boolean;    // Nodes loaded on-demand
  chunkedStorage: boolean; // Tree nodes stored as chunks
  binarySearch: boolean;   // O(log n) operations within nodes
}
```

**Optimizations:**
- **Lazy Store**: Configurable cache with 100MB default limit
- **Chunk Caching**: Recently accessed nodes kept in memory
- **Binary Search**: Efficient lookups within tree nodes
- **Reference Counting**: Automatic cleanup of unused chunks

### IndexedDB Integration
- **Graceful Degradation**: Automatic fallback to memory storage
- **Transaction Support**: ACID properties maintained
- **Connection Handling**: Automatic reconnection on database issues
- **Quota Management**: Handles storage quota exceeded gracefully

## Reactive Queries

### Subscription System
```typescript
// High-level reactive subscriptions
rep.subscribe(
  (tx) => tx.scan({prefix: 'user/'}).entries().toArray(),
  (users) => updateUI(users)
)

// Low-level change watching
rep.experimentalWatch((diff) => {
  for (const change of diff) {
    console.log(`${change.op}: ${change.key}`)
  }
})
```

**Features:**
- **Dependency Tracking**: Only rerun when accessed keys change
- **Deep Equality**: Avoid unnecessary updates with value comparison
- **Lazy Evaluation**: Defer computation until needed
- **Binary Search**: Efficient diff matching for prefix subscriptions

### Subscription Architecture
- `src/subscriptions.ts` - Subscription manager and lifecycle
- **Memory Management**: Weak references for automatic cleanup
- **Performance**: Binary search for efficient diff matching
- **Batching**: Multiple subscription updates batched together

## Connection Management

### Connection Loop (`src/connection-loop.ts`)
**States:**
1. **Pending**: Waiting for send request or watchdog timer
2. **Debounce**: Batching multiple rapid requests
3. **Wait for Connection**: Respecting concurrent limits
4. **Send**: Active network request
5. **Recovery**: Exponential backoff on failures

**Features:**
- **Exponential Backoff**: Delays increase with consecutive failures
- **Connection Limits**: Maximum concurrent requests
- **Jitter**: Prevents thundering herd problems
- **Graceful Offline**: Continues working when network unavailable

### Authentication
```typescript
interface ReplicacheAuth {
  getAuth?: () => MaybePromise<string | null>;
  onAuthError?: (error: 'invalid-token') => void;
}
```

- **JWT Support**: Token-based authentication
- **Automatic Refresh**: Detects 401 responses and refreshes tokens
- **Retry Logic**: Retries failed requests with new credentials

## Transaction System

### ACID Properties
```typescript
interface WriteTransaction extends ReadTransaction {
  set(key: string, value: JSONValue): Promise<void>;
  del(key: string): Promise<boolean>;
  // Atomicity: All operations succeed or fail together
  // Consistency: Data integrity maintained
  // Isolation: Transactions don't interfere
  // Durability: Changes persist across sessions
}
```

### Mutation System
```typescript
// Optimistic mutations with server sync
await rep.mutate.updateUser({id: 1, name: 'John'});

// Mutation implementation with tracking
async #mutate<R, A>(
  trackingData: MutationTrackingData | undefined,
  name: string,
  mutatorImpl: (tx: WriteTransaction, args?: A) => MaybePromise<R>,
  args: A | undefined,
  timestamp: number,
): Promise<R>
```

## Testing Infrastructure

### Test Utilities
```typescript
// Factory for test instances
const rep = await replicacheForTesting('test', {
  mutators: {
    testMut: async (tx, args) => {
      await tx.set(args.key, args.value);
    },
  },
});

// Test storage
class TestMemStore implements Store {
  // Memory-only implementation for testing
}
```

### Testing Patterns
- **Mock Network**: `fetch-mock` for network request simulation
- **Time Control**: `tickUntil()` for controllable time progression
- **Background Disabling**: Disable background processes for deterministic tests
- **Storage Abstraction**: Swap storage backends for different test scenarios

### Test Structure
- `src/*.test.ts` - Core functionality tests
- `src/persist/*.test.ts` - Persistence layer tests
- `src/sync/*.test.ts` - Synchronization protocol tests
- Comprehensive integration tests with realistic scenarios

## Performance Optimizations

### Memory Management
```typescript
interface PerformanceOptimizations {
  lazyLoading: boolean;     // Load data on-demand
  chunkCaching: boolean;    // Cache hot data in memory
  weakReferences: boolean;  // Automatic cleanup
  copyOnWrite: boolean;     // Minimize allocations
}
```

### Sync Optimizations
- **Differential Updates**: Only changed data transmitted
- **Debouncing**: Batch rapid mutations into single sync
- **Connection Pooling**: Reuse connections across operations
- **Compression**: Efficient patch format for minimal bandwidth

### Query Optimizations
- **Binary Search**: O(log n) lookups in B-tree nodes
- **Prefix Scans**: Efficient range queries
- **Index Reuse**: Shared indexes across transactions
- **Lazy Evaluation**: Defer expensive operations

## Key Files Reference

### Core Implementation
- `src/replicache.ts` - Public API (416 lines)
- `src/replicache-impl.ts` - Core implementation (1,683 lines)
- `src/transactions.ts` - Transaction interfaces and types

### Storage Layer
- `src/kv/idb-store.ts` - IndexedDB storage implementation
- `src/kv/mem-store.ts` - In-memory storage for testing
- `src/btree/` - B+ tree implementation directory

### Sync Layer
- `src/sync/pull.ts` - Pull protocol implementation
- `src/sync/push.ts` - Push protocol implementation
- `src/sync/diff.ts` - Differential update computation

### Database Layer
- `src/db/commit.ts` - Commit-based transaction system
- `src/db/read.ts` - Read transaction implementation
- `src/db/write.ts` - Write transaction implementation

## Development Workflows

### Build Commands
```bash
npm run build          # Production build
npm run test           # Run test suite  
npm run test:watch     # Watch mode testing
npm run check-types    # TypeScript validation
npm run lint           # Code linting
```

### Debugging
- **Debug Logging**: Comprehensive logging with contexts
- **Transaction Inspection**: Examine transaction state
- **Subscription Monitoring**: Track subscription lifecycle
- **Performance Profiling**: Built-in timing and metrics

### Common Development Tasks
1. **Adding Mutators**: Define mutation functions with proper types
2. **Custom Storage**: Implement `Store` interface for custom backends
3. **Subscription Patterns**: Use reactive subscriptions for UI updates
4. **Error Handling**: Implement proper error recovery and user feedback

## Integration Points

### Package Dependencies
- **`shared`**: Common utilities and data structures
- Development dependencies for Zero integration

### Framework Integration
- **React**: Via `zero-react` package
- **Solid**: Via `zero-solid` package  
- **Vanilla JS**: Direct integration via public API

## Common Issues

### Storage Quota
Handle `QuotaExceededError` by implementing storage cleanup or user notification.

### Offline Scenarios
Design mutations to work offline and sync when connection restored.

### Memory Leaks
Always clean up subscriptions and use proper lifecycle management.

### Performance
Monitor subscription count and B-tree cache size for memory usage.

Replicache represents a production-ready implementation of offline-first sync with excellent performance characteristics, robust error handling, and comprehensive testing. It successfully bridges the gap between client-side data management and server-side synchronization while maintaining consistency and reliability.