# Zero-Protocol Package - Communication Protocol

## Overview

Zero-protocol defines the **bidirectional WebSocket-based communication protocol** between Zero clients and servers. It provides strongly-typed message schemas, AST definitions for queries, and comprehensive protocol versioning for reliable client-server communication.

## Protocol Architecture

### Message Categories
**Upstream Messages (Client → Server):**
- `initConnection` - Initialize connection with queries and schema
- `ping` - Heartbeat and keepalive
- `pull` - Request data sync for mutation recovery
- `push` - Send mutations (CRUD + custom)
- `changeDesiredQueries` - Update active query set
- `closeConnection` - Graceful connection termination
- `deleteClients` - Remove inactive client instances
- `ackMutationResponses` - Acknowledge mutation results

**Downstream Messages (Server → Client):**
- `connected` - Connection confirmation with session ID
- `error` - Protocol errors with backoff strategies
- `pong` - Ping response
- `pokeStart/pokePart/pokeEnd` - Multi-part data sync
- `pullResponse` - Mutation recovery data
- `pushResponse` - Mutation processing results

### Message Format
All messages use **tuple format**: `[messageType, messageBody]`
```typescript
type UpstreamMessage = ['ping', PingBody] | ['push', PushBody] | ...
type DownstreamMessage = ['connected', ConnectedBody] | ['error', ErrorBody] | ...
```

## Query AST System

### AST Structure (`src/ast.ts`)
```typescript
type AST = {
  schema?: string;           // Optional database schema  
  table: string;            // Target table name
  alias?: string;           // Query alias for subqueries
  where?: Condition;        // Filter conditions
  related?: CorrelatedSubquery[];  // JOIN-like relationships
  start?: Bound;           // Pagination start point
  limit?: number;          // Result limit
  orderBy?: Ordering;      // Sort specification
}
```

### Condition System
**Simple Conditions**: Basic comparisons
- Operators: `=`, `!=`, `<`, `>`, `<=`, `>=`, `LIKE`, `IN`, `IS NULL`
- Support for literal values, column references, and parameters

**Complex Conditions**:
- **Conjunction**: AND operations with multiple conditions
- **Disjunction**: OR operations with condition alternatives
- **CorrelatedSubqueryCondition**: EXISTS/NOT EXISTS subqueries

### Value References
```typescript
type Value = 
  | LiteralReference    // Static values (string, number, boolean, null, arrays)
  | ColumnReference     // Database column references  
  | ParameterReference  // Runtime parameters (auth data, pre-mutation rows)
```

## Protocol Messages

### Connection Flow (`src/connect.ts`)
```typescript
// 1. Client opens WebSocket to /sync/v{VERSION}/connect
// 2. Server sends connected message
type ConnectedBody = {
  wsid: string;           // WebSocket session ID
  timestamp: number;      // Server timestamp
}

// 3. Client initializes connection
type InitConnectionBody = {
  clientID: ClientID;
  clientGroupID?: ClientGroupID;
  baseCookie?: string;
  schemaVersion: string;
  desiredQueries: DesiredQueriesMap;
  debugPerf?: boolean;
}
```

### Multi-Part Poke Protocol (`src/poke.ts`)
**Purpose**: Handle large data updates without memory issues

```typescript
// 1. Start poke with metadata
type PokeStartBody = {
  pokeID: string;
  baseCookie?: string;
  schemaVersions?: Record<string, number>;
  timestamp: number;
}

// 2. Send data parts (can be many)
type PokePartBody = {
  pokeID: string;
  rowsPatch?: RowsPatch;
  desiredQueriesPatches?: DesiredQueriesPatch[];
  clientViewRecordPatches?: ClientViewRecordPatch[];
}

// 3. End poke with final cookie
type PokeEndBody = {
  pokeID: string;
  cookie: string;
  cancel?: boolean;
}
```

### Mutation Protocol (`src/push.ts`)
```typescript
type PushBody = {
  clientGroupID?: ClientGroupID;
  mutations: Mutation[];
  pushVersion: number;
  schemaVersion: string;
  timestamp: number;
  requestID: string;
}

type Mutation = {
  type: 'CRUD' | 'custom';
  name: string;
  args: readonly unknown[];
  id: MutationID;
}
```

## Error Handling

### Error Categories (`src/error.ts`)
**Basic Errors:**
- `AuthInvalidated` - Authentication token invalid
- `ClientNotFound` - Client state not found on server
- `InvalidMessage` - Malformed protocol message
- `Unauthorized` - Permission denied

**Backoff Errors** (with retry parameters):
- `Rebalance` - Server rebalancing, retry with delay
- `Rehome` - Client should connect to different server
- `ServerOverloaded` - Server busy, retry with backoff

**Version Errors:**
- `VersionNotSupported` - Protocol version mismatch
- `SchemaVersionNotSupported` - Schema version incompatible

**Mutation Errors:**
- `MutationFailed` - Mutation processing failed
- `MutationRateLimited` - Too many mutations, backoff required

### Error Response Format
```typescript
type ErrorBody = {
  kind: ErrorKind;
  message?: string;
  retryDelayMs?: number;    // For backoff errors
  supportedVersions?: number[]; // For version errors
}
```

## Query Optimization

### Hash-Based Deduplication
```typescript
// AST queries normalized and hashed
function hashOfAST(ast: AST): string {
  const normalized = normalizeAST(ast);
  return h64(JSON.stringify(normalized)).toString(36);
}

// Custom queries hashed by name + args
function hashOfNameAndArgs(name: string, args: readonly unknown[]): string {
  return h64(`${name}:${JSON.stringify(args)}`).toString(36);
}
```

### AST Normalization Process
1. **Field Sorting**: Conditions and related queries sorted for consistency
2. **Condition Flattening**: Nested AND/OR operations flattened
3. **Duplicate Elimination**: Remove redundant conditions
4. **Caching**: Normalized ASTs cached using WeakMap

### Query Management
- **Incremental Updates**: Only changed queries sent via patches
- **TTL Support**: Queries can expire automatically server-side
- **Reference Counting**: Track query usage across clients

## Key Files Reference

### Core Protocol
- `src/up.ts` - Upstream message union type
- `src/down.ts` - Downstream message union type  
- `src/ast.ts` - Query AST definitions (668 lines)
- `src/mod.ts` - Main exports and public API

### Message Types
- `src/connect.ts` - Connection establishment messages
- `src/poke.ts` - Multi-part data synchronization protocol
- `src/push.ts` - Mutation protocol definitions
- `src/pull.ts` - Data pull for mutation recovery

### Supporting Types
- `src/data.ts` - Core data structures and row types
- `src/error.ts` - Error definitions and handling
- `src/query-hash.ts` - Query hashing and normalization
- `src/primary-key.ts` - Primary key utilities

### Protocol Versioning
- `src/protocol-version.ts` - Version constants and compatibility
- `src/version.ts` - Package version information

## Protocol Versioning

### Current Protocol Version: 24
**Version History Highlights:**
- **V5-V6**: Cookie handling improvements
- **V7**: Client schema support
- **V8**: Dropped V5 support
- **V11**: Inspect queries
- **V15**: User push parameters
- **V17-V21**: AST deprecation and custom query support
- **V22-V24**: Enhanced mutation results and acknowledgments

### Compatibility Strategy
- **Graceful Degradation**: Clear error messages for unsupported versions
- **Schema Evolution**: AST changes trigger major version bumps
- **Migration Support**: Server supports multiple protocol versions
- **Backward Compatibility**: Limited support for previous versions

## Testing & Validation

### Schema Validation
Uses **Valita** for runtime schema validation and type inference:
```typescript
import * as v from '@badrap/valita';

const ASTSchema = v.object({
  table: v.string(),
  where: v.optional(ConditionSchema),
  related: v.optional(v.array(CorrelatedSubquerySchema)),
  // ... other fields
});
```

### Test Coverage
- **Message Serialization**: Round-trip testing for all message types
- **AST Normalization**: Property-based testing with fast-check
- **Protocol Compatibility**: Version compatibility testing
- **Name Mapping**: Client ↔ Server schema name translation

### Key Test Files
- `src/ast.test.ts` - AST construction and normalization
- `src/connect.test.ts` - Connection protocol testing
- `src/push.test.ts` - Mutation protocol validation
- `src/protocol-version.test.ts` - Version compatibility

## Development Workflows

### Build Commands
```bash
npm run build          # TypeScript compilation
npm run test           # Run test suite
npm run check-types    # Type checking
npm run lint           # Code linting
```

### Adding New Message Types
1. Define message body schema with Valita
2. Add to appropriate union type (`up.ts` or `down.ts`)
3. Update protocol version if breaking change
4. Add comprehensive tests for serialization
5. Update integration tests in client/server packages

### Protocol Evolution
1. **Backward Compatible**: Add optional fields, increment minor version
2. **Breaking Changes**: Modify existing fields, increment major version
3. **Deprecation**: Mark old features as deprecated before removal
4. **Migration**: Provide clear migration path in documentation

## Integration Points

### Package Dependencies
- **`@badrap/valita`**: Runtime schema validation and type safety
- **`shared`**: Common utilities (hash functions, arrays, validation)
- **`zero-schema`**: Name mapping between client/server schemas

### Used By
- **`zero-cache`**: Server-side protocol implementation
- **`zero-client`**: Client-side protocol implementation
- **`replicache`**: Underlying sync mechanism integration

### External Integration
- **WebSocket Transport**: Protocol designed for WebSocket communication
- **JSON Serialization**: All messages JSON-serializable
- **Compression Friendly**: Structured format works well with compression

## Performance Considerations

### Message Efficiency
- **Tuple Format**: Minimal serialization overhead
- **Hash-based IDs**: Compact query identification
- **Multi-part Protocol**: Prevents memory issues with large updates
- **Incremental Updates**: Only changed data transmitted

### Protocol Optimizations
- **Connection Parameters**: Header-based optimization hints
- **Query Deduplication**: Avoid duplicate network requests
- **Batch Operations**: Multiple mutations in single message
- **Efficient Encoding**: Base36 hashes for compact representation

## Common Issues

### Protocol Version Mismatches
Ensure client and server protocol versions are compatible. Check `supportedVersions` in error responses.

### Message Size Limits
Use multi-part poke protocol for large data updates to avoid WebSocket frame size limits.

### AST Complexity
Keep query ASTs reasonably simple to avoid serialization/parsing overhead.

### Authentication Integration
Properly handle auth token refresh cycles and invalid token scenarios.

Zero-protocol provides a robust, type-safe foundation for real-time data synchronization with careful attention to performance, reliability, and protocol evolution. Its structured approach to message definition and validation ensures reliable communication between Zero clients and servers.