# ZQL Package - Incremental View Maintenance Engine

## Overview

ZQL is the **Incremental View Maintenance (IVM) engine** that powers Zero's reactive query system. It implements a sophisticated pipeline architecture where operators process data streams incrementally, enabling real-time updates to materialized views with O(log n) performance characteristics.

## Architecture

### Pipeline Pattern
ZQL uses a **chain of responsibility pattern** where data flows through a series of operators:

```
Source → Filter → Join → Take → Skip → View
  ↓        ↓       ↓      ↓      ↓
Storage  Storage Storage Storage Storage
```

Each operator implements `Input` and `Output` interfaces, maintaining minimal state for incremental updates.

### Key Design Principles
- **Lazy Evaluation**: Data streams process on-demand
- **Copy-on-Write**: Efficient memory usage with shared nodes
- **Reference Counting**: Tracks data lifecycle in views
- **Binary Search**: O(log n) operations in sorted data structures

## Core Components

### Interfaces (`src/ivm/operator.ts`)
```typescript
interface Input<T> {
  fetch(): AsyncIterable<T>;
}

interface Output<T> {
  push(change: Change<T>): Promise<void>;
  cleanup(): Promise<void>;
}

interface Operator<I, O> extends Input<O>, Output<I> {
  setOutput(output: Output<O>): void;
}
```

### Change Types (`src/ivm/change.ts`)
- **AddChange**: New row added to result set
- **RemoveChange**: Row removed from result set
- **EditChange**: Row modified (may split into remove + add)
- **ChildChange**: Change in relationship data

### Core Operators
- **`MemorySource`**: Root data provider with BTree indexing
- **`Filter`**: Stateless filtering using predicates
- **`Join`**: Hierarchical relationships with size caching
- **`Take`**: Limit operator with bound tracking
- **`Skip`**: Offset operator for pagination
- **`Exists`**: EXISTS/NOT EXISTS with relationship caching

## Key Files

### Core Implementation
- `src/ivm/operator.ts` - Base interfaces and types
- `src/ivm/memory-source.ts` - In-memory data source with BTree
- `src/ivm/filter.ts` - Filtering operator
- `src/ivm/join.ts` - Relationship join operator
- `src/ivm/take.ts` - Limit operator with sophisticated bounds
- `src/ivm/exists.ts` - EXISTS operator with size caching

### Data Structures
- `src/ivm/data.ts` - Core data types (Node, Row)
- `src/ivm/stream.ts` - Lazy stream abstraction
- `src/ivm/memory-storage.ts` - Key-value storage for operator state

### Query Processing
- `src/builder/builder.ts` - Pipeline construction from AST
- `src/query/query-impl.ts` - High-level query interface

## IVM System Operation

### Incremental Updates
1. **Change received**: Source detects data change
2. **Change propagation**: Flows through operator pipeline
3. **State updates**: Each operator updates minimal state
4. **View notification**: Final view receives incremental change

### Memory Source Features
- **BTree Indexing**: O(log n) insertions/deletions
- **Overlay System**: Handles concurrent modifications
- **Index Reuse**: Shared indexes across connections
- **Reference Counting**: Tracks row lifecycle

### Join Operator Capabilities
- **Hierarchical Data**: Supports nested relationships
- **Size Caching**: Caches relationship sizes for performance
- **Incremental Updates**: Efficiently processes relationship changes
- **Edit Splitting**: Complex edits split into atomic operations

## Testing

### Test Structure
- `src/ivm/test/*.test.ts` - Core operator tests
- `src/ivm/test/push-tests.ts` - Comprehensive push testing framework
- Uses **Vitest** with 20-second timeouts
- Extensive **snapshot testing** for regression prevention
- **Property-based testing** with `fast-check`

### Key Test Files
- `memory-source.test.ts` - Source implementation tests
- `join.push.test.ts` - Join operator incremental updates
- `take.push.test.ts` - Take operator boundary handling
- `fan-out-fan-in.test.ts` - Pipeline integration tests

## Performance Optimizations

### Hot Path Optimizations
- **Binary Search**: Used extensively for sorted data
- **Comparator Caching**: Pre-computed for performance
- **UTF-8 String Comparison**: Optimized using `compare-utf8`
- **Monomorphic Dispatch**: Consistent object shapes for V8

### Memory Management
- **Lazy Streams**: Deferred processing until consumption
- **State Minimization**: Operators maintain minimal necessary state
- **Cleanup Protocols**: Explicit resource management
- **Reference Counting**: Automatic cleanup via view system

## Development Workflows

### Adding New Operators
1. Implement `Input` and `Output` interfaces
2. Add state management via `Storage` interface
3. Implement `fetch()` for initial data retrieval
4. Implement `push()` for incremental updates
5. Add comprehensive tests with push scenarios
6. Consider performance implications and optimization

### Debugging
- **Logging Framework**: Built-in with operator identification
- **State Inspection**: Storage contents examination in tests
- **Change Tracing**: Full change propagation tracking
- **Snapshot Comparison**: Expected vs actual result analysis

### Performance Tuning
- **Index Analysis**: Memory source index key inspection
- **Stream Profiling**: Stream consumption pattern analysis
- **State Size Monitoring**: Operator storage monitoring
- **Change Volume Analysis**: Push frequency and volume measurement

## Integration Points

### Package Dependencies
- **`zero-protocol`**: AST definitions, data types, query hashing
- **`zero-schema`**: Schema definitions and table metadata
- **`shared`**: Common utilities (asserts, iterables, btree-set)

### Used By
- **`zero-client`**: Reactive queries and client-side IVM
- **`zero-cache`**: Server-side query processing
- **`zero-server`**: High-level server query interface

## Common Issues

### Edit Splitting
When key fields change, `EditChange` may be split into `RemoveChange` + `AddChange` for correct index maintenance.

### Bound Tracking
Take operator maintains complex bounds that adjust dynamically as data changes, requiring careful state management.

### Memory Leaks
Always call `cleanup()` on operators to prevent resource leaks. Use reference counting for automatic cleanup.

## Build Commands

```bash
npm run test          # Run all tests
npm run check-types   # TypeScript type checking
npm test -- --watch  # Watch mode testing
```

The ZQL engine represents one of the most sophisticated implementations of incremental view maintenance, enabling Zero to provide real-time reactive queries with excellent performance characteristics.