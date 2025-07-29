# ZQLite Package - SQLite Integration for ZQL

## Overview

ZQLite provides **SQLite-based data sources** for ZQL's Incremental View Maintenance system. It's primarily used on the server side (zero-cache) for local SQLite replica management and query execution, enabling ZQL operators to work directly with SQLite databases.

## Architecture

### Core Integration Pattern
```
ZQL Pipeline
    ↓
ZQLite TableSource (implements ZQL Source interface)
    ↓
Enhanced SQLite Database Wrapper
    ↓
SQLite Database with Performance Monitoring
```

### Key Design Principles
- **ZQL Source Interface**: Implements ZQL's Source interface for seamless integration
- **Performance Monitoring**: Built-in query timing and performance tracking
- **Memory Efficiency**: Cursor-based pagination and streaming results
- **Change Integration**: Supports incremental updates from change streams

## Core Components

### Database Wrapper (`src/db.ts`)
**Purpose**: Enhanced SQLite interface with monitoring and optimization
```typescript
export class Database {
  constructor(
    public readonly filename: string,
    options?: DatabaseOptions
  );
  
  // Enhanced query execution with timing
  prepare<P extends Record<string, unknown>, R>(
    sql: string
  ): Statement<P, R>;
  
  // Transaction support
  transaction<T>(fn: () => T): T;
  
  // Performance monitoring
  getStats(): DatabaseStats;
}
```

**Features:**
- **Statement Caching**: Prepared statements cached for reuse
- **Query Timing**: Automatic timing collection for all queries
- **Memory Monitoring**: Track memory usage and query patterns
- **WAL Mode Support**: Optimized for concurrent read/write access

### Table Source (`src/table-source.ts`)
**Purpose**: Implements ZQL Source interface for SQLite tables
```typescript
export class SQLiteTableSource implements Source<Row> {
  constructor(
    db: Database,
    tableName: string,
    options?: TableSourceOptions
  );
  
  // ZQL Source interface implementation
  fetch(): AsyncIterable<Row>;
  push(change: Change<Row>): Promise<void>;
  cleanup(): Promise<void>;
  
  // SQLite-specific optimizations
  createIndex(columns: string[]): Promise<void>;
  vacuum(): Promise<void>;
}
```

**ZQL Integration Features:**
- **Incremental Updates**: Efficient handling of Add/Remove/Edit changes
- **Sorted Results**: Binary search optimization for ordered data
- **Cursor Pagination**: Memory-efficient streaming of large result sets
- **Index Management**: Automatic index creation for query optimization

### Query Delegate (`src/query-delegate.ts`)
**Purpose**: Bridges ZQL queries to SQLite execution
```typescript
interface QueryDelegate {
  executeQuery(query: QueryAST): AsyncIterable<Row>;
  executeMutation(mutation: MutationAST): Promise<MutationResult>;
  subscribeToChanges(callback: ChangeCallback): Subscription;
}
```

**Query Processing:**
- **AST Translation**: Converts ZQL queries to optimized SQLite SQL
- **Parameter Binding**: Safe parameterized query execution
- **Result Streaming**: Lazy evaluation of query results
- **Change Notifications**: Real-time change propagation to ZQL pipeline

## SQLite Optimizations

### Performance Features
```typescript
interface SQLiteOptimizations {
  // Connection optimizations
  walMode: boolean;              // Write-Ahead Logging for concurrency
  synchronous: 'NORMAL';         // Balanced durability/performance
  cacheSize: number;             // Page cache size optimization
  
  // Query optimizations  
  statementCaching: boolean;     // Reuse prepared statements
  indexHints: boolean;           // Automatic index recommendations
  queryPlanning: boolean;        // EXPLAIN QUERY PLAN analysis
}
```

### Memory Management
- **Cursor-Based Iteration**: Stream large result sets without loading all into memory
- **Statement Cleanup**: Automatic prepared statement disposal
- **Connection Pooling**: Reuse connections across operations
- **Cache Tuning**: Optimized page cache settings for workload

### Write Optimizations
- **Batch Inserts**: Group multiple inserts into single transaction
- **Binary Search Inserts**: Maintain sorted order with O(log n) insertions
- **Change Batching**: Accumulate changes before applying to reduce I/O
- **Vacuum Scheduling**: Automatic database maintenance

## Integration with ZQL

### Source Interface Implementation
```typescript
// SQLiteTableSource implements ZQL's Source interface
class SQLiteTableSource implements Source<Row> {
  async *fetch(): AsyncIterable<Row> {
    const stmt = this.db.prepare('SELECT * FROM ? ORDER BY ?');
    for (const row of stmt.iterate(this.tableName, this.orderBy)) {
      yield this.transformRow(row);
    }
  }
  
  async push(change: Change<Row>): Promise<void> {
    switch (change.type) {
      case 'add':
        await this.insertRow(change.row);
        break;
      case 'remove':
        await this.deleteRow(change.row);
        break;
      case 'edit':
        await this.updateRow(change.oldRow, change.newRow);
        break;
    }
  }
}
```

### Change Stream Integration
- **Incremental Updates**: Efficiently processes Add/Remove/Edit changes from upstream
- **Order Preservation**: Maintains sort order during incremental updates
- **Binary Search**: Uses binary search for efficient insertions in sorted data
- **Cleanup Protocol**: Proper resource cleanup when sources are disposed

## Key Files Reference

### Core Implementation
- `src/mod.ts` - Main exports and public API
- `src/db.ts` - Enhanced SQLite database wrapper (primary implementation)
- `src/table-source.ts` - ZQL Source interface implementation
- `src/query-delegate.ts` - Query execution bridge

### Configuration & Options
- `src/options.ts` - Configuration interfaces and default settings

### Testing
- `src/db.test.ts` - Database wrapper functionality tests
- `src/table-source.test.ts` - ZQL Source interface compliance tests
- `src/query.test.ts` - Query execution and optimization tests

### Debugging
- `src/runtime-debug.ts` - Development debugging utilities

## Usage Patterns

### Basic Table Source Setup
```typescript
import { Database, SQLiteTableSource } from '@rocicorp/zqlite';

// Create database with optimization
const db = new Database(':memory:', {
  walMode: true,
  cacheSize: 2000,
  synchronous: 'NORMAL'
});

// Create table source for ZQL pipeline
const userSource = new SQLiteTableSource(db, 'users', {
  orderBy: ['id'],
  indexColumns: ['email', 'created_at']
});

// Use in ZQL pipeline
const pipeline = userSource
  .pipe(new FilterOperator(user => user.active))
  .pipe(new TakeOperator(100));
```

### Performance Monitoring
```typescript
// Get database performance statistics
const stats = db.getStats();
console.log(`Queries executed: ${stats.queryCount}`);
console.log(`Average query time: ${stats.averageQueryTime}ms`);
console.log(`Cache hit rate: ${stats.cacheHitRate * 100}%`);

// Monitor specific table performance
const tableStats = userSource.getStats();
console.log(`Rows processed: ${tableStats.rowsProcessed}`);
console.log(`Index usage: ${tableStats.indexUsage}`);
```

### Change Stream Processing
```typescript
// Process incremental changes from upstream
async function processChanges(changes: Change<Row>[]) {
  await db.transaction(() => {
    for (const change of changes) {
      userSource.push(change);
    }
  });
}

// Subscribe to upstream changes
changeStream.on('batch', processChanges);
```

## Testing Infrastructure

### Test Environment Setup
- **In-Memory Databases**: Fast test execution with `:memory:` databases
- **Temporary Files**: Isolated test databases with automatic cleanup
- **Performance Benchmarks**: Query timing and throughput measurement
- **ZQL Compliance**: Verify Source interface implementation correctness

### Performance Testing
```typescript
// Benchmark query performance
test('large table scan performance', async () => {
  const db = new Database(':memory:');
  await populateTestData(db, 100000);
  
  const start = performance.now();
  const results = [];
  for await (const row of source.fetch()) {
    results.push(row);
  }
  const duration = performance.now() - start;
  
  expect(duration).toBeLessThan(1000); // Should complete in <1s
  expect(results).toHaveLength(100000);
});
```

## Development Workflows

### Build Commands
```bash
npm run build          # TypeScript compilation
npm run test           # Run test suite
npm run check-types    # Type validation
npm run lint           # Code linting
```

### Debugging
- **Query Analysis**: Built-in EXPLAIN QUERY PLAN integration
- **Performance Profiling**: Detailed timing statistics
- **Memory Monitoring**: Track memory usage patterns
- **Change Tracing**: Debug incremental update processing

### Optimization Workflow
1. **Profile Queries**: Use built-in timing to identify slow queries
2. **Analyze Plans**: EXPLAIN QUERY PLAN for optimization opportunities
3. **Add Indexes**: Create indexes for frequently filtered columns
4. **Batch Operations**: Group related changes for better performance
5. **Monitor Results**: Verify performance improvements

## Integration Points

### Package Dependencies
- **`better-sqlite3`**: High-performance SQLite Node.js binding
- **`zql`**: Core ZQL interfaces and types
- **`shared`**: Common utilities and helpers

### Used By
- **`zero-cache`**: Server-side SQLite replica management
- **`zero-server`**: High-level server query processing
- Production deployments requiring local SQLite caching

### SQLite Version Compatibility
- **SQLite 3.38+**: Modern SQLite features and optimizations
- **WAL Mode**: Write-Ahead Logging for better concurrency
- **FTS Support**: Full-text search capabilities (when enabled)
- **JSON Functions**: Native JSON support for complex data types

## Performance Characteristics

### Query Performance
- **Table Scans**: O(n) with memory-efficient streaming
- **Index Lookups**: O(log n) with proper index usage
- **Sorted Results**: O(n log n) for ordering, O(log n) for insertions
- **Aggregations**: Optimized using SQLite's built-in functions

### Memory Usage
- **Streaming Results**: Constant memory usage regardless of result size
- **Statement Caching**: Minimal memory overhead with significant performance benefit
- **Connection Reuse**: Single connection per database instance
- **Page Cache**: Configurable balance between memory and performance

### Concurrency
- **WAL Mode**: Multiple readers, single writer concurrency
- **Transaction Support**: ACID guarantees with minimal blocking
- **Connection Safety**: Thread-safe operations through better-sqlite3

ZQLite serves as a critical bridge between ZQL's incremental view maintenance system and SQLite's efficient storage engine, providing the performance and reliability needed for production server-side deployments while maintaining full compatibility with ZQL's operator pipeline architecture.