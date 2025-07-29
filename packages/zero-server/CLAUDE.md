# Zero-Server Package - High-Level Server Integration

## Overview

Zero-server provides a **simplified, high-level API** for integrating Zero into server applications. It handles query processing, mutation management, database schema introspection, and provides an abstraction layer over the lower-level zero-cache infrastructure.

## Architecture

### Layered Server Architecture
```
Zero-Server API (Simplified Interface)
    ↓
Z2S (ZQL to SQL Compilation)
    ↓  
ZQL (Query Processing & Validation)
    ↓
Database Adapters (PostgreSQL, Drizzle)
    ↓
Database Layer (PostgreSQL)
```

### Core Design Philosophy
- **Developer Experience**: Simple API for complex database operations
- **Type Safety**: Full TypeScript integration with zero-schema
- **Database Agnostic**: Support for multiple database systems via adapters
- **Production Ready**: Built-in error handling, monitoring, and performance optimization

## Core Components

### Query Processing (`src/query.ts`)
**Purpose**: High-level query interface with database integration
```typescript
interface QueryProcessor {
  executeQuery<T>(
    query: Query<T>,
    context: QueryContext
  ): Promise<QueryResult<T>>;
  
  subscribeToQuery<T>(
    query: Query<T>,
    callback: (result: QueryResult<T>) => void
  ): Subscription;
}
```

**Features:**
- **PostgreSQL Integration**: Direct integration with PostgreSQL databases
- **Query Optimization**: Automatic query optimization and caching
- **Result Streaming**: Memory-efficient handling of large result sets
- **Error Handling**: Comprehensive error handling with context information

### Schema Introspection (`src/schema.ts`)
**Purpose**: Runtime PostgreSQL schema discovery and validation
```typescript
interface SchemaIntrospector {
  introspectDatabase(connectionString: string): Promise<IntrospectedSchema>;
  validateSchema(clientSchema: Schema): ValidationResult;
  generateMigrations(from: Schema, to: Schema): Migration[];
}
```

**Introspection Capabilities:**
- **Automatic Discovery**: Extract schema from existing PostgreSQL databases
- **Type Mapping**: Convert PostgreSQL types to Zero schema types
- **Constraint Detection**: Identify primary keys, foreign keys, unique constraints
- **Index Analysis**: Discover existing indexes and optimization opportunities

### Push Processor (`src/push-processor.ts`)
**Purpose**: Handles client mutation processing with transaction support
```typescript
interface PushProcessor {
  processMutations(
    mutations: Mutation[],
    context: MutationContext
  ): Promise<MutationResult[]>;
  
  processCustomMutation(
    name: string,
    args: unknown[],
    context: MutationContext
  ): Promise<unknown>;
}
```

**Mutation Features:**
- **ACID Transactions**: All mutations processed within database transactions
- **Batch Processing**: Multiple mutations processed efficiently in batches
- **Custom Mutators**: Support for server-side custom business logic
- **Conflict Resolution**: Automatic handling of concurrent mutation conflicts

### Custom Mutations (`src/custom.ts`)
**Purpose**: Server-side custom mutator support with transaction handling
```typescript
type CustomMutator<Args = unknown, Return = unknown> = (
  tx: DatabaseTransaction,
  args: Args,
  context: MutationContext
) => Promise<Return>;

interface CustomMutatorRegistry {
  register<Args, Return>(
    name: string,
    mutator: CustomMutator<Args, Return>
  ): void;
  
  execute(
    name: string,
    args: unknown[],
    context: MutationContext
  ): Promise<unknown>;
}
```

## Database Integration

### ZQL Database Bridge (`src/zql-database.ts`)
**Purpose**: Bridges ZQL queries with SQL database execution
```typescript
class ZQLDatabase {
  constructor(
    private readonly adapter: DatabaseAdapter,
    private readonly schema: Schema
  );
  
  async executeQuery(query: QueryAST): Promise<QueryResult>;
  async executeMutation(mutation: MutationAST): Promise<MutationResult>;
  
  // Real-time subscriptions
  subscribe(query: QueryAST, callback: ChangeCallback): Subscription;
}
```

**Integration Features:**
- **Query Compilation**: Uses z2s to compile ZQL queries to optimized SQL
- **Type Safety**: Maintains type safety through schema validation
- **Performance Monitoring**: Built-in query timing and optimization analysis
- **Change Streaming**: Real-time change notifications for reactive updates

### Database Adapters (`src/adapters/`)
**Purpose**: Support for different database systems and ORMs

**Drizzle ORM Adapter** (`src/adapters/drizzle-pg.ts`):
```typescript
interface DrizzleAdapter extends DatabaseAdapter {
  execute(sql: string, params: unknown[]): Promise<unknown[]>;
  transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;
  introspectSchema(): Promise<DatabaseSchema>;
}
```

**PostgresJS Adapter** (`src/adapters/postgresjs.ts`):
```typescript
interface PostgresJSAdapter extends DatabaseAdapter {
  // Direct PostgreSQL integration
  query(sql: TemplateStringsArray, ...params: unknown[]): Promise<unknown[]>;
  begin(): Promise<PostgresJSTransaction>;
}
```

## High-Level API

### Server Setup
```typescript
import { createZeroServer } from '@rocicorp/zero-server';

const server = createZeroServer({
  // Database connection
  database: {
    connectionString: process.env.DATABASE_URL,
    adapter: 'postgresql', // or 'drizzle'
  },
  
  // Schema definition
  schema: myZeroSchema,
  
  // Custom mutations
  mutators: {
    promoteUser: async (tx, userId: string) => {
      await tx.update('users')
        .set({ role: 'admin' })
        .where({ id: userId });
    },
    
    createPost: async (tx, { title, content, authorId }) => {
      const post = await tx.insert('posts')
        .values({ title, content, authorId })
        .returning();
      return post[0];
    }
  },
  
  // Authentication
  auth: {
    secret: process.env.JWT_SECRET,
    permissions: myPermissions,
  }
});

// Start server
await server.listen(8080);
```

### Query Processing
```typescript
// Execute queries directly  
const users = await server.query(q => q.user.findMany({
  where: { active: true },
  include: { posts: true }
}));

// Subscribe to real-time updates
const subscription = server.subscribe(
  q => q.user.findMany(),
  (users) => {
    console.log('Users updated:', users);
  }
);
```

### Custom Mutation Integration
```typescript
// Register custom server-side logic
server.registerMutator('calculateUserStats', async (tx, userId: string) => {
  const posts = await tx.select()
    .from('posts')
    .where({ authorId: userId });
    
  const comments = await tx.select()
    .from('comments')
    .where({ authorId: userId });
    
  return {
    postCount: posts.length,
    commentCount: comments.length,
    totalEngagement: posts.length + comments.length
  };
});

// Client can call this mutation
await zero.mutate.calculateUserStats('user123');
```

## Testing Infrastructure

### Testing Patterns
```typescript
// Integration testing with real PostgreSQL
describe('Zero Server Integration', () => {
  let server: ZeroServer;
  let testDb: TestDatabase;
  
  beforeEach(async () => {
    testDb = await createTestDatabase();
    server = createZeroServer({
      database: { connectionString: testDb.url },
      schema: testSchema
    });
  });
  
  test('processes mutations correctly', async () => {
    const result = await server.processMutation({
      type: 'custom',
      name: 'createUser',
      args: [{ name: 'Alice', email: 'alice@example.com' }]
    });
    
    expect(result.success).toBe(true);
    expect(result.data.id).toBeDefined();
  });
});
```

### Multi-Database Testing
- **PostgreSQL 15, 16, 17**: Tests against multiple PostgreSQL versions
- **Custom Database**: Support for custom PostgreSQL instances via environment variables
- **Isolated Tests**: Each test uses isolated database instances
- **Performance Benchmarks**: Query performance regression testing

### Key Test Files
- `src/query.pg-test.ts` - Query processing with PostgreSQL
- `src/push-processor.pg-test.ts` - Mutation processing tests
- `src/custom.pg-test.ts` - Custom mutator functionality
- `src/schema.test.ts` - Schema introspection validation

## Development Workflows

### Build Commands
```bash
npm run build          # TypeScript compilation
npm run test           # Run all tests
npm run test:pg-15     # Test against PostgreSQL 15
npm run test:pg-16     # Test against PostgreSQL 16
npm run test:pg-17     # Test against PostgreSQL 17
npm run test:no-pg     # Tests without PostgreSQL dependency
npm run check-types    # TypeScript validation
```

### Development Setup
```typescript
// Development server with hot reload
const devServer = createZeroServer({
  database: { connectionString: 'postgresql://localhost/myapp_dev' },
  schema: mySchema,
  development: {
    logQueries: true,
    enableIntrospection: true,
    hotReload: true
  }
});
```

### Production Deployment
```typescript
// Production-optimized configuration
const prodServer = createZeroServer({
  database: {
    connectionString: process.env.DATABASE_URL,
    pool: {
      min: 2,
      max: 10,
      idleTimeoutMillis: 30000
    }
  },
  schema: mySchema,
  production: {
    logLevel: 'warn',
    enableMetrics: true,
    healthChecks: true
  }
});
```

## Key Files Reference

### Core Implementation
- `src/mod.ts` - Main exports and public API
- `src/query.ts` - High-level query processing
- `src/push-processor.ts` - Mutation handling and processing
- `src/schema.ts` - Database schema introspection

### Database Integration
- `src/zql-database.ts` - ZQL-to-database bridge
- `src/db.ts` - Database abstraction layer
- `src/adapters/` - Database adapter implementations

### Query Processing
- `src/queries/process-queries.ts` - Query compilation and execution

### Custom Logic
- `src/custom.ts` - Custom mutator system

## Integration Points

### Package Dependencies
- **`z2s`**: ZQL to SQL compilation for query execution
- **`zql`**: Query processing and incremental view maintenance
- **`zero-schema`**: Type-safe schema definitions and validation
- **`zero-protocol`**: Client-server communication protocol

### Database Dependencies
- **PostgreSQL**: Primary database support with multiple version compatibility
- **Drizzle ORM**: Optional ORM integration for type-safe database operations
- **PostgresJS**: Direct PostgreSQL driver integration

### Framework Integration
- **Express.js**: HTTP server integration for REST endpoints
- **Fastify**: High-performance HTTP server integration
- **Next.js**: Server-side rendering and API route integration
- **Node.js**: Native Node.js server applications

## Performance Considerations

### Query Optimization
- **Query Compilation**: Z2S optimizes ZQL queries into efficient SQL
- **Connection Pooling**: Configurable connection pool for database efficiency
- **Query Caching**: Prepared statement caching for repeated queries
- **Index Analysis**: Automatic index usage optimization

### Memory Management
- **Streaming Results**: Large result sets streamed without loading into memory
- **Connection Reuse**: Efficient database connection management
- **Transaction Scoping**: Minimize transaction duration for better concurrency

### Monitoring & Observability
- **Query Timing**: Built-in query performance monitoring
- **Error Tracking**: Comprehensive error logging and context
- **Health Checks**: Built-in health monitoring endpoints
- **Metrics Integration**: OpenTelemetry support for production monitoring

## Common Use Cases

### REST API Server
```typescript
// Express.js integration
app.get('/api/users', async (req, res) => {
  const users = await zeroServer.query(q => q.user.findMany({
    where: req.query,
    take: parseInt(req.query.limit) || 20
  }));
  res.json(users);
});
```

### GraphQL Integration
```typescript
// GraphQL resolver integration
const resolvers = {
  Query: {
    users: () => zeroServer.query(q => q.user.findMany()),
    user: (_, { id }) => zeroServer.query(q => q.user.findUnique({ where: { id } }))
  },
  Mutation: {
    createUser: (_, args) => zeroServer.mutate.user.insert([args])
  }
};
```

### Microservice Architecture
```typescript
// Service-to-service communication
class UserService {
  async getUser(id: string) {
    return await zeroServer.query(q => q.user.findUnique({ where: { id } }));
  }
  
  async updateUserPreferences(userId: string, preferences: UserPreferences) {
    return await zeroServer.mutate.updateUserPreferences(userId, preferences);
  }
}
```

Zero-server represents a production-ready, developer-friendly interface to Zero's distributed database capabilities, providing the simplicity needed for rapid application development while maintaining the performance and reliability required for production deployments.