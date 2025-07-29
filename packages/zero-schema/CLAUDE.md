# Zero-Schema Package - Type-Safe Schema Definition

## Overview

Zero-schema provides a **TypeScript-first schema definition system** for Zero databases with full type safety, validation, and transformation capabilities. It enables developers to define database schemas using a fluent builder API with automatic TypeScript type inference.

## Architecture

### Builder Pattern Design

```
SchemaBuilder (entry point)
    ↓
TableBuilder (table definition)
    ↓
ColumnBuilder (column definition)
    ↓
RelationshipBuilder (foreign keys)
```

### Core Philosophy

- **TypeScript-First**: Schema definitions drive TypeScript types
- **Compile-Time Safety**: Catch schema errors during development
- **Runtime Validation**: Ensure data integrity at runtime
- **Name Mapping**: Separate client/server column naming conventions

## Core Components

### Schema Builder (`src/builder/schema-builder.ts`)

**Purpose**: Main entry point for schema definition

```typescript
export class SchemaBuilder {
  table<TName extends string>(
    name: TName,
    tableFn: (t: TableBuilder) => TableSchema,
  ): SchemaBuilder;

  build(): Schema;
}

// Usage example
const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
        email: string(),
      })
      .primaryKey('id'),
  ],
  relationships: [],
});
```

### Table Builder (`src/builder/table-builder.ts`)

**Purpose**: Type-safe column definitions with validation

```typescript
export class TableBuilder {
  // Column types
  columns(c: {[columnName: string]: ColumnBuilder});
}
```

**Column Features:**

- **Type Inference**: Automatic TypeScript type derivation
- **Validation Rules**: Runtime validation for data integrity
- **Constraints**: Primary keys, unique constraints, nullability
- **Default Values**: Support for column defaults

### Relationship Builder (`src/builder/relationship-builder.ts`)

**Purpose**: Define foreign key relationships between tables

```typescript
const userRelationships = relationships(user, ({many}) => ({
  createdIssues: many({
    sourceField: ['id'],
    destField: ['creatorID'],
    destSchema: issue,
  }),
}));
```

**Relationship Types:**

- **One**: Single record relationships
- **Many**: Parent-child relationships or junction table relationships

## Name Mapping System

### Client-Server Translation (`src/name-mapper.ts`)

**Purpose**: Map between client-friendly and database column names

```typescript
interface NameMapper {
  clientToServer(tableName: string, columnName: string): string;
  serverToClient(tableName: string, columnName: string): string;
}

// Example: client 'firstName' <-> server 'first_name'
const mapper = new NameMapper({
  user: {
    firstName: 'first_name',
    lastName: 'last_name',
    createdAt: 'created_at',
  },
});
```

**Mapping Features:**

- **Bidirectional**: Client ↔ Server name translation
- **Convention Support**: camelCase ↔ snake_case conversion
- **Custom Mappings**: Override default conventions
- **Type Safety**: Compile-time validation of mapped names

## Schema Configuration

### Runtime Schema (`src/schema-config.ts`)

```typescript
interface SchemaConfig {
  version: number;
  tables: Record<string, TableConfig>;
  relationships: RelationshipConfig[];
  permissions: PermissionConfig[];
}

interface TableConfig {
  columns: Record<string, ColumnConfig>;
  primaryKey: string[];
  indexes: IndexConfig[];
}
```

**Configuration Features:**

- **Version Management**: Schema evolution support
- **Index Definitions**: Database performance optimization
- **Validation Rules**: Runtime data validation
- **Metadata Storage**: Schema introspection capabilities

## Type System Integration

### Type Inference (`src/table-schema.ts`)

```typescript
// Automatic type derivation from schema
type UserSchema = Row<typeof schema.tables.user>;

// Inferred from schema definition
const user: UserSchema = await zero.query.user.where('id', '123').one();
```

**Type Safety Features:**

- **Compile-Time Validation**: Catch type errors during development
- **IntelliSense Support**: Full autocompletion in IDEs
- **Query Type Checking**: Validate queries against schema
- **Mutation Safety**: Type-safe create/update operations

### Server Schema Integration (`src/server-schema.ts`)

**Purpose**: Bridge between client schema and server database

```typescript
interface ServerSchema {
  introspectPostgreSQL(connectionString: string): Promise<Schema>;
  validateCompatibility(clientSchema: Schema): ValidationResult;
  generateMigrations(from: Schema, to: Schema): Migration[];
}
```

## Key Files Reference

### Core Implementation

- `src/builder/schema-builder.ts` - Main schema definition API
- `src/builder/table-builder.ts` - Column definition and constraints
- `src/builder/relationship-builder.ts` - Foreign key relationships
- `src/mod.ts` - Public API exports

### Type System

- `src/table-schema.ts` - TypeScript type definitions
- `src/schema-config.ts` - Runtime schema configuration
- `src/server-schema.ts` - Server-side schema integration

### Permission System

- `src/permissions.ts` - Permission rule definitions
- `src/compiled-permissions.ts` - Optimized permission evaluation

### Utilities

- `src/name-mapper.ts` - Client/server name mapping
- `src/schema-type-test.ts` - Type-level testing

## Testing

### Type-Level Testing

```typescript
import {expectTypeOf} from 'vitest';

// Validate inferred types match expectations
expectTypeOf<UserFromSchema<typeof userSchema>>().toEqualTypeOf<{
  id: string;
  name: string;
  email: string;
}>();
```

### Runtime Testing

- **Schema Validation**: Ensure schema definitions are valid
- **Permission Testing**: Verify row-level security rules
- **Name Mapping**: Test client/server name translation
- **Type Inference**: Validate TypeScript type derivation

### Key Test Files

- `src/schema.test.ts` - Core schema functionality
- `src/permissions.test.ts` - Permission system validation
- `src/name-mapper.test.ts` - Name mapping behavior
- `src/table-schema.test.ts` - Type system testing

## Development Workflows

### Schema Definition

```typescript
// Define schema with full type safety
const user = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string().optional(),
    avatar: string(),
    role: enumeration<Role>(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    shortID: number().optional(),
    title: string(),
    open: boolean(),
    modified: number(),
    created: number(),
    creatorID: string(),
    assigneeID: string().optional(),
    description: string(),
    visibility: enumeration<'internal' | 'public'>(),
  })
  .primaryKey('id');

const userRelationships = relationships(user, ({many}) => ({
  createdIssues: many({
    sourceField: ['id'],
    destField: ['creatorID'],
    destSchema: issue,
  }),
}));

export const schema = createSchema({
  tables: [user, issue],
  relationships: [userRelationships],
});
```

### Build Commands

```bash
npm run build          # TypeScript compilation
npm run test           # Run test suite
npm run check-types    # Type validation
npm run lint           # Code linting
```

## Integration Points

### Package Dependencies

- **`@badrap/valita`**: Runtime validation and schema definitions
- **`shared`**: Common utilities and type definitions

### Used By

- **`zero-client`**: Type-safe query generation and validation
- **`zero-server`**: Server-side schema validation and introspection
- **`zero-protocol`**: AST generation with schema metadata
- **`zero-cache`**: Permission evaluation and data validation

### Framework Integration

- **React/Solid**: Type-safe component props and state
- **Node.js**: Server-side schema validation and migration
- **Database Systems**: PostgreSQL schema introspection

## Common Patterns

### Schema Definition Best Practices

1. **Start Simple**: Begin with basic tables and add complexity gradually
2. **Type Safety**: Leverage TypeScript inference for compile-time validation
3. **Naming Consistency**: Use consistent naming conventions across tables

### Performance Considerations

- **Schema Caching**: Compiled schemas are cached for performance
- **Permission Optimization**: Permissions compiled for fast evaluation
- **Name Mapping**: Mapping tables cached to avoid repeated lookups
- **Type Checking**: Compile-time validation prevents runtime overhead
