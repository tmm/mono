# Zero-React Package - React Integration

## Overview

Zero-react provides **React hooks and components** for seamless Zero integration in React applications. It offers reactive query hooks with automatic subscriptions, optimistic updates, and full TypeScript integration for building real-time applications.

## Architecture

### React Integration Pattern

```
React Components
    ↓
useQuery Hook (React-specific)
    ↓
Zero Client API (core functionality)
    ↓
Replicache Sync Engine
```

### Core Design Principles

- **React-First**: Designed specifically for React's rendering model
- **Automatic Subscriptions**: Hooks manage subscription lifecycle automatically
- **Performance Optimized**: Uses React's `useSyncExternalStore` for efficient updates
- **Developer Experience**: Full TypeScript integration with IntelliSense support

## Core Components

### useQuery Hook (`src/use-query.tsx`)

**Purpose**: Primary hook for reactive database queries

```typescript
function useQuery<T>(
  query: Query<T>,
  options?: UseQueryOptions,
): UseQueryResult<T> {
  // Uses React's useSyncExternalStore for optimal performance
  // Manages subscription lifecycle automatically
  // Provides loading states and error handling
}

interface UseQueryResult<T> {
  data: T | undefined;
  loading: boolean;
  error: Error | undefined;
}
```

**Features:**

- **Automatic Subscriptions**: Subscribe/unsubscribe based on component lifecycle
- **Loading States**: Built-in loading and error state management
- **Dependency Tracking**: Only re-render when accessed data changes
- **Memory Efficient**: Shared subscriptions across components using same query

### ZeroProvider (`src/zero-provider.tsx`)

**Purpose**: React context provider for Zero client instances

```typescript
interface ZeroProviderProps {
  zero: Zero<any>;
  children: React.ReactNode;
}

function ZeroProvider({ zero, children }: ZeroProviderProps): JSX.Element {
  // Provides Zero client instance to all child components
  // Manages client lifecycle and cleanup
  // Enables useQuery and other hooks throughout component tree
}

// Usage
function App() {
  const zero = useZero({
    server: 'ws://localhost:4848',
    schema: mySchema,
  });

  return (
    <ZeroProvider zero={zero}>
      <UserList />
    </ZeroProvider>
  );
}
```

### Zero Inspector (`src/components/zero-inspector.tsx`)

**Purpose**: Development tool for debugging queries and connection state

```typescript
interface ZeroInspectorProps {
  zero: Zero<any>;
  position?: 'top-left' | 'top-right' | 'bottom-left' | 'bottom-right';
}

function ZeroInspector({zero, position}: ZeroInspectorProps): JSX.Element {
  // Shows active queries, connection state, and performance metrics
  // Toggle visibility in development mode
  // Provides query introspection and debugging information
}
```

**Inspector Features:**

- **Query Monitoring**: View all active queries and their results
- **Connection Status**: Real-time connection state and health
- **Performance Metrics**: Query timing and subscription statistics
- **Development Only**: Automatically disabled in production builds

## React Integration Patterns

### Basic Query Usage

```typescript
import { useQuery } from '@rocicorp/zero/react';

function UserList() {
  const { data: users, loading, error } = useQuery(
    zero.query.user.where('active', true).orderBy('createdAt', 'desc')
  );

  if (loading) return <div>Loading users...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <ul>
      {users?.map(user => (
        <li key={user.id}>{user.name}</li>
      ))}
    </ul>
  );
}
```

### Real-time Updates

```typescript
function ChatMessages({ channelId }: { channelId: string }) {
  // Automatically updates when new messages arrive
  const { data: messages } = useQuery(
    zero.query.message.where('channelId', channelId).related('author').orderBy('createdAt', 'asc')
  );

  return (
    <div className="messages">
      {messages?.map(message => (
        <Message
          key={message.id}
          content={message.content}
          author={message.author}
          timestamp={message.createdAt}
        />
      ))}
    </div>
  );
}
```

### Optimistic Updates

```typescript
function CreateUserForm() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // Optimistic update - UI updates immediately
    await zero.mutate.user.insert([{
      name,
      email,
      createdAt: new Date().toISOString()
    }]);

    // Form reset after successful mutation
    setName('');
    setEmail('');
  };

  return (
    <form onSubmit={handleSubmit}>
      <input
        value={name}
        onChange={e => setName(e.target.value)}
        placeholder="Name"
      />
      <input
        value={email}
        onChange={e => setEmail(e.target.value)}
        placeholder="Email"
      />
      <button type="submit">Create User</button>
    </form>
  );
}
```

## Performance Optimizations

### React-Specific Optimizations

```typescript
interface PerformanceFeatures {
  // React 18+ concurrent features
  useSyncExternalStore: boolean; // Optimal subscription management

  // Subscription sharing
  queryDeduplication: boolean; // Share subscriptions across components

  // Update batching
  batchUpdates: boolean; // Batch multiple updates together

  // Memory management
  automaticCleanup: boolean; // Cleanup subscriptions on unmount
}
```

### Subscription Management

- **Shared Subscriptions**: Multiple components using same query share subscription
- **Automatic Cleanup**: Subscriptions cleaned up when last component unmounts
- **Lazy Subscription**: Subscriptions created only when components mount
- **TTL Management**: Cached queries expire based on usage patterns

### Rendering Optimization

```typescript
// Only re-render when actual data changes
const { data: user } = useQuery(zero.query.user.where('id', userId).one());

// React.memo optimization for child components
const UserCard = React.memo(({ user }: { user: User }) => {
  return (
    <div>
      <h3>{user.name}</h3>
      <p>{user.email}</p>
    </div>
  );
});
```

## Key Files Reference

### Core Hooks

- `src/use-query.tsx` - Primary reactive query hook
- `src/zero-provider.tsx` - React context provider for Zero client

### Components

- `src/components/zero-inspector.tsx` - Development debugging component
- `src/components/mark-icon.tsx` - UI icons for inspector
- `src/components/inspector.tsx` - Core inspector implementation

### Module Exports

- `src/mod.ts` - Main package exports and public API

## Integration Examples

### Vite + React SPA

```typescript
// src/main.tsx
import {ZeroProvider} from '@rocicorp/zero/react';
import {useLogin} from './hooks/use-login.tsx';
import {createMutators} from '../shared/mutators.ts';
import {useMemo, type ReactNode} from 'react';
import {schema} from '../shared/schema.ts';

export function ZeroInit({children}: {children: ReactNode}) {
  const login = useLogin();

  const props = useMemo(() => {
    return {
      schema,
      server: import.meta.env.VITE_PUBLIC_SERVER,
      userID: login.loginState?.decoded?.sub ?? 'anon',
      mutators: createMutators(login.loginState?.decoded),
      logLevel: 'info' as const,
      auth: (error?: 'invalid-token') => {
        if (error === 'invalid-token') {
          login.logout();
          return undefined;
        }
        return login.loginState?.encoded;
      },
    };
  }, [login]);

  return <ZeroProvider {...props}>{children}</ZeroProvider>;
}
```

## Build Commands

```bash
npm run build          # TypeScript compilation
npm run test           # Run test suite
npm run check-types    # TypeScript validation
npm run lint           # Code linting
```
