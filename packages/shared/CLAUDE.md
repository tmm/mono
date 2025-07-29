# Shared Package - Foundation Utilities

## Overview

The shared package provides **foundational utilities and data structures** used across all other packages in the Rocicorp monorepo. It implements performance-critical algorithms, cross-platform compatibility layers, and common development tools that enable Zero and Replicache to operate efficiently.

## Architecture

### Functional Categories
```
Data Structures & Algorithms
├── btree-set.ts          # B+ Tree sorted collections
├── binary-search.ts      # Generic binary search
├── queue.ts             # Async producer-consumer queue
└── cache.ts             # Time-based caching

Type System & Validation  
├── valita.ts            # Enhanced validation wrapper
├── json-schema.ts       # JSON schema validation
├── types.ts             # Common type definitions
└── immutable.ts         # Deep immutable types

Environment & Compatibility
├── browser-env.ts       # Browser global access
├── navigator.ts         # Safe navigator access
└── broadcast-channel.ts # Cross-tab communication

Performance & Encoding
├── hash.ts              # xxHash utilities
├── base62.ts            # Base62 encoding
└── float-to-ordered-string.ts # Sortable float encoding
```

## Core Data Structures

### BTreeSet (`src/btree-set.ts`)
**Purpose**: High-performance sorted set implementation
```typescript
class BTreeSet<K> {
  readonly comparator: Comparator<K>;
  
  // O(log n) operations
  add(key: K): boolean;
  delete(key: K): boolean; 
  has(key: K): boolean;
  
  // Copy-on-write semantics
  clone(): BTreeSet<K>;
  
  // Range queries
  iterateFrom(start?: K, reverse?: boolean): IterableIterator<K>;
}
```

**Key Features:**
- **O(log n)** insertions, deletions, and lookups
- **Copy-on-write** semantics with shared nodes
- **Maximum node size** of 32 for optimal cache performance
- **Range queries** with forward/reverse iteration
- **Binary search** within nodes for efficiency

### Binary Search (`src/binary-search.ts`)
```typescript
export function binarySearch<T>(
  array: ArrayLike<T>,
  target: T,
  compareFn: (a: T, b: T) => number,
  start = 0,
  end = array.length,
): number
```

**Implementation Details:**
- Uses **bit shifting** for mid calculation to avoid overflow
- Returns **insertion point** for missing elements (negative value)
- **Generic implementation** works with any comparable type
- Used extensively throughout ZQL and Replicache

### Async Queue (`src/queue.ts`)
**Purpose**: Producer-consumer coordination with timeout support
```typescript
class Queue<T> implements AsyncIterable<T> {
  // Producer methods
  enqueue(value: T): void;
  close(): void;
  
  // Consumer methods  
  dequeue(timeoutMs?: number, signal?: AbortSignal): Promise<T>;
  delete(value: T): boolean; // Delete by identity
  
  // AsyncIterable support
  [Symbol.asyncIterator](): AsyncIterator<T>;
}
```

**Features:**
- **Immediate resolution** when values available
- **Async waiting** when queue empty
- **Timeout support** with AbortSignal integration
- **Value deletion** by identity for cancellation
- **AsyncIterable** interface for `for-await` loops

## Performance-Critical Utilities

### Hash Functions (`src/hash.ts`)
```typescript
export const h32 = (s: string) => xxHash32(s, 0);
export const h64 = (s: string) => hash(s, 2);  // 2x xxHash32
export const h128 = (s: string) => hash(s, 4); // 4x xxHash32
```

**Features:**
- **xxHash32** foundation for speed and quality
- **Multiple hash sizes** by combining multiple rounds
- **Good collision resistance** for distributed systems
- **Consistent across platforms** for protocol compatibility

### Reference Counting (`src/ref-count.ts`)
```typescript
export class RefCount<T extends WeakKey = WeakKey> {
  inc(value: T): boolean;  // Returns true if newly added
  dec(value: T): boolean;  // Returns true if now zero
  get(value: T): number;   // Current reference count
}
```

**Implementation:**
- **WeakMap-based** to avoid memory leaks
- **Automatic cleanup** when objects become unreachable
- **Thread-safe** operations (within JavaScript event loop)
- Critical for **ZQL view lifecycle** management

### Float Encoding (`src/float-to-ordered-string.ts`)
**Purpose**: Convert floats to lexicographically sortable strings
```typescript
export function encodeFloat64AsString(n: number): string {
  // IEEE 754 bit manipulation for sort order preservation
  // Handles negative numbers, zero, infinity, NaN
  // Returns 13-character base36 string
}
```

**Use Cases:**
- **Database indexing** where string sorting needed
- **Network protocols** requiring sortable representations
- **B-tree keys** when float ordering required

## Cross-Platform Compatibility

### Environment Abstraction (`src/browser-env.ts`)
```typescript
// Global access with override support
export function getBrowserGlobal<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] | undefined;

// Method binding support  
export function getBrowserGlobalMethod<T extends keyof GlobalThis>(
  name: T,
): GlobalThis[T] | undefined;
```

**Features:**
- **Override system** for testing environments
- **Safe access** to browser APIs that may not exist
- **Type safety** with GlobalThis key constraints
- **Method binding** to ensure correct `this` context

### Navigator Abstraction (`src/navigator.ts`)
```typescript
export const navigator: Navigator | undefined = 
  typeof navigator !== 'undefined' ? navigator : undefined;
```

**Purpose:**
- **Safe access** to navigator object in Node.js environments
- **Prevents errors** when navigator undefined
- **Type safety** maintains Navigator interface

### BroadcastChannel Fallback (`src/broadcast-channel.ts`)
```typescript
const bc: typeof BroadcastChannel =
  typeof BroadcastChannel === 'undefined'
    ? NoopBroadcastChannel
    : BroadcastChannel;
```

**Features:**
- **No-op fallback** when BroadcastChannel unavailable
- **Cross-tab communication** in supporting environments
- **Graceful degradation** in limited environments

## Type System & Validation

### Enhanced Valita (`src/valita.ts`)
**Purpose**: Improved error messages and additional utilities
```typescript
// Enhanced union error messages
export function parseUnion<T>(input: unknown, union: UnionType<T>): T;

// Deep partial type transformation
export type DeepPartial<T> = T extends ReadonlyArray<infer U> 
  ? ReadonlyArray<DeepPartial<U>>
  : T extends object 
  ? { readonly [K in keyof T]?: DeepPartial<T[K]> }
  : T;
```

**Improvements over base Valita:**
- **Better error paths** for nested union types
- **Custom error formatting** for developer experience
- **Additional type utilities** for complex schemas
- **Performance optimizations** for hot paths

### JSON Type System (`src/json.ts`)
```typescript
export type ReadonlyJSONValue =
  | null | string | boolean | number
  | ReadonlyArray<ReadonlyJSONValue>
  | ReadonlyJSONObject;

export function deepEqual(
  a: ReadonlyJSONValue | undefined,
  b: ReadonlyJSONValue | undefined,
): boolean;
```

**Features:**
- **Immutable JSON types** for safety
- **Optimized deep comparison** without allocation
- **Type guards** for runtime JSON validation
- **Serialization utilities** for network protocols

## Testing Infrastructure

### Multi-Environment Testing (`src/tool/vitest-config.ts`)
```typescript
export function createVitestConfig(options: VitestOptions): UserConfig {
  return {
    test: {
      environment: options.environment, // 'node' | 'happy-dom'
      timeout: 20000,
      // ... browser configuration for Playwright
    }
  };
}
```

**Features:**
- **Browser testing** via Playwright (Chromium, Firefox, WebKit)
- **Node.js testing** for server-side code
- **Shared configuration** across all packages
- **Timeout management** for async operations

### Test Utilities (`src/logging-test-utils.ts`)
```typescript
export class TestLogSink implements LogSink {
  messages: [LogLevel, Context | undefined, unknown[]][] = [];
  
  expectLogEntry(level: LogLevel, ...expectedArgs: unknown[]): void;
  expectNoLogs(): void;
}
```

**Testing Support:**
- **Log capture** for testing logging behavior
- **Expectation helpers** for common test patterns
- **Mock implementations** for external dependencies

## Algorithms & Data Structures

### Time-Based Cache (`src/cache.ts`)
```typescript
export class TimedCache<T> {
  constructor(private readonly ttlMs: number);
  
  set(key: string, value: T): void;
  get(key: string): T | undefined;
  
  // Automatic cleanup every 2x TTL
  readonly #intervalHandle: NodeJS.Timeout;
}
```

**Features:**
- **Automatic expiration** based on TTL
- **Background cleanup** to prevent memory leaks
- **Generic type support** for any cached value
- **High performance** with Map-based storage

### Iterators & Streams (`src/iterables.ts`)
```typescript
// Async iterator utilities
export function map<T, U>(
  iterable: AsyncIterable<T>,
  fn: (value: T) => U | Promise<U>,
): AsyncIterable<U>;

export function filter<T>(
  iterable: AsyncIterable<T>,
  predicate: (value: T) => boolean | Promise<boolean>,
): AsyncIterable<T>;
```

**Utilities:**
- **Lazy evaluation** for memory efficiency
- **Async support** for I/O bound operations
- **Composable operations** for data processing pipelines
- **Memory efficient** streaming operations

## Key Files Reference

### Core Utilities
- `src/asserts.ts` - Assertion functions and type guards
- `src/must.ts` - Non-null assertion with error messages
- `src/arrays.ts` - Array manipulation utilities
- `src/objects.ts` - Object utilities and transformations

### Data Structures
- `src/btree-set.ts` - B+ tree sorted set (651 lines)
- `src/binary-search.ts` - Generic binary search algorithm
- `src/queue.ts` - Async producer-consumer queue
- `src/custom-key-map.ts` - Map with custom key transformation

### Performance Tools
- `src/hash.ts` - Hash functions and utilities
- `src/tdigest.ts` - T-Digest for quantile estimation
- `src/logarithmic-histogram.ts` - Performance histograms
- `src/timed.ts` - Execution timing utilities

### Environment Support
- `src/browser-env.ts` - Browser API abstraction
- `src/document-visible.ts` - Document visibility detection
- `src/sleep.ts` - Cross-platform sleep implementation

## Development Workflows

### Build System Integration
```typescript
// Build-time constants
const define = {
  ...makeDefine(),
  ['TESTING']: 'true',
};

// Package detection
export function getExternalFromPackageJson(packageJsonPath: string): string[];
export function getInternalPackages(): string[];
```

### Development Commands
```bash
npm run build          # TypeScript compilation
npm run test           # Run test suite (Node.js + browser)
npm run test:node     # Node.js only tests
npm run test:web      # Browser only tests  
npm run check-types   # TypeScript validation
```

### Performance Monitoring
```typescript
// Timing utilities
export const timed = <T>(fn: () => T): [T, number] => {
  const start = performance.now();
  const result = fn();
  return [result, performance.now() - start];
};

// Memory monitoring  
export function createMemoryMonitor(): MemoryUsage;
```

## Integration Patterns

### Package Dependencies
- **Minimal external dependencies** for stability
- **Cross-platform libraries** for compatibility
- **Performance-focused choices** (xxhash, etc.)

### Internal Package Integration
- **Universal utilities** used by all other packages
- **Standardized patterns** for common operations
- **Type safety** enforced through TypeScript

### Build Tool Integration
- **Vitest configuration** shared across packages
- **External package detection** for bundling
- **Environment variable** management

## Common Utilities Reference

### String & Encoding
- `src/string-compare.ts` - String comparison utilities
- `src/base62.ts` - Base62 encoding/decoding
- `src/bigint-json.ts` - BigInt JSON serialization
- `src/parse-big-int.ts` - Safe BigInt parsing

### Math & Statistics
- `src/centroid.ts` - Centroid calculations
- `src/tdigest.ts` - T-Digest quantile estimation
- `src/random-values.ts` - Secure random number generation
- `src/float-to-ordered-string.ts` - Sortable float encoding

### Control Flow
- `src/resolved-promises.ts` - Promise utilities
- `src/lazy.ts` - Lazy evaluation wrapper
- `src/writable.ts` - Writable type utilities

The shared package represents the foundation that enables the entire Zero/Replicache ecosystem to operate efficiently across different environments while maintaining high performance and reliability standards. Its careful attention to cross-platform compatibility, performance optimization, and type safety makes it an exemplary foundational library.