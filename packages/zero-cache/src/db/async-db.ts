import {resolver} from '@rocicorp/resolver';
import async from 'sqlite3';
import {must} from '../../../shared/src/must.ts';
import {mapEntries} from '../../../shared/src/objects.ts';

/**
 * Types (fully) supported by
 * https://github.com/TryGhost/node-sqlite3/blob/528e15ae605bac7aab8de60dd7c46e9fdc1fffd0/src/statement.cc#L178
 *
 * Note that `bigint` is *NOT* supported
 * (https://github.com/TryGhost/node-sqlite3/issues/1058),
 * so numeric values should be passed in as strings, leaving
 * the conversion to SQLite's dynamic type system.
 */
export type SQLitePrimitive = null | string | number | boolean | Uint8Array;

/**
 * Wraps the callback-based {@link async.Database Database} object in
 * a Promise-based API. Methods accept parameters as lists of primitives
 * corresponding to `?`'s in the statement (or numbered versions, e.g. `?23`),
 * or an object whose fields correspond to named args with the `@` prefix.
 *
 * Note that named arg method is slightly less efficient.
 */
export class AsyncDatabase {
  static async connect(filename: string): Promise<AsyncDatabase> {
    const [done, cb] = voidCallback();
    const db = new async.Database(filename, cb);
    await done;
    return new AsyncDatabase(db);
  }

  readonly #db: async.Database;

  private constructor(db: async.Database) {
    this.#db = db;
  }

  run(stmt: string, ...args: SQLitePrimitive[]): Promise<void>;
  run(stmt: string, namedArgs: Record<string, SQLitePrimitive>): Promise<void>;
  run(
    stmt: string,
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<void> {
    const [done, cb] = voidCallback();
    this.#db.run(stmt, mapNamed(args), cb);
    return done;
  }

  exec(stmt: string): Promise<void> {
    const [done, cb] = voidCallback();
    this.#db.exec(stmt, cb);
    return done;
  }

  get<T>(stmt: string, ...args: SQLitePrimitive[]): Promise<T | undefined>;
  get<T>(
    stmt: string,
    namedArgs: Record<string, SQLitePrimitive>,
  ): Promise<T | undefined>;
  get<T>(
    stmt: string,
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<T | undefined> {
    const [result, cb] = getCallback<T>();
    this.#db.get(stmt, mapNamed(args), cb);
    return result;
  }

  mustGet<T>(stmt: string, ...args: SQLitePrimitive[]): Promise<T>;
  mustGet<T>(
    stmt: string,
    namedArgs: Record<string, SQLitePrimitive>,
  ): Promise<T>;
  async mustGet<T>(
    stmt: string,
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<T> {
    const [result, cb] = getCallback<T>();
    this.#db.get(stmt, mapNamed(args), cb);
    return must(await result);
  }

  all<T>(stmt: string, ...args: SQLitePrimitive[]): Promise<T[]>;
  all<T>(
    stmt: string,
    namedArgs: Record<string, SQLitePrimitive>,
  ): Promise<T[]>;
  all<T>(
    stmt: string,
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<T[]> {
    const [result, cb] = allCallback<T>();
    this.#db.all(stmt, mapNamed(args), cb);
    return result;
  }

  async prepare(sql: string): Promise<AsyncStatement> {
    const [done, cb] = voidCallback();
    const stmt = this.#db.prepare(sql, cb);
    await done;
    return new AsyncStatement(stmt);
  }

  /**
   * Runs all statements in the (synchronous) `fn` serially. `fn` is a
   * synchronous function to guarantee that all statements are executed (and
   * pipelined) within the function block. The last resulting Promise can be
   * stored and awaited after `fn` has finished executing.
   */
  // TODO: Consider make a better API for this.
  pipeline(fn: () => void) {
    this.#db.serialize(fn);
  }

  close(): Promise<void> {
    const [done, cb] = voidCallback();
    this.#db.close(cb);
    return done;
  }
}

export class AsyncStatement {
  readonly #stmt: async.Statement;

  constructor(stmt: async.Statement) {
    this.#stmt = stmt;
  }

  run(...args: SQLitePrimitive[]): Promise<void>;
  run(namedArgs: Record<string, SQLitePrimitive>): Promise<void>;
  run(
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<void> {
    const [done, cb] = voidCallback();
    this.#stmt.run(mapNamed(args), cb);
    return done;
  }

  get<T>(...args: SQLitePrimitive[]): Promise<T | undefined>;
  get<T>(namedArgs: Record<string, SQLitePrimitive>): Promise<T | undefined>;
  get<T>(
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<T | undefined> {
    const [result, cb] = getCallback<T>();
    this.#stmt.get(mapNamed(args), cb);
    return result;
  }

  all<T>(...args: SQLitePrimitive[]): Promise<T[]>;
  all<T>(namedArgs: Record<string, SQLitePrimitive>): Promise<T[]>;
  all<T>(
    ...args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]
  ): Promise<T[]> {
    const [result, cb] = allCallback<T>();
    this.#stmt.all(mapNamed(args), cb);
    return result;
  }

  finalize(): Promise<void> {
    const [done, cb] = voidCallback();
    this.#stmt.finalize(cb);
    return done;
  }
}

function mapNamed(args: SQLitePrimitive[] | [Record<string, SQLitePrimitive>]) {
  return args.length === 1 && typeof args[0] === 'object' && args[0] !== null
    ? // Named args
      mapEntries(args[0] as Record<string, unknown>, (k, v) => ['@' + k, v])
    : args;
}

function voidCallback(): [Promise<void>, (err: Error | null) => void] {
  const {promise, resolve, reject} = resolver();
  return [promise, err => (err ? reject(err) : resolve())];
}

function getCallback<T = unknown>(): [
  Promise<T | undefined>,
  (err: Error | null, row?: T) => void,
] {
  const {promise, resolve, reject} = resolver<T | undefined>();
  return [promise, (err, row) => (err === null ? resolve(row) : reject(err))];
}

function allCallback<T = unknown>(): [
  Promise<T[]>,
  (err: Error | null, rows?: T[]) => void,
] {
  const {promise, resolve, reject} = resolver<T[]>();
  return [
    promise,
    (err, rows) => (err === null && rows ? resolve(rows) : reject(err)),
  ];
}
