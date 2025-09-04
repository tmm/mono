import {Lock, RWLock} from '@rocicorp/lock';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Read, Store, Write} from './store.ts';

/**
 * A SQLite prepared statement.
 *
 * `run` executes the statement with optional parameters.
 * `all` executes the statement and returns the result rows.
 * `finalize` releases the statement.
 */
export interface PreparedStatement {
  run(...params: unknown[]): Promise<void>;
  all<T>(...params: unknown[]): Promise<T[]>;
  finalize(): void;
}

export interface SQLiteDatabase {
  /**
   * Close the database connection.
   */
  close(): void;

  /**
   * Destroy or delete the database (e.g. delete file).
   */
  destroy(): void;

  /**
   * Prepare a SQL string, returning a statement you can execute.
   * E.g. `const stmt = db.prepare("SELECT * FROM todos WHERE id=?");`
   */
  prepare(sql: string): PreparedStatement;
}

type SQLiteTransactionPreparedStatements = {
  begin: PreparedStatement;
  beginImmediate: PreparedStatement;
  commit: PreparedStatement;
  rollback: PreparedStatement;
};

const getTransactionPreparedStatements = (
  db: SQLiteDatabase,
): SQLiteTransactionPreparedStatements => ({
  begin: db.prepare('BEGIN'),
  beginImmediate: db.prepare('BEGIN IMMEDIATE'),
  commit: db.prepare('COMMIT'),
  rollback: db.prepare('ROLLBACK'),
});

type SQLiteRWPreparedStatements = {
  get: PreparedStatement;
  put: PreparedStatement;
  del: PreparedStatement;
};

const getRWPreparedStatements = (
  db: SQLiteDatabase,
): SQLiteRWPreparedStatements => ({
  get: db.prepare('SELECT value FROM entry WHERE key = ?'),
  put: db.prepare('INSERT OR REPLACE INTO entry (key, value) VALUES (?, ?)'),
  del: db.prepare('DELETE FROM entry WHERE key = ?'),
});

type SQLitePreparedStatements = SQLiteTransactionPreparedStatements &
  SQLiteRWPreparedStatements;

interface SQLiteConnectionManager {
  acquire(): Promise<{
    preparedStatements: SQLitePreparedStatements;
    release: () => void;
  }>;
}

type SQLitePreparedStatementPoolEntry = {
  lock: Lock;
  preparedStatements: SQLitePreparedStatements;
};

/**
 * Manages a pool of read-only SQLite connections.
 *
 * Each connection in the pool is protected by its own `Lock` instance which
 * guarantees that it is held by at most one reader at a time. Consumers call
 * {@link acquire} to get access to a set of prepared statements for a
 * connection and must invoke the provided `release` callback when they are
 * finished.
 *
 * The connection pool is eagerly created during instantiation to ensure
 * connections are ready for immediate use.
 *
 * Use the static `create` method to create instances of this class.
 */
class SQLiteReadConnectionManager implements SQLiteConnectionManager {
  readonly #pool: SQLitePreparedStatementPoolEntry[] = [];
  #nextIndex = 0;
  readonly #rwLock: RWLock;

  private constructor(
    rwLock: RWLock,
    pool: SQLitePreparedStatementPoolEntry[],
  ) {
    this.#rwLock = rwLock;
    this.#pool = pool;
  }

  static async create(
    name: string,
    manager: SQLiteDatabaseManager,
    rwLock: RWLock,
    opts: SQLiteDatabaseManagerOptions,
  ): Promise<SQLiteReadConnectionManager> {
    const pool: SQLitePreparedStatementPoolEntry[] = [];
    for (let i = 0; i < opts.readPoolSize; i++) {
      // create a new readonly SQLiteDatabase for each instance in the pool
      const {preparedStatements} = await manager.open(name, opts);
      pool.push({
        lock: new Lock(),
        preparedStatements,
      });
    }

    return new SQLiteReadConnectionManager(rwLock, pool);
  }

  /**
   * Acquire a round-robin read connection from the pool.
   *
   * The returned `release` callback **must** be invoked once the caller is done
   * using the prepared statements, otherwise other readers may be blocked
   * indefinitely.
   */
  async acquire(): Promise<{
    preparedStatements: SQLitePreparedStatements;
    release: () => void;
  }> {
    const slot = this.#nextIndex;
    this.#nextIndex = (this.#nextIndex + 1) % this.#pool.length;

    const entry = this.#pool[slot];

    // we have two levels of locking
    // 1. the RWLock to prevent concurrent read operations while a write is in progress
    // 2. the Lock to prevent concurrent read operations on the same connection
    const releaseRWLock = await this.#rwLock.read();
    const releaseLock = await entry.lock.lock();

    return {
      preparedStatements: entry.preparedStatements,
      release: () => {
        releaseRWLock();
        releaseLock();
      },
    };
  }
}

/**
 * Manages a single write connection with an external RWLock.
 *
 * Use the static `create` method to create instances of this class.
 */
class SQLiteWriteConnectionManager implements SQLiteConnectionManager {
  readonly #rwLock: RWLock;
  readonly #preparedStatements: SQLitePreparedStatements;

  private constructor(
    rwLock: RWLock,
    preparedStatements: SQLitePreparedStatements,
  ) {
    this.#preparedStatements = preparedStatements;
    this.#rwLock = rwLock;
  }

  static async create(
    name: string,
    manager: SQLiteDatabaseManager,
    rwLock: RWLock,
    opts: SQLiteDatabaseManagerOptions,
  ): Promise<SQLiteWriteConnectionManager> {
    const {preparedStatements} = await manager.open(name, opts);
    return new SQLiteWriteConnectionManager(rwLock, preparedStatements);
  }

  async acquire(): Promise<{
    preparedStatements: SQLitePreparedStatements;
    release: () => void;
  }> {
    const release = await this.#rwLock.write();
    return {preparedStatements: this.#preparedStatements, release};
  }
}

/**
 * A SQLite-based Store implementation.
 *
 * This store provides a generic SQLite implementation that can be used with different
 * SQLite providers (expo-sqlite, better-sqlite3, etc). It implements the Store
 * interface using a single 'entry' table with key-value pairs.
 *
 * The store uses a single RWLock to prevent concurrent read and write operations.
 *
 * The store also uses a pool of read connections to allow concurrent reads with separate transactions.
 */
export class SQLiteStore implements Store {
  readonly #name: string;
  readonly #dbm: SQLiteDatabaseManager;
  #writeConnectionManager!: SQLiteConnectionManager;
  #readConnectionManager!: SQLiteConnectionManager;
  readonly #rwLock = new RWLock();
  readonly #initialized: Promise<void>;

  #closed = false;

  constructor(
    name: string,
    dbm: SQLiteDatabaseManager,
    opts: SQLiteDatabaseManagerOptions,
  ) {
    if (opts.readPoolSize <= 1) {
      throw new Error('readPoolSize must be greater than 1');
    }

    this.#name = name;
    this.#dbm = dbm;

    // Initialize connections sequentially to avoid concurrent schema creation
    this.#initialized = this.#initialize(name, dbm, opts);
  }

  async #initialize(
    name: string,
    dbm: SQLiteDatabaseManager,
    opts: SQLiteDatabaseManagerOptions,
  ): Promise<void> {
    // Initialize write connection first (this creates the schema)
    // We need to ensure the write connection is fully created before read connections
    // to avoid race conditions where read connections try to access tables that don't exist yet
    this.#writeConnectionManager = await SQLiteWriteConnectionManager.create(
      name,
      dbm,
      this.#rwLock,
      opts,
    );

    // Then initialize read connections
    this.#readConnectionManager = await SQLiteReadConnectionManager.create(
      name,
      dbm,
      this.#rwLock,
      opts,
    );
  }

  async read(): Promise<Read> {
    await this.#initialized;
    const {preparedStatements, release} =
      await this.#readConnectionManager.acquire();
    return SQLiteStoreRead.create(preparedStatements, release);
  }

  async write(): Promise<Write> {
    await this.#initialized;
    const {preparedStatements, release} =
      await this.#writeConnectionManager.acquire();
    return SQLiteStoreWrite.create(preparedStatements, release);
  }

  close(): Promise<void> {
    this.#dbm.close(this.#name);
    this.#closed = true;

    return promiseVoid;
  }

  get closed(): boolean {
    return this.#closed;
  }
}

class SQLiteStoreRWBase {
  protected readonly _preparedStatements: SQLitePreparedStatements;
  readonly #release: () => void;
  #closed = false;

  protected constructor(
    preparedStatements: SQLitePreparedStatements,
    release: () => void,
  ) {
    this._preparedStatements = preparedStatements;
    this.#release = release;
  }

  async has(key: string): Promise<boolean> {
    const unsafeValue = await this.#getSql(key);
    return unsafeValue !== undefined;
  }

  async get(key: string): Promise<ReadonlyJSONValue | undefined> {
    const unsafeValue = await this.#getSql(key);
    if (unsafeValue === undefined) return undefined;
    const parsedValue = JSON.parse(unsafeValue) as ReadonlyJSONValue;
    const frozenValue = deepFreeze(parsedValue);
    return frozenValue;
  }

  async #getSql(key: string): Promise<string | undefined> {
    const rows = await this._preparedStatements.get.all<{value: string}>(key);
    if (rows.length === 0) return undefined;
    return rows[0].value;
  }

  protected _release(): void {
    this.#closed = true;
    this.#release();
  }

  get closed(): boolean {
    return this.#closed;
  }
}

class SQLiteStoreRead extends SQLiteStoreRWBase implements Read {
  static async create(
    preparedStatements: SQLitePreparedStatements,
    release: () => void,
  ): Promise<SQLiteStoreRead> {
    const instance = new SQLiteStoreRead(preparedStatements, release);
    // BEGIN transaction
    await instance._preparedStatements.begin.run();
    return instance;
  }

  release(): void {
    // COMMIT the read transaction
    void this._preparedStatements.commit.run();
    this._release();
  }
}

class SQLiteStoreWrite extends SQLiteStoreRWBase implements Write {
  #committed = false;

  static async create(
    preparedStatements: SQLitePreparedStatements,
    release: () => void,
  ): Promise<SQLiteStoreWrite> {
    const instance = new SQLiteStoreWrite(preparedStatements, release);
    // BEGIN IMMEDIATE grabs a RESERVED lock
    await instance._preparedStatements.beginImmediate.run();
    return instance;
  }

  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    return this._preparedStatements.put.run(key, JSON.stringify(value));
  }

  del(key: string): Promise<void> {
    return this._preparedStatements.del.run(key);
  }

  async commit(): Promise<void> {
    // COMMIT
    await this._preparedStatements.commit.run();
    this.#committed = true;
  }

  release(): void {
    if (!this.#committed) {
      // ROLLBACK if not committed
      void this._preparedStatements.rollback.run();
    }
    this._release();
  }
}

export interface GenericSQLiteDatabaseManager {
  open(fileName: string): SQLiteDatabase;
}

// we replace non-alphanumeric characters with underscores
// because SQLite doesn't allow them in database names
export function safeFilename(name: string) {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

export type SQLiteDatabaseManagerOptions = {
  /**
   * The number of read connections to keep open.
   *
   * This must be greater than 1 to support concurrent reads.
   */
  readPoolSize: number;

  busyTimeout?: number | undefined;
  journalMode?: 'WAL' | 'DELETE' | undefined;
  synchronous?: 'NORMAL' | 'FULL' | undefined;
  readUncommitted?: boolean | undefined;
};

const OPEN = 1;
const CLOSED = 0;

type DBInstance = {
  instances: {
    db: SQLiteDatabase;
    preparedStatements: SQLitePreparedStatements;
    state: typeof OPEN | typeof CLOSED;
  }[];
};

export class SQLiteDatabaseManager {
  readonly #dbm: GenericSQLiteDatabaseManager;
  readonly #dbInstances = new Map<string, DBInstance>();

  constructor(dbm: GenericSQLiteDatabaseManager) {
    this.#dbm = dbm;
  }

  clearAllStoresForTesting(): void {
    for (const [name] of this.#dbInstances) {
      this.destroy(name);
    }
  }

  async open(
    name: string,
    opts: Omit<SQLiteDatabaseManagerOptions, 'poolSize'>,
  ): Promise<{
    db: SQLiteDatabase;
    preparedStatements: SQLitePreparedStatements;
  }> {
    const dbInstance = this.#dbInstances.get(name);
    const fileName = safeFilename(name);
    const newDb = this.#dbm.open(fileName);

    const txPreparedStatements = getTransactionPreparedStatements(newDb);

    const exec = async (sql: string) => {
      const statement = newDb.prepare(sql);
      await statement.run();
      statement.finalize();
    };

    if (opts.busyTimeout !== undefined) {
      // we set a busy timeout to wait for write locks to be released
      await exec(`PRAGMA busy_timeout = ${opts.busyTimeout}`);
    }
    if (opts.journalMode !== undefined) {
      // WAL allows concurrent readers (improves write throughput ~15x and read throughput ~1.5x)
      // but does not work on all platforms (e.g. Expo)
      await exec(`PRAGMA journal_mode = ${opts.journalMode}`);
    }
    if (opts.synchronous !== undefined) {
      await exec(`PRAGMA synchronous = ${opts.synchronous}`);
    }
    if (opts.readUncommitted !== undefined) {
      await exec(
        `PRAGMA read_uncommitted = ${opts.readUncommitted ? 'true' : 'false'}`,
      );
    }

    // If this is the first connection for this database, create the schema
    if (!dbInstance) {
      await this.#ensureSchema(exec, txPreparedStatements);
    } else {
      // For subsequent connections, do a simple verification that the table exists
      // SQLite sometimes has timing issues with different database handles,
      // so we retry a few times if needed.
      for (let attempt = 0; attempt < 10; attempt++) {
        try {
          const checkStmt = newDb.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='entry'",
          );
          const result = await checkStmt.all();
          checkStmt.finalize();

          if (result.length > 0) {
            break; // Table exists, we're good
          }

          if (attempt === 9) {
            throw new Error("Table 'entry' does not exist after 10 attempts");
          }

          await sleep(1);
        } catch (e) {
          if (attempt === 9) {
            throw e;
          }
          await sleep(1);
        }
      }
    }

    // we prepare these after the schema is created and all pragmas are set
    const rwPreparedStatements = getRWPreparedStatements(newDb);

    const preparedStatements = {
      ...txPreparedStatements,
      ...rwPreparedStatements,
    };

    this.#dbInstances.set(name, {
      instances: [
        ...(dbInstance?.instances ?? []),
        {db: newDb, preparedStatements, state: OPEN},
      ],
    });

    return {
      db: newDb,
      preparedStatements,
    };
  }

  #closeDBInstance(name: string): DBInstance | undefined {
    const dbInstance = this.#dbInstances.get(name);
    if (dbInstance) {
      for (const instance of dbInstance.instances) {
        if (instance.state === CLOSED) {
          continue;
        }

        for (const stmt of Object.values(instance.preparedStatements)) {
          stmt.finalize();
        }
        instance.db.close();
        instance.state = CLOSED;
      }
    }
    return dbInstance;
  }

  close(name: string) {
    this.#closeDBInstance(name);
  }

  destroy(name: string): void {
    const dbInstance = this.#closeDBInstance(name);

    // All the instances in dbInstance share one underlying file.
    dbInstance?.instances[0].db.destroy();

    this.#dbInstances.delete(name);
  }

  async #ensureSchema(
    exec: (sql: string) => Promise<void>,
    preparedStatements: SQLiteTransactionPreparedStatements,
  ): Promise<void> {
    await preparedStatements.begin.run();

    try {
      // WITHOUT ROWID increases write throughput
      await exec(
        'CREATE TABLE IF NOT EXISTS entry (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID',
      );
      await preparedStatements.commit.run();

      // Verify the table was created successfully
      // This verification is done once during schema creation
      await exec('SELECT 1 FROM entry LIMIT 0');
    } catch (e) {
      await preparedStatements.rollback.run();
      throw e;
    }
  }
}
