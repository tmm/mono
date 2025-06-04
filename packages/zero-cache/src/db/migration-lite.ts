import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {randInt} from '../../../shared/src/rand.ts';
import * as v from '../../../shared/src/valita.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {AsyncDatabase} from './async-db.ts';

export type Db = AsyncDatabase | Database;

type Operations<D extends Db> = (
  log: LogContext,
  tx: D,
) => Promise<void> | void;

/**
 * Encapsulates the logic for setting up or upgrading to a new schema. After the
 * Migration code successfully completes, {@link runSchemaMigrations}
 * will update the schema version and commit the transaction.
 */
export type Migration<D extends Db> = {
  /**
   * Perform database operations that create or alter table structure. This is
   * called at most once during lifetime of the application. If a `migrateData()`
   * operation is defined, that will be performed after `migrateSchema()` succeeds.
   */
  migrateSchema?: Operations<D>;

  /**
   * Perform database operations to migrate data to the new schema. This is
   * called after `migrateSchema()` (if defined), and may be called again
   * to re-migrate data after the server was rolled back to an earlier version,
   * and rolled forward again.
   *
   * Consequently, the logic in `migrateData()` must be idempotent.
   */
  migrateData?: Operations<D>;

  /**
   * Sets the `minSafeVersion` to the specified value, prohibiting running
   * any earlier code versions.
   */
  minSafeVersion?: number;
};

/**
 * Mapping of incremental migrations to move from the previous old code
 * version to next one. Versions must be non-zero.
 *
 * The schema resulting from performing incremental migrations should be
 * equivalent to that of the `setupMigration` on a blank database.
 *
 * The highest destinationVersion of this map denotes the current
 * "code version", and is also used as the destination version when
 * running the initial setup migration on a blank database.
 */
export type IncrementalMigrationMap<D extends Db> = {
  [destinationVersion: number]: Migration<D>;
};

export async function runSchemaMigrations(
  log: LogContext,
  debugName: string,
  dbPath: string,
  setupMigration: Migration<Database>,
  incrementalMigrationMap: IncrementalMigrationMap<Database>,
): Promise<void> {
  const db = new Database(log, dbPath);
  db.unsafeMode(true); // Enables journal_mode = OFF

  await runSchemaMigrationsImpl(
    log,
    debugName,
    db,
    setupMigration,
    incrementalMigrationMap,
  );
}

export async function runSchemaMigrationsAsync(
  log: LogContext,
  debugName: string,
  dbPath: string,
  setupMigration: Migration<AsyncDatabase>,
  incrementalMigrationMap: IncrementalMigrationMap<AsyncDatabase>,
): Promise<void> {
  await runSchemaMigrationsImpl(
    log,
    debugName,
    await AsyncDatabase.connect(dbPath),
    setupMigration,
    incrementalMigrationMap,
  );
}

/**
 * Ensures that the schema is compatible with the current code, updating and
 * migrating the schema if necessary.
 */
async function runSchemaMigrationsImpl<D extends Db>(
  log: LogContext,
  debugName: string,
  db: D,
  setupMigration: Migration<D>,
  incrementalMigrationMap: IncrementalMigrationMap<D>,
): Promise<void> {
  const start = Date.now();
  log = log.withContext(
    'initSchema',
    randInt(0, Number.MAX_SAFE_INTEGER).toString(36),
  );

  try {
    const versionMigrations = sorted(incrementalMigrationMap);
    assert(
      versionMigrations.length,
      `Must specify a at least one version migration`,
    );
    assert(
      versionMigrations[0][0] > 0,
      `Versions must be non-zero positive numbers`,
    );
    const codeVersion = versionMigrations[versionMigrations.length - 1][0];
    log.info?.(
      `Checking schema for compatibility with ${debugName} at schema v${codeVersion}`,
    );

    let versions = await runTransaction(log, db, async tx => {
      const versions = await getVersionHistory(tx);
      if (codeVersion < versions.minSafeVersion) {
        throw new Error(
          `Cannot run ${debugName} at schema v${codeVersion} because rollback limit is v${versions.minSafeVersion}`,
        );
      }

      if (versions.dataVersion > codeVersion) {
        log.info?.(
          `Data is at v${versions.dataVersion}. Resetting to v${codeVersion}`,
        );
        return updateVersionHistory(log, tx, versions, codeVersion);
      }
      return versions;
    });

    if (versions.dataVersion < codeVersion) {
      await db.exec(
        `
      PRAGMA locking_mode = EXCLUSIVE;
      PRAGMA foreign_keys = OFF;
      PRAGMA journal_mode = OFF;
      PRAGMA synchronous = OFF;
      `,
        // Unfortunately, AUTO_VACUUM is not compatible with BEGIN CONCURRENT,
        // so it is not an option for the replica file.
        // https://sqlite.org/forum/forumpost/25f183416a
        // PRAGMA auto_vacuum = INCREMENTAL;
      );

      const migrations =
        versions.dataVersion === 0
          ? // For the empty database v0, only run the setup migration.
            ([[codeVersion, setupMigration]] as const)
          : versionMigrations;

      for (const [dest, migration] of migrations) {
        if (versions.dataVersion < dest) {
          log.info?.(
            `Migrating schema from v${versions.dataVersion} to v${dest}`,
          );
          void log.flush(); // Flush logs before each migration to help debug crash-y migrations.

          versions = await runTransaction(log, db, async tx => {
            // Fetch meta from within the transaction to make the migration atomic.
            let versions = await getVersionHistory(tx);
            if (versions.dataVersion < dest) {
              versions = await runMigration(log, tx, versions, dest, migration);
              assert(versions.dataVersion === dest);
            }
            return versions;
          });
        }
      }

      await db.exec('ANALYZE main');
      log.info?.('ANALYZE completed');
    } else {
      // Run optimize whenever opening an sqlite db file as recommended in
      // https://www.sqlite.org/pragma.html#pragma_optimize
      // It is important to run the same initialization steps as is done
      // in the view-syncer (i.e. when preparing database for serving
      // replication) so that any corruption detected in the view-syncer is
      // similarly detected in the change-streamer, facilitating an eventual
      // recovery by resyncing the replica anew.
      await db.exec('PRAGMA optimize = 0x10002;');

      // TODO: Investigate running `integrity_check` or `quick_check` as well,
      // provided that they are not inordinately expensive on large databases.
    }

    await db.exec('PRAGMA synchronous = NORMAL;');

    assert(versions.dataVersion === codeVersion);
    log.info?.(
      `Running ${debugName} at schema v${codeVersion} (${
        Date.now() - start
      } ms)`,
    );
  } catch (e) {
    log.error?.('Error in ensureSchemaMigrated', e);
    throw e;
  } finally {
    await db.close();
    void log.flush(); // Flush the logs but do not block server progress on it.
  }
}

function sorted<D extends Db>(
  incrementalMigrationMap: IncrementalMigrationMap<D>,
): [number, Migration<D>][] {
  const versionMigrations: [number, Migration<D>][] = [];
  for (const [v, m] of Object.entries(incrementalMigrationMap)) {
    versionMigrations.push([Number(v), m]);
  }
  return versionMigrations.sort(([a], [b]) => a - b);
}

// Exposed for tests.
export const versionHistory = v.object({
  /**
   * The `schemaVersion` is highest code version that has ever been run
   * on the database, and is used to delineate the structure of the tables
   * in the database. A schemaVersion only moves forward; rolling back to
   * an earlier (safe) code version does not revert schema changes that
   * have already been applied.
   */
  schemaVersion: v.number(),

  /**
   * The data version is the code version of the latest server that ran.
   * Note that this may be less than the schemaVersion in the case that
   * a server is rolled back to an earlier version after a schema change.
   * In such a case, data (but not schema), may need to be re-migrated
   * when rolling forward again.
   */
  dataVersion: v.number(),

  /**
   * The minimum code version that is safe to run. This is used when
   * a schema migration is not backwards compatible with an older version
   * of the code.
   */
  minSafeVersion: v.number(),
});

// Exposed for tests.
export type VersionHistory = v.Infer<typeof versionHistory>;

const getVersionHistoryStmt = /*sql*/ `
  SELECT dataVersion, schemaVersion, minSafeVersion FROM "_zero.versionHistory"`;

// Exposed for tests
export async function getVersionHistory(db: Db): Promise<VersionHistory> {
  // Note: The `lock` column transparently ensures that at most one row exists.
  await db.exec(
    `
    CREATE TABLE IF NOT EXISTS "_zero.versionHistory" (
      dataVersion INTEGER NOT NULL,
      schemaVersion INTEGER NOT NULL,
      minSafeVersion INTEGER NOT NULL,

      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
  `,
  );
  const result =
    db instanceof AsyncDatabase
      ? await db.get<VersionHistory>(getVersionHistoryStmt)
      : db.prepare(getVersionHistoryStmt).get<VersionHistory>();

  return result ?? {dataVersion: 0, schemaVersion: 0, minSafeVersion: 0};
}

const updateVersionHistoryStmt = /*sql*/ `
  INSERT INTO "_zero.versionHistory" (dataVersion, schemaVersion, minSafeVersion, lock)
    VALUES (@dataVersion, @schemaVersion, @minSafeVersion, 1)
    ON CONFLICT (lock) DO UPDATE
    SET dataVersion=@dataVersion,
        schemaVersion=@schemaVersion,
        minSafeVersion=@minSafeVersion
  `;

async function updateVersionHistory(
  log: LogContext,
  db: Db,
  prev: VersionHistory,
  newVersion: number,
  minSafeVersion?: number,
): Promise<VersionHistory> {
  assert(newVersion > 0);
  const meta = {
    dataVersion: newVersion,
    // The schemaVersion never moves backwards.
    schemaVersion: Math.max(newVersion, prev.schemaVersion),
    minSafeVersion: getMinSafeVersion(log, prev, minSafeVersion),
  };

  if (db instanceof AsyncDatabase) {
    await db.run(updateVersionHistoryStmt, meta);
  } else {
    db.prepare(updateVersionHistoryStmt).run(meta);
  }

  return meta;
}

async function runMigration<D extends Db>(
  log: LogContext,
  tx: D,
  versions: VersionHistory,
  destinationVersion: number,
  migration: Migration<D>,
): Promise<VersionHistory> {
  if (versions.schemaVersion < destinationVersion) {
    await migration.migrateSchema?.(log, tx);
  }
  if (versions.dataVersion < destinationVersion) {
    await migration.migrateData?.(log, tx);
  }
  return updateVersionHistory(
    log,
    tx,
    versions,
    destinationVersion,
    migration.minSafeVersion,
  );
}

/**
 * Bumps the rollback limit [[toAtLeast]] the specified version.
 * Leaves the rollback limit unchanged if it is equal or greater.
 */
function getMinSafeVersion(
  log: LogContext,
  current: VersionHistory,
  proposedSafeVersion?: number,
): number {
  if (proposedSafeVersion === undefined) {
    return current.minSafeVersion;
  }
  if (current.minSafeVersion >= proposedSafeVersion) {
    // The rollback limit must never move backwards.
    log.debug?.(
      `rollback limit is already at ${current.minSafeVersion}, ` +
        `don't need to bump to ${proposedSafeVersion}`,
    );
    return current.minSafeVersion;
  }
  log.info?.(
    `bumping rollback limit from ${current.minSafeVersion} to ${proposedSafeVersion}`,
  );
  return proposedSafeVersion;
}

// Note: We use a custom transaction wrapper (instead of db.begin(...)) in order
// to support async operations within the transaction.
async function runTransaction<D extends Db, T>(
  log: LogContext,
  db: D,
  tx: (db: D) => Promise<T> | T,
): Promise<T> {
  await db.exec('BEGIN EXCLUSIVE');
  try {
    const result = await tx(db);
    await db.exec('COMMIT');
    return result;
  } catch (e) {
    await db.exec('ROLLBACK');
    log.error?.('Aborted transaction due to error', e);
    throw e;
  }
}
