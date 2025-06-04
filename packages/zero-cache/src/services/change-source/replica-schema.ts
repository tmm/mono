import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import type {Database} from '../../../../zqlite/src/db.ts';
import {AsyncDatabase} from '../../db/async-db.ts';
import {
  runSchemaMigrations,
  runSchemaMigrationsAsync,
  type Db,
  type IncrementalMigrationMap,
  type Migration,
} from '../../db/migration-lite.ts';
import {AutoResetSignal} from '../change-streamer/schema/tables.ts';
import {
  CREATE_RUNTIME_EVENTS_TABLE,
  recordEvent,
} from '../replicator/schema/replication-state.ts';

export async function initReplica(
  log: LogContext,
  debugName: string,
  dbPath: string,
  initialSync: (lc: LogContext, tx: Database) => Promise<void>,
): Promise<void> {
  const setupMigration: Migration<Database> = {
    migrateSchema: (log, tx) => initialSync(log, tx),
    minSafeVersion: 1,
  };

  try {
    await runSchemaMigrations(
      log,
      debugName,
      dbPath,
      setupMigration,
      schemaVersionMigrationMap,
    );
  } catch (e) {
    if (e instanceof SqliteError && e.code === 'SQLITE_CORRUPT') {
      throw new AutoResetSignal(e.message);
    }
    throw e;
  }
}

export async function initReplicaAsync(
  log: LogContext,
  debugName: string,
  dbPath: string,
  initialSync: (lc: LogContext, tx: AsyncDatabase) => Promise<void>,
): Promise<void> {
  const setupMigration: Migration<AsyncDatabase> = {
    migrateSchema: (log, tx) => initialSync(log, tx),
    minSafeVersion: 1,
  };

  try {
    await runSchemaMigrationsAsync(
      log,
      debugName,
      dbPath,
      setupMigration,
      schemaVersionMigrationMap,
    );
  } catch (e) {
    if (e instanceof SqliteError && e.code === 'SQLITE_CORRUPT') {
      throw new AutoResetSignal(e.message);
    }
    throw e;
  }
}

export async function upgradeReplica(
  log: LogContext,
  debugName: string,
  dbPath: string,
) {
  await runSchemaMigrations(
    log,
    debugName,
    dbPath,
    // setupMigration should never be invoked
    {
      migrateSchema: () => {
        throw new Error(
          'This should only be called for already synced replicas',
        );
      },
    },
    schemaVersionMigrationMap,
  );
}

export const schemaVersionMigrationMap: IncrementalMigrationMap<Db> = {
  // There's no incremental migration from v1. Just reset the replica.
  4: {
    migrateSchema: () => {
      throw new AutoResetSignal('upgrading replica to new schema');
    },
    minSafeVersion: 3,
  },

  5: {
    migrateSchema: (_, db) => db.exec(CREATE_RUNTIME_EVENTS_TABLE),

    migrateData: (_, db) =>
      db instanceof AsyncDatabase // needed to appease TypeScript
        ? recordEvent(db, 'upgrade')
        : recordEvent(db, 'upgrade'),
  },
};
