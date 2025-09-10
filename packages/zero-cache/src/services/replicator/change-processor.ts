import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import {
  columnDef,
  createFTS5Statements,
  createIndexStatement,
  createTableStatement,
} from '../../db/create.ts';
import {
  computeZqlSpecs,
  listIndexes,
  listTables,
} from '../../db/lite-tables.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteIndex,
} from '../../db/pg-to-lite.ts';
import type {LiteTableSpec} from '../../db/specs.ts';
import type {StatementRunner} from '../../db/statements.ts';
import type {LexiVersion} from '../../types/lexi-version.ts';
import {
  JSON_PARSED,
  liteRow,
  type JSONFormat,
  type LiteRow,
  type LiteRowKey,
  type LiteValueType,
} from '../../types/lite.ts';
import {liteTableName} from '../../types/names.ts';
import {id} from '../../types/sql.ts';
import type {
  Change,
  ColumnAdd,
  ColumnDrop,
  ColumnUpdate,
  IndexCreate,
  IndexDrop,
  MessageCommit,
  MessageDelete,
  MessageInsert,
  MessageRelation,
  MessageTruncate,
  MessageUpdate,
  TableCreate,
  TableDrop,
  TableRename,
} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {loadIndices} from '../../indices/load-indices.ts';
import type {ReplicatorMode} from './replicator.ts';
import {
  logDeleteOp,
  logResetOp,
  logSetOp,
  logTruncateOp,
} from './schema/change-log.ts';
import {
  ZERO_VERSION_COLUMN_NAME,
  updateReplicationWatermark,
} from './schema/replication-state.ts';

export type ChangeProcessorMode = ReplicatorMode | 'initial-sync';

export type CommitResult = {
  watermark: string;
  schemaUpdated: boolean;
};

/**
 * The ChangeProcessor partitions the stream of messages into transactions
 * by creating a {@link TransactionProcessor} when a transaction begins, and dispatching
 * messages to it until the commit is received.
 *
 * From https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES-FLOW :
 *
 * "The logical replication protocol sends individual transactions one by one.
 *  This means that all messages between a pair of Begin and Commit messages
 *  belong to the same transaction."
 */
export class ChangeProcessor {
  readonly #db: StatementRunner;
  readonly #mode: ChangeProcessorMode;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  // The TransactionProcessor lazily loads table specs into this Map,
  // and reloads them after a schema change. It is cached here to avoid
  // reading them from the DB on every transaction.
  readonly #tableSpecs = new Map<string, LiteTableSpec>();

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    mode: ChangeProcessorMode,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#db = db;
    this.#mode = mode;
    this.#failService = failService;
  }

  #fail(lc: LogContext, err: unknown) {
    if (!this.#failure) {
      this.#currentTx?.abort(lc); // roll back any pending transaction.

      this.#failure = ensureError(err);

      if (!(err instanceof AbortError)) {
        // Propagate the failure up to the service.
        lc.error?.('Message Processing failed:', this.#failure);
        this.#failService(lc, this.#failure);
      }
    }
  }

  abort(lc: LogContext) {
    this.#fail(lc, new AbortError());
  }

  /** @return If a transaction was committed. */
  processMessage(
    lc: LogContext,
    downstream: ChangeStreamData,
  ): CommitResult | null {
    const [type, message] = downstream;
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return null;
    }
    try {
      const watermark =
        type === 'begin'
          ? downstream[2].commitWatermark
          : type === 'commit'
            ? downstream[2].watermark
            : undefined;
      return this.#processMessage(lc, message, watermark);
    } catch (e) {
      this.#fail(lc, e);
    }
    return null;
  }

  #beginTransaction(
    lc: LogContext,
    commitVersion: string,
    jsonFormat: JSONFormat,
  ): TransactionProcessor {
    const start = Date.now();

    // litestream can technically hold the lock for an arbitrary amount of time
    // when checkpointing a large commit. Crashing on the busy-timeout in this
    // scenario will either produce a corrupt backup or otherwise prevent
    // replication from proceeding.
    //
    // Instead, retry the lock acquisition indefinitely. If this masks
    // an unknown deadlock situation, manual intervention will be necessary.
    for (let i = 0; ; i++) {
      try {
        return new TransactionProcessor(
          lc,
          this.#db,
          this.#mode,
          this.#tableSpecs,
          commitVersion,
          jsonFormat,
        );
      } catch (e) {
        if (e instanceof SqliteError && e.code === 'SQLITE_BUSY') {
          lc.warn?.(
            `SQLITE_BUSY for ${Date.now() - start} ms (attempt ${i + 1}). ` +
              `This is only expected if litestream is performing a large ` +
              `checkpoint.`,
            e,
          );
          continue;
        }
        throw e;
      }
    }
  }

  /** @return If a transaction was committed. */
  #processMessage(
    lc: LogContext,
    msg: Change,
    watermark: string | undefined,
  ): CommitResult | null {
    if (msg.tag === 'begin') {
      if (this.#currentTx) {
        throw new Error(`Already in a transaction ${stringify(msg)}`);
      }
      this.#currentTx = this.#beginTransaction(
        lc,
        must(watermark),
        msg.json ?? JSON_PARSED,
      );
      return null;
    }

    // For non-begin messages, there should be a #currentTx set.
    const tx = this.#currentTx;
    if (!tx) {
      throw new Error(
        `Received message outside of transaction: ${stringify(msg)}`,
      );
    }

    if (msg.tag === 'commit') {
      // Undef this.#currentTx to allow the assembly of the next transaction.
      this.#currentTx = null;

      assert(watermark);
      const schemaUpdated = tx.processCommit(msg, watermark);
      return {watermark, schemaUpdated};
    }

    if (msg.tag === 'rollback') {
      this.#currentTx?.abort(lc);
      this.#currentTx = null;
      return null;
    }

    switch (msg.tag) {
      case 'insert':
        tx.processInsert(msg);
        break;
      case 'update':
        tx.processUpdate(msg);
        break;
      case 'delete':
        tx.processDelete(msg);
        break;
      case 'truncate':
        tx.processTruncate(msg);
        break;
      case 'create-table':
        tx.processCreateTable(msg);
        break;
      case 'rename-table':
        tx.processRenameTable(msg);
        break;
      case 'add-column':
        tx.processAddColumn(msg);
        break;
      case 'update-column':
        tx.processUpdateColumn(msg);
        break;
      case 'drop-column':
        tx.processDropColumn(msg);
        break;
      case 'drop-table':
        tx.processDropTable(msg);
        break;
      case 'create-index':
        tx.processCreateIndex(msg);
        break;
      case 'drop-index':
        tx.processDropIndex(msg);
        break;
      default:
        unreachable(msg);
    }

    return null;
  }
}

/**
 * The {@link TransactionProcessor} handles the sequence of messages from
 * upstream, from `BEGIN` to `COMMIT` and executes the corresponding mutations
 * on the {@link postgres.TransactionSql} on the replica.
 *
 * When applying row contents to the replica, the `_0_version` column is added / updated,
 * and a corresponding entry in the `ChangeLog` is added. The version value is derived
 * from the watermark of the preceding transaction (stored as the `nextStateVersion` in the
 * `ReplicationState` table).
 *
 *   Side note: For non-streaming Postgres transactions, the commitEndLsn (and thus
 *   commit watermark) is available in the `begin` message, so it could theoretically
 *   be used for the row version of changes within the transaction. However, the
 *   commitEndLsn is not available in the streaming (in-progress) transaction
 *   protocol, and may not be available for CDC streams of other upstream types.
 *   Therefore, the zero replication protocol is designed to not require the commit
 *   watermark when a transaction begins.
 *
 * Also of interest is the fact that all INSERT Messages are logically applied as
 * UPSERTs. See {@link processInsert} for the underlying motivation.
 */
class TransactionProcessor {
  readonly #lc: LogContext;
  readonly #startMs: number;
  readonly #db: StatementRunner;
  readonly #mode: ChangeProcessorMode;
  readonly #version: LexiVersion;
  readonly #tableSpecs: Map<string, LiteTableSpec>;
  readonly #jsonFormat: JSONFormat;

  #schemaChanged = false;

  constructor(
    lc: LogContext,
    db: StatementRunner,
    mode: ChangeProcessorMode,
    tableSpecs: Map<string, LiteTableSpec>,
    commitVersion: LexiVersion,
    jsonFormat: JSONFormat,
  ) {
    this.#startMs = Date.now();
    this.#mode = mode;
    this.#jsonFormat = jsonFormat;

    switch (mode) {
      case 'serving':
        // Although the Replicator / Incremental Syncer is the only writer of the replica,
        // a `BEGIN CONCURRENT` transaction is used to allow View Syncers to simulate
        // (i.e. and `ROLLBACK`) changes on historic snapshots of the database for the
        // purpose of IVM).
        //
        // This TransactionProcessor is the only logic that will actually
        // `COMMIT` any transactions to the replica.
        db.beginConcurrent();
        break;
      case 'backup':
        // For the backup-replicator (i.e. replication-manager), there are no View Syncers
        // and thus BEGIN CONCURRENT is not necessary. In fact, BEGIN CONCURRENT can cause
        // deadlocks with forced wal-checkpoints (which `litestream replicate` performs),
        // so it is important to use vanilla transactions in this configuration.
        db.beginImmediate();
        break;
      case 'initial-sync':
        // When the ChangeProcessor is used for initial-sync, the calling code
        // handles the transaction boundaries.
        break;
      default:
        unreachable();
    }
    this.#db = db;
    this.#version = commitVersion;
    this.#lc = lc.withContext('version', commitVersion);
    this.#tableSpecs = tableSpecs;

    if (this.#tableSpecs.size === 0) {
      this.#reloadTableSpecs();
    }
  }

  #reloadTableSpecs() {
    this.#tableSpecs.clear();
    // zqlSpecs include the primary key derived from unique indexes
    const zqlSpecs = computeZqlSpecs(this.#lc, this.#db.db);
    for (let spec of listTables(this.#db.db)) {
      if (!spec.primaryKey) {
        spec = {
          ...spec,
          primaryKey: [
            ...(zqlSpecs.get(spec.name)?.tableSpec.primaryKey ?? []),
          ],
        };
      }
      this.#tableSpecs.set(spec.name, spec);
    }
  }

  #tableSpec(name: string) {
    return must(this.#tableSpecs.get(name), `Unknown table ${name}`);
  }

  #getKey(
    {row, numCols}: {row: LiteRow; numCols: number},
    {relation}: {relation: MessageRelation},
  ): LiteRowKey {
    const keyColumns =
      relation.replicaIdentity !== 'full'
        ? relation.keyColumns // already a suitable key
        : this.#tableSpec(liteTableName(relation)).primaryKey;
    if (!keyColumns?.length) {
      throw new Error(
        `Cannot replicate table "${relation.name}" without a PRIMARY KEY or UNIQUE INDEX`,
      );
    }
    // For the common case (replica identity default), the row is already the
    // key for deletes and updates, in which case a new object can be avoided.
    if (numCols === keyColumns.length) {
      return row;
    }
    const key: Record<string, LiteValueType> = {};
    for (const col of keyColumns) {
      key[col] = row[col];
    }
    return key;
  }

  processInsert(insert: MessageInsert) {
    const table = liteTableName(insert.relation);
    const newRow = liteRow(
      insert.new,
      this.#tableSpec(table),
      this.#jsonFormat,
    );

    this.#upsert(table, {
      ...newRow.row,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    });

    if (insert.relation.keyColumns.length === 0) {
      // INSERTs can be replicated for rows without a PRIMARY KEY or a
      // UNIQUE INDEX. These are written to the replica but not recorded
      // in the changeLog, because these rows cannot participate in IVM.
      //
      // (Once the table schema has been corrected to include a key, the
      //  associated schema change will reset pipelines and data can be
      //  loaded via hydration.)
      return;
    }
    const key = this.#getKey(newRow, insert);
    this.#logSetOp(table, key);
  }

  #upsert(table: string, row: LiteRow) {
    const columns = Object.keys(row).map(c => id(c));
    this.#db.run(
      `
      INSERT OR REPLACE INTO ${id(table)} (${columns.join(',')})
        VALUES (${new Array(columns.length).fill('?').join(',')})
      `,
      Object.values(row),
    );
  }

  // Updates by default are applied as UPDATE commands to support partial
  // row specifications from the change source. In particular, this is needed
  // to handle updates for which unchanged TOASTed values are not sent:
  //
  // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TUPLEDATA
  //
  // However, in certain cases an UPDATE may be received for a row that
  // was not initially synced, such as when:
  // (1) an existing table is added to the app's publication, or
  // (2) a new sharding key is added to a shard during resharding.
  //
  // In order to facilitate "resumptive" replication, the logic falls back to
  // an INSERT if the update did not change any rows.
  // TODO: Figure out a solution for resumptive replication of rows
  //       with TOASTed values.
  processUpdate(update: MessageUpdate) {
    const table = liteTableName(update.relation);
    const newRow = liteRow(
      update.new,
      this.#tableSpec(table),
      this.#jsonFormat,
    );
    const row = {...newRow.row, [ZERO_VERSION_COLUMN_NAME]: this.#version};

    // update.key is set with the old values if the key has changed.
    const oldKey = update.key
      ? this.#getKey(
          liteRow(update.key, this.#tableSpec(table), this.#jsonFormat),
          update,
        )
      : null;
    const newKey = this.#getKey(newRow, update);

    if (oldKey) {
      this.#logDeleteOp(table, oldKey);
    }
    this.#logSetOp(table, newKey);

    const currKey = oldKey ?? newKey;
    const conds = Object.keys(currKey).map(col => `${id(col)}=?`);
    const setExprs = Object.keys(row).map(col => `${id(col)}=?`);

    const {changes} = this.#db.run(
      `
      UPDATE ${id(table)}
        SET ${setExprs.join(',')}
        WHERE ${conds.join(' AND ')}
      `,
      [...Object.values(row), ...Object.values(currKey)],
    );

    // If the UPDATE did not affect any rows, perform an UPSERT of the
    // new row for resumptive replication.
    if (changes === 0) {
      this.#upsert(table, row);
    }

    // Check if this is an update to the indices table
    if (table.endsWith('.indices')) {
      this.#lc.info?.(
        'Indices configuration updated, rebuilding FTS tables...',
      );
      this.#rebuildFTSTables(table);
    }
  }

  processDelete(del: MessageDelete) {
    const table = liteTableName(del.relation);
    const rowKey = this.#getKey(
      liteRow(del.key, this.#tableSpec(table), this.#jsonFormat),
      del,
    );

    this.#delete(table, rowKey);

    if (this.#mode === 'serving') {
      this.#logDeleteOp(table, rowKey);
    }
  }

  #delete(table: string, rowKey: LiteRowKey) {
    const conds = Object.keys(rowKey).map(col => `${id(col)}=?`);
    this.#db.run(
      `DELETE FROM ${id(table)} WHERE ${conds.join(' AND ')}`,
      Object.values(rowKey),
    );
  }

  processTruncate(truncate: MessageTruncate) {
    for (const relation of truncate.relations) {
      const table = liteTableName(relation);
      // Update replica data.
      this.#db.run(`DELETE FROM ${id(table)}`);

      // Update change log.
      this.#logTruncateOp(table);
    }
  }
  processCreateTable(create: TableCreate) {
    const table = mapPostgresToLite(create.spec);
    this.#db.db.exec(createTableStatement(table));

    this.#logResetOp(table.name);
    this.#lc.info?.(create.tag, table.name);
  }

  processRenameTable(rename: TableRename) {
    const oldName = liteTableName(rename.old);
    const newName = liteTableName(rename.new);
    this.#db.db.exec(`ALTER TABLE ${id(oldName)} RENAME TO ${id(newName)}`);

    this.#bumpVersions(newName);
    this.#logResetOp(oldName);
    this.#lc.info?.(rename.tag, oldName, newName);
  }

  processAddColumn(msg: ColumnAdd) {
    const table = liteTableName(msg.table);
    const {name} = msg.column;
    const spec = mapPostgresToLiteColumn(table, msg.column);
    this.#db.db.exec(
      `ALTER TABLE ${id(table)} ADD ${id(name)} ${columnDef(spec)}`,
    );

    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, msg.column);
  }

  processUpdateColumn(msg: ColumnUpdate) {
    const table = liteTableName(msg.table);
    let oldName = msg.old.name;
    const newName = msg.new.name;

    // update-column can ignore defaults because it does not change the values
    // in existing rows.
    //
    // https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-SET-DROP-DEFAULT
    //
    // "The new default value will only apply in subsequent INSERT or UPDATE
    //  commands; it does not cause rows already in the table to change."
    //
    // This allows support for _changing_ column defaults to any expression,
    // since it does not affect what the replica needs to do.
    const oldSpec = mapPostgresToLiteColumn(table, msg.old, 'ignore-default');
    const newSpec = mapPostgresToLiteColumn(table, msg.new, 'ignore-default');

    // The only updates that are relevant are the column name and the data type.
    if (oldName === newName && oldSpec.dataType === newSpec.dataType) {
      this.#lc.info?.(msg.tag, 'no thing to update', oldSpec, newSpec);
      return;
    }
    // If the data type changes, we have to make a new column with the new data type
    // and copy the values over.
    if (oldSpec.dataType !== newSpec.dataType) {
      // Remember (and drop) the indexes that reference the column.
      const indexes = listIndexes(this.#db.db).filter(
        idx => idx.tableName === table && oldName in idx.columns,
      );
      const stmts = indexes.map(idx => `DROP INDEX IF EXISTS ${id(idx.name)};`);
      const tmpName = `tmp.${newName}`;
      stmts.push(`
        ALTER TABLE ${id(table)} ADD ${id(tmpName)} ${columnDef(newSpec)};
        UPDATE ${id(table)} SET ${id(tmpName)} = ${id(oldName)};
        ALTER TABLE ${id(table)} DROP ${id(oldName)};
        `);
      for (const idx of indexes) {
        // Re-create the indexes to reference the new column.
        idx.columns[tmpName] = idx.columns[oldName];
        delete idx.columns[oldName];
        stmts.push(createIndexStatement(idx));
      }
      this.#db.db.exec(stmts.join(''));
      oldName = tmpName;
    }
    if (oldName !== newName) {
      this.#db.db.exec(
        `ALTER TABLE ${id(table)} RENAME ${id(oldName)} TO ${id(newName)}`,
      );
    }
    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, msg.new);
  }

  processDropColumn(msg: ColumnDrop) {
    const table = liteTableName(msg.table);
    const {column} = msg;
    this.#db.db.exec(`ALTER TABLE ${id(table)} DROP ${id(column)}`);

    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, column);
  }

  processDropTable(drop: TableDrop) {
    const name = liteTableName(drop.id);
    this.#db.db.exec(`DROP TABLE IF EXISTS ${id(name)}`);

    this.#logResetOp(name);
    this.#lc.info?.(drop.tag, name);
  }

  processCreateIndex(create: IndexCreate) {
    const index = mapPostgresToLiteIndex(create.spec);
    
    // Only create regular indices, not fulltext (those come from config)
    this.#db.db.exec(createIndexStatement(index));

    // indexes affect tables visibility (e.g. sync-ability is gated on
    // having a unique index), so reset pipelines to refresh table schemas.
    this.#logResetOp(index.tableName);
    this.#lc.info?.(create.tag, index.name);
  }

  processDropIndex(drop: IndexDrop) {
    const name = liteTableName(drop.id);
    this.#db.db.exec(`DROP INDEX IF EXISTS ${id(name)}`);
    this.#lc.info?.(drop.tag, name);
  }

  #bumpVersions(table: string) {
    this.#db.run(
      `UPDATE ${id(table)} SET ${id(ZERO_VERSION_COLUMN_NAME)} = ?`,
      this.#version,
    );
    this.#logResetOp(table);
  }

  #logSetOp(table: string, key: LiteRowKey) {
    if (this.#mode === 'serving') {
      logSetOp(this.#db, this.#version, table, key);
    }
  }

  #logDeleteOp(table: string, key: LiteRowKey) {
    if (this.#mode === 'serving') {
      logDeleteOp(this.#db, this.#version, table, key);
    }
  }

  #logTruncateOp(table: string) {
    if (this.#mode === 'serving') {
      logTruncateOp(this.#db, this.#version, table);
    }
  }

  #logResetOp(table: string) {
    this.#schemaChanged = true;
    if (this.#mode === 'serving') {
      logResetOp(this.#db, this.#version, table);
    }
    this.#reloadTableSpecs();
  }

  #rebuildFTSTables(indicesTableName: string) {
    // Extract appID from table name (format: {appID}.indices)
    const appID = indicesTableName.replace('.indices', '');

    // Load the new indices configuration
    const {indices: indicesConfig} = loadIndices(this.#lc, this.#db, appID);

    // Get existing FTS tables and their columns
    const existingFTS = this.#getExistingFTSTables();
    
    // Track what tables we've processed
    const processedTables = new Set<string>();
    const toCreate: Array<{tableName: string; columns: string[]; allColumns: string[]}> = [];
    const toDrop = new Set<string>();
    let hasChanges = false;

    if (indicesConfig && indicesConfig.tables) {
      // Check each table in the new configuration
      for (const [tableName, tableIndices] of Object.entries(
        indicesConfig.tables,
      )) {
        const mappedTableName = liteTableName({schema: appID, name: tableName});
        const ftsTableName = `${mappedTableName}_fts`;
        processedTables.add(ftsTableName);

        if (tableIndices.fulltext && tableIndices.fulltext.length > 0) {
          // Collect all columns to index from configuration
          const ftsColumns = new Set<string>();
          for (const ftConfig of tableIndices.fulltext) {
            ftConfig.columns.forEach(col => ftsColumns.add(col));
          }

          // Get existing columns for this FTS table
          const existingColumns = existingFTS.get(ftsTableName);
          
          // Check if configuration has changed
          const configChanged = !existingColumns || 
            existingColumns.size !== ftsColumns.size ||
            ![...existingColumns].every(col => ftsColumns.has(col));

          if (configChanged) {
            // Configuration changed, need to rebuild
            if (existingColumns) {
              this.#lc.info?.(
                `FTS configuration changed for ${mappedTableName}: ` +
                `[${[...existingColumns].join(', ')}] -> [${[...ftsColumns].join(', ')}]`,
              );
              toDrop.add(mappedTableName);
            } else {
              this.#lc.info?.(
                `Creating new FTS table for ${mappedTableName} with columns: ${[...ftsColumns].join(', ')}`,
              );
            }

            // Get table spec for all columns
            const tableSpec = this.#tableSpecs.get(mappedTableName);
            if (!tableSpec) {
              this.#lc.warn?.(`Table ${mappedTableName} not found in table specs`);
              continue;
            }

            const allColumns = Object.keys(tableSpec.columns).filter(
              col => col !== '_0_version',
            );

            toCreate.push({
              tableName: mappedTableName,
              columns: Array.from(ftsColumns),
              allColumns,
            });
            hasChanges = true;
          } else {
            this.#lc.debug?.(
              `FTS configuration unchanged for ${mappedTableName}, keeping existing`,
            );
          }
        } else if (existingFTS.has(ftsTableName)) {
          // Table no longer has FTS configuration but FTS table exists
          this.#lc.info?.(
            `Removing FTS table for ${mappedTableName} (no longer in configuration)`,
          );
          toDrop.add(mappedTableName);
          hasChanges = true;
        }
      }
    }

    // Check for orphaned FTS tables (not in configuration anymore)
    for (const ftsTableName of existingFTS.keys()) {
      if (!processedTables.has(ftsTableName)) {
        const baseTableName = ftsTableName.replace('_fts', '');
        this.#lc.info?.(
          `Removing orphaned FTS table: ${ftsTableName}`,
        );
        toDrop.add(baseTableName);
        hasChanges = true;
      }
    }

    // Drop tables that need to be removed or rebuilt
    for (const tableName of toDrop) {
      this.#dropFTSTable(tableName);
    }

    // Create new or rebuilt FTS tables
    for (const {tableName, columns, allColumns} of toCreate) {
      const ftsStatements = createFTS5Statements(
        tableName,
        columns,
        allColumns,
      );

      for (const stmt of ftsStatements) {
        this.#db.db.exec(stmt);
      }
    }

    // Only mark schema as changed if we actually made changes
    if (hasChanges) {
      this.#lc.info?.('FTS tables updated, marking schema as changed');
      this.#schemaChanged = true;
    } else {
      this.#lc.debug?.('No FTS table changes needed');
    }
  }

  #getExistingFTSTables(): Map<string, Set<string>> {
    const result = new Map<string, Set<string>>();
    
    // Get all FTS tables
    const ftsTables = this.#db.db
      .prepare(
        `SELECT name, sql FROM sqlite_master 
         WHERE type='table' AND name LIKE '%_fts' 
         AND sql LIKE '%fts5%'`,
      )
      .all() as Array<{name: string; sql: string}>;

    for (const {name, sql} of ftsTables) {
      // Parse columns from the CREATE VIRTUAL TABLE statement
      // Format: CREATE VIRTUAL TABLE ... USING fts5(col1, col2, content=..., ...)
      const match = sql.match(/USING\s+fts5\s*\(([^)]+)\)/i);
      if (match) {
        const columnsPart = match[1];
        const columns = new Set<string>();
        
        // Split by comma and extract column names (skip options like content=, tokenize=)
        const parts = columnsPart.split(',').map(s => s.trim());
        for (const part of parts) {
          // Skip FTS options (they contain '=')
          if (!part.includes('=')) {
            columns.add(part);
          }
        }
        
        if (columns.size > 0) {
          result.set(name, columns);
        }
      }
    }
    
    return result;
  }

  #dropFTSTable(tableName: string) {
    const ftsTableName = `${tableName}_fts`;
    const viewName = `${tableName}_view`;
    
    // Drop triggers
    this.#db.db.exec(`DROP TRIGGER IF EXISTS ${id(`${ftsTableName}_insert`)}`);
    this.#db.db.exec(`DROP TRIGGER IF EXISTS ${id(`${ftsTableName}_update`)}`);
    this.#db.db.exec(`DROP TRIGGER IF EXISTS ${id(`${ftsTableName}_delete`)}`);
    
    // Drop view
    this.#db.db.exec(`DROP VIEW IF EXISTS ${id(viewName)}`);
    
    // Drop FTS table
    this.#db.db.exec(`DROP TABLE IF EXISTS ${id(ftsTableName)}`);
    
    this.#lc.debug?.(`Dropped FTS table and related objects for ${tableName}`);
  }

  /** @returns `true` if the schema was updated. */
  processCommit(commit: MessageCommit, watermark: string): boolean {
    if (watermark !== this.#version) {
      throw new Error(
        `'commit' version ${watermark} does not match 'begin' version ${
          this.#version
        }: ${stringify(commit)}`,
      );
    }
    updateReplicationWatermark(this.#db, watermark);

    if (this.#schemaChanged) {
      const start = Date.now();
      this.#db.db.pragma('optimize');
      this.#lc.info?.(
        `PRAGMA optimized after schema change (${Date.now() - start} ms)`,
      );
    }

    if (this.#mode !== 'initial-sync') {
      this.#db.commit();
    }

    const elapsedMs = Date.now() - this.#startMs;
    this.#lc.debug?.(`Committed tx@${this.#version} (${elapsedMs} ms)`);

    return this.#schemaChanged;
  }

  abort(lc: LogContext) {
    lc.info?.(`aborting transaction ${this.#version}`);
    this.#db.rollback();
  }
}

function ensureError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  const error = new Error();
  error.cause = err;
  return error;
}
