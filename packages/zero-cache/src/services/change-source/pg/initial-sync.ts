import {PG_INSUFFICIENT_PRIVILEGE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import postgres from 'postgres';
import {Database} from '../../../../../zqlite/src/db.ts';
import {
  createIndexStatement,
  createTableStatement,
} from '../../../db/create.ts';
import * as Mode from '../../../db/mode-enum.ts';
import {NULL_BYTE, TextTransform} from '../../../db/pg-copy.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteIndex,
} from '../../../db/pg-to-lite.ts';
import {getTypeParsers} from '../../../db/pg-type-parser.ts';
import type {IndexSpec, PublishedTableSpec} from '../../../db/specs.ts';
import type {LexiVersion} from '../../../types/lexi-version.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {
  pgClient,
  type PostgresDB,
  type PostgresTransaction,
  type PostgresValueType,
} from '../../../types/pg.ts';
import type {ShardConfig} from '../../../types/shards.ts';
import {ALLOWED_APP_ID_CHARACTERS} from '../../../types/shards.ts';
import {id} from '../../../types/sql.ts';
import {initChangeLog} from '../../replicator/schema/change-log.ts';
import {
  initReplicationState,
  ZERO_VERSION_COLUMN_NAME,
} from '../../replicator/schema/replication-state.ts';
import {CopyRunner} from './copy-runner.ts';
import {getPartsToDownload, type TablePart} from './download-manager.ts';
import {toLexiVersion} from './lsn.ts';
import {ensureShardSchema} from './schema/init.ts';
import {getPublicationInfo} from './schema/published.ts';
import {
  addReplica,
  dropShard,
  getInternalShardConfig,
  newReplicationSlot,
  validatePublications,
} from './schema/shard.ts';

export type InitialSyncOptions = {
  tableCopyWorkers: number;
  minDownloadPartSize?: number;
};

export async function initialSync(
  lc: LogContext,
  shard: ShardConfig,
  tx: Database,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
) {
  if (!ALLOWED_APP_ID_CHARACTERS.test(shard.appID)) {
    throw new Error(
      'The App ID may only consist of lower-case letters, numbers, and the underscore character',
    );
  }
  const {tableCopyWorkers: numWorkers} = syncOptions;
  const sql = pgClient(lc, upstreamURI);
  // The typeClient's reason for existence is to configure the type
  // parsing for the copy workers, which skip JSON parsing for efficiency.
  const typeClient = pgClient(lc, upstreamURI, {}, 'json-as-string');
  // Fire off an innocuous request to initialize a connection and thus fetch
  // the array types that will be used to parse the COPY stream.
  void typeClient`SELECT 1`.execute();
  const replicationSession = pgClient(lc, upstreamURI, {
    ['fetch_types']: false, // Necessary for the streaming protocol
    connection: {replication: 'database'}, // https://www.postgresql.org/docs/current/protocol-replication.html
  });
  const slotName = newReplicationSlot(shard);
  try {
    await checkUpstreamConfig(sql);

    const {publications} = await ensurePublishedTables(lc, sql, shard);
    lc.info?.(`Upstream is setup with publications [${publications}]`);

    const {database, host} = sql.options;
    lc.info?.(`opening replication session to ${database}@${host}`);

    let slot: ReplicationSlot;
    for (let first = true; ; first = false) {
      try {
        slot = await createReplicationSlot(lc, replicationSession, slotName);
        break;
      } catch (e) {
        if (
          first &&
          e instanceof postgres.PostgresError &&
          e.code === PG_INSUFFICIENT_PRIVILEGE
        ) {
          // Some Postgres variants (e.g. Google Cloud SQL) require that
          // the user have the REPLICATION role in order to create a slot.
          // Note that this must be done by the upstreamDB connection, and
          // does not work in the replicationSession itself.
          await sql`ALTER ROLE current_user WITH REPLICATION`;
          lc.info?.(`Added the REPLICATION role to database user`);
          continue;
        }
        throw e;
      }
    }
    const {snapshot_name: snapshot, consistent_point: lsn} = slot;
    const initialVersion = toLexiVersion(lsn);

    initReplicationState(tx, publications, initialVersion);
    initChangeLog(tx);

    // Run up to MAX_WORKERS to copy of tables at the replication slot's snapshot.
    const start = performance.now();
    const copyRunner = new CopyRunner(
      lc,
      () =>
        pgClient(lc, upstreamURI, {
          // No need to fetch array types for these connections, as pgClient
          // streams the COPY data as plain text; type parsing is done in the
          // copy worker, which gets its types from the typeClient. This
          // eliminates one round trip when each db connection is established.
          ['fetch_types']: false,
          connection: {['application_name']: 'initial-sync-copy-worker'},
        }),
      numWorkers,
      snapshot,
    );
    try {
      // Retrieve the published schema at the consistent_point.
      const published = await sql.begin(Mode.READONLY, async tx => {
        await tx.unsafe(/* sql*/ `SET TRANSACTION SNAPSHOT '${snapshot}'`);
        const pub = await getPublicationInfo(tx, publications);
        const sizes = await Promise.all(
          pub.tables.map(({oid, schema, name}) =>
            tx<{rows: bigint; bytes: bigint}[]>/*sql*/ `
            SELECT COUNT(*) AS rows, pg_table_size(${oid}) AS bytes 
              FROM ${tx(schema)}.${tx(name)}
          `.then(([result]) => result),
          ),
        );
        return {
          ...pub,
          sizes: sizes.map(({rows, bytes}) => ({
            // It's okay for these to be estimates; to account for more rows
            // than Number.MAX_SAFE_INTEGER, the last downloaded part must
            // simply not have a LIMIT.
            rows: Number(rows),
            bytes: Number(bytes),
          })),
        };
      });
      // Note: If this throws, initial-sync is aborted.
      validatePublications(lc, published);

      // Now that tables have been validated, kick off the copiers.
      const {tables, indexes, sizes} = published;
      const numTables = tables.length;
      createLiteTables(tx, tables);

      const parts = getPartsToDownload(
        lc,
        tables.map((t, i) => ({...t, ...sizes[i]})),
        numWorkers,
        // syncOptions.minDownloadPartSize,
        1000 * 1000 * 1024 * 1024,
      );
      const rowCounts = await Promise.all(
        parts.map(table =>
          copyRunner.run((db, lc) =>
            copy(lc, table, table.part, typeClient, db, tx, initialVersion),
          ),
        ),
      );
      const total = rowCounts.reduce(
        (acc, curr) => ({
          rows: acc.rows + curr.rows,
          flushTime: acc.flushTime + curr.flushTime,
        }),
        {rows: 0, flushTime: 0},
      );

      const indexStart = performance.now();
      createLiteIndices(tx, indexes);
      const index = performance.now() - indexStart;
      lc.info?.(`Created indexes (${index.toFixed(3)} ms)`);

      await addReplica(sql, shard, slotName, initialVersion, published);

      const elapsed = performance.now() - start;
      lc.info?.(
        `Synced ${total.rows.toLocaleString()} rows of ${numTables} tables in ${publications} up to ${lsn} ` +
          `(flush: ${total.flushTime.toFixed(3)}, index: ${index.toFixed(3)}, total: ${elapsed.toFixed(3)} ms)`,
      );
    } finally {
      copyRunner.close();
    }
  } catch (e) {
    // If initial-sync did not succeed, make a best effort to drop the
    // orphaned replication slot to avoid running out of slots in
    // pathological cases that result in repeated failures.
    lc.warn?.(`dropping replication slot ${slotName}`, e);
    await sql`
      SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
        WHERE slot_name = ${slotName};
    `;
    throw e;
  } finally {
    await replicationSession.end();
    await sql.end();
    await typeClient.end();
  }
}

async function checkUpstreamConfig(sql: PostgresDB) {
  const {walLevel, version} = (
    await sql<{walLevel: string; version: number}[]>`
      SELECT current_setting('wal_level') as "walLevel", 
             current_setting('server_version_num') as "version";
  `
  )[0];

  if (walLevel !== 'logical') {
    throw new Error(
      `Postgres must be configured with "wal_level = logical" (currently: "${walLevel})`,
    );
  }
  if (version < 150000) {
    throw new Error(
      `Must be running Postgres 15 or higher (currently: "${version}")`,
    );
  }
}

async function ensurePublishedTables(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardConfig,
  validate = true,
): Promise<{publications: string[]}> {
  const {database, host} = sql.options;
  lc.info?.(`Ensuring upstream PUBLICATION on ${database}@${host}`);

  await ensureShardSchema(lc, sql, shard);
  const {publications} = await getInternalShardConfig(sql, shard);

  if (validate) {
    const exists = await sql`
      SELECT pubname FROM pg_publication WHERE pubname IN ${sql(publications)}
      `.values();
    if (exists.length !== publications.length) {
      lc.warn?.(
        `some configured publications [${publications}] are missing: ` +
          `[${exists.flat()}]. resyncing`,
      );
      await sql.unsafe(dropShard(shard.appID, shard.shardNum));
      return ensurePublishedTables(lc, sql, shard, false);
    }
  }
  return {publications};
}

/* eslint-disable @typescript-eslint/naming-convention */
// Row returned by `CREATE_REPLICATION_SLOT`
type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};
/* eslint-enable @typescript-eslint/naming-convention */

// Note: The replication connection does not support the extended query protocol,
//       so all commands must be sent using sql.unsafe(). This is technically safe
//       because all placeholder values are under our control (i.e. "slotName").
async function createReplicationSlot(
  lc: LogContext,
  session: postgres.Sql,
  slotName: string,
): Promise<ReplicationSlot> {
  const slot = (
    await session.unsafe<ReplicationSlot[]>(
      /*sql*/ `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
    )
  )[0];
  lc.info?.(`Created replication slot ${slotName}`, slot);
  return slot;
}

function createLiteTables(tx: Database, tables: PublishedTableSpec[]) {
  for (const t of tables) {
    tx.exec(createTableStatement(mapPostgresToLite(t)));
  }
}

function createLiteIndices(tx: Database, indices: IndexSpec[]) {
  for (const index of indices) {
    tx.exec(createIndexStatement(mapPostgresToLiteIndex(index)));
  }
}

// Verified empirically that batches of 50 seem to be the sweet spot,
// similar to the report in https://sqlite.org/forum/forumpost/8878a512d3652655
//
// Exported for testing.
export const INSERT_BATCH_SIZE = 50;

const MB = 1024 * 1024;
const MAX_BUFFERED_ROWS = 10_000;
const BUFFERED_SIZE_THRESHOLD = 8 * MB;

async function copy(
  lc: LogContext,
  table: PublishedTableSpec,
  part: TablePart,
  dbClient: PostgresDB,
  from: PostgresTransaction,
  to: Database,
  initialVersion: LexiVersion,
) {
  const start = performance.now();
  let rows = 0;
  let flushTime = 0;

  const tableName = liteTableName(table);
  const orderedColumns = Object.entries(table.columns);

  const columnSpecs = orderedColumns.map(([_name, spec]) => spec);
  const selectColumns = orderedColumns.map(([c]) => id(c)).join(',');
  const insertColumns = [
    ...orderedColumns.map(([c]) => c),
    ZERO_VERSION_COLUMN_NAME,
  ];
  const insertColumnList = insertColumns.map(c => id(c)).join(',');

  // (?,?,?,?,?)
  const valuesSql = `(${new Array(insertColumns.length).fill('?').join(',')})`;
  const insertSql = /*sql*/ `
    INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${valuesSql}`;
  const insertStmt = to.prepare(insertSql);
  // INSERT VALUES (?,?,?,?,?),... x INSERT_BATCH_SIZE
  const insertBatchStmt = to.prepare(
    insertSql + `,${valuesSql}`.repeat(INSERT_BATCH_SIZE - 1),
  );

  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const selectParts = [
    `SELECT ${selectColumns} FROM ${id(table.schema)}.${id(table.name)}`,
  ];
  if (filterConditions.length) {
    selectParts.push(`WHERE (${filterConditions.join(' OR ')})`);
  }
  // Only add a limit for the non-final part, since the number of rows may
  // be an estimate.
  const limit = part.partNum < part.totalParts;
  if (part.offset || limit) {
    // use the ctid system column for efficient deterministic ordering.
    // https://www.postgresql.org/docs/current/ddl-system-columns.html
    selectParts.push(`ORDER BY ctid`);
    if (limit) {
      selectParts.push(`LIMIT ${part.limit}`);
    }
    selectParts.push(`OFFSET ${part.offset}`);
  }
  const selectStmt = selectParts.join(' ');

  const valuesPerRow = columnSpecs.length + 1; // includes _0_version column
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

  // Preallocate the buffer of values to reduce memory allocation churn.
  const pendingValues: LiteValueType[] = Array.from({
    length: MAX_BUFFERED_ROWS * valuesPerRow,
  });
  let pendingRows = 0;
  let pendingSize = 0;

  function flush() {
    const start = performance.now();
    const flushedRows = pendingRows;
    const flushedSize = pendingSize;

    let l = 0;
    for (; pendingRows > INSERT_BATCH_SIZE; pendingRows -= INSERT_BATCH_SIZE) {
      insertBatchStmt.run(pendingValues.slice(l, (l += valuesPerBatch)));
    }
    // Insert the remaining rows individually.
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(pendingValues.slice(l, (l += valuesPerRow)));
    }
    for (let i = 0; i < flushedRows; i++) {
      // Reuse the array and unreference the values to allow GC.
      // This is faster than allocating a new array every time.
      pendingValues[i] = undefined as unknown as LiteValueType;
    }
    pendingSize = 0;
    rows += flushedRows;

    const elapsed = performance.now() - start;
    flushTime += elapsed;
    lc.debug?.(
      `flushed ${flushedRows} ${tableName} rows` +
        ` (part ${part.partNum} of ${part.totalParts})` +
        ` (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
  }

  lc.info?.(`Starting copy stream of ${tableName}:`, selectStmt);
  const pgParsers = await getTypeParsers(dbClient);
  const parsers = columnSpecs.map(c => {
    const pgParse = pgParsers.getTypeParser(c.typeOID);
    return (val: string) =>
      val === NULL_BYTE
        ? null
        : liteValue(
            pgParse(val) as PostgresValueType,
            c.dataType,
            JSON_STRINGIFIED,
          );
  });

  let col = 0;
  let firstRowTime = 0;

  await pipeline(
    await from.unsafe(`COPY (${selectStmt}) TO STDOUT`).readable(),
    new TextTransform(),
    new Writable({
      objectMode: true,

      write: (
        text: string,
        _encoding: string,
        callback: (error?: Error) => void,
      ) => {
        if (rows === 0 && pendingSize === 0) {
          firstRowTime = performance.now() - start;
        }
        try {
          // Give every value at least 4 bytes.
          pendingSize += 4 + (text === NULL_BYTE ? 0 : text.length);
          pendingValues[pendingRows * valuesPerRow + col] = parsers[col](text);

          if (++col === parsers.length) {
            // The last column is the _0_version.
            pendingValues[pendingRows * valuesPerRow + col] = initialVersion;
            col = 0;
            if (
              ++pendingRows >= MAX_BUFFERED_ROWS - valuesPerRow ||
              pendingSize >= BUFFERED_SIZE_THRESHOLD
            ) {
              flush();
            }
          }
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },

      final: (callback: (error?: Error) => void) => {
        try {
          flush();
          callback();
        } catch (e) {
          callback(e instanceof Error ? e : new Error(String(e)));
        }
      },
    }),
  );

  const elapsed = performance.now() - start;
  lc.info?.(
    `Finished copying ${tableName} table` +
      ` part ${part.partNum} of ${part.totalParts}` +
      ` (rows: ${rows}) (firstRow: ${firstRowTime.toFixed(3)} ms)` +
      ` (flush: ${flushTime.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
  );
  return {rows, flushTime};
}
