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
import {
  NULL_BYTE,
  TextTransform,
  type TextTransformOutput,
} from '../../../db/pg-copy.ts';
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
  isPgStringType,
  PASSTHROUGH_PARSER,
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
        return getPublicationInfo(tx, publications);
      });
      // Note: If this throws, initial-sync is aborted.
      validatePublications(lc, published);

      // Now that tables have been validated, kick off the copiers.
      const {tables, indexes} = published;
      const numTables = tables.length;
      createLiteTables(tx, tables);

      const rowCounts = await Promise.all(
        tables.map(table =>
          copyRunner.run((db, lc) =>
            copy(lc, table, typeClient, db, tx, initialVersion),
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

  const valuesPerRow = columnSpecs.length + 1; // includes _0_version column
  const valuesPerBatch = valuesPerRow * INSERT_BATCH_SIZE;

  const pgParsers = await getTypeParsers(dbClient);
  const makeValueList = (_ignored?: unknown, row = 0) =>
    `(${[
      ...columnSpecs.map((spec, i) => {
        const param = `?${row * valuesPerRow + i + 1}`; // 1-indexed
        // Pass string types to SQLite directly as Buffers, using SQLite
        // format() to encode it as a string. This moves the work of
        // decoding and allocating the string from Node to C++.
        if (
          isPgStringType(spec.dataType.toLowerCase()) ||
          pgParsers.getTypeParser(spec.typeOID) === PASSTHROUGH_PARSER
        ) {
          return `iif(${param} NOTNULL,format('%s',${param}),NULL)`;
        }
        // TODO: Handle integers and floats this way, after vetting
        //       that SQLite's format() understands PG's text formatting.
        return param;
      }),
      `?${(row + 1) * valuesPerRow}`, // ZERO_VERSION_COLUMN is a string
    ].join(',')})`;

  // (?1,?2,?3,?4,?5)
  const singleRowValuesList = makeValueList();
  // (?1,?2,?3,?4,?5),(?6,?7,?8,?9,?10),... x INSERT_BATCH_SIZE
  const batchValuesList = Array.from(
    {length: INSERT_BATCH_SIZE},
    makeValueList,
  ).join(',');

  const insertStmt = to.prepare(/*sql*/ `
    INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${singleRowValuesList}`);
  const insertBatchStmt = to.prepare(/*sql*/ `
    INSERT INTO "${tableName}" (${insertColumnList}) VALUES ${batchValuesList}`);

  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const selectStmt =
    /*sql*/ `
    SELECT ${selectColumns} FROM ${id(table.schema)}.${id(table.name)}` +
    (filterConditions.length === 0
      ? ''
      : /*sql*/ ` WHERE ${filterConditions.join(' OR ')}`);

  // Preallocate the buffer of values to reduce memory allocation churn.
  const pendingValues: LiteValueType[] = Array.from({
    length: MAX_BUFFERED_ROWS * valuesPerRow,
  });

  // better-sqlite3 has an "interesting" interpretation of numbered bind
  // parameters, whereby "?1, ?2, ?3" parameters expect the supplied
  // values to be in the for of an object like `{1: ..., 2: ..., 3: ...}`.
  //
  // https://github.com/WiseLibs/better-sqlite3/issues/576
  function bindParameters(values: LiteValueType[]) {
    return Object.fromEntries(values.map((v, i) => [i + 1, v]));
  }

  let pendingRows = 0;
  let pendingSize = 0;

  function flush() {
    const start = performance.now();
    const flushedRows = pendingRows;
    const flushedSize = pendingSize;

    let l = 0;
    for (; pendingRows > INSERT_BATCH_SIZE; pendingRows -= INSERT_BATCH_SIZE) {
      insertBatchStmt.run(
        bindParameters(pendingValues.slice(l, (l += valuesPerBatch))),
      );
    }
    // Insert the remaining rows individually.
    for (; pendingRows > 0; pendingRows--) {
      insertStmt.run(
        bindParameters(pendingValues.slice(l, (l += valuesPerRow))),
      );
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
      `flushed ${flushedRows} ${tableName} rows (${flushedSize} bytes) in ${elapsed.toFixed(3)} ms`,
    );
  }

  lc.info?.(`Starting copy stream of ${tableName}:`, selectStmt);
  const parsers = columnSpecs.map(c => {
    const pgParse = pgParsers.getTypeParser(c.typeOID);
    // Sent to SQLite directly as a Buffer.
    // TODO: Handle more types this way.
    const passThroughAsBuffer =
      isPgStringType(c.dataType) || pgParse === PASSTHROUGH_PARSER;
    return (val: TextTransformOutput) =>
      val === NULL_BYTE
        ? null
        : passThroughAsBuffer
          ? val
          : liteValue(
              pgParse(val.toString('utf8')) as PostgresValueType,
              c.dataType,
              JSON_STRINGIFIED,
            );
  });

  let col = 0;

  await pipeline(
    await from.unsafe(`COPY (${selectStmt}) TO STDOUT`).readable(),
    new TextTransform(),
    new Writable({
      objectMode: true,

      write: (
        text: TextTransformOutput,
        _encoding: string,
        callback: (error?: Error) => void,
      ) => {
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
    `Finished copying ${rows} rows into ${tableName} ` +
      `(flush: ${flushTime.toFixed(3)} ms) (total: ${elapsed.toFixed(3)} ms) `,
  );
  return {rows, flushTime};
}
