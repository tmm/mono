import type {LogContext} from '@rocicorp/logger';
import {
  isMainThread,
  MessagePort,
  parentPort,
  workerData,
} from 'node:worker_threads';
import {assert} from '../../../../../shared/src/asserts.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {NULL_BYTE, TextTransform} from '../../../db/pg-copy.ts';
import {getTypeParsers, type TypeParsers} from '../../../db/pg-type-parser.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {createLogContext} from '../../../server/logging.ts';
import {
  JSON_STRINGIFIED,
  liteValue,
  type LiteValueType,
} from '../../../types/lite.ts';
import {liteTableName} from '../../../types/names.ts';
import {pgClient, type PostgresValueType} from '../../../types/pg.ts';
import {id} from '../../../types/sql.ts';
import {type Worker} from '../../../types/workers.ts';
import {
  type PipelineInit,
  pipelineInitSchema,
  startCopyStream,
  startCopyStreamer,
} from './copy-pipeline.ts';

export type StartCopyMessage = {
  type: 'copy';
  snapshotID: string;
  table: PublishedTableSpec;
  port: MessagePort;
};

export type TokenMessage = {
  type: 'token';
};

export type ValuesMessage = {
  values: LiteValueType[] | null;
};

export default function runWorker(
  {log, db, numBuffers, bufferSize}: PipelineInit,
  parent: MessagePort,
) {
  const lc = createLogContext({log}, {worker: 'parse-worker'});

  // Start the upstream copy-stream worker.
  const copyStreamWorker = startCopyStreamer(
    lc,
    {log, db},
    numBuffers,
    bufferSize,
  );

  // Make a one-time connection to the database to fetch type parsers for array
  // types. This is reused for all parse commands processed in the worker.
  const typeParsers = getTypeParsers(pgClient(lc, db, {}, 'json-as-string'));

  const tokens = new Queue<TokenMessage>();
  parent.on('message', async (msg: TokenMessage | StartCopyMessage) => {
    if (msg.type === 'token') {
      tokens.enqueue(msg);
      return;
    }
    assert(msg.type === 'copy');

    const {snapshotID, table, port} = msg;
    doCopy(
      lc,
      await copyStreamWorker,
      tokens,
      table,
      await typeParsers,
      snapshotID,
      port,
    );
  });

  // Allow the Worker to exit when the parent is no longer referencing it.
  parent.unref();
}

async function doCopy(
  lc: LogContext,
  copyStreamWorker: Worker,
  tokens: Queue<TokenMessage>,
  table: PublishedTableSpec,
  pgParsers: TypeParsers,
  snapshotID: string,
  dest: MessagePort,
) {
  const copyStart = performance.now();
  const orderedColumns = Object.entries(table.columns);

  const columnSpecs = orderedColumns.map(([_name, spec]) => spec);
  const selectColumns = orderedColumns.map(([c]) => id(c)).join(',');

  const filterConditions = Object.values(table.publications)
    .map(({rowFilter}) => rowFilter)
    .filter(f => !!f); // remove nulls
  const selectStmt =
    /*sql*/ `
    SELECT ${selectColumns} FROM ${id(table.schema)}.${id(table.name)}` +
    (filterConditions.length === 0
      ? ''
      : /*sql*/ ` WHERE ${filterConditions.join(' OR ')}`);

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
  const valuesPerRow = columnSpecs.length;

  const t = new TextTransform();
  const values: LiteValueType[] = Array.from({length: 50_000});

  let parseTime = 0;
  let postTime = 0;
  let numRows = 0;
  let numBytes = 0;

  lc.info?.(
    `starting COPY (available tokens: ${tokens.size()}): ${table.name}`,
  );

  startCopyStream(lc, copyStreamWorker, snapshotID, selectStmt, async chunk => {
    await tokens.dequeue();
    const parseStart = performance.now();

    if (chunk === null) {
      dest.postMessage({values: null} satisfies ValuesMessage);
      lc.info?.(
        `finished parsing ${numRows} rows of ${liteTableName(table)} ` +
          `(bytes: ${numBytes.toLocaleString()}) ` +
          `(parse: ${parseTime.toFixed(3)} ms) ` +
          `(post: ${postTime.toFixed(3)} ms) ` +
          `(total: ${(performance.now() - copyStart).toFixed(3)} ms)`,
      );
      return;
    }

    // Note: The values array is reused for each parse to reduce GC
    // churn. The `length` is reset before the structuredClone() is
    // run when posting the message to the destination.
    let pos = 0;
    for (const str of t.parse(chunk)) {
      if (pos === values.length) {
        values.length += 100;
      }
      values[pos] = parsers[pos % valuesPerRow](str);
      if (++pos % valuesPerRow === 0) {
        numRows++;
      }
    }

    values.length = pos;
    numBytes += chunk.length;
    const postStart = performance.now();

    dest.postMessage({values} satisfies ValuesMessage);

    parseTime += postStart - parseStart;
    postTime += performance.now() - postStart;
  });
}

if (!isMainThread) {
  v.assert(workerData, pipelineInitSchema);
  runWorker(workerData, must(parentPort));
}
