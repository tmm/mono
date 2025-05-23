import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {Readable, Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {
  isMainThread,
  MessagePort,
  parentPort,
  workerData,
} from 'node:worker_threads';
import {must} from '../../../../../shared/src/must.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {READONLY} from '../../../db/mode-enum.ts';
import {createLogContext} from '../../../server/logging.ts';
import {pgClient} from '../../../types/pg.ts';
import {childWorker} from '../../../types/workers.ts';

const workerDataSchema = v.object({
  log: v.object({
    level: v.literalUnion('debug', 'info', 'warn', 'error').default('info'),
    format: v.literalUnion('text', 'json').default('text'),
  }),
  db: v.string(),
  snapshotID: v.string(),
  copySelection: v.string(),
  bufferSize: v.number(),
});

type WorkerData = v.Infer<typeof workerDataSchema>;

type StreamChunk = {
  array: Uint8Array;
  length: number;
  last: boolean;
};

export function startCopyWorker(workerData: WorkerData): Readable {
  const child = childWorker(
    './services/change-source/pg/copy-worker.ts',
    workerData,
  );

  let requestDataOnRead = true;

  const readable = new Readable({
    highWaterMark: workerData.bufferSize,
    read() {
      if (requestDataOnRead) {
        child.postMessage({});
      }
    },
  });

  child.on('message', ({array, length, last}: StreamChunk) => {
    if (readable.push(Buffer.from(array.buffer, 0, length))) {
      child.postMessage({});
      requestDataOnRead = false;
    } else {
      requestDataOnRead = true;
    }
    if (last) {
      readable.push(null);
    }
  });
  child.on('error', err => readable.destroy(err));

  return readable;
}

export default async function runWorker(
  {log, db, snapshotID, copySelection, bufferSize}: WorkerData,
  parent: MessagePort,
) {
  const lc = createLogContext({log}, {worker: 'copy-worker'});
  lc.debug?.('started copy worker');

  const sql = pgClient(lc, db, {
    // No need to fetch array types for these connections, as pgClient
    // streams the COPY data as plain text; type parsing is done in the parent.
    // This eliminates one round trip when each db connection is established.
    ['fetch_types']: false,
    connection: {['application_name']: 'initial-sync-copy-worker'},
  });

  let forceExitTimeout: NodeJS.Timeout | undefined;

  await sql.begin(READONLY, async tx => {
    if (snapshotID) {
      void tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`).execute();
    }
    const start = performance.now();
    lc.info?.(`starting COPY: ${copySelection}`);
    const copyStream = await tx
      .unsafe(`COPY (${copySelection}) TO STDOUT`)
      .readable();
    const parentSink = new ParentSink(lc, parent, bufferSize);
    await pipeline(copyStream, parentSink);

    const elapsed = performance.now() - start;
    lc.info?.(
      `finished COPY (blocked: ${parentSink.blockedTime.toFixed(3)}) (total: ${elapsed.toFixed(3)} ms): ${copySelection}`,
    );

    if (!isMainThread) {
      // If Postgres hangs after the COPY, the COMMIT will not complete.
      // Force the thread to exit so that the connection can be dropped and
      // upstream resources reclaimed.
      forceExitTimeout = setTimeout(() => {
        lc.warn?.(`COPY connection may be hung. Forcing exit.`);
        process.exit(0);
      }, 500);
    }
  });

  clearTimeout(forceExitTimeout);
}

class ParentSink extends Writable {
  readonly #lc: LogContext;
  readonly #parent: MessagePort;
  readonly #defaultBufferSize: number;

  #array: Uint8Array;
  #length: number;
  #lastBufferConsumed: Resolver<void>;
  #blockedTime = 0;

  get blockedTime() {
    return this.#blockedTime;
  }

  constructor(lc: LogContext, parent: MessagePort, bufferSize: number) {
    super({highWaterMark: bufferSize});
    this.#lc = lc;
    this.#parent = parent;
    this.#defaultBufferSize = bufferSize;

    this.#array = new Uint8Array(bufferSize);
    this.#length = 0;
    this.#lastBufferConsumed = resolver();
    this.#lastBufferConsumed.resolve();

    parent.on('message', () => this.#lastBufferConsumed.resolve());
  }

  async #flush(nextChunkSize?: number) {
    const last = nextChunkSize === undefined;

    const start = performance.now();
    await this.#lastBufferConsumed.promise;
    const elapsed = performance.now() - start;
    this.#blockedTime += elapsed;

    const array = this.#array;
    const length = this.#length;

    // Prepare the next buffer to receive more data.
    this.#array = new Uint8Array(
      last ? 0 : Math.max(this.#defaultBufferSize, nextChunkSize),
    );
    this.#length = 0;
    this.#lastBufferConsumed = resolver();

    this.#lc.debug?.(
      `sending ${length} bytes after waiting ${elapsed.toFixed(3)} ms for last flush`,
    );
    this.#parent.postMessage({array, length, last} satisfies StreamChunk, [
      array.buffer,
    ]);
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  async _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ) {
    try {
      if (chunk.length + this.#length > this.#array.length) {
        await this.#flush(chunk.length);
      }
      chunk.copy(this.#array, this.#length);
      this.#length += chunk.length;
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  async _final(callback: (error?: Error | null) => void) {
    try {
      await this.#flush();
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}

if (!isMainThread) {
  v.assert(workerData, workerDataSchema);
  await runWorker(workerData, must(parentPort));
}
