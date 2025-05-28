import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {Readable, Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {
  isMainThread,
  MessageChannel,
  MessagePort,
  parentPort,
  workerData,
} from 'node:worker_threads';
import {assert} from '../../../../../shared/src/asserts.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {READONLY} from '../../../db/mode-enum.ts';
import {createLogContext} from '../../../server/logging.ts';
import {pgClient, type PostgresDB} from '../../../types/pg.ts';
import {childWorker, type Worker} from '../../../types/workers.ts';

const workerInitSchema = v.object({
  log: v.object({
    level: v.literalUnion('debug', 'info', 'warn', 'error').default('info'),
    format: v.literalUnion('text', 'json').default('text'),
  }),
  db: v.string(),
});

export type WorkerInit = v.Infer<typeof workerInitSchema>;

type BufferMessage = {
  type: 'buffer';
  array: Uint8Array;
};

type CopyMessage = {
  type: 'copy';
  snapshotID: string;
  selection: string;
  port: MessagePort;
};

type StreamChunk = {
  array: Uint8Array | null;
  length: number;
  last: boolean;
};

export async function startCopyWorker(
  lc: LogContext,
  workerData: WorkerInit,
  numBuffers: number,
  bufferSize: number,
): Promise<Worker> {
  const start = performance.now();
  const worker = childWorker(
    lc,
    './services/change-source/pg/copy-worker.ts',
    workerData,
  );

  const {promise, resolve, reject} = resolver();
  worker.on('online', () => {
    for (let i = 0; i < numBuffers; i++) {
      const array = new Uint8Array(bufferSize);
      worker.postMessage({type: 'buffer', array} satisfies BufferMessage, [
        array.buffer,
      ]);
    }
    resolve();
  });
  worker.on('exit', () => lc.debug?.('copy worker exited'));
  worker.on('error', reject);

  await promise;
  lc.info?.(
    `copy worker started (${(performance.now() - start).toFixed(3)} ms)`,
  );
  return worker;
}

export function startCopy(
  _lc: LogContext,
  worker: Worker,
  snapshotID: string,
  selection: string,
): Readable {
  let lastBuffer: Uint8Array | null = null;

  function returnBuffer() {
    if (lastBuffer) {
      worker.postMessage(
        {type: 'buffer', array: lastBuffer} satisfies BufferMessage,
        [lastBuffer.buffer],
      );
    }
    lastBuffer = null;
  }

  const readable = new Readable({read: returnBuffer});
  const {port1, port2} = new MessageChannel();

  port1.on('message', ({array, length, last}: StreamChunk) => {
    lastBuffer = array;
    if (array && readable.push(Buffer.from(array.buffer, 0, length))) {
      returnBuffer();
    }
    if (last) {
      readable.push(null);
    }
  });

  worker.postMessage(
    {
      type: 'copy',
      snapshotID,
      selection,
      port: port2,
    } satisfies CopyMessage,
    [port2],
  );

  return readable;
}

export default function runWorker({log, db}: WorkerInit, parent: MessagePort) {
  const lc = createLogContext({log}, {worker: 'copy-worker'});
  lc.debug?.('started copy worker');

  const buffers = new Queue<Uint8Array>();
  parent.on('message', async (msg: BufferMessage | CopyMessage) => {
    if (msg.type === 'buffer') {
      buffers.enqueue(msg.array);
      return;
    }
    assert(msg.type === 'copy');
    const {snapshotID, selection, port} = msg;

    const sql = pgClient(lc, db, {
      // No need to fetch array types for these connections, as pgClient
      // streams the COPY data as plain text; type parsing is done downstream.
      // This eliminates one round trip when each db connection is established.
      ['fetch_types']: false,
      connection: {['application_name']: 'initial-sync-copy-worker'},
    });
    try {
      await doCopy(sql, snapshotID, selection, port);
    } catch (e) {
      // Exit the worker if the COPY fails.
      lc.error?.(`error in copy worker`, e);
      process.exit(-1);
    } finally {
      // Errors closing the connection do not affect server functionality;
      // log as warning to flag and identify pathological situations.
      void sql.end().catch(e => lc.warn?.(`error closing COPY connection`, e));
    }
  });

  // Allow the Worker to exit when the parent is no longer referencing it.
  parent.unref();

  function doCopy(
    sql: PostgresDB,
    snapshotID: string,
    selection: string,
    dest: MessagePort,
  ) {
    const start = performance.now();
    lc.info?.(
      `starting COPY (available buffers: ${buffers.size()}): ${selection.trim()}`,
    );
    const {promise: copyDone, resolve, reject} = resolver();

    void sql.begin(READONLY, async tx => {
      if (snapshotID) {
        void tx.unsafe(`SET TRANSACTION SNAPSHOT '${snapshotID}'`).execute();
      }
      const copyStream = await tx
        .unsafe(`COPY (${selection}) TO STDOUT`)
        .readable();
      const sink = new MessagePortSink(lc, dest, buffers);

      void pipeline(copyStream, sink)
        .then(resolve, reject)
        .finally(() => {
          const elapsed = performance.now() - start;
          lc.info?.(
            `finished COPY (blocked: ${sink.blockedTime.toFixed(3)}) (total: ${elapsed.toFixed(3)} ms): ${selection.trim()}`,
          );
        });
    });

    return copyDone;
  }
}

class MessagePortSink extends Writable {
  readonly #lc: LogContext;
  readonly #dest: MessagePort;
  readonly #buffers: Queue<Uint8Array>;

  #array: Uint8Array | null = null;
  #length: number = 0;
  #blockedTime = 0;

  get blockedTime() {
    return this.#blockedTime;
  }

  constructor(lc: LogContext, dest: MessagePort, buffers: Queue<Uint8Array>) {
    super();
    this.#lc = lc;
    this.#dest = dest;
    this.#buffers = buffers;
  }

  async #getNextBuffer(minSize: number) {
    this.#flush();

    const start = performance.now();
    const buffer = await this.#buffers.dequeue();
    this.#blockedTime += performance.now() - start;

    if (buffer.length >= minSize) {
      this.#array = buffer;
    } else {
      this.#lc.warn?.(`allocating new buffer of size ${minSize}`);
      this.#array = new Uint8Array(minSize);
    }
    this.#length = 0;
    return this.#array;
  }

  #flush(last = false) {
    if (this.#array || last) {
      this.#dest.postMessage(
        {array: this.#array, length: this.#length, last} satisfies StreamChunk,
        this.#array ? [this.#array.buffer] : undefined,
      );
      this.#array = null;
      this.#length = 0;
    }
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  async _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ) {
    if (1) {
      callback();
      return;
    }
    try {
      const array =
        this.#array !== null &&
        this.#array.length >= chunk.length + this.#length
          ? this.#array
          : await this.#getNextBuffer(chunk.length);
      // chunk.copy(array, this.#length);
      array;
      this.#length += chunk.length;
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  _final(callback: (error?: Error | null) => void) {
    try {
      this.#flush(true);
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}

if (!isMainThread) {
  v.assert(workerData, workerInitSchema);
  runWorker(workerData, must(parentPort));
}
