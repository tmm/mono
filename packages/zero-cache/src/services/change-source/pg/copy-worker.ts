import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
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
import {READONLY} from '../../../db/mode-enum.ts';
import {createLogContext} from '../../../server/logging.ts';
import {pgClient, type PostgresDB} from '../../../types/pg.ts';
import {
  type CopyStreamerInit,
  copyStreamerInitSchema,
} from './copy-pipeline.ts';

export type StartCopyStreamMessage = {
  type: 'copy';
  snapshotID: string;
  selection: string;
  port: MessagePort;
};
export type BufferMessage = {
  type: 'buffer';
  array: Uint8Array;
};

export type StreamChunk = {
  array: Uint8Array | null;
  length: number;
  last: boolean;
};

export default function runWorker(
  {log, db}: CopyStreamerInit,
  parent: MessagePort,
) {
  const lc = createLogContext({log}, {worker: 'copy-worker'});

  const buffers = new Queue<Uint8Array>();
  parent.on('message', async (msg: BufferMessage | StartCopyStreamMessage) => {
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
      await doCopy(lc, buffers, sql, snapshotID, selection, port);
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
  // parent.unref();
}

function doCopy(
  lc: LogContext,
  buffers: Queue<Uint8Array>,
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
          `finished COPY (${sink.totalBytes.toLocaleString()} bytes) ` +
            `(blocked: ${sink.blockedTime.toFixed(3)} ms) ` +
            `(total: ${elapsed.toFixed(3)} ms): ${selection.trim()}`,
        );
      });
  });

  return copyDone;
}

class MessagePortSink extends Writable {
  readonly #lc: LogContext;
  readonly #dest: MessagePort;
  readonly #buffers: Queue<Uint8Array>;

  #array: Uint8Array | null = null;
  #length: number = 0;
  #blockedTime = 0;
  #totalBytes = 0;

  get blockedTime() {
    return this.#blockedTime;
  }

  get totalBytes() {
    return this.#totalBytes;
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
      this.#totalBytes += this.#length;
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
    try {
      const array =
        this.#array !== null &&
        this.#array.length >= chunk.length + this.#length
          ? this.#array
          : await this.#getNextBuffer(chunk.length);
      chunk.copy(array, this.#length);
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
  v.assert(workerData, copyStreamerInitSchema);
  runWorker(workerData, must(parentPort));
}
