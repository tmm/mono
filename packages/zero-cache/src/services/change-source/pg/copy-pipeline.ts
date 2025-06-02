import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {Readable} from 'stream';
import {MessageChannel, type Worker as NodeWorker} from 'worker_threads';
import * as v from '../../../../../shared/src/valita.ts';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {childWorker, type Worker} from '../../../types/workers.ts';
import type {
  BufferMessage,
  StartCopyStreamMessage,
  StreamChunk,
} from './copy-worker.ts';
import type {
  StartCopyMessage,
  TokenMessage,
  ValuesMessage,
} from './parse-worker.ts';

export const pipelineInitSchema = v.object({
  log: v.object({
    level: v.literalUnion('debug', 'info', 'warn', 'error').default('info'),
    format: v.literalUnion('text', 'json').default('text'),
  }),
  db: v.string(),
  numBuffers: v.number(),
  bufferSize: v.number(),
});

export type PipelineInit = v.Infer<typeof pipelineInitSchema>;

export async function startCopyWorkerPipeline(
  lc: LogContext,
  workerData: PipelineInit,
): Promise<Worker> {
  const start = performance.now();
  const worker = childWorker(
    lc,
    './services/change-source/pg/parse-worker.ts',
    workerData,
  );

  const {promise, resolve, reject} = resolver();
  worker.on('online', () => {
    for (let i = 0; i < workerData.numBuffers; i++) {
      worker.postMessage({type: 'token'} satisfies TokenMessage);
    }
    resolve();
  });
  worker.on('exit', () => lc.debug?.('parse worker exited'));
  worker.on('error', reject);

  lc.info?.(
    `waiting for worker to come online`,
    (worker as NodeWorker).threadId,
  );
  await promise;
  lc.info?.(
    `copy worker pipeline started (${(performance.now() - start).toFixed(3)} ms)`,
  );
  return worker;
}

export function startCopy(
  _lc: LogContext,
  worker: Worker,
  snapshotID: string,
  table: PublishedTableSpec,
  initialVersion: string,
): Readable {
  let outstandingTokens = 0;

  function returnToken() {
    if (outstandingTokens) {
      worker.postMessage({type: 'token'} satisfies TokenMessage);
      outstandingTokens--;
    }
  }

  const readable = new Readable({
    objectMode: true,
    read: returnToken,
  });
  const {port1, port2} = new MessageChannel();

  port1.on('message', ({values}: ValuesMessage) => {
    outstandingTokens++;
    // The worker awaits a token to send the last `values: null` message
    // for proper ordering, but the Readable will not call read() after
    // the stream is closed, so the token is immediately returned if
    // values === null.
    if (readable.push(values) || values === null) {
      returnToken();
    }
  });

  worker.postMessage(
    {
      type: 'copy',
      snapshotID,
      table,
      initialVersion,
      port: port2,
    } satisfies StartCopyMessage,
    [port2],
  );

  return readable;
}

export const copyStreamerInitSchema = v.object({
  log: v.object({
    level: v.literalUnion('debug', 'info', 'warn', 'error').default('info'),
    format: v.literalUnion('text', 'json').default('text'),
  }),
  db: v.string(),
});

export type CopyStreamerInit = v.Infer<typeof copyStreamerInitSchema>;

export async function startCopyStreamer(
  lc: LogContext,
  workerData: CopyStreamerInit,
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

export function startCopyStream(
  lc: LogContext,
  worker: Worker,
  snapshotID: string,
  selection: string,
  handle: (chunk: Buffer | null) => Promise<void>,
) {
  const exitOnError = (err: unknown) => {
    lc.error?.('error processing copy stream', err);
    process.exit(-1);
  };

  const {port1, port2} = new MessageChannel();

  port1.on('message', ({array, length, last}: StreamChunk) => {
    if (array) {
      handle(Buffer.from(array.buffer, 0, length)).then(
        () =>
          // Return the buffer
          worker.postMessage({type: 'buffer', array} satisfies BufferMessage, [
            array.buffer,
          ]),
        exitOnError,
      );
    }
    if (last) {
      handle(null).catch(exitOnError);
    }
  });

  worker.postMessage(
    {
      type: 'copy',
      snapshotID,
      selection,
      port: port2,
    } satisfies StartCopyStreamMessage,
    [port2],
  );
}
