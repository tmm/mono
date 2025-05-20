import {resolver} from '@rocicorp/resolver';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {assert} from '../../../../../shared/src/asserts.ts';
import {must} from '../../../../../shared/src/must.ts';
import {getZeroConfig} from '../../../config/zero-config.ts';
import {createLogContext} from '../../../server/logging.ts';
import {pgClient} from '../../../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../../../types/processes.ts';
import {exitAfter, runUntilKilled} from '../../life-cycle.ts';
import type {Service} from '../../service.ts';
import {CopyRunner} from './copy-runner.ts';

export type CopyMessage = ['copy', {table: string; query: string}];

export type RowsMessage = ['rows', {table: string; buffers: Buffer[][]}];

export type CopyDoneMessage = ['copyDone', {table: string}];

const BUFFERED_SIZE_THRESHOLD = 2 * 1024 * 1024;

export default function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `snapshot not specified`);
  const snapshot = args[0];

  const config = getZeroConfig(env, args.slice(1, 5));
  const {
    upstream,
    initialSync: {tableCopyWorkers},
  } = config;
  const lc = createLogContext(config, {worker: 'copy-streamer'});
  const {promise: done, resolve: stop, reject} = resolver();

  const copyRunner = new CopyRunner(
    lc,
    () =>
      pgClient(lc, upstream.db, {
        // No need to fetch array types for these connections, as pgClient
        // streams the COPY data as text, and type parsing is done in the
        // the RowTransform, which gets its types from the typeClient.
        // This eliminates one round trip when each db
        // connection is established.
        ['fetch_types']: false,
        connection: {['application_name']: 'initial-sync-copy-worker'},
      }),
    tableCopyWorkers,
    snapshot,
  );

  parent.onMessageType('stop', () => {
    copyRunner.close();
    stop();
  });

  parent.onMessageType<CopyMessage>('copy', ({table, query}) => {
    void copyRunner
      .run(async (tx, lc) => {
        lc.info?.(`streaming COPY: ${query}`);
        const buffers: Buffer[] = [];
        let bytes = 0;
        const stream = await tx.unsafe(query).readable();
        await pipeline(
          stream,
          new Writable({
            writev: (chunks: {chunk: Buffer}[], callback: () => void) => {
              for (const {chunk} of chunks) {
                buffers.push(chunk);
                bytes += chunk.length;
              }
              if (bytes < BUFFERED_SIZE_THRESHOLD) {
                callback();
                return;
              }
              parent.send(['rows', {table, buffers}], undefined, callback);
              buffers.length = 0;
              bytes = 0;
            },

            final: (callback: () => void) => {
              if (buffers.length) {
                parent.send(['rows', {table, buffers}]);
              }
              lc.info?.(`done streaming COPY: ${query}`);
              parent.send(['copyDone', {table}], undefined, callback);
            },
          }),
        );
      })
      .catch(e => {
        lc.error?.(e);
        reject(e);
      });
  });

  parent.send(['ready', {ready: true}]);

  const dummyService: Service = {
    id: 'copy-streamer',
    run: () => done,
    stop: () => {
      stop();
      return done;
    },
  };
  return runUntilKilled(lc, parent, dummyService);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
