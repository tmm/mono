import EventEmitter from 'node:events';
import path from 'node:path';
import {MessageChannel, Worker as NodeWorker} from 'node:worker_threads';
import {singleProcessMode} from './processes.ts';

export type Worker = EventEmitter & Pick<NodeWorker, 'postMessage'>;

/**
 *
 * @param modulePath Path to the module file, relative to zero-cache/src/
 */
export function childWorker(modulePath: string, workerData: unknown): Worker {
  const ext = path.extname(import.meta.url);
  // modulePath is .ts. If we have been compiled, it should be changed to .js
  modulePath = modulePath.replace(/\.ts$/, ext);
  const moduleUrl = new URL(`../${modulePath}`, import.meta.url);

  if (singleProcessMode()) {
    const {port1: parentPort, port2: childPort} = new MessageChannel();
    const worker = new EventEmitter();

    import(moduleUrl.href)
      .then(async ({default: runWorker}) => {
        try {
          worker.emit('online');
          await runWorker(workerData, parentPort);
          worker.emit('exit', 0);
          return;
        } catch (err) {
          worker.emit('error', err);
          worker.emit('exit', -1);
        }
      })
      .catch(err => worker.emit('error', err));

    childPort.on('message', msg => worker.emit('message', msg));
    childPort.on('messageerror', err => worker.emit('messageerror', err));

    return Object.assign(worker, {
      postMessage: (message: unknown) => childPort.postMessage(message),
    });
  }
  return new NodeWorker(moduleUrl, {workerData});
}
