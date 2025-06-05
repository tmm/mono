import type {LogContext} from '@rocicorp/logger';
import type {ShardConfig} from '../../../types/shards.ts';
import {initReplicaAsync} from '../replica-schema.ts';
import {initialSync, type InitialSyncOptions} from './initial-sync.ts';

export async function initSyncSchema(
  log: LogContext,
  debugName: string,
  shard: ShardConfig,
  dbPath: string,
  upstreamURI: string,
  syncOptions: InitialSyncOptions,
): Promise<void> {
  await initReplicaAsync(log, debugName, dbPath, (log, tx) =>
    initialSync(log, shard, tx, upstreamURI, syncOptions, dbPath),
  );
}
