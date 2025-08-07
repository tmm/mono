import {trace} from '@opentelemetry/api';
import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {JWTPayload} from 'jose';
import type {Row} from 'postgres';
import {
  manualSpan,
  startAsyncSpan,
  startSpan,
} from '../../../../otel/src/span.ts';
import {version} from '../../../../otel/src/version.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {CustomKeyMap} from '../../../../shared/src/custom-key-map.ts';
import {must} from '../../../../shared/src/must.ts';
import {randInt} from '../../../../shared/src/rand.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {ChangeDesiredQueriesMessage} from '../../../../zero-protocol/src/change-desired-queries.ts';
import type {
  InitConnectionBody,
  InitConnectionMessage,
} from '../../../../zero-protocol/src/connect.ts';
import type {DeleteClientsMessage} from '../../../../zero-protocol/src/delete-clients.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import type {
  InspectUpBody,
  InspectUpMessage,
} from '../../../../zero-protocol/src/inspect-up.ts';
import {clampTTL, MAX_TTL_MS} from '../../../../zql/src/query/ttl.ts';
import {
  transformAndHashQuery,
  type TransformedAndHashed,
} from '../../auth/read-authorizer.ts';
import {type ZeroConfig} from '../../config/zero-config.ts';
import {CustomQueryTransformer} from '../../custom-queries/transform-query.ts';
import {
  getOrCreateCounter,
  getOrCreateHistogram,
  getOrCreateUpDownCounter,
} from '../../observability/metrics.ts';
import {ErrorForClient, getLogLevel} from '../../types/error-for-client.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {rowIDString, type RowKey} from '../../types/row-key.ts';
import type {ShardID} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/replication-state.ts';
import type {ActivityBasedService} from '../service.ts';
import {
  ClientHandler,
  startPoke,
  type PatchToVersion,
  type PokeHandler,
  type RowPatch,
} from './client-handler.ts';
import {CVRStore} from './cvr-store.ts';
import {
  CVRConfigDrivenUpdater,
  CVRQueryDrivenUpdater,
  CVRUpdater,
  nextEvictionTime,
  type CVRSnapshot,
  type RowUpdate,
} from './cvr.ts';
import type {DrainCoordinator} from './drain-coordinator.ts';
import {PipelineDriver, type RowChange} from './pipeline-driver.ts';
import {
  cmpVersions,
  EMPTY_CVR_VERSION,
  versionFromString,
  versionString,
  versionToCookie,
  type ClientQueryRecord,
  type CustomQueryRecord,
  type CVRVersion,
  type InternalQueryRecord,
  type NullableCVRVersion,
  type QueryRecord,
  type RowID,
} from './schema/types.ts';
import {ResetPipelinesSignal} from './snapshotter.ts';
import {
  ttlClockAsNumber,
  ttlClockFromNumber,
  type TTLClock,
} from './ttl-clock.ts';

export type TokenData = {
  readonly raw: string;
  readonly decoded: JWTPayload;
};

export type SyncContext = {
  readonly clientID: string;
  readonly wsID: string;
  readonly baseCookie: string | null;
  readonly protocolVersion: number;
  readonly schemaVersion: number | null;
  readonly tokenData: TokenData | undefined;
  readonly httpCookie: string | undefined;
};

const tracer = trace.getTracer('view-syncer', version);

const PROTOCOL_VERSION_ATTR = 'protocol.version';

export interface ViewSyncer {
  initConnection(
    ctx: SyncContext,
    msg: InitConnectionMessage,
  ): Source<Downstream>;

  changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void>;

  deleteClients(ctx: SyncContext, msg: DeleteClientsMessage): Promise<void>;
  inspect(context: SyncContext, msg: InspectUpMessage): Promise<void>;
}

const DEFAULT_KEEPALIVE_MS = 5_000;

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

type SetTimeout = (
  fn: (...args: unknown[]) => void,
  delay?: number,
) => ReturnType<typeof setTimeout>;

/**
 * We update the ttlClock in flush that writes to the CVR but
 * some flushes do not write to the CVR and in those cases we
 * use a timer to update the ttlClock every minute.
 */
export const TTL_CLOCK_INTERVAL = 60_000;

/**
 * This is some extra time we delay the TTL timer to allow for some
 * slack in the timing of the timer. This is to allow multiple evictions
 * to happen in a short period of time without having to wait for the
 * next tick of the timer.
 */
export const TTL_TIMER_HYSTERESIS = 50; // ms

export class ViewSyncerService implements ViewSyncer, ActivityBasedService {
  readonly id: string;
  readonly #shard: ShardID;
  readonly #lc: LogContext;
  readonly #pipelines: PipelineDriver;
  readonly #stateChanges: Subscription<ReplicaState>;
  readonly #drainCoordinator: DrainCoordinator;
  readonly #keepaliveMs: number;
  readonly #slowHydrateThreshold: number;
  readonly #queryConfig: ZeroConfig['query'];

  // The ViewSyncerService is only started in response to a connection,
  // so #lastConnectTime is always initialized to now(). This is necessary
  // to handle race conditions in which, e.g. the replica is ready and the
  // CVR is accessed before the first connection sends a request.
  //
  // Note: It is fine to update this variable outside of the lock.
  #lastConnectTime = Date.now();

  /**
   * The TTL clock is used to determine the time at which queries are considered
   * expired.
   */
  #ttlClock: TTLClock | undefined;

  /**
   * The base time for the TTL clock. This is used to compute the current TTL
   * clock value. The first time a connection is made, this is set to the
   * current time. On subsequent connections, the TTL clock is computed as the
   * difference between the current time and this base time.
   *
   * Every time we write the ttlClock this is update to the current time. That
   * way we can compute how much time has passed since the last time we set the
   * ttlClock. When we set the ttlClock we just increment it by the amount of
   * time that has passed since the last time we set it.
   */
  #ttlClockBase = Date.now();

  /**
   * We update the ttlClock every minute to ensure that it is not too much
   * out of sync with the current time.
   */
  #ttlClockInterval: ReturnType<SetTimeout> | 0 = 0;

  // Note: It is okay to add/remove clients without acquiring the lock.
  readonly #clients = new Map<string, ClientHandler>();

  // Serialize on this lock for:
  // (1) storage or database-dependent operations
  // (2) updating member variables.
  readonly #lock = new Lock();
  readonly #cvrStore: CVRStore;
  readonly #stopped = resolver();

  #cvr: CVRSnapshot | undefined;
  #pipelinesSynced = false;
  // DEPRECATED: remove `authData` in favor of forwarding
  // auth and cookie headers directly
  #authData: TokenData | undefined;
  #httpCookie: string | undefined;

  #expiredQueriesTimer: ReturnType<SetTimeout> | 0 = 0;
  readonly #setTimeout: SetTimeout;
  readonly #customQueryTransformer: CustomQueryTransformer | undefined;

  readonly #activeClients = getOrCreateUpDownCounter(
    'sync',
    'active-clients',
    'Number of active sync clients',
  );
  readonly #hydrations = getOrCreateCounter(
    'sync',
    'hydration',
    'Number of query hydrations',
  );
  readonly #hydrationTime = getOrCreateHistogram('sync', 'hydration-time', {
    description: 'Time to hydrate a query.',
    unit: 's',
  });
  readonly #transactionAdvanceTime = getOrCreateHistogram(
    'sync',
    'advance-time',
    {
      description:
        'Time to advance all queries for a given client group after applying a new transaction to the replica.',
      unit: 's',
    },
  );

  constructor(
    pullConfig: ZeroConfig['query'],
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    clientGroupID: string,
    cvrDb: PostgresDB,
    upstreamDb: PostgresDB | undefined,
    pipelineDriver: PipelineDriver,
    versionChanges: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
    slowHydrateThreshold: number,
    keepaliveMs = DEFAULT_KEEPALIVE_MS,
    setTimeoutFn: SetTimeout = setTimeout.bind(globalThis),
  ) {
    this.id = clientGroupID;
    this.#shard = shard;
    this.#queryConfig = pullConfig;
    this.#lc = lc;
    this.#pipelines = pipelineDriver;
    this.#stateChanges = versionChanges;
    this.#drainCoordinator = drainCoordinator;
    this.#keepaliveMs = keepaliveMs;
    this.#slowHydrateThreshold = slowHydrateThreshold;
    this.#cvrStore = new CVRStore(
      lc,
      cvrDb,
      upstreamDb,
      shard,
      taskID,
      clientGroupID,
      // On failure, cancel the #stateChanges subscription. The run()
      // loop will then await #cvrStore.flushed() which rejects if necessary.
      () => this.#stateChanges.cancel(),
    );
    this.#setTimeout = setTimeoutFn;

    if (pullConfig.url) {
      this.#customQueryTransformer = new CustomQueryTransformer(
        {
          url: pullConfig.url,
          forwardCookies: pullConfig.forwardCookies,
        },
        shard,
      );
    }

    // Wait for the first connection to init.
    this.keepalive();
  }

  #runInLockWithCVR(
    fn: (lc: LogContext, cvr: CVRSnapshot) => Promise<void> | void,
  ): Promise<void> {
    const rid = randomID();
    this.#lc.debug?.('about to acquire lock for cvr ', rid);
    return this.#lock.withLock(async () => {
      this.#lc.debug?.('acquired lock in #runInLockWithCVR ', rid);
      const lc = this.#lc.withContext('lock', rid);
      if (!this.#stateChanges.active) {
        this.#lc.debug?.('state changes are inactive');
        clearTimeout(this.#expiredQueriesTimer);
        return; // view-syncer has been shutdown
      }
      // If all clients have disconnected, cancel all pending work.
      if (await this.#checkForShutdownConditionsInLock()) {
        this.#lc.info?.(`closing clientGroupID=${this.id}`);
        this.#stateChanges.cancel(); // Note: #stateChanges.active becomes false.
        return;
      }
      if (!this.#cvr) {
        this.#lc.debug?.('loading CVR');
        this.#cvr = await this.#cvrStore.load(lc, this.#lastConnectTime);
        this.#ttlClock = this.#cvr.ttlClock;
        this.#ttlClockBase = Date.now();
      } else {
        // Make sure the CVR ttlClock is up to date.
        const now = Date.now();
        this.#cvr = {
          ...this.#cvr,
          ttlClock: this.#getTTLClock(now),
        };
      }

      try {
        await fn(lc, this.#cvr);
      } catch (e) {
        // Clear cached state if an error is encountered.
        this.#cvr = undefined;
        throw e;
      }
    });
  }

  async run(): Promise<void> {
    try {
      for await (const {state} of this.#stateChanges) {
        if (this.#drainCoordinator.shouldDrain()) {
          this.#lc.debug?.(`draining view-syncer ${this.id} (elective)`);
          break;
        }
        assert(state === 'version-ready', 'state should be version-ready'); // This is the only state change used.

        await this.#runInLockWithCVR(async (lc, cvr) => {
          if (!this.#pipelines.initialized()) {
            // On the first version-ready signal, connect to the replica.
            this.#pipelines.init(cvr.clientSchema);
          }
          if (
            cvr.replicaVersion !== null &&
            cvr.version.stateVersion !== '00' &&
            this.#pipelines.replicaVersion < cvr.replicaVersion
          ) {
            const message = `Cannot sync from older replica: CVR=${
              cvr.replicaVersion
            }, DB=${this.#pipelines.replicaVersion}`;
            lc.info?.(`resetting CVR: ${message}`);
            throw new ErrorForClient({kind: ErrorKind.ClientNotFound, message});
          }

          if (this.#pipelinesSynced) {
            const result = await this.#advancePipelines(lc, cvr);
            if (result === 'success') {
              return;
            }
            lc.info?.(`resetting pipelines: ${result.message}`);
            this.#pipelines.reset(cvr.clientSchema);
          }

          // Advance the snapshot to the current version.
          const version = this.#pipelines.advanceWithoutDiff();
          const cvrVer = versionString(cvr.version);

          if (version < cvr.version.stateVersion) {
            lc.debug?.(`replica@${version} is behind cvr@${cvrVer}`);
            return; // Wait for the next advancement.
          }

          // stateVersion is at or beyond CVR version for the first time.
          lc.info?.(`init pipelines@${version} (cvr@${cvrVer})`);
          await this.#hydrateUnchangedQueries(lc, cvr);
          await this.#syncQueryPipelineSet(lc, cvr);
          this.#pipelinesSynced = true;
        });
      }

      // If this view-syncer exited due to an elective or forced drain,
      // set the next drain timeout.
      if (this.#drainCoordinator.shouldDrain()) {
        this.#drainCoordinator.drainNextIn(this.#totalHydrationTimeMs());
      }
      this.#cleanup();
    } catch (e) {
      this.#lc[getLogLevel(e)]?.(`stopping view-syncer: ${String(e)}`, e);
      this.#cleanup(e);
    } finally {
      // Always wait for the cvrStore to flush, regardless of how the service
      // was stopped.
      await this.#cvrStore
        .flushed(this.#lc)
        .catch(e => this.#lc[getLogLevel(e)]?.(e));
      this.#lc.info?.('view-syncer stopped');
      this.#stopped.resolve();
    }
  }

  // must be called from within #lock
  #removeExpiredQueries = async (
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<void> => {
    if (hasExpiredQueries(cvr)) {
      lc = lc.withContext('method', '#removeExpiredQueries');
      lc.debug?.('Queries have expired');
      // #syncQueryPipelineSet() will remove the expired queries.
      await this.#syncQueryPipelineSet(lc, cvr);
      this.#pipelinesSynced = true;
    }

    // Even if we have expired queries, we still need to schedule next eviction
    // since there might be inactivated queries that need to be expired queries
    // in the future.
    this.#scheduleExpireEviction(lc, cvr);
  };

  #totalHydrationTimeMs(): number {
    return this.#pipelines.totalHydrationTimeMs();
  }

  #keepAliveUntil: number = 0;

  /**
   * Guarantees that the ViewSyncer will remain running for at least
   * its configured `keepaliveMs`. This is called when establishing a
   * new connection to ensure that its associated ViewSyncer isn't
   * shutdown before it receives the connection.
   *
   * @return `true` if the ViewSyncer will stay alive, `false` if the
   *         ViewSyncer is shutting down.
   */
  keepalive(): boolean {
    if (!this.#stateChanges.active) {
      return false;
    }
    this.#keepAliveUntil = Date.now() + this.#keepaliveMs;
    return true;
  }

  #shutdownTimer: NodeJS.Timeout | null = null;

  #scheduleShutdown(delayMs = 0) {
    this.#shutdownTimer ??= this.#setTimeout(() => {
      this.#shutdownTimer = null;

      // All lock tasks check for shutdown so that queued work is immediately
      // canceled when clients disconnect. Queue an empty task to ensure that
      // this check happens.
      void this.#runInLockWithCVR(() => {}).catch(e =>
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e),
      );
    }, delayMs);
  }

  async #checkForShutdownConditionsInLock(): Promise<boolean> {
    if (this.#clients.size > 0) {
      return false; // common case.
    }

    // Keep the view-syncer alive if there are pending rows being flushed.
    // It's better to do this before shutting down since it may take a
    // while, during which new connections may come in.
    await this.#cvrStore.flushed(this.#lc);

    if (Date.now() <= this.#keepAliveUntil) {
      this.#scheduleShutdown(this.#keepaliveMs); // check again later
      return false;
    }

    // If no clients have connected while waiting for the row flush, shutdown.
    return this.#clients.size === 0;
  }

  #deleteClientDueToDisconnect(clientID: string, client: ClientHandler) {
    // Note: It is okay to delete / cleanup clients without acquiring the lock.
    // In fact, it is important to do so in order to guarantee that idle cleanup
    // is performed in a timely manner, regardless of the amount of work
    // queued on the lock.
    const c = this.#clients.get(clientID);
    if (c === client) {
      this.#clients.delete(clientID);

      if (this.#clients.size === 0) {
        // It is possible to delete a client before we read the ttl clock from
        // the CVR.
        if (this.#ttlClock !== undefined) {
          this.#updateTTLClockInCVRWithoutLock(this.#lc);
        }
        this.#stopExpireTimer();
        this.#scheduleShutdown();
      }
    }
  }

  #stopExpireTimer() {
    this.#lc.debug?.('Stopping expired queries timer');
    clearTimeout(this.#expiredQueriesTimer);
    this.#expiredQueriesTimer = 0;
  }

  initConnection(
    ctx: SyncContext,
    initConnectionMessage: InitConnectionMessage,
  ): Source<Downstream> {
    this.#lc.debug?.('viewSyncer.initConnection');
    return startSpan(tracer, 'vs.initConnection', () => {
      const {
        clientID,
        wsID,
        baseCookie,
        schemaVersion,
        tokenData,
        httpCookie,
        protocolVersion,
      } = ctx;
      this.#authData = pickToken(this.#lc, this.#authData, tokenData);
      this.#lc.debug?.(
        `Picked auth token: ${JSON.stringify(this.#authData?.decoded)}`,
      );
      this.#httpCookie = httpCookie;

      const lc = this.#lc
        .withContext('clientID', clientID)
        .withContext('wsID', wsID);

      // Setup the downstream connection.
      const downstream = Subscription.create<Downstream>({
        cleanup: (_, err) => {
          err
            ? lc[getLogLevel(err)]?.(`client closed with error`, err)
            : lc.info?.('client closed');
          this.#deleteClientDueToDisconnect(clientID, newClient);
          this.#activeClients.add(-1, {
            [PROTOCOL_VERSION_ATTR]: protocolVersion,
          });
        },
      });
      this.#activeClients.add(1, {
        [PROTOCOL_VERSION_ATTR]: protocolVersion,
      });

      if (this.#clients.size === 0) {
        // First connection to this ViewSyncerService.

        // initConnection must be synchronous so that the downstream
        // subscription is returned immediately.
        const now = Date.now();
        this.#ttlClockBase = now;
      }

      const newClient = new ClientHandler(
        lc,
        this.id,
        clientID,
        wsID,
        this.#shard,
        baseCookie,
        schemaVersion,
        downstream,
      );
      this.#clients.get(clientID)?.close(`replaced by wsID: ${wsID}`);
      this.#clients.set(clientID, newClient);

      // Note: initConnection() must be synchronous so that `downstream` is
      // immediately returned to the caller (connection.ts). This ensures
      // that if the connection is subsequently closed, the `downstream`
      // subscription can be properly canceled even if #runInLockForClient()
      // has not had a chance to run.
      void this.#runInLockForClient(
        ctx,
        initConnectionMessage,
        this.#handleConfigUpdate,
        newClient,
      ).catch(e => newClient.fail(e));

      return downstream;
    });
  }

  async changeDesiredQueries(
    ctx: SyncContext,
    msg: ChangeDesiredQueriesMessage,
  ): Promise<void> {
    await this.#runInLockForClient(ctx, msg, this.#handleConfigUpdate);
  }

  async deleteClients(
    ctx: SyncContext,
    msg: DeleteClientsMessage,
  ): Promise<void> {
    try {
      await this.#runInLockForClient(
        ctx,
        [msg[0], {deleted: msg[1]}],
        this.#handleConfigUpdate,
      );
    } catch (e) {
      this.#lc.error?.('deleteClients failed', e);
    }
  }

  #getTTLClock(now: number): TTLClock {
    // We will update ttlClock with delta from the ttlClockBase to the current time.
    const delta = now - this.#ttlClockBase;
    assert(this.#ttlClock !== undefined, 'ttlClock should be defined');
    const ttlClock = ttlClockFromNumber(
      ttlClockAsNumber(this.#ttlClock) + delta,
    );
    assert(
      ttlClockAsNumber(ttlClock) <= now,
      'ttlClock should be less than or equal to now',
    );
    this.#ttlClock = ttlClock;
    this.#ttlClockBase = now;
    return ttlClock as TTLClock;
  }

  async #flushUpdater(
    lc: LogContext,
    updater: CVRUpdater,
  ): Promise<CVRSnapshot> {
    const now = Date.now();
    const ttlClock = this.#getTTLClock(now);
    const {cvr, flushed} = await updater.flush(
      lc,
      this.#lastConnectTime,
      now,
      ttlClock,
    );

    if (flushed) {
      // If the CVR was flushed, we restart the ttlClock interval.
      this.#startTTLClockInterval(lc);
    }

    return cvr;
  }

  #startTTLClockInterval(lc: LogContext): void {
    this.#stopTTLClockInterval();
    this.#ttlClockInterval = this.#setTimeout(() => {
      this.#updateTTLClockInCVRWithoutLock(lc);
      this.#startTTLClockInterval(lc);
    }, TTL_CLOCK_INTERVAL);
  }

  #stopTTLClockInterval(): void {
    clearTimeout(this.#ttlClockInterval);
    this.#ttlClockInterval = 0;
  }

  #updateTTLClockInCVRWithoutLock(lc: LogContext): void {
    lc.debug?.('Syncing ttlClock');
    const now = Date.now();
    const ttlClock = this.#getTTLClock(now);
    this.#cvrStore.updateTTLClock(ttlClock, now).catch(e => {
      lc.error?.('failed to update TTL clock', e);
    });
  }

  async #updateCVRConfig(
    lc: LogContext,
    cvr: CVRSnapshot,
    clientID: string,
    fn: (updater: CVRConfigDrivenUpdater) => PatchToVersion[],
  ): Promise<CVRSnapshot> {
    const updater = new CVRConfigDrivenUpdater(
      this.#cvrStore,
      cvr,
      this.#shard,
    );
    updater.ensureClient(clientID);
    const patches = fn(updater);

    this.#cvr = await this.#flushUpdater(lc, updater);

    if (cmpVersions(cvr.version, this.#cvr.version) < 0) {
      // Send pokes to catch up clients that are up to date.
      // (Clients that are behind the cvr.version need to be caught up in
      //  #syncQueryPipelineSet(), as row data may be needed for catchup)
      const newCVR = this.#cvr;
      const pokers = startPoke(this.#getClients(cvr.version), newCVR.version);
      for (const patch of patches) {
        await pokers.addPatch(patch);
      }
      await pokers.end(newCVR.version);
    }

    if (this.#pipelinesSynced) {
      await this.#syncQueryPipelineSet(lc, this.#cvr);
    }

    return this.#cvr;
  }

  /**
   * Runs the given `fn` to process the `msg` from within the `#lock`,
   * optionally adding the `newClient` if supplied.
   */
  #runInLockForClient<B, M extends [cmd: string, B] = [string, B]>(
    ctx: SyncContext,
    msg: M,
    fn: (
      lc: LogContext,
      clientID: string,
      body: B,
      cvr: CVRSnapshot,
    ) => Promise<void>,
    newClient?: ClientHandler,
  ): Promise<void> {
    this.#lc.debug?.('viewSyncer.#runInLockForClient');
    const {clientID, wsID} = ctx;
    const [cmd, body] = msg;

    if (newClient || !this.#clients.has(clientID)) {
      this.#lastConnectTime = Date.now();
    }

    return startAsyncSpan(
      tracer,
      `vs.#runInLockForClient(${cmd})`,
      async () => {
        let client: ClientHandler | undefined;
        try {
          await this.#runInLockWithCVR((lc, cvr) => {
            lc = lc
              .withContext('clientID', clientID)
              .withContext('wsID', wsID)
              .withContext('cmd', cmd);
            lc.debug?.('acquired lock for cvr');

            client = this.#clients.get(clientID);
            if (client?.wsID !== wsID) {
              lc.debug?.('mismatched wsID', client?.wsID, wsID);
              // Only respond to messages of the currently connected client.
              // Connections may have been drained or dropped due to an error.
              return;
            }

            if (newClient) {
              assert(
                newClient === client,
                'newClient must match existing client',
              );
              checkClientAndCVRVersions(client.version(), cvr.version);
            } else if (!this.#clients.has(clientID)) {
              lc.warn?.(`Processing ${cmd} before initConnection was received`);
            }

            lc.debug?.(cmd, body);
            return fn(lc, clientID, body, cvr);
          });
        } catch (e) {
          const lc = this.#lc
            .withContext('clientID', clientID)
            .withContext('wsID', wsID)
            .withContext('cmd', cmd);
          lc[getLogLevel(e)]?.(`closing connection with error`, e);
          if (client) {
            // Ideally, propagate the exception to the client's downstream subscription ...
            client.fail(e);
          } else {
            // unless the exception happened before the client could be looked up.
            throw e;
          }
        }
      },
    );
  }

  #getClients(atVersion?: CVRVersion): ClientHandler[] {
    const clients = [...this.#clients.values()];
    return atVersion
      ? clients.filter(
          c => cmpVersions(c.version() ?? EMPTY_CVR_VERSION, atVersion) === 0,
        )
      : clients;
  }

  // Must be called from within #lock.
  readonly #handleConfigUpdate = (
    lc: LogContext,
    clientID: string,

    {
      clientSchema,
      deleted,
      desiredQueriesPatch,
      activeClients,
    }: Partial<InitConnectionBody>,
    cvr: CVRSnapshot,
  ) =>
    startAsyncSpan(tracer, 'vs.#patchQueries', async () => {
      const deletedClientIDs: string[] = [];
      const deletedClientGroupIDs: string[] = [];

      cvr = await this.#updateCVRConfig(lc, cvr, clientID, updater => {
        const {ttlClock} = cvr;
        const patches: PatchToVersion[] = [];

        if (clientSchema) {
          updater.setClientSchema(lc, clientSchema);
        }

        // Apply requested patches.
        lc.debug?.(`applying ${desiredQueriesPatch?.length} query patches`);
        if (desiredQueriesPatch?.length) {
          for (const patch of desiredQueriesPatch) {
            switch (patch.op) {
              case 'put':
                patches.push(...updater.putDesiredQueries(clientID, [patch]));
                break;
              case 'del':
                patches.push(
                  ...updater.markDesiredQueriesAsInactive(
                    clientID,
                    [patch.hash],
                    ttlClock,
                  ),
                );
                break;
              case 'clear':
                patches.push(...updater.clearDesiredQueries(clientID));
                break;
            }
          }
        }

        const clientIDsToDelete: Set<string> = new Set();

        if (activeClients) {
          // We find all the clients in this client group that are not active.
          const allClientIDs = Object.keys(cvr.clients);
          const activeClientsSet = new Set(activeClients);
          for (const id of allClientIDs) {
            if (!activeClientsSet.has(id)) {
              clientIDsToDelete.add(id);
            }
          }
        }

        if (deleted?.clientIDs?.length) {
          for (const cid of deleted.clientIDs) {
            assert(cid !== clientID, 'cannot delete self');
            clientIDsToDelete.add(cid);
          }
        }

        for (const cid of clientIDsToDelete) {
          const patchesDueToClient = updater.deleteClient(cid, ttlClock);
          patches.push(...patchesDueToClient);
          deletedClientIDs.push(cid);
        }

        if (deleted?.clientGroupIDs?.length) {
          if (deleted?.clientGroupIDs) {
            for (const clientGroupID of deleted.clientGroupIDs) {
              assert(clientGroupID !== this.id, 'cannot delete self');
              updater.deleteClientGroup(clientGroupID);
            }
          }
        }

        return patches;
      });

      // Send 'deleteClients' ack to the clients.
      if (
        (deletedClientIDs.length && deleted?.clientIDs?.length) ||
        deletedClientGroupIDs.length
      ) {
        const clients = this.#getClients();
        await Promise.allSettled(
          clients.map(client =>
            client.sendDeleteClients(
              lc,
              deletedClientIDs,
              deletedClientGroupIDs,
            ),
          ),
        );
      }

      this.#scheduleExpireEviction(lc, cvr);
    });

  #scheduleExpireEviction(lc: LogContext, cvr: CVRSnapshot): void {
    const {ttlClock} = cvr;
    this.#stopExpireTimer();

    // first see if there is any inactive query with a ttl.
    const next = nextEvictionTime(cvr);

    if (next === undefined) {
      lc.debug?.('no inactive queries with ttl');
      // no inactive queries with a ttl. Cancel existing timeout if any.
      return;
    }

    // It is common for many queries to be evicted close to the same time, so
    // we add a small delay so we can collapse multiple evictions into a
    // single timer. However, don't add the delay if we're already at the
    // maximum timer limit, as that's not about collapsing.
    const delay = Math.max(
      TTL_TIMER_HYSTERESIS,
      Math.min(
        ttlClockAsNumber(next) -
          ttlClockAsNumber(ttlClock) +
          TTL_TIMER_HYSTERESIS,
        MAX_TTL_MS,
      ),
    );

    lc.debug?.('Scheduling eviction timer to run in ', delay, 'ms');
    this.#expiredQueriesTimer = this.#setTimeout(() => {
      this.#expiredQueriesTimer = 0;
      this.#runInLockWithCVR((lc, cvr) =>
        this.#removeExpiredQueries(lc, cvr),
      ).catch(e =>
        // If an error occurs (e.g. ownership change), propagate the error
        // to the main run() loop via the #stateChanges Subscription.
        this.#stateChanges.fail(e),
      );
    }, delay);
  }

  /**
   * Adds and hydrates pipelines for queries whose results are already
   * recorded in the CVR. Namely:
   *
   * 1. The CVR state version and database version are the same.
   * 2. The transformation hash of the queries equal those in the CVR.
   *
   * Note that by definition, only "got" queries can satisfy condition (2),
   * as desired queries do not have a transformation hash.
   *
   * This is an initialization step that sets up pipeline state without
   * the expensive of loading and diffing CVR row state.
   *
   * This must be called from within the #lock.
   */
  async #hydrateUnchangedQueries(lc: LogContext, cvr: CVRSnapshot) {
    assert(this.#pipelines.initialized(), 'pipelines must be initialized');

    const dbVersion = this.#pipelines.currentVersion();
    const cvrVersion = cvr.version;

    if (cvrVersion.stateVersion !== dbVersion) {
      lc.info?.(
        `CVR (${versionToCookie(cvrVersion)}) is behind db ${dbVersion}`,
      );
      return; // hydration needs to be run with the CVR updater.
    }

    const gotQueries = Object.entries(cvr.queries).filter(
      ([_, state]) => state.transformationHash !== undefined,
    );

    const customQueries: CustomQueryRecord[] = [];
    const otherQueries: (ClientQueryRecord | InternalQueryRecord)[] = [];

    for (const [, query] of gotQueries) {
      if (
        query.type !== 'internal' &&
        Object.values(query.clientState).every(
          ({inactivatedAt}) => inactivatedAt !== undefined,
        )
      ) {
        continue; // No longer desired.
      }

      if (query.type === 'custom') {
        customQueries.push(query);
      } else {
        otherQueries.push(query);
      }
    }

    const transformedQueries: TransformedAndHashed[] = [];
    if (customQueries.length > 0 && !this.#customQueryTransformer) {
      lc.error?.(
        'Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache.',
      );
    }
    if (this.#customQueryTransformer && customQueries.length > 0) {
      const transformedCustomQueries =
        await this.#customQueryTransformer.transform(
          {
            apiKey: this.#queryConfig.apiKey,
            token: this.#authData?.raw,
            cookie: this.#queryConfig.forwardCookies
              ? this.#httpCookie
              : undefined,
          },
          customQueries,
        );

      // TODO: collected errors need to make it downstream to the client.
      if (Array.isArray(transformedCustomQueries)) {
        for (const q of transformedCustomQueries) {
          if ('error' in q) {
            lc.error?.(`Error transforming custom query ${q.name}: ${q.error}`);
            continue;
          }
          transformedQueries.push(q);
        }
      } else {
        // If the result is not an array, it is an error.
        lc.error?.(
          'Error calling API server to transform custom queries',
          transformedCustomQueries,
        );
      }
    }

    for (const q of otherQueries) {
      const transformed = transformAndHashQuery(
        lc,
        q.id,
        q.ast,
        must(this.#pipelines.currentPermissions()).permissions ?? {
          tables: {},
        },
        this.#authData?.decoded,
        q.type === 'internal',
      );
      if (transformed.transformationHash === q.transformationHash) {
        // only processing unchanged queries here
        transformedQueries.push(transformed);
      }
    }

    for (const {
      id: hash,
      transformationHash,
      transformedAst,
    } of transformedQueries) {
      const timer = new Timer();
      let count = 0;
      await startAsyncSpan(
        tracer,
        'vs.#hydrateUnchangedQueries.addQuery',
        async span => {
          span.setAttribute('queryHash', hash);
          span.setAttribute('transformationHash', transformationHash);
          span.setAttribute('table', transformedAst.table);
          for (const _ of this.#pipelines.addQuery(
            transformationHash,
            transformedAst,
            timer.start(),
          )) {
            if (++count % TIME_SLICE_CHECK_SIZE === 0) {
              if (timer.elapsedLap() > TIME_SLICE_MS) {
                timer.stopLap();
                await yieldProcess(this.#setTimeout);
                timer.startLap();
              }
            }
          }
        },
      );

      const elapsed = timer.totalElapsed();
      this.#hydrations.add(1);
      this.#hydrationTime.record(elapsed / 1000);
      lc.debug?.(`hydrated ${count} rows for ${hash} (${elapsed} ms)`);
    }
  }

  /**
   * Adds and/or removes queries to/from the PipelineDriver to bring it
   * in sync with the set of queries in the CVR (both got and desired).
   * If queries are added, removed, or queried due to a new state version,
   * a new CVR version is created and pokes sent to connected clients.
   *
   * This must be called from within the #lock.
   */
  #syncQueryPipelineSet(lc: LogContext, cvr: CVRSnapshot) {
    return startAsyncSpan(tracer, 'vs.#syncQueryPipelineSet', async () => {
      assert(
        this.#pipelines.initialized(),
        'pipelines must be initialized (syncQueryPipelineSet)',
      );

      const hydratedQueries = this.#pipelines.addedQueries();

      // Convert queries to their transformed ast's and hashes
      const hashToIDs = new Map<string, string[]>();

      if (this.#ttlClock === undefined) {
        // Get it from the CVR or initialize it to now.
        this.#ttlClock = cvr.ttlClock;
      }
      const now = Date.now();
      const ttlClock = this.#getTTLClock(now);

      // group cvr queries into:
      // 1. custom queries
      // 2. everything else
      // Handle transformation appropriately
      // Then hydrate as `serverQueries`
      const cvrQueryEntires = Object.entries(cvr.queries);
      const customQueries: Map<string, CustomQueryRecord> = new Map();
      const otherQueries: {
        id: string;
        query: ClientQueryRecord | InternalQueryRecord;
      }[] = [];
      const transformedQueries: {
        id: string;
        origQuery: QueryRecord;
        transformed: TransformedAndHashed;
      }[] = [];
      for (const [id, query] of cvrQueryEntires) {
        if (query.type === 'custom') {
          // This should always match, no?
          assert(id === query.id, 'custom query id mismatch');
          customQueries.set(id, query);
        } else {
          otherQueries.push({id, query});
        }
      }

      for (const {id, query: origQuery} of otherQueries) {
        // This should always match, no?
        assert(id === origQuery.id, 'query id mismatch');
        const transformed = transformAndHashQuery(
          lc,
          origQuery.id,
          origQuery.ast,
          must(this.#pipelines.currentPermissions()).permissions ?? {
            tables: {},
          },
          this.#authData?.decoded,
          origQuery.type === 'internal',
        );
        transformedQueries.push({
          id,
          origQuery,
          transformed,
        });
      }

      if (customQueries.size > 0 && !this.#customQueryTransformer) {
        lc.error?.(
          'Custom/named queries were requested but no `ZERO_QUERY_URL` is configured for Zero Cache.',
        );
      }

      if (this.#customQueryTransformer && customQueries.size > 0) {
        const transformedCustomQueries =
          await this.#customQueryTransformer.transform(
            {
              apiKey: this.#queryConfig.apiKey,
              token: this.#authData?.raw,
              cookie: this.#httpCookie,
            },
            customQueries.values(),
          );

        // TODO: collected errors need to make it downstream to the client.
        if (Array.isArray(transformedCustomQueries)) {
          for (const q of transformedCustomQueries) {
            if ('error' in q) {
              lc.error?.(
                `Error transforming custom query ${q.name}: ${q.error}`,
              );
              continue;
            }
            transformedQueries.push({
              id: q.id,
              origQuery: must(customQueries.get(q.id)),
              transformed: q,
            });
          }
        } else {
          // If the result is not an array, it is an error.
          lc.error?.(
            'Error calling API server to transform custom queries',
            transformedCustomQueries,
          );
        }
      }

      const serverQueries = transformedQueries.map(
        ({id, origQuery, transformed}) => {
          const ids = hashToIDs.get(transformed.transformationHash);
          if (ids) {
            ids.push(id);
          } else {
            hashToIDs.set(transformed.transformationHash, [id]);
          }
          return {
            id,
            ast: transformed.transformedAst,
            transformationHash: transformed.transformationHash,
            remove: expired(ttlClock, origQuery),
          };
        },
      );

      const addQueries = serverQueries.filter(
        q => !q.remove && !hydratedQueries.has(q.transformationHash),
      );
      const removeQueries = serverQueries.filter(q => q.remove);
      const desiredQueries = new Set(
        serverQueries.filter(q => !q.remove).map(q => q.transformationHash),
      );
      const unhydrateQueries = [...hydratedQueries].filter(
        transformationHash => !desiredQueries.has(transformationHash),
      );

      for (const q of addQueries) {
        const orig = cvr.queries[q.id];
        lc.debug?.(
          'ViewSyncer adding query',
          q.ast,
          'transformed from',
          orig.type === 'custom' ? orig.name : orig.ast,
        );
      }

      if (
        addQueries.length > 0 ||
        removeQueries.length > 0 ||
        unhydrateQueries.length > 0
      ) {
        await this.#addAndRemoveQueries(
          lc,
          cvr,
          addQueries,
          removeQueries,
          unhydrateQueries,
          hashToIDs,
        );
      } else {
        await this.#catchupClients(lc, cvr);
      }
    });
  }

  // This must be called from within the #lock.
  #addAndRemoveQueries(
    lc: LogContext,
    cvr: CVRSnapshot,
    addQueries: {id: string; ast: AST; transformationHash: string}[],
    removeQueries: {id: string; transformationHash: string}[],
    unhydrateQueries: string[],
    hashToIDs: Map<string, string[]>,
  ): Promise<void> {
    return startAsyncSpan(tracer, 'vs.#addAndRemoveQueries', async () => {
      assert(
        addQueries.length > 0 ||
          removeQueries.length > 0 ||
          unhydrateQueries.length > 0,
        'Must have queries to add or remove',
      );
      const start = performance.now();

      const stateVersion = this.#pipelines.currentVersion();
      lc = lc.withContext('stateVersion', stateVersion);
      lc.info?.(`hydrating ${addQueries.length} queries`);

      const updater = new CVRQueryDrivenUpdater(
        this.#cvrStore,
        cvr,
        stateVersion,
        this.#pipelines.replicaVersion,
      );

      // Note: This kicks off background PG queries for CVR data associated with the
      // executed and removed queries.
      const {newVersion, queryPatches} = updater.trackQueries(
        lc,
        addQueries,
        removeQueries,
      );
      const clients = this.#getClients();
      const pokers = startPoke(
        clients,
        newVersion,
        this.#pipelines.currentSchemaVersions(),
      );
      for (const patch of queryPatches) {
        await pokers.addPatch(patch);
      }

      // Removing queries is easy. The pipelines are dropped, and the CVR
      // updater handles the updates and pokes.
      for (const q of removeQueries) {
        this.#pipelines.removeQuery(q.transformationHash);
      }
      for (const hash of unhydrateQueries) {
        this.#pipelines.removeQuery(hash);
      }

      let totalProcessTime = 0;
      const timer = new Timer();
      const pipelines = this.#pipelines;
      const hydrations = this.#hydrations;
      const hydrationTime = this.#hydrationTime;

      function* generateRowChanges(slowHydrateThreshold: number) {
        for (const q of addQueries) {
          lc = lc
            .withContext('hash', q.id)
            .withContext('transformationHash', q.transformationHash);
          lc.debug?.(`adding pipeline for query`, q.ast);

          yield* pipelines.addQuery(q.transformationHash, q.ast, timer.start());
          const elapsed = timer.stop();

          totalProcessTime += elapsed;
          if (elapsed > slowHydrateThreshold) {
            lc.warn?.('Slow query materialization', elapsed, q.ast);
          }
          manualSpan(tracer, 'vs.addAndConsumeQuery', elapsed, {
            hash: q.id,
            transformationHash: q.transformationHash,
          });
        }
        hydrations.add(1);
        hydrationTime.record(totalProcessTime / 1000);
      }
      // #processChanges does batched de-duping of rows. Wrap all pipelines in
      // a single generator in order to maximize de-duping.
      await this.#processChanges(
        lc,
        timer,
        generateRowChanges(this.#slowHydrateThreshold),
        updater,
        pokers,
        hashToIDs,
      );

      for (const patch of await updater.deleteUnreferencedRows(lc)) {
        await pokers.addPatch(patch);
      }

      // Commit the changes and update the CVR snapshot.
      this.#cvr = await this.#flushUpdater(lc, updater);

      const finalVersion = this.#cvr.version;

      // Before ending the poke, catch up clients that were behind the old CVR.
      await this.#catchupClients(
        lc,
        cvr,
        finalVersion,
        addQueries.map(q => q.id),
        pokers,
      );

      // Signal clients to commit.
      await pokers.end(finalVersion);

      const wallTime = performance.now() - start;
      lc.info?.(
        `finished processing queries (process: ${totalProcessTime} ms, wall: ${wallTime} ms)`,
      );
    });
  }

  /**
   * @param cvr The CVR to which clients should be caught up to. This does
   *     not necessarily need to be the current CVR.
   * @param current The expected current CVR version. Before performing
   *     catchup, the snapshot read will verify that the CVR has not been
   *     concurrently modified. Note that this only needs to be done for
   *     catchup because it is the only time data from the CVR DB is
   *     "exported" without being gated by a CVR flush (which provides
   *     concurrency protection in all other cases).
   *
   *     If unspecified, the version of the `cvr` is used.
   * @param excludeQueryHashes Exclude patches from rows associated with
   *     the specified queries.
   * @param usePokers If specified, sends pokes on existing PokeHandlers,
   *     in which case the caller is responsible for sending the `pokeEnd`
   *     messages. If unspecified, the pokes will be started and ended
   *     using the version from the supplied `cvr`.
   */
  // Must be called within #lock
  #catchupClients(
    lc: LogContext,
    cvr: CVRSnapshot,
    current?: CVRVersion,
    excludeQueryHashes: string[] = [],
    usePokers?: PokeHandler,
  ) {
    return startAsyncSpan(tracer, 'vs.#catchupClients', async span => {
      current ??= cvr.version;
      const clients = this.#getClients();
      const pokers =
        usePokers ??
        startPoke(
          clients,
          cvr.version,
          this.#pipelines.currentSchemaVersions(),
        );
      span.setAttribute('numClients', clients.length);

      const catchupFrom = clients
        .map(c => c.version())
        .reduce((a, b) => (cmpVersions(a, b) < 0 ? a : b), cvr.version);

      // This is an AsyncGenerator which won't execute until awaited.
      const rowPatches = this.#cvrStore.catchupRowPatches(
        lc,
        catchupFrom,
        cvr,
        current,
        excludeQueryHashes,
      );

      // This is a plain async function that kicks off immediately.
      const configPatches = this.#cvrStore.catchupConfigPatches(
        lc,
        catchupFrom,
        cvr,
        current,
      );

      // await the rowPatches first so that the AsyncGenerator kicks off.
      let rowPatchCount = 0;
      for await (const rows of rowPatches) {
        for (const row of rows) {
          const {schema, table} = row;
          const rowKey = row.rowKey as RowKey;
          const toVersion = versionFromString(row.patchVersion);

          const id: RowID = {schema, table, rowKey};
          let patch: RowPatch;
          if (!row.refCounts) {
            patch = {type: 'row', op: 'del', id};
          } else {
            const row = must(
              this.#pipelines.getRow(table, rowKey),
              `Missing row ${table}:${stringify(rowKey)}`,
            );
            const {contents} = contentsAndVersion(row);
            patch = {type: 'row', op: 'put', id, contents};
          }
          const patchToVersion = {patch, toVersion};
          await pokers.addPatch(patchToVersion);
          rowPatchCount++;
        }
      }
      span.setAttribute('rowPatchCount', rowPatchCount);
      if (rowPatchCount) {
        lc.debug?.(`sent ${rowPatchCount} row patches`);
      }

      // Then await the config patches which were fetched in parallel.
      for (const patch of await configPatches) {
        await pokers.addPatch(patch);
      }

      if (!usePokers) {
        await pokers.end(cvr.version);
      }
    });
  }

  #processChanges(
    lc: LogContext,
    timer: Timer,
    changes: Iterable<RowChange>,
    updater: CVRQueryDrivenUpdater,
    pokers: PokeHandler,
    hashToIDs: Map<string, string[]>,
  ) {
    return startAsyncSpan(tracer, 'vs.#processChanges', async () => {
      const start = performance.now();

      const rows = new CustomKeyMap<RowID, RowUpdate>(rowIDString);
      let total = 0;

      const processBatch = () =>
        startAsyncSpan(tracer, 'processBatch', async () => {
          const wallElapsed = performance.now() - start;
          total += rows.size;
          lc.debug?.(
            `processing ${rows.size} (of ${total}) rows (${wallElapsed} ms)`,
          );
          const patches = await updater.received(lc, rows);

          for (const patch of patches) {
            await pokers.addPatch(patch);
          }
          rows.clear();
        });

      await startAsyncSpan(tracer, 'loopingChanges', async span => {
        for (const change of changes) {
          const {
            type,
            queryHash: transformationHash,
            table,
            rowKey,
            row,
          } = change;
          const queryIDs = must(
            hashToIDs.get(transformationHash),
            'could not find the original hash for the transformation hash',
          );
          const rowID: RowID = {schema: '', table, rowKey: rowKey as RowKey};

          let parsedRow = rows.get(rowID);
          if (!parsedRow) {
            parsedRow = {refCounts: {}};
            rows.set(rowID, parsedRow);
          }
          queryIDs.forEach(hash => (parsedRow.refCounts[hash] ??= 0));

          const updateVersion = (row: Row) => {
            // IVM can output multiple versions of a row as it goes through its
            // intermediate stages. Always update the version and contents;
            // the last version will reflect the final state.
            const {version, contents} = contentsAndVersion(row);
            parsedRow.version = version;
            parsedRow.contents = contents;
          };
          switch (type) {
            case 'add':
              updateVersion(row);
              queryIDs.forEach(hash => parsedRow.refCounts[hash]++);
              break;
            case 'edit':
              updateVersion(row);
              // No update to refCounts.
              break;
            case 'remove':
              queryIDs.forEach(hash => parsedRow.refCounts[hash]--);
              break;
            default:
              unreachable(type);
          }

          if (rows.size % CURSOR_PAGE_SIZE === 0) {
            await processBatch();
          }

          if (rows.size % TIME_SLICE_CHECK_SIZE === 0) {
            if (timer.elapsedLap() > TIME_SLICE_MS) {
              timer.stopLap();
              await yieldProcess(this.#setTimeout);
              timer.startLap();
            }
          }
        }
        if (rows.size) {
          await processBatch();
        }
        span.setAttribute('totalRows', total);
      });
    });
  }

  /**
   * Advance to the current snapshot of the replica and apply / send
   * changes.
   *
   * Must be called from within the #lock.
   *
   * Returns false if the advancement failed due to a schema change.
   */
  #advancePipelines(
    lc: LogContext,
    cvr: CVRSnapshot,
  ): Promise<'success' | ResetPipelinesSignal> {
    return startAsyncSpan(tracer, 'vs.#advancePipelines', async () => {
      assert(
        this.#pipelines.initialized(),
        'pipelines must be initialized (advancePipelines',
      );
      const start = performance.now();

      const timer = new Timer();
      const {version, numChanges, changes} = this.#pipelines.advance(timer);
      lc = lc.withContext('newVersion', version);

      // Probably need a new updater type. CVRAdvancementUpdater?
      const updater = new CVRQueryDrivenUpdater(
        this.#cvrStore,
        cvr,
        version,
        this.#pipelines.replicaVersion,
      );
      // Only poke clients that are at the cvr.version. New clients that
      // are behind need to first be caught up when their initConnection
      // message is processed (and #syncQueryPipelines is called).
      const pokers = startPoke(
        this.#getClients(cvr.version),
        updater.updatedVersion(),
        this.#pipelines.currentSchemaVersions(),
      );
      lc.debug?.(`applying ${numChanges} to advance to ${version}`);
      const hashToIDs = createHashToIDs(cvr);

      try {
        await this.#processChanges(
          lc,
          timer.start(),
          changes,
          updater,
          pokers,
          hashToIDs,
        );
      } catch (e) {
        if (e instanceof ResetPipelinesSignal) {
          await pokers.cancel();
          return e;
        }
        throw e;
      }

      // Commit the changes and update the CVR snapshot.
      this.#cvr = await this.#flushUpdater(lc, updater);
      const finalVersion = this.#cvr.version;

      // Signal clients to commit.
      await pokers.end(finalVersion);

      const elapsed = performance.now() - start;
      lc.info?.(
        `finished processing advancement of ${numChanges} changes (${elapsed} ms)`,
      );
      this.#transactionAdvanceTime.record(elapsed / 1000);
      return 'success';
    });
  }

  inspect(context: SyncContext, msg: InspectUpMessage): Promise<void> {
    return this.#runInLockForClient(context, msg, this.#handleInspect);
  }

  // eslint-disable-next-line require-await
  #handleInspect = async (
    lc: LogContext,
    clientID: string,
    body: InspectUpBody,
    cvr: CVRSnapshot,
  ): Promise<void> => {
    const client = must(this.#clients.get(clientID));
    body.op satisfies 'queries';
    client.sendInspectResponse(lc, {
      op: 'queries',
      id: body.id,
      value: await this.#cvrStore.inspectQueries(
        lc,
        cvr.ttlClock,
        body.clientID,
      ),
    });
  };

  stop(): Promise<void> {
    this.#lc.info?.('stopping view syncer');
    this.#stateChanges.cancel();
    return this.#stopped.promise;
  }

  #cleanup(err?: unknown) {
    this.#stopTTLClockInterval();
    this.#stopExpireTimer();

    this.#pipelines.destroy();
    for (const client of this.#clients.values()) {
      if (err) {
        client.fail(err);
      } else {
        client.close(`closed clientGroupID=${this.id}`);
      }
    }
  }
}

// Update CVR after every 10000 rows.
const CURSOR_PAGE_SIZE = 10000;
// Check the elapsed time every 100 rows.
const TIME_SLICE_CHECK_SIZE = 100;
// Yield the process after churning for > 500ms.
const TIME_SLICE_MS = 500;

function createHashToIDs(cvr: CVRSnapshot) {
  const hashToIDs = new Map<string, string[]>();
  for (const {id, transformationHash} of Object.values(cvr.queries)) {
    if (!transformationHash) {
      continue;
    }
    if (hashToIDs.has(transformationHash)) {
      must(hashToIDs.get(transformationHash)).push(id);
    } else {
      hashToIDs.set(transformationHash, [id]);
    }
  }
  return hashToIDs;
}

function yieldProcess(setTimeoutFn: SetTimeout) {
  return new Promise(resolve => setTimeoutFn(resolve, 0));
}

function contentsAndVersion(row: Row) {
  const {[ZERO_VERSION_COLUMN_NAME]: version, ...contents} = row;
  if (typeof version !== 'string' || version.length === 0) {
    throw new Error(`Invalid _0_version in ${stringify(row)}`);
  }
  return {contents, version};
}

const NEW_CVR_VERSION = {stateVersion: '00'};

function checkClientAndCVRVersions(
  client: NullableCVRVersion,
  cvr: CVRVersion,
) {
  if (
    cmpVersions(cvr, NEW_CVR_VERSION) === 0 &&
    cmpVersions(client, NEW_CVR_VERSION) > 0
  ) {
    // CVR is empty but client is not.
    throw new ErrorForClient({
      kind: ErrorKind.ClientNotFound,
      message: 'Client not found',
    });
  }

  if (cmpVersions(client, cvr) > 0) {
    // Client is ahead of a non-empty CVR.
    throw new ErrorForClient({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: `CVR is at version ${versionString(cvr)}`,
    });
  }
}

export function pickToken(
  lc: LogContext,
  previousToken: TokenData | undefined,
  newToken: TokenData | undefined,
) {
  if (previousToken === undefined) {
    lc.debug?.(`No previous token, using new token`);
    return newToken;
  }

  if (newToken) {
    if (previousToken.decoded.sub !== newToken.decoded.sub) {
      throw new ErrorForClient({
        kind: ErrorKind.Unauthorized,
        message:
          'The user id in the new token does not match the previous token. Client groups are pinned to a single user.',
      });
    }

    if (previousToken.decoded.iat === undefined) {
      lc.debug?.(`No issued at time for the existing token, using new token`);
      // No issued at time for the existing token? We take the most recently received token.
      return newToken;
    }

    if (newToken.decoded.iat === undefined) {
      throw new ErrorForClient({
        kind: ErrorKind.Unauthorized,
        message:
          'The new token does not have an issued at time but the prior token does. Tokens for a client group must either all have issued at times or all not have issued at times',
      });
    }

    // The new token is newer, so we take it.
    if (previousToken.decoded.iat < newToken.decoded.iat) {
      lc.debug?.(`New token is newer, using it`);
      return newToken;
    }

    // if the new token is older or the same, we keep the existing token.
    lc.debug?.(`New token is older or the same, using existing token`);
    return previousToken;
  }

  // previousToken !== undefined but newToken is undefined
  throw new ErrorForClient({
    kind: ErrorKind.Unauthorized,
    message:
      'No token provided. An unauthenticated client cannot connect to an authenticated client group.',
  });
}

/**
 * A query must be expired for all clients in order to be considered
 * expired.
 */
function expired(
  ttlClock: TTLClock,
  q: InternalQueryRecord | ClientQueryRecord | CustomQueryRecord,
): boolean {
  if (q.type === 'internal') {
    return false;
  }

  for (const clientState of Object.values(q.clientState)) {
    const {ttl, inactivatedAt} = clientState;
    if (inactivatedAt === undefined) {
      return false;
    }

    const clampedTTL = clampTTL(ttl);
    if (
      ttlClockAsNumber(inactivatedAt) + clampedTTL >
      ttlClockAsNumber(ttlClock)
    ) {
      return false;
    }
  }
  return true;
}

function hasExpiredQueries(cvr: CVRSnapshot): boolean {
  const {ttlClock} = cvr;
  for (const q of Object.values(cvr.queries)) {
    if (expired(ttlClock, q)) {
      return true;
    }
  }
  return false;
}

export class Timer {
  #total = 0;
  #start = 0;

  start() {
    this.#total = 0;
    this.startLap();
    return this;
  }

  startLap() {
    assert(this.#start === 0, 'already running');
    this.#start = performance.now();
  }

  elapsedLap() {
    assert(this.#start !== 0, 'not running');
    return performance.now() - this.#start;
  }

  stopLap() {
    assert(this.#start !== 0, 'not running');
    this.#total += performance.now() - this.#start;
    this.#start = 0;
  }

  /** @returns the total elapsed time */
  stop(): number {
    this.stopLap();
    return this.#total;
  }

  /**
   * @returns the elapsed time. This can be called while the Timer is running
   *          or after it has been stopped.
   */
  totalElapsed(): number {
    return this.#start === 0
      ? this.#total
      : this.#total + performance.now() - this.#start;
  }
}
