import type {LogLevel} from '@rocicorp/logger';
import type {StoreProvider} from '../../../replicache/src/kv/store.ts';
import type {MaybePromise} from '../../../shared/src/types.ts';
import * as v from '../../../shared/src/valita.ts';
import type {
  UserMutateParams,
  UserQueryParams,
} from '../../../zero-protocol/src/connect.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {CustomMutatorDefs} from './custom.ts';
import type {OnError} from './on-error.ts';
import {UpdateNeededReasonType} from './update-needed-reason-type.ts';

/**
 * Configuration for {@linkcode Zero}.
 */
export interface ZeroOptions<
  S extends Schema,
  MD extends CustomMutatorDefs | undefined = undefined,
> {
  /**
   * URL to the zero-cache. This can be a simple hostname, e.g.
   * - "https://myapp-myteam.zero.ms"
   * or a prefix with a single path component, e.g.
   * - "https://myapp-myteam.zero.ms/zero"
   * - "https://myapp-myteam.zero.ms/db"
   *
   * The latter is useful for configuring routing rules (e.g. "/zero/\*") when
   * the zero-cache is hosted on the same domain as the application. **Note that
   * only a single path segment is allowed (e.g. it cannot be "/proxy/zero/\*")**.
   */
  server?: string | null | undefined;

  /**
   * A JWT to identify and authenticate the user. Can be provided as either:
   * - A string containing the JWT token
   * - A function that returns a JWT token
   * - `undefined` if there is no logged in user
   *
   * Token validation behavior:
   * 1. **For function providers:**
   *    When zero-cache reports that a token is invalid (expired, malformed,
   *    or has an invalid signature), Zero will call the function again with
   *    `error='invalid-token'` to obtain a new token.
   *
   * 2. **For string tokens:**
   *    Zero will continue to use the provided token even if zero-cache initially
   *    reports it as invalid. This is because zero-cache may be able to validate
   *    the token after fetching new public keys from its configured JWKS URL
   *    (if `ZERO_AUTH_JWKS_URL` is set).
   */
  auth?:
    | string
    | ((error?: 'invalid-token') => MaybePromise<string | undefined>)
    | undefined;

  /**
   * A unique identifier for the user. Must be non-empty.
   *
   * Each userID gets its own client-side storage so that the app can switch
   * between users without losing state.
   *
   * This must match the `sub` claim of the `auth` token if
   * `auth` is provided.
   */
  userID: string;

  /**
   * Distinguishes the storage used by this Zero instance from that of other
   * instances with the same userID. Useful in the case where the app wants to
   * have multiple Zero instances for the same user for different parts of the
   * app.
   */
  storageKey?: string | undefined;

  /**
   * Determines the level of detail at which Zero logs messages about
   * its operation. Messages are logged to the `console`.
   *
   * When this is set to `'debug'`, `'info'` and `'error'` messages are also
   * logged. When set to `'info'`, `'info'` and `'error'` but not
   * `'debug'` messages are logged. When set to `'error'` only `'error'`
   * messages are logged.
   *
   * Default is `'error'`.
   */
  logLevel?: LogLevel | undefined;

  /**
   * This defines the schema of the tables used in Zero and their relationships
   * to one another.
   */
  schema: S;

  /**
   * `mutators` is a map of custom mutator definitions. The keys are
   * namespaces or names of the mutators. The values are the mutator
   * implementations. Client side mutators must be idempotent as a
   * mutation can be rebased multiple times when folding in authoritative
   * changes from the server to the client.
   */
  mutators?: MD | undefined;

  /**
   * Custom mutations are pushed to zero-cache and then to
   * your API server.
   *
   * push.queryParams can be used to augment the URL
   * used to connect to your API server so it includes
   * variables in the query string.
   *
   * DEPRECATED: Use `userMutateParams` instead.
   */
  push?: UserMutateParams;
  mutate?: UserMutateParams;
  query?: UserQueryParams;

  /**
   * `onOnlineChange` is called when the Zero instance's online status changes.
   *
   * @deprecated Use `onOnline` on the Zero instance instead. e.g.
   * ```ts
   * const zero = new Zero({...});
   * zero.onOnline((online) => { ... });
   * ```
   */
  onOnlineChange?: ((online: boolean) => void) | undefined;

  /**
   * `onUpdateNeeded` is called when a client code update is needed.
   *
   * See {@link UpdateNeededReason} for why updates can be needed.
   *
   * The default behavior is to reload the page (using `location.reload()`).
   * Provide your own function to prevent the page from
   * reloading automatically. You may want to display a toast to inform the end
   * user there is a new version of your app available and prompt them to
   * refresh.
   */
  onUpdateNeeded?: ((reason: UpdateNeededReason) => void) | undefined;

  /**
   * `onClientStateNotFound` is called when this client is no longer able
   * to sync with the zero-cache due to missing synchronization state.  This
   * can be because:
   * - the local persistent synchronization state has been garbage collected.
   *   This can happen if the client has no pending mutations and has not been
   *   used for a while (e.g. the client's tab has been hidden for a long time).
   * - the zero-cache fails to find the server side synchronization state for
   *   this client.
   *
   * The default behavior is to reload the page (using `location.reload()`).
   * Provide your own function to prevent the page from reloading automatically.
   */
  onClientStateNotFound?: (() => void) | undefined;

  /**
   * The number of milliseconds to wait before disconnecting a Zero
   * instance whose tab has become hidden.
   *
   * Instances in hidden tabs are disconnected to save resources.
   *
   * Default is 5_000.
   */
  hiddenTabDisconnectDelay?: number | undefined;

  /**
   * This gets called when the Zero instance encounters an error. The default
   * behavior is to log the error to the console. Provide your own function to
   * prevent the default behavior.
   */
  onError?: OnError | undefined;

  /**
   * Determines what kind of storage implementation to use on the client.
   *
   * Defaults to `'idb'` which means that Zero uses an IndexedDB storage
   * implementation. This allows the data to be persisted on the client and
   * enables faster syncs between application restarts.
   *
   * By setting this to `'mem'`, Zero uses an in memory storage and
   * the data is not persisted on the client.
   *
   * You can also set this to a function that is used to create new KV stores,
   * allowing a custom implementation of the underlying storage layer.
   */
  kvStore?: 'mem' | 'idb' | StoreProvider | undefined;

  /**
   * The maximum number of bytes to allow in a single header.
   *
   * Zero adds some extra information to headers on initialization if possible.
   * This speeds up data synchronization. This number should be kept less than
   * or equal to the maximum header size allowed by the zero-cache and any load
   * balancers.
   *
   * Default value: 8kb.
   */
  maxHeaderLength?: number | undefined;

  /**
   * The maximum amount of milliseconds to wait for a materialization to
   * complete (including network/server time) before printing a warning to the
   * console.
   *
   * Default value: 5_000.
   */
  slowMaterializeThreshold?: number | undefined;

  /**
   * UI rendering libraries will often provide a utility for batching multiple
   * state updates into a single render. Some examples are React's
   * `unstable_batchedUpdates`, and solid-js's `batch`.
   *
   * This option enables integrating these batch utilities with Zero.
   *
   * When `batchViewUpdates` is provided, Zero will call it whenever
   * it updates query view state with an `applyViewUpdates` function
   * that performs the actual state updates.
   *
   * Zero updates query view state when:
   * 1. creating a new view
   * 2. updating all existing queries' views to a new consistent state
   *
   * When creating a new view, that single view's creation will be wrapped
   * in a `batchViewUpdates` call.
   *
   * When updating existing queries, all queries will be updated in a single
   * `batchViewUpdates` call, so that the transition to the new consistent
   * state can be done in a single render.
   *
   * Implementations must always call `applyViewUpdates` synchronously.
   */
  batchViewUpdates?: ((applyViewUpdates: () => void) => void) | undefined;

  /**
   * The maximum number of recent queries, no longer subscribed to by a preload
   * or view, to continue syncing.
   *
   * Defaults is 0.
   *
   * @deprecated Use ttl instead
   */
  maxRecentQueries?: number | undefined;

  /**
   * Changes to queries are sent to server in batches. This option controls
   * the number of milliseconds to wait before sending the next batch.
   *
   * Defaults is 10.
   */
  queryChangeThrottleMs?: number | undefined;
}

/**
 * @deprecated Use {@link ZeroOptions} instead.
 */
export interface ZeroAdvancedOptions<
  S extends Schema,
  MD extends CustomMutatorDefs | undefined = undefined,
> extends ZeroOptions<S, MD> {}

export type UpdateNeededReason =
  | {type: UpdateNeededReasonType.NewClientGroup}
  | {type: UpdateNeededReasonType.VersionNotSupported}
  | {type: UpdateNeededReasonType.SchemaVersionNotSupported};

export const updateNeededReasonTypeSchema: v.Type<UpdateNeededReason['type']> =
  v.literalUnion(
    UpdateNeededReasonType.NewClientGroup,
    UpdateNeededReasonType.VersionNotSupported,
    UpdateNeededReasonType.SchemaVersionNotSupported,
  );
