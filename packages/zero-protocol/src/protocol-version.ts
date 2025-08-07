import {assert} from '../../shared/src/asserts.ts';

/**
 * The current `PROTOCOL_VERSION` of the code.
 *
 * The `PROTOCOL_VERSION` encompasses both the wire-protocol of the `/sync/...`
 * connection between the browser and `zero-cache`, as well as the format of
 * the `AST` objects stored in both components (i.e. IDB and CVR).
 *
 * A change in the `AST` schema (e.g. new functionality added) must be
 * accompanied by an increment of the `PROTOCOL_VERSION` and a new major
 * release. The server (`zero-cache`) must be deployed before clients start
 * running the new code.
 */
// History:
// -- Version 5 adds support for `pokeEnd.cookie`. (0.14)
// -- Version 6 makes `pokeStart.cookie` optional. (0.16)
// -- Version 7 introduces the initConnection.clientSchema field. (0.17)
// -- Version 8 drops support for Version 5 (0.18).
// -- Version 11 adds inspect queries. (0.18)
// -- Version 12 adds 'timestamp' and 'date' types to the ClientSchema ValueType. (not shipped, reversed by version 14)
// -- Version 14 removes 'timestamp' and 'date' types from the ClientSchema ValueType. (0.18)
// -- Version 15 adds a `userPushParams` field to `initConnection` (0.19)
// -- Version 16 adds a new error type (alreadyProcessed) to mutation responses (0.19)
// -- Version 17 deprecates `AST` in downstream query puts. It was never used anyway. (0.21)
// -- Version 18 adds `name` and `args` to the `queries-patch` protocol (0.21)
// -- Version 19 adds `activeClients` to the `initConnection` protocol (0.22)
// -- Version 20 changes inspector down message (0.22)
// -- Version 21 removes `AST` in downstream query puts which was deprecated in Version 17, removes support for versions < 18 (0.22)
// -- Version 22 adds an optional 'userQueryParams' field to `initConnection` (0.22)
// -- Version 23 add `mutationResults` to poke (0.22)
// -- Version 24 adds `ackMutationResults` to upstream (0.22).
// -- version 25 modifies `mutationsResults` to include `del` patches (0.22)
export const PROTOCOL_VERSION = 25;

/**
 * The minimum server-supported sync protocol version (i.e. the version
 * declared in the "/sync/v{#}/connect" URL). The contract for
 * backwards compatibility is that a `zero-cache` supports the current
 * `PROTOCOL_VERSION` and at least the previous one (i.e. `PROTOCOL_VERSION - 1`)
 * if not earlier ones as well. This corresponds to supporting clients running
 * the current release and the previous (major) release. Any client connections
 * from protocol versions before `MIN_SERVER_SUPPORTED_PROTOCOL_VERSION` are
 * closed with a `VersionNotSupported` error.
 */
export const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL = 18;

assert(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL < PROTOCOL_VERSION);
