import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type Mock,
  type MockedFunction,
  test,
  vi,
} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {
  TransformResponseBody,
  TransformResponseMessage,
} from '../../../../zero-protocol/src/custom-queries.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import type {ErrorBody} from '../../../../zero-protocol/src/error.ts';
import type {
  PokeEndBody,
  PokePartBody,
  PokeStartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {DEFAULT_TTL_MS} from '../../../../zql/src/query/ttl.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {testDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import {ErrorForClient} from '../../types/error-for-client.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cvrSchema} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {updateReplicationWatermark} from '../replicator/schema/replication-state.ts';
import {type FakeReplicator} from '../replicator/test-utils.ts';
import {CVRStore} from './cvr-store.ts';
import {CVRQueryDrivenUpdater} from './cvr.ts';
import {type ClientGroupStorage} from './database-storage.ts';
import {DrainCoordinator} from './drain-coordinator.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';
import {
  app2Messages,
  appMessages,
  COMMENTS_QUERY,
  EXPECTED_LMIDS_AST,
  expectNoPokes,
  ISSUES_QUERY,
  ISSUES_QUERY2,
  ISSUES_QUERY_WITH_EXISTS,
  ISSUES_QUERY_WITH_EXISTS_AND_RELATED,
  ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED,
  ISSUES_QUERY_WITH_RELATED,
  messages,
  nextPoke,
  nextPokeParts,
  ON_FAILURE,
  permissionsAll,
  REPLICA_VERSION,
  serviceID,
  setup,
  SHARD,
  TASK_ID,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import {type SyncContext, ViewSyncerService} from './view-syncer.ts';

describe('view-syncer/service', () => {
  let storageDB: Database;
  let replicaDbFile: DbFile;
  let replica: Database;
  let cvrDB: PostgresDB;
  let upstreamDb: PostgresDB;
  const lc = createSilentLogContext();
  let stateChanges: Subscription<ReplicaState>;
  let drainCoordinator: DrainCoordinator;

  let operatorStorage: ClientGroupStorage;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let replicator: FakeReplicator;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema,
  ) => Queue<Downstream>;
  let connectWithQueueAndSource: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema,
    activeClients?: string[],
  ) => {
    queue: Queue<Downstream>;
    source: Source<Downstream>;
  };
  let setTimeoutFn: Mock<typeof setTimeout>;

  function callNextSetTimeout(delta: number) {
    // Sanity check that the system time is the mocked time.
    expect(vi.getRealSystemTime()).not.toBe(vi.getMockedSystemTime());
    vi.setSystemTime(Date.now() + delta);
    const fn = setTimeoutFn.mock.lastCall![0];
    fn();
  }

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: undefined,
    httpCookie: undefined,
  };

  beforeEach(async () => {
    ({
      storageDB,
      replicaDbFile,
      replica,
      cvrDB,
      upstreamDb,
      stateChanges,
      drainCoordinator,
      operatorStorage,
      vs,
      viewSyncerDone,
      replicator,
      connect,
      connectWithQueueAndSource,
      setTimeoutFn,
    } = await setup('view_syncer_service_test', permissionsAll));
  });

  afterEach(async () => {
    vi.useRealTimers();
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    replicaDbFile.delete();
  });

  async function getCVROwner() {
    const [{owner}] = await cvrDB<{owner: string}[]>`
    SELECT owner FROM ${cvrDB(cvrSchema(SHARD))}.instances
       WHERE "clientGroupID" = ${serviceID};
  `;
    return owner;
  }

  test('adds desired queries from initConnectionMessage', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client);

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash1'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'query-hash1': {
          ast: ISSUES_QUERY,
          type: 'client',
          clientState: {foo: {version: {stateVersion: '00', minorVersion: 1}}},
          id: 'query-hash1',
        },
      },
      version: {stateVersion: '00', minorVersion: 1},
    });
  });

  test('responds to changeDesiredQueries patch', async () => {
    const now = Date.UTC(2025, 1, 20);
    const ttlClock = 0;
    vi.setSystemTime(now);
    connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Ignore messages from an old websockets.
    await vs.changeDesiredQueries({...SYNC_CONTEXT, wsID: 'old-wsid'}, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash-1234567890', ast: USERS_QUERY},
        ],
      },
    ]);

    const inactivatedAt = ttlClock;
    // Change the set of queries.
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash2', ast: USERS_QUERY},
          {op: 'del', hash: 'query-hash1'},
        ],
      },
    ]);

    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await cvrStore.load(lc, Date.now());
    expect(cvr).toMatchObject({
      clients: {
        foo: {
          desiredQueryIDs: ['query-hash2'],
          id: 'foo',
        },
      },
      id: '9876',
      queries: {
        'lmids': {
          ast: EXPECTED_LMIDS_AST,
          type: 'internal',
          id: 'lmids',
        },
        'query-hash1': {
          ast: ISSUES_QUERY,
          type: 'client',
          clientState: {
            foo: {
              inactivatedAt,
              ttl: DEFAULT_TTL_MS,
              version: {minorVersion: 2, stateVersion: '00'},
            },
          },
          id: 'query-hash1',
        },
        'query-hash2': {
          ast: USERS_QUERY,
          type: 'client',
          clientState: {
            foo: {
              inactivatedAt: undefined,
              ttl: DEFAULT_TTL_MS,
              version: {stateVersion: '00', minorVersion: 2},
            },
          },
          id: 'query-hash2',
        },
      },
      version: {stateVersion: '00', minorVersion: 2},
    });
  });

  test('initial hydration', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "this_app_2.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  describe('custom queries', () => {
    const mockFetch = vi.fn() as MockedFunction<typeof fetch>;
    beforeEach(() => {
      vi.stubGlobal('fetch', mockFetch);
    });

    afterEach(() => {
      mockFetch.mockClear();
      vi.unstubAllGlobals();
    });

    function mockFetchImpl(queryResponses: TransformResponseBody) {
      mockFetch.mockImplementation(url => {
        if (
          url ===
          'http://my-pull-endpoint.dev/api/zero/pull?schema=this_app_2&appID=this_app'
        ) {
          return Promise.resolve(
            new Response(
              JSON.stringify([
                'transformed',
                queryResponses,
              ] satisfies TransformResponseMessage),
            ),
          );
        }
        return Promise.reject(new Error('Unexpected fetch call ' + url));
      });
    }

    test('initial hydration of a custom query', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query', args: ['thing']},
      ]);
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "custom-1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
              "schemaVersions": {
                "maxSupportedVersion": 3,
                "minSupportedVersion": 2,
              },
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
                  "op": "put",
                },
              ],
              "lastMutationIDChanges": {
                "foo": 42,
              },
              "pokeID": "01",
              "rowsPatch": [
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 9007199254740991,
                    "id": "1",
                    "json": null,
                    "owner": "100",
                    "parent": null,
                    "title": "parent issue foo",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": -9007199254740991,
                    "id": "2",
                    "json": null,
                    "owner": "101",
                    "parent": null,
                    "title": "parent issue bar",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 123,
                    "id": "3",
                    "json": null,
                    "owner": "102",
                    "parent": "1",
                    "title": "foo",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 100,
                    "id": "4",
                    "json": null,
                    "owner": "101",
                    "parent": "2",
                    "title": "bar",
                  },
                },
              ],
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `);

      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
          Result [
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "lmids": 1,
              },
              "rowKey": {
                "clientGroupID": "9876",
                "clientID": "foo",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "this_app_2.clients",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "1",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "2",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "3",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
            {
              "clientGroupID": "9876",
              "patchVersion": "01",
              "refCounts": {
                "custom-1": 1,
              },
              "rowKey": {
                "id": "4",
              },
              "rowVersion": "01",
              "schema": "",
              "table": "issues",
            },
          ]
        `);
    });

    test('different custom queries end up with the same query after transformation', async () => {
      mockFetchImpl([
        {
          ast: ISSUES_QUERY,
          id: 'custom-1',
          name: 'named-query-1',
        },
        {
          ast: ISSUES_QUERY,
          id: 'custom-2',
          name: 'named-query-2',
        },
      ]);
      const client = connect(SYNC_CONTEXT, [
        {op: 'put', hash: 'custom-1', name: 'named-query-1', args: ['thing']},
        {op: 'put', hash: 'custom-2', name: 'named-query-2', args: ['thing']},
      ]);

      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": null,
              "pokeID": "00:01",
            },
          ],
          [
            "pokePart",
            {
              "desiredQueriesPatches": {
                "foo": [
                  {
                    "hash": "custom-1",
                    "op": "put",
                  },
                  {
                    "hash": "custom-2",
                    "op": "put",
                  },
                ],
              },
              "pokeID": "00:01",
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "00:01",
              "pokeID": "00:01",
            },
          ],
        ]
      `);
      stateChanges.push({state: 'version-ready'});
      expect(await nextPoke(client)).toMatchInlineSnapshot(`
        [
          [
            "pokeStart",
            {
              "baseCookie": "00:01",
              "pokeID": "01",
              "schemaVersions": {
                "maxSupportedVersion": 3,
                "minSupportedVersion": 2,
              },
            },
          ],
          [
            "pokePart",
            {
              "gotQueriesPatch": [
                {
                  "hash": "custom-1",
                  "op": "put",
                },
                {
                  "hash": "custom-2",
                  "op": "put",
                },
              ],
              "lastMutationIDChanges": {
                "foo": 42,
              },
              "pokeID": "01",
              "rowsPatch": [
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 9007199254740991,
                    "id": "1",
                    "json": null,
                    "owner": "100",
                    "parent": null,
                    "title": "parent issue foo",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": -9007199254740991,
                    "id": "2",
                    "json": null,
                    "owner": "101",
                    "parent": null,
                    "title": "parent issue bar",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 123,
                    "id": "3",
                    "json": null,
                    "owner": "102",
                    "parent": "1",
                    "title": "foo",
                  },
                },
                {
                  "op": "put",
                  "tableName": "issues",
                  "value": {
                    "big": 100,
                    "id": "4",
                    "json": null,
                    "owner": "101",
                    "parent": "2",
                    "title": "bar",
                  },
                },
              ],
            },
          ],
          [
            "pokeEnd",
            {
              "cookie": "01",
              "pokeID": "01",
            },
          ],
        ]
      `);
      expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
        .toMatchInlineSnapshot(`
        Result [
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "lmids": 1,
            },
            "rowKey": {
              "clientGroupID": "9876",
              "clientID": "foo",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "this_app_2.clients",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "custom-1": 1,
              "custom-2": 1,
            },
            "rowKey": {
              "id": "1",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "custom-1": 1,
              "custom-2": 1,
            },
            "rowKey": {
              "id": "2",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "custom-1": 1,
              "custom-2": 1,
            },
            "rowKey": {
              "id": "3",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
          {
            "clientGroupID": "9876",
            "patchVersion": "01",
            "refCounts": {
              "custom-1": 1,
              "custom-2": 1,
            },
            "rowKey": {
              "id": "4",
            },
            "rowVersion": "01",
            "schema": "",
            "table": "issues",
          },
        ]
      `);
    });

    // not yet supported: test('a single custom query that returns many queries' () => {});
  });

  test('delete client', async () => {
    const ttl = 5000; // 5s
    vi.setSystemTime(Date.UTC(2025, 2, 4));

    const {queue: client1} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY, ttl},
    ]);

    const {queue: client2, source: connectSource2} = connectWithQueueAndSource(
      {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'},
      [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY, ttl}],
    );

    await nextPoke(client1);
    await nextPoke(client2);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(client1);
    await nextPoke(client1);

    await nextPoke(client2);
    await nextPoke(client2);

    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients`,
    ).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientID": "foo",
        },
        {
          "clientID": "bar",
        },
      ]
    `,
    );

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);

    connectSource2.cancel();

    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

    expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
      [
        {
          "desiredQueriesPatches": {
            "bar": [
              {
                "hash": "query-hash2",
                "op": "del",
              },
            ],
          },
          "pokeID": "01:01",
        },
      ]
    `);

    expect(await client1.dequeue()).toMatchInlineSnapshot(`
      [
        "deleteClients",
        {
          "clientIDs": [
            "bar",
            "no-such-client",
          ],
        },
      ]
    `);

    await expectNoPokes(client1);

    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients`,
    ).toMatchInlineSnapshot(
      `
      Result [
        {
          "clientID": "foo",
        },
      ]
    `,
    );

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": true,
          "inactivatedAt": 0,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);

    callNextSetTimeout(ttl);

    expect(await nextPokeParts(client1)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hash2",
              "op": "del",
            },
          ],
          "pokeID": "01:02",
          "rowsPatch": [
            {
              "id": {
                "id": "100",
              },
              "op": "del",
              "tableName": "users",
            },
            {
              "id": {
                "id": "101",
              },
              "op": "del",
              "tableName": "users",
            },
            {
              "id": {
                "id": "102",
              },
              "op": "del",
              "tableName": "users",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(client1);

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "ttl", "inactivatedAt" from "this_app_2/cvr".desires`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "foo",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hash1",
          "ttl": "00:00:05",
        },
        {
          "clientID": "bar",
          "deleted": true,
          "inactivatedAt": 0,
          "queryHash": "query-hash2",
          "ttl": "00:00:05",
        },
      ]
    `);
  });

  test('activeClients inactivates queries from inactive clients', async () => {
    const ttl = 5000; // 5s
    vi.setSystemTime(Date.UTC(2025, 5, 30));

    // First, connect client A with queries
    const ctxA = {...SYNC_CONTEXT, clientID: 'clientA', wsID: 'wsA'};
    const {source: streamA, queue: clientA} = connectWithQueueAndSource(ctxA, [
      {op: 'put', hash: 'query-hashA', ast: ISSUES_QUERY, ttl},
    ]);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(clientA); // desire query-hashA
    await nextPoke(clientA); // Got query-hashA and rows

    // Now connect client B and C using initConnection
    const ctxB = {...SYNC_CONTEXT, clientID: 'clientB', wsID: 'wsB'};
    const ctxC = {...SYNC_CONTEXT, clientID: 'clientC', wsID: 'wsC'};

    // Connect client B
    const {source: streamB, queue: clientB} = connectWithQueueAndSource(ctxB, [
      {op: 'put', hash: 'query-hashB', ast: USERS_QUERY, ttl},
    ]);

    // Connect client C
    const {source: streamC, queue: clientC} = connectWithQueueAndSource(ctxC, [
      {op: 'put', hash: 'query-hashC', ast: COMMENTS_QUERY, ttl},
    ]);

    await nextPoke(clientA); // desire query-hashB
    await nextPoke(clientA); // Got query-hashB and rows

    await nextPoke(clientA); // desire query-hashC
    await nextPoke(clientA); // Got query-hashC
    await expectNoPokes(clientA);

    await nextPoke(clientB); // Desire and got A & B and rows
    await nextPoke(clientB); // desire query-hashC
    await nextPoke(clientB); // Got query-hashC
    await expectNoPokes(clientB);

    await nextPoke(clientC); // Desire and got A & B and rows
    await nextPoke(clientC); // desire query-hashC
    await nextPoke(clientC); // Got query-hashC
    await expectNoPokes(clientC);

    // Verify all three clients are active and have their queries
    expect(
      await cvrDB`SELECT "clientID" from "this_app_2/cvr".clients ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
        },
        {
          "clientID": "clientB",
        },
        {
          "clientID": "clientC",
        },
      ]
    `);

    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt" from "this_app_2/cvr".desires ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // Close client A & C
    streamA.cancel();
    streamC.cancel();

    await expectNoPokes(clientA);
    await expectNoPokes(clientB);
    await expectNoPokes(clientC);

    // Verify that the clients' queries are NOT inactivated
    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt"
        FROM "this_app_2/cvr".desires
        ORDER BY "clientID"`,
    ).toMatchInlineSnapshot(`
      Result [
        {
          "clientID": "clientA",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashA",
        },
        {
          "clientID": "clientB",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashB",
        },
        {
          "clientID": "clientC",
          "deleted": false,
          "inactivatedAt": null,
          "queryHash": "query-hashC",
        },
      ]
    `);

    // Simulate the passage of time.
    const ONE_HOUR = 60 * 60 * 1000;
    vi.setSystemTime(Date.now() + ONE_HOUR);

    // Now reconnect client A with activeClients [clientA, clientB]
    // This should inactivate clientC's queries
    const newCtxA = {...ctxA, baseCookie: '01:04', wsID: 'wsA2'};
    const {source: newStreamA, queue: newClientA} = connectWithQueueAndSource(
      newCtxA,
      [{op: 'put', hash: 'query-hashA', ast: ISSUES_QUERY, ttl}],
      undefined,
      ['clientA', 'clientB'],
    );

    expect(await nextPokeParts(newClientA)).toMatchInlineSnapshot(`
      [
        {
          "desiredQueriesPatches": {
            "clientC": [
              {
                "hash": "query-hashC",
                "op": "del",
              },
            ],
          },
          "pokeID": "01:05",
        },
      ]
    `);

    await expectNoPokes(newClientA);

    await nextPoke(clientB); // desire delete query-hashC
    await expectNoPokes(clientB);

    // Verify that clientC's query remains present but is inactivated.
    expect(
      await cvrDB`SELECT "clientID", "deleted", "queryHash", "inactivatedAt" FROM "this_app_2/cvr".desires`,
    ).toEqual([
      {
        clientID: 'clientA',
        deleted: false,
        inactivatedAt: null,
        queryHash: 'query-hashA',
      },
      {
        clientID: 'clientB',
        deleted: false,
        inactivatedAt: null,
        queryHash: 'query-hashB',
      },
      {
        clientID: 'clientC',
        deleted: true,
        inactivatedAt: 60 * 60 * 1000,
        queryHash: 'query-hashC',
      },
    ]);

    // If we move time forward 5s the inactivated query should be deleted
    callNextSetTimeout(ttl);

    expect(await nextPokeParts(newClientA)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hashC",
              "op": "del",
            },
          ],
          "pokeID": "01:06",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "comments",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "comments",
            },
          ],
        },
      ]
    `);
    expect(await nextPokeParts(clientB)).toMatchInlineSnapshot(`
      [
        {
          "gotQueriesPatch": [
            {
              "hash": "query-hashC",
              "op": "del",
            },
          ],
          "pokeID": "01:06",
          "rowsPatch": [
            {
              "id": {
                "id": "1",
              },
              "op": "del",
              "tableName": "comments",
            },
            {
              "id": {
                "id": "2",
              },
              "op": "del",
              "tableName": "comments",
            },
          ],
        },
      ]
    `);

    await expectNoPokes(newClientA);
    await expectNoPokes(clientB);

    // Clean up the streams
    newStreamA.cancel();
    streamB.cancel();

    await expectNoPokes(newClientA);
    await expectNoPokes(clientB);
  });

  test('initial hydration, rows in multiple queries', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      // Test multiple queries that normalize to the same hash.
      {op: 'put', hash: 'query-hash1.1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "hash": "query-hash1.1",
                  "op": "put",
                },
                {
                  "hash": "query-hash2",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
              {
                "hash": "query-hash1.1",
                "op": "put",
              },
              {
                "hash": "query-hash2",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "5",
                  "json": [
                    123,
                    {
                      "bar": 789,
                      "foo": 456,
                    },
                    "baz",
                  ],
                  "owner": "101",
                  "parent": "2",
                  "title": "not matched",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "this_app_2.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash1.1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('initial hydration, schemaVersion unsupported', async () => {
    const client = connect({...SYNC_CONTEXT, schemaVersion: 1}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);
    stateChanges.push({state: 'version-ready'});

    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 1 is not in range of supported schema versions [2, 3].',
    });
  });

  test('initial hydration, schema unsupported', async () => {
    const client = connect(
      {...SYNC_CONTEXT, schemaVersion: 1},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
      {
        tables: {foo: {columns: {bar: {type: 'string'}}}},
      },
    );
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);
    stateChanges.push({state: 'version-ready'});

    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'The "foo" table does not exist or is not one of the replicated tables: "comments","issueLabels","issues","labels","users".',
    });
  });

  test('initial hydration, schemaVersion unsupported with bad query', async () => {
    vi.setSystemTime(Date.UTC(2025, 6, 14));
    // Simulate a connection when the replica is already ready.
    stateChanges.push({state: 'version-ready'});
    await sleep(5);

    const client = connect({...SYNC_CONTEXT, schemaVersion: 1}, [
      {
        op: 'put',
        hash: 'query-hash1',
        ast: {
          ...ISSUES_QUERY,
          // simulate an "invalid" query for an old schema version with an empty orderBy
          orderBy: [],
        },
      },
    ]);

    let err;
    try {
      // Depending on the ordering of events, the error can happen on
      // the first or second poke.
      await nextPoke(client);
      await nextPoke(client);
    } catch (e) {
      err = e;
    }
    // Make sure it's the SchemaVersionNotSupported error that gets
    // propagated, and not any error related to the bad query.
    expect(err).toBeInstanceOf(ErrorForClient);
    expect((err as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 1 is not in range of supported schema versions [2, 3].',
    });
  });

  test('process advancements', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
                {
                  "hash": "query-hash2",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": "00:01",
          "pokeID": "01",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);

    // Perform an unrelated transaction that does not affect any queries.
    // This should not result in a poke.
    replicator.processTransaction(
      '101',
      messages.insert('users', {
        id: '103',
        name: 'Dude',
      }),
    );
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Then, a relevant change should bump the client from '01' directly to '123'.
    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "this_app_2.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": {
            "query-hash1": 1,
            "query-hash2": 1,
          },
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "123",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);

    replicator.processTransaction('124', messages.truncate('issues'));

    stateChanges.push({state: 'version-ready'});

    // Then a poke that deletes issues rows in the CVR.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "123",
            "pokeID": "124",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "124",
            "rowsPatch": [
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "3",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "4",
                },
                "op": "del",
                "tableName": "issues",
              },
              {
                "id": {
                  "id": "5",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "124",
            "pokeID": "124",
          },
        ],
      ]
    `);

    expect(await cvrDB`SELECT * from "this_app_2/cvr".rows`)
      .toMatchInlineSnapshot(`
      Result [
        {
          "clientGroupID": "9876",
          "patchVersion": "01",
          "refCounts": {
            "lmids": 1,
          },
          "rowKey": {
            "clientGroupID": "9876",
            "clientID": "foo",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "this_app_2.clients",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "123",
          "refCounts": null,
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "123",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "3",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "4",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "9876",
          "patchVersion": "124",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });

  test('process advancement that results in client having an unsupported schemaVersion', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    // Note: client2 is behind, so it does not get an immediate update on connect.
    //       It has to wait until a hydrate to catchup. However, client1 will get
    //       updated about client2.
    const client2 = connect(
      {...SYNC_CONTEXT, clientID: 'bar', schemaVersion: 3},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "00:02",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:02",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:02",
            "pokeID": "00:02",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client1))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": "00:02",
          "pokeID": "01",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);
    expect((await nextPoke(client2))[0]).toMatchInlineSnapshot(`
      [
        "pokeStart",
        {
          "baseCookie": null,
          "pokeID": "01",
          "schemaVersions": {
            "maxSupportedVersion": 3,
            "minSupportedVersion": 2,
          },
        },
      ]
    `);

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
      appMessages.update('schemaVersions', {
        lock: true,
        minSupportedVersion: 3,
        maxSupportedVersion: 3,
      }),
    );

    stateChanges.push({state: 'version-ready'});

    // client1 now has an unsupported version and is sent an error and no poke
    // client2 still has a supported version and is sent a poke with the
    // updated schemaVersions range
    const dequeuePromise = client1.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'Schema version 2 is not in range of supported schema versions [3, 3].',
    });

    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 3,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);
  });

  test('process advancement with schema change', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: '00:01',
        pokeID: '01',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction(
      '07',
      messages.addColumn('issues', 'newColumn', {dataType: 'TEXT', pos: 0}),
    );

    stateChanges.push({state: 'version-ready'});

    // The "newColumn" should be arrive in the nextPoke.
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "07",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "07",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "newColumn": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": -9007199254740991,
                  "id": "2",
                  "json": null,
                  "newColumn": null,
                  "owner": "101",
                  "parent": null,
                  "title": "parent issue bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "newColumn": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "newColumn": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "07",
            "pokeID": "07",
          },
        ],
      ]
    `);
  });

  test('process advancement with schema change that breaks client support', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect((await nextPoke(client))[0]).toEqual([
      'pokeStart',
      {
        baseCookie: '00:01',
        pokeID: '01',
        schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
      },
    ]);

    replicator.processTransaction('07', messages.dropColumn('issues', 'owner'));

    stateChanges.push({state: 'version-ready'});

    const dequeuePromise = client.dequeue();
    await expect(dequeuePromise).rejects.toBeInstanceOf(ErrorForClient);
    await expect(dequeuePromise).rejects.toHaveProperty('errorBody', {
      kind: ErrorKind.SchemaVersionNotSupported,
      message:
        'The "issues"."owner" column does not exist or is not one of the replicated columns: "big","id","json","parent","title".',
    });
  });

  test('process advancement with lmid change, client has no queries.  See https://bugs.rocicorp.dev/issue/3628', async () => {
    const client = connect(SYNC_CONTEXT, []);
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "00:01",
            "pokeID": "01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01",
            "pokeID": "01",
          },
        ],
      ]
    `);

    replicator.processTransaction(
      '02',
      app2Messages.update('clients', {
        clientGroupID: serviceID,
        clientID: SYNC_CONTEXT.clientID,
        userID: null,
        lastMutationID: 43,
      }),
    );
    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "lastMutationIDChanges": {
              "foo": 43,
            },
            "pokeID": "02",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "02",
            "pokeID": "02",
          },
        ],
      ]
    `);
  });

  test('catchup client', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "00:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "00:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "00:01",
            "pokeID": "00:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[2][1] as PokeEndBody;
    expect(preAdvancement).toEqual({
      cookie: '01',
      pokeID: '01',
    });

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    const advancement = (await nextPoke(client1))[1][1] as PokePartBody;
    expect(advancement).toEqual({
      rowsPatch: [
        {
          tableName: 'issues',
          op: 'put',
          value: {
            big: 9007199254740991,
            id: '1',
            owner: '100.0',
            parent: null,
            title: 'new title',
            json: null,
          },
        },
        {
          id: {id: '2'},
          tableName: 'issues',
          op: 'del',
        },
      ],
      pokeID: '123',
    });

    // Connect with another client (i.e. tab) at older version '00:02'
    // (i.e. pre-advancement).
    const client2 = connect(
      {
        clientID: 'bar',
        wsID: '9382',
        baseCookie: preAdvancement.cookie,
        protocolVersion: PROTOCOL_VERSION,
        schemaVersion: 2,
        tokenData: undefined,
        httpCookie: undefined,
      },
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    // Response should catch client2 up with the rowsPatch from
    // the advancement.
    const response2 = await nextPoke(client2);
    expect(response2[1][1]).toMatchObject({
      ...advancement,
      pokeID: '123:01',
    });
    expect(response2).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123:01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "123:01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);

    // client1 should be poked to get the new client2 config,
    // but no new entities.
    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "123",
            "pokeID": "123:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "pokeID": "123:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);
  });

  test('catchup new client before advancement', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client1);

    stateChanges.push({state: 'version-ready'});
    const preAdvancement = (await nextPoke(client1))[0][1] as PokeStartBody;
    expect(preAdvancement).toEqual({
      baseCookie: '00:01',
      pokeID: '01',
      schemaVersions: {minSupportedVersion: 2, maxSupportedVersion: 3},
    });

    replicator.processTransaction(
      '123',
      messages.update('issues', {
        id: '1',
        title: 'new title',
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});

    // Connect a second client right as the advancement is about to be processed.
    await sleep(0.5);
    const client2 = connect({...SYNC_CONTEXT, clientID: 'bar'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Response should catch client2 from scratch.
    expect(await nextPoke(client2)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "123:01",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "bar": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "123:01",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 123,
                  "id": "3",
                  "json": null,
                  "owner": "102",
                  "parent": "1",
                  "title": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "bar",
                },
              },
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100.0",
                  "parent": null,
                  "title": "new title",
                },
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123:01",
            "pokeID": "123:01",
          },
        ],
      ]
    `);
  });

  test('waits for replica to catchup', async () => {
    // Before connecting, artificially set the CVR version to '07',
    // which is ahead of the current replica version '01'.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      REPLICA_VERSION,
    ).flush(lc, now, now, ttlClock);

    // Connect the client.
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Signal that the replica is ready.
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Manually simulate advancements in the replica.
    const db = new StatementRunner(replica);
    replica.prepare(`DELETE from issues where id = '1'`).run();
    updateReplicationWatermark(db, '03');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica.prepare(`DELETE from issues where id = '2'`).run();
    updateReplicationWatermark(db, '05');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica.prepare(`DELETE from issues where id = '3'`).run();
    updateReplicationWatermark(db, '06');
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    replica
      .prepare(`UPDATE issues SET title = 'caught up' where id = '4'`)
      .run();
    updateReplicationWatermark(db, '07'); // Caught up with stateVersion=07, watermark=09.
    stateChanges.push({state: 'version-ready'});

    // The single poke should only contain issues {id='4', title='caught up'}
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": null,
            "pokeID": "07:02",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "foo": [
                {
                  "hash": "query-hash1",
                  "op": "put",
                },
              ],
            },
            "gotQueriesPatch": [
              {
                "hash": "query-hash1",
                "op": "put",
              },
            ],
            "lastMutationIDChanges": {
              "foo": 42,
            },
            "pokeID": "07:02",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 100,
                  "id": "4",
                  "json": null,
                  "owner": "101",
                  "parent": "2",
                  "title": "caught up",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "07:02",
            "pokeID": "07:02",
          },
        ],
      ]
    `);
  });

  test('sends reset for CVR from older replica version up', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      '1' + REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, now, now, ttlClock);

    // Connect the client.
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Signal that the replica is ready.
    stateChanges.push({state: 'version-ready'});

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ErrorForClient);
    expect((result as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.ClientNotFound,
      message: 'Cannot sync from older replica: CVR=101, DB=01',
    } satisfies ErrorBody);
  });

  test('sends client not found if CVR is not found', async () => {
    // Connect the client at a non-empty base cookie.
    const client = connect({...SYNC_CONTEXT, baseCookie: '00:02'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ErrorForClient);
    expect((result as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.ClientNotFound,
      message: 'Client not found',
    } satisfies ErrorBody);
  });

  test('initial CVR ownership takeover', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      'some-other-task-id',
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const otherTaskOwnershipTime = now - 600_000;
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, otherTaskOwnershipTime),
      '07',
      REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, otherTaskOwnershipTime, now, ttlClock);

    expect(await getCVROwner()).toBe('some-other-task-id');

    // Signal that the replica is ready before any connection
    // message is received.
    stateChanges.push({state: 'version-ready'});

    // Wait for the fire-and-forget takeover to happen.
    await sleep(1000);
    expect(await getCVROwner()).toBe(TASK_ID);
  });

  test('deleteClients before init connection initiates takeover', async () => {
    // First simulate a takeover that has happened since the view-syncer
    // was started.
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      'some-other-task-id',
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const otherTaskOwnershipTime = now;
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, otherTaskOwnershipTime),
      '07',
      REPLICA_VERSION, // CVR is at a newer replica version.
    ).flush(lc, otherTaskOwnershipTime, now, ttlClock);

    expect(await getCVROwner()).toBe('some-other-task-id');

    // deleteClients should be considered a new connection and
    // take over the CVR.
    await vs.deleteClients(SYNC_CONTEXT, [
      'deleteClients',
      {clientIDs: ['bar', 'no-such-client']},
    ]);

    // Wait for the fire-and-forget takeover to happen.
    await sleep(1000);
    expect(await getCVROwner()).toBe(TASK_ID);
  });

  test('sends invalid base cookie if client is ahead of CVR', async () => {
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    await new CVRQueryDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      '07',
      REPLICA_VERSION,
    ).flush(lc, now, now, ttlClock);

    // Connect the client with a base cookie from the future.
    const client = connect({...SYNC_CONTEXT, baseCookie: '08'}, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    let result;
    try {
      result = await client.dequeue();
    } catch (e) {
      result = e;
    }
    expect(result).toBeInstanceOf(ErrorForClient);
    expect((result as ErrorForClient).errorBody).toEqual({
      kind: ErrorKind.InvalidConnectionRequestBaseCookie,
      message: 'CVR is at version 07',
    } satisfies ErrorBody);
  });

  test('clean up operator storage on close', async () => {
    const storage = operatorStorage.createStorage();
    storage.set('foo', 'bar');
    expect(storageDB.prepare('SELECT * from storage').all()).toHaveLength(1);

    await vs.stop();
    await viewSyncerDone;

    expect(storageDB.prepare('SELECT * from storage').all()).toHaveLength(0);
  });

  // Does not test the actual timeout logic, but better than nothing.
  test('keepalive return value', () => {
    expect(vs.keepalive()).toBe(true);
    void vs.stop();
    expect(vs.keepalive()).toBe(false);
  });

  test('elective drain', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY2},
      {op: 'put', hash: 'query-hash3', ast: USERS_QUERY},
    ]);

    stateChanges.push({state: 'version-ready'});
    // This should result in computing a non-zero hydration time.
    await nextPoke(client);

    drainCoordinator.drainNextIn(0);
    expect(drainCoordinator.shouldDrain()).toBe(true);
    const now = Date.now();
    // Bump time forward to verify that the timeout is reset later.
    vi.setSystemTime(now + 3);

    // Enqueue a dummy task so that the view-syncer can elect to drain.
    stateChanges.push({state: 'version-ready'});

    // Upon completion, the view-syncer should have called drainNextIn()
    // with its hydration time so that the next drain is not triggered
    // until that interval elapses.
    await viewSyncerDone;
    expect(drainCoordinator.nextDrainTime).toBeGreaterThan(now);
  });

  test('retracting an exists relationship', async () => {
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY_WITH_RELATED},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY_WITH_EXISTS},
    ]);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);
    await nextPoke(client);

    replicator.processTransaction(
      '123',
      messages.delete('issueLabels', {
        issueID: '1',
        labelID: '1',
      }),
      messages.delete('issues', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});
    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "id": {
                  "issueID": "1",
                  "labelID": "1",
                },
                "op": "del",
                "tableName": "issueLabels",
              },
              {
                "id": {
                  "id": "1",
                },
                "op": "del",
                "tableName": "labels",
              },
              {
                "id": {
                  "id": "2",
                },
                "op": "del",
                "tableName": "issues",
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);
  });

  test('query with exists and related', async () => {
    const client = connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: 'query-hash',
        ast: ISSUES_QUERY_WITH_EXISTS_AND_RELATED,
      },
    ]);
    await nextPoke(client); // config update
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    // Satisfy the exists condition
    replicator.processTransaction(
      '123',
      messages.update('comments', {
        id: '1',
        text: 'foo',
      }),
    );

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "foo",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "2",
                  "issueID": "1",
                  "text": "bar",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);
  });

  test('query with not exists and related', async () => {
    const client = connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: 'query-hash',
        ast: ISSUES_QUERY_WITH_NOT_EXISTS_AND_RELATED,
      },
    ]);
    await nextPoke(client); // config update
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    // Satisfy the not-exists condition by deleting the comment
    // that matches text='bar'.
    replicator.processTransaction(
      '123',
      messages.delete('comments', {id: '2'}),
    );

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "123",
            "schemaVersions": {
              "maxSupportedVersion": 3,
              "minSupportedVersion": 2,
            },
          },
        ],
        [
          "pokePart",
          {
            "pokeID": "123",
            "rowsPatch": [
              {
                "op": "put",
                "tableName": "issues",
                "value": {
                  "big": 9007199254740991,
                  "id": "1",
                  "json": null,
                  "owner": "100",
                  "parent": null,
                  "title": "parent issue foo",
                },
              },
              {
                "op": "put",
                "tableName": "comments",
                "value": {
                  "id": "1",
                  "issueID": "1",
                  "text": "comment 1",
                },
              },
            ],
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "123",
            "pokeID": "123",
          },
        ],
      ]
    `);
  });
});
