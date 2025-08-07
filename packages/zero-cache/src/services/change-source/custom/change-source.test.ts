import websocket from '@fastify/websocket';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance} from 'fastify';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import type WebSocket from 'ws';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {DbFile, expectTables} from '../../../test/lite.ts';
import {stream, type Sink} from '../../../types/streams.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import type {ChangeStreamMessage} from '../protocol/current/downstream.ts';
import {
  changeSourceUpstreamSchema,
  type ChangeSourceUpstream,
} from '../protocol/current/upstream.ts';
import {initializeCustomChangeSource} from './change-source.ts';

const APP_ID = 'bongo';

describe('change-source/custom', () => {
  let lc: LogContext;
  let downstream: Promise<Sink<ChangeStreamMessage>>;
  let server: FastifyInstance;
  let changeSourceURI: string;
  let replicaDbFile: DbFile;

  beforeEach(async () => {
    lc = createSilentLogContext();
    server = Fastify();
    await server.register(websocket);

    const {promise, resolve} = resolver<Sink<ChangeStreamMessage>>();
    downstream = promise;
    server.get('/', {websocket: true}, (ws: WebSocket) => {
      const {outstream} = stream<ChangeSourceUpstream, ChangeStreamMessage>(
        lc,
        ws,
        changeSourceUpstreamSchema,
      );
      resolve(outstream);
    });
    changeSourceURI = await server.listen({port: 0});
    lc.info?.(`server running on ${changeSourceURI}`);
    replicaDbFile = new DbFile('custom-change-source');
  });

  afterEach(async () => {
    await server.close();
    replicaDbFile.delete();
  });

  async function streamChanges(changes: ChangeStreamMessage[]) {
    const sink = await downstream;
    for (const change of changes) {
      sink.push(change);
    }
  }

  test('initial-sync', async () => {
    void streamChanges([
      ['begin', {tag: 'begin'}, {commitWatermark: '123'}],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'public',
            name: 'foo',
            primaryKey: ['id'],
            columns: {
              id: {pos: 0, dataType: 'text', notNull: true},
              bar: {pos: 1, dataType: 'text'},
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'public_foo_index',
            schema: 'public',
            tableName: 'foo',
            columns: {id: 'ASC'},
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'insert',
          relation: {
            schema: 'public',
            name: 'foo',
            keyColumns: ['id'],
          },
          new: {id: 'abcde', bar: 'baz'},
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo_0',
            name: 'clients',
            primaryKey: ['clientGroupID', 'clientID'],
            columns: {
              clientGroupID: {pos: 0, dataType: 'text', notNull: true},
              clientID: {pos: 1, dataType: 'text', notNull: true},
              lastMutationID: {pos: 2, dataType: 'bigint'},
              userID: {pos: 3, dataType: 'text'},
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_clients_key',
            schema: 'bongo_0',
            tableName: 'clients',
            columns: {
              clientGroupID: 'ASC',
              clientID: 'ASC',
            },
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo_0',
            name: 'mutations',
            primaryKey: ['clientGroupID', 'clientID', 'mutationID'],
            columns: {
              clientGroupID: {pos: 0, dataType: 'text', notNull: true},
              clientID: {pos: 1, dataType: 'text', notNull: true},
              mutationID: {pos: 2, dataType: 'bigint', notNull: true},
              mutation: {pos: 3, dataType: 'json'},
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_mutations_key',
            schema: 'bongo_0',
            tableName: 'mutations',
            columns: {
              clientGroupID: 'ASC',
              clientID: 'ASC',
              mutationID: 'ASC',
            },
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo',
            name: 'schemaVersions',
            primaryKey: ['lock'],
            columns: {
              lock: {pos: 0, dataType: 'bool', notNull: true},
              minSupportedVersion: {pos: 1, dataType: 'int'},
              maxSupportedVersion: {pos: 2, dataType: 'int'},
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_schemaVersions_key',
            schema: 'bongo',
            tableName: 'schemaVersions',
            columns: {lock: 'ASC'},
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-table',
          spec: {
            schema: 'bongo',
            name: 'permissions',
            primaryKey: ['lock'],
            columns: {
              lock: {pos: 0, dataType: 'bool', notNull: true},
              permissions: {pos: 1, dataType: 'json'},
              hash: {pos: 2, dataType: 'text'},
            },
          },
        },
      ],
      [
        'data',
        {
          tag: 'create-index',
          spec: {
            name: 'bongo_permissions_key',
            schema: 'bongo',
            tableName: 'permissions',
            columns: {lock: 'ASC'},
            unique: true,
          },
        },
      ],
      [
        'data',
        {
          tag: 'insert',
          relation: {
            schema: 'bongo',
            name: 'schemaVersions',
            keyColumns: ['lock'],
          },
          new: {lock: true, minSupportedVersion: 1, maxSupportedVersion: 1},
        },
      ],
      ['commit', {tag: 'commit'}, {watermark: '123'}],
    ]);

    await initializeCustomChangeSource(
      lc,
      changeSourceURI,
      {appID: APP_ID, shardNum: 0, publications: ['b', 'a']},
      replicaDbFile.path,
    );

    expectTables(replicaDbFile.connect(lc), {
      foo: [{id: 'abcde', bar: 'baz', ['_0_version']: '123'}],
      ['bongo.schemaVersions']: [
        {
          lock: 1,
          minSupportedVersion: 1,
          maxSupportedVersion: 1,
          ['_0_version']: '123',
        },
      ],
      ['bongo_0.clients']: [],
      ['_zero.replicationState']: [{lock: 1, stateVersion: '123'}],
      ['_zero.replicationConfig']: [
        {
          lock: 1,
          replicaVersion: '123',
          publications: '["a","b"]',
        },
      ],
      ['_zero.changeLog']: [
        // changeLog should be set up but empty, since it is
        // unnecessary / wasteful to record the initial state
        // in the change log.
      ],
    });
  });

  test('reset-required in initial-sync', async () => {
    void streamChanges([
      ['control', {tag: 'reset-required', message: 'watermark is too old yo'}],
    ]);

    await expect(
      initializeCustomChangeSource(
        lc,
        changeSourceURI,
        {appID: APP_ID, shardNum: 0, publications: ['b', 'a']},
        replicaDbFile.path,
      ),
    ).rejects.toThrowError(AutoResetSignal);
  });
});
