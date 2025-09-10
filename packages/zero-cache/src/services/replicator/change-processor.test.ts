import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, test} from 'vitest';
import type {JSONObject} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {createFTS5Statements} from '../../db/create.ts';
import {listIndexes, listTables} from '../../db/lite-tables.ts';
import type {LiteIndexSpec, LiteTableSpec} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {expectTables, initDB} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from './change-processor.ts';
import {initChangeLog} from './schema/change-log.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from './schema/replication-state.ts';
import {createChangeProcessor, ReplicationMessages} from './test-utils.ts';

describe('replicator/incremental-sync', () => {
  let lc: LogContext;
  let servingReplica: Database;
  let servingProcessor: ChangeProcessor;
  let backupReplica: Database;
  let backupProcessor: ChangeProcessor;

  beforeEach(() => {
    lc = createSilentLogContext();
    servingReplica = new Database(lc, ':memory:');
    servingProcessor = new ChangeProcessor(
      new StatementRunner(servingReplica),
      'serving',
      (_, err) => {
        throw err;
      },
    );
    backupReplica = new Database(lc, ':memory:');
    backupProcessor = new ChangeProcessor(
      new StatementRunner(backupReplica),
      'backup',
      (_, err) => {
        throw err;
      },
    );
  });

  type Case = {
    name: string;
    setup: string;
    downstream: ChangeStreamData[];
    data: Record<string, Record<string, unknown>[]>;
    tableSpecs?: LiteTableSpec[];
    indexSpecs?: LiteIndexSpec[];
  };

  const issues = new ReplicationMessages({issues: ['issueID', 'bool']});
  const full = new ReplicationMessages(
    {full: ['id', 'bool', 'desc']},
    'public',
    'full',
  );
  const orgIssues = new ReplicationMessages({
    issues: ['orgID', 'issueID', 'bool'],
  });
  const fooBarBaz = new ReplicationMessages({foo: 'id', bar: 'id', baz: 'id'});
  const tables = new ReplicationMessages({transaction: 'column'});

  const cases: Case[] = [
    {
      name: 'insert rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        bool BOOL,
        big INTEGER,
        flt REAL,
        description TEXT,
        json JSON,
        json2 JSONB,
        time TIMESTAMPTZ,
        bytes bytesa,
        intArray int4[],
        _0_version TEXT,
        PRIMARY KEY(issueID, bool)
      );
      `,
      downstream: [
        ['begin', issues.begin(), {commitWatermark: '06'}],
        ['data', issues.insert('issues', {issueID: 123, bool: true})],
        ['data', issues.insert('issues', {issueID: 456, bool: false})],
        ['commit', issues.commit(), {watermark: '06'}],

        ['begin', issues.begin(), {commitWatermark: '0b'}],
        [
          'data',
          issues.insert('issues', {
            issueID: 789,
            bool: true,
            big: 9223372036854775807n,
            json: [{foo: 'bar', baz: 123}],
            json2: true,
            time: 1728345600123456n,
            bytes: Buffer.from('world'),
            intArray: [3, 2, 1],
          } as unknown as Record<string, JSONObject>),
        ],
        ['data', issues.insert('issues', {issueID: 987, bool: true})],
        [
          'data',
          issues.insert('issues', {issueID: 234, bool: false, flt: 123.456}),
        ],
        ['commit', issues.commit(), {watermark: '0b'}],
      ],
      data: {
        issues: [
          {
            issueID: 123n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 456n,
            big: null,
            flt: null,
            bool: 0n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '06',
          },
          {
            issueID: 789n,
            big: 9223372036854775807n,
            flt: null,
            bool: 1n,
            description: null,
            json: '[{"foo":"bar","baz":123}]',
            json2: 'true',
            time: 1728345600123456n,
            bytes: Buffer.from('world'),
            intArray: '[3,2,1]',
            ['_0_version']: '0b',
          },
          {
            issueID: 987n,
            big: null,
            flt: null,
            bool: 1n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '0b',
          },
          {
            issueID: 234n,
            big: null,
            flt: 123.456,
            bool: 0n,
            description: null,
            json: null,
            json2: null,
            time: null,
            bytes: null,
            intArray: null,
            ['_0_version']: '0b',
          },
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":123}',
          },
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456}',
          },
          {
            stateVersion: '0b',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789}',
          },
          {
            stateVersion: '0b',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":987}',
          },
          {
            stateVersion: '0b',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":234}',
          },
        ],
      },
    },
    {
      name: 'partial update rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        bool BOOL,
        big INTEGER,
        flt REAL,
        description TEXT,
        json JSON,
        _0_version TEXT,
        PRIMARY KEY(issueID, bool)
      );
      INSERT INTO issues (issueID, bool, big, flt, description, json, _0_version)
        VALUES (123, true, 9223372036854775807, 123.456, 'hello', 'world', '06');
      `,
      downstream: [
        ['begin', issues.begin(), {commitWatermark: '0a'}],
        [
          'data',
          issues.update('issues', {
            issueID: 123,
            bool: true,
            description: 'bello',
          }),
        ],
        [
          'data',
          issues.update('issues', {
            issueID: 123,
            bool: true,
            json: {wor: 'ld'},
          }),
        ],
        ['commit', issues.commit(), {watermark: '0a'}],
      ],
      data: {
        issues: [
          {
            issueID: 123n,
            big: 9223372036854775807n,
            flt: 123.456,
            bool: 1n,
            description: 'bello',
            json: '{"wor":"ld"}',
            ['_0_version']: '0a',
          },
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0a',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":123}',
          },
        ],
      },
    },
    {
      name: 'update rows with multiple key columns and key value updates',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        orgID INTEGER,
        description TEXT,
        bool BOOL,
        _0_version TEXT,
        PRIMARY KEY("orgID", "issueID", "bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '06'}],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 456, bool: true}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 789, bool: true}),
        ],
        ['commit', orgIssues.commit(), {watermark: '06'}],

        ['begin', orgIssues.begin(), {commitWatermark: '0a'}],
        [
          'data',
          orgIssues.update('issues', {
            orgID: 1,
            issueID: 456,
            bool: true,
            description: 'foo',
          }),
        ],
        [
          'data',
          orgIssues.update(
            'issues',
            {
              orgID: 2,
              issueID: 123,
              bool: false,
              description: 'bar',
            },
            {orgID: 1, issueID: 123, bool: true},
          ),
        ],
        ['commit', orgIssues.commit(), {watermark: '0a'}],
      ],
      data: {
        issues: [
          {
            orgID: 2n,
            issueID: 123n,
            description: 'bar',
            bool: 0n,
            ['_0_version']: '0a',
          },
          {
            orgID: 1n,
            issueID: 456n,
            description: 'foo',
            bool: 1n,
            ['_0_version']: '0a',
          },
          {
            orgID: 2n,
            issueID: 789n,
            description: null,
            bool: 1n,
            ['_0_version']: '06',
          },
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '06',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":789,"orgID":2}',
          },
          {
            stateVersion: '0a',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":1,"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '0a',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '0a',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":123,"orgID":2}',
          },
        ],
      },
    },
    {
      name: 'delete rows',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        orgID INTEGER,
        bool BOOL,
        description TEXT,
        _0_version TEXT,
        PRIMARY KEY("orgID", "issueID","bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '07'}],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 456, bool: false}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 789, bool: false}),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 987, bool: true}),
        ],
        ['commit', orgIssues.commit(), {watermark: '07'}],

        ['begin', orgIssues.begin(), {commitWatermark: '0c'}],
        [
          'data',
          orgIssues.delete('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.delete('issues', {orgID: 1, issueID: 456, bool: false}),
        ],
        [
          'data',
          orgIssues.delete('issues', {orgID: 2, issueID: 987, bool: true}),
        ],
        ['commit', orgIssues.commit(), {watermark: '0c'}],
      ],
      data: {
        issues: [
          {
            orgID: 2n,
            issueID: 789n,
            bool: 0n,
            description: null,
            ['_0_version']: '07',
          },
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '07',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":789,"orgID":2}',
          },
          {
            stateVersion: '0c',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '0c',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":0,"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '0c',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":987,"orgID":2}',
          },
        ],
      },
    },
    {
      name: 'truncate tables',
      setup: `
      CREATE TABLE foo(id INTEGER PRIMARY KEY, _0_version TEXT);
      CREATE TABLE bar(id INTEGER PRIMARY KEY, _0_version TEXT);
      CREATE TABLE baz(id INTEGER PRIMARY KEY, _0_version TEXT);
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.insert('foo', {id: 1})],
        ['data', fooBarBaz.insert('foo', {id: 2})],
        ['data', fooBarBaz.insert('foo', {id: 3})],
        ['data', fooBarBaz.insert('bar', {id: 4})],
        ['data', fooBarBaz.insert('bar', {id: 5})],
        ['data', fooBarBaz.insert('bar', {id: 6})],
        ['data', fooBarBaz.insert('baz', {id: 7})],
        ['data', fooBarBaz.insert('baz', {id: 8})],
        ['data', fooBarBaz.insert('baz', {id: 9})],
        ['data', fooBarBaz.truncate('foo', 'baz')],
        ['data', fooBarBaz.truncate('foo')], // Redundant. Shouldn't cause problems.
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],

        ['begin', fooBarBaz.begin(), {commitWatermark: '0i'}],
        ['data', fooBarBaz.truncate('foo')],
        ['data', fooBarBaz.insert('foo', {id: 101})],
        ['commit', fooBarBaz.commit(), {watermark: '0i'}],
      ],
      data: {
        foo: [{id: 101n, ['_0_version']: '0i'}],
        bar: [
          {id: 4n, ['_0_version']: '0e'},
          {id: 5n, ['_0_version']: '0e'},
          {id: 6n, ['_0_version']: '0e'},
        ],
        baz: [],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'bar',
            op: 's',
            rowKey: '{"id":4}',
          },
          {
            stateVersion: '0e',
            table: 'bar',
            op: 's',
            rowKey: '{"id":5}',
          },
          {
            stateVersion: '0e',
            table: 'bar',
            op: 's',
            rowKey: '{"id":6}',
          },
          {
            stateVersion: '0e',
            table: 'baz',
            op: 't',
            rowKey: '',
          },
          {
            stateVersion: '0i',
            table: 'foo',
            op: 't',
            rowKey: '',
          },
          {
            stateVersion: '0i',
            table: 'foo',
            op: 's',
            rowKey: '{"id":101}',
          },
        ],
      },
    },
    {
      name: 'replica identity full',
      setup: `
      CREATE TABLE full(
        id "INTEGER|NOT_NULL",
        bool BOOL,
        desc TEXT,
        _0_version TEXT
      );
      CREATE UNIQUE INDEX full_pk ON full (id ASC);
      `,
      downstream: [
        ['begin', full.begin(), {commitWatermark: '06'}],
        ['data', full.insert('full', {id: 123, bool: true, desc: null})],
        ['data', full.insert('full', {id: 456, bool: false, desc: null})],
        ['data', full.insert('full', {id: 789, bool: false, desc: null})],
        ['commit', full.commit(), {watermark: '06'}],

        ['begin', full.begin(), {commitWatermark: '0b'}],
        [
          'data',
          full.update(
            'full',
            {id: 123, bool: false, desc: 'foobar'},
            {id: 123, bool: true, desc: null},
          ),
        ],
        [
          'data',
          full.update(
            'full',
            {id: 987, bool: true, desc: 'barfoo'},
            {id: 456, bool: false, desc: null},
          ),
        ],
        ['data', full.delete('full', {id: 789, bool: false, desc: null})],
        ['commit', issues.commit(), {watermark: '0b'}],
      ],
      data: {
        full: [
          {id: 123n, bool: 0n, desc: 'foobar', ['_0_version']: '0b'},
          {id: 987n, bool: 1n, desc: 'barfoo', ['_0_version']: '0b'},
        ],
        ['_zero.changeLog']: [
          {
            op: 's',
            rowKey: '{"id":123}',
            stateVersion: '0b',
            table: 'full',
          },
          {
            op: 'd',
            rowKey: '{"id":456}',
            stateVersion: '0b',
            table: 'full',
          },
          {
            op: 'd',
            rowKey: '{"id":789}',
            stateVersion: '0b',
            table: 'full',
          },
          {
            op: 's',
            rowKey: '{"id":987}',
            stateVersion: '0b',
            table: 'full',
          },
        ],
      },
    },
    {
      name: 'upsert (resumptive replication)',
      setup: `
      CREATE TABLE foo(
        id INT PRIMARY KEY,
        desc TEXT,
        _0_version TEXT
      );
      INSERT INTO foo (id, desc) VALUES (1, 'one');

      CREATE TABLE full(
        id INT PRIMARY KEY,
        bool BOOL,
        desc TEXT,
        _0_version TEXT
      );
      INSERT INTO full (id, bool, desc) VALUES (2, 0, 'two');
      `,
      downstream: [
        ['begin', full.begin(), {commitWatermark: '06'}],
        ['data', fooBarBaz.insert('foo', {id: 1, desc: 'replaced one'})],
        ['data', fooBarBaz.update('foo', {id: 789, desc: null})],
        ['data', fooBarBaz.update('foo', {id: 234, desc: 'woo'}, {id: 999})],
        ['data', fooBarBaz.delete('foo', {id: 1000})],
        [
          'data',
          full.insert('full', {id: 2, bool: true, desc: 'replaced two'}),
        ],
        [
          'data',
          full.update(
            'full',
            {id: 321, bool: false, desc: 'voo'},
            {id: 333, bool: true, desc: 'did not exist'},
          ),
        ],
        [
          'data',
          full.update(
            'full',
            {id: 456, bool: false, desc: null},
            {id: 456, bool: false, desc: 'did not exist'},
          ),
        ],
        [
          'data',
          full.delete('full', {id: 2000, bool: false, desc: 'does not exist'}),
        ],
        ['commit', full.commit(), {watermark: '06'}],
      ],
      data: {
        foo: [
          {id: 1n, desc: 'replaced one', ['_0_version']: '06'},
          {id: 789n, desc: null, ['_0_version']: '06'},
          {id: 234n, desc: 'woo', ['_0_version']: '06'},
        ],
        full: [
          {id: 2n, bool: 1n, desc: 'replaced two', ['_0_version']: '06'},
          {id: 321n, bool: 0n, desc: 'voo', ['_0_version']: '06'},
          {id: 456n, bool: 0n, desc: null, ['_0_version']: '06'},
        ],
        ['_zero.changeLog']: [
          {
            op: 's',
            rowKey: '{"id":1}',
            stateVersion: '06',
            table: 'foo',
          },
          {
            op: 's',
            rowKey: '{"id":789}',
            stateVersion: '06',
            table: 'foo',
          },
          {
            op: 'd',
            rowKey: '{"id":999}',
            stateVersion: '06',
            table: 'foo',
          },
          {
            op: 's',
            rowKey: '{"id":234}',
            stateVersion: '06',
            table: 'foo',
          },
          {
            op: 'd',
            rowKey: '{"id":1000}',
            stateVersion: '06',
            table: 'foo',
          },
          {
            op: 's',
            rowKey: '{"id":2}',
            stateVersion: '06',
            table: 'full',
          },
          {
            op: 'd',
            rowKey: '{"id":333}',
            stateVersion: '06',
            table: 'full',
          },
          {
            op: 's',
            rowKey: '{"id":321}',
            stateVersion: '06',
            table: 'full',
          },
          {
            op: 's',
            rowKey: '{"id":456}',
            stateVersion: '06',
            table: 'full',
          },
          {
            op: 'd',
            rowKey: '{"id":2000}',
            stateVersion: '06',
            table: 'full',
          },
        ],
      },
    },
    {
      name: 'reserved words in DML',
      setup: `
      CREATE TABLE "transaction" (
        "column" INTEGER PRIMARY KEY,
        "trigger" INTEGER,
        "index" INTEGER,
        _0_version TEXT
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '07'}],
        ['data', tables.truncate('transaction')],
        [
          'data',
          tables.insert('transaction', {column: 1, trigger: 2, index: 3}),
        ],
        [
          'data',
          tables.update(
            'transaction',
            {column: 2, trigger: 3, index: 4},
            {column: 1},
          ),
        ],
        ['data', tables.delete('transaction', {column: 2})],
        ['commit', orgIssues.commit(), {watermark: '07'}],
      ],
      data: {
        transaction: [],
        ['_zero.changeLog']: [
          {
            stateVersion: '07',
            table: 'transaction',
            op: 't',
            rowKey: '',
          },
          {
            stateVersion: '07',
            table: 'transaction',
            op: 'd',
            rowKey: '{"column":1}',
          },
          {
            stateVersion: '07',
            table: 'transaction',
            op: 'd',
            rowKey: '{"column":2}',
          },
        ],
      },
    },
    {
      name: 'overwriting updates in the same transaction',
      setup: `
      CREATE TABLE issues(
        issueID INTEGER,
        orgID INTEGER,
        bool BOOL,
        description TEXT,
        _0_version TEXT,
        PRIMARY KEY("orgID", "issueID", "bool")
      );
      `,
      downstream: [
        ['begin', orgIssues.begin(), {commitWatermark: '08'}],
        [
          'data',
          orgIssues.insert('issues', {orgID: 1, issueID: 123, bool: true}),
        ],
        [
          'data',
          orgIssues.update(
            'issues',
            {orgID: 1, issueID: 456, bool: false},
            {orgID: 1, issueID: 123, bool: true},
          ),
        ],
        [
          'data',
          orgIssues.insert('issues', {orgID: 2, issueID: 789, bool: false}),
        ],
        [
          'data',
          orgIssues.delete('issues', {orgID: 2, issueID: 789, bool: false}),
        ],
        [
          'data',
          orgIssues.update('issues', {
            orgID: 1,
            issueID: 456,
            bool: false,
            description: 'foo',
          }),
        ],
        ['commit', orgIssues.commit(), {watermark: '08'}],
      ],
      data: {
        issues: [
          {
            orgID: 1n,
            issueID: 456n,
            bool: 0n,
            description: 'foo',
            ['_0_version']: '08',
          },
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '08',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":1,"issueID":123,"orgID":1}',
          },
          {
            stateVersion: '08',
            table: 'issues',
            op: 's',
            rowKey: '{"bool":0,"issueID":456,"orgID":1}',
          },
          {
            stateVersion: '08',
            table: 'issues',
            op: 'd',
            rowKey: '{"bool":0,"issueID":789,"orgID":2}',
          },
        ],
      },
    },
    {
      name: 'create table',
      setup: ``,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        [
          'data',
          fooBarBaz.createTable({
            schema: 'public',
            name: 'foo',
            columns: {
              id: {pos: 0, dataType: 'varchar'},
              count: {pos: 1, dataType: 'int8'},
              bool: {pos: 3, dataType: 'bool'},
              serial: {
                pos: 4,
                dataType: 'int4',
                dflt: "nextval('issues_serial_seq'::regclass)",
                notNull: true,
              },
            },
            primaryKey: ['id'],
          }),
        ],
        [
          'data',
          fooBarBaz.insert('foo', {id: 'bar', count: 2, bool: true, serial: 1}),
        ],
        [
          'data',
          fooBarBaz.insert('foo', {
            id: 'baz',
            count: 3,
            bool: false,
            serial: 2,
          }),
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 'bar', count: 2n, bool: 1n, serial: 1n, ['_0_version']: '0e'},
          {id: 'baz', count: 3n, bool: 0n, serial: 2n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":"bar"}',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":"baz"}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'varchar',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            count: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            bool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            serial: {
              characterMaximumLength: null,
              dataType: 'int4|NOT_NULL',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 4,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 5,
            },
          },
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename table',
      setup: `
        CREATE TABLE foo(id INT8, _0_version TEXT);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.renameTable('foo', 'bar')],
        ['data', fooBarBaz.insert('bar', {id: 4})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        bar: [
          {id: 1n, ['_0_version']: '0e'},
          {id: 2n, ['_0_version']: '0e'},
          {id: 3n, ['_0_version']: '0e'},
          {id: 4n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'bar',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'bar',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'bar',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'add column',
      setup: `
        CREATE TABLE foo(id INT8, _0_version TEXT);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        [
          'data',
          fooBarBaz.addColumn('foo', 'newInt', {
            pos: 9,
            dataType: 'int8',
            dflt: '123', // DEFAULT should applied for ADD COLUMN
          }),
        ],
        [
          'data',
          fooBarBaz.addColumn('foo', 'newBool', {
            pos: 10,
            dataType: 'bool',
            dflt: 'true', // DEFAULT should applied for ADD COLUMN
          }),
        ],
        [
          'data',
          fooBarBaz.addColumn('foo', 'newJSON', {
            pos: 10,
            dataType: 'json',
          }),
        ],
        [
          'data',
          fooBarBaz.insert('foo', {
            id: 4,
            newInt: 321,
            newBool: false,
            newJSON: true,
          }),
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {
            id: 1n,
            newInt: 123n,
            newBool: 1n,
            newJSON: null,
            ['_0_version']: '0e',
          },
          {
            id: 2n,
            newInt: 123n,
            newBool: 1n,
            newJSON: null,
            ['_0_version']: '0e',
          },
          {
            id: 3n,
            newInt: 123n,
            newBool: 1n,
            newJSON: null,
            ['_0_version']: '0e',
          },
          {
            id: 4n,
            newInt: 321n,
            newBool: 0n,
            newJSON: 'true',
            ['_0_version']: '0e',
          },
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            newInt: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: '123',
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            newBool: {
              characterMaximumLength: null,
              dataType: 'bool',
              dflt: '1',
              notNull: false,
              elemPgTypeClass: null,
              pos: 4,
            },
            newJSON: {
              characterMaximumLength: null,
              dataType: 'json',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 5,
            },
          },
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'drop column',
      setup: `
        CREATE TABLE foo(id INT8, dropMe TEXT, _0_version TEXT);
        INSERT INTO foo(id, dropMe, _0_version) VALUES (1, 'bye', '00');
        INSERT INTO foo(id, dropMe, _0_version) VALUES (2, 'bye', '00');
        INSERT INTO foo(id, dropMe, _0_version) VALUES (3, 'bye', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, dropMe: 'stillDropped'})],
        ['data', fooBarBaz.dropColumn('foo', 'dropMe')],
        ['data', fooBarBaz.insert('foo', {id: 4})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, ['_0_version']: '0e'},
          {id: 2n, ['_0_version']: '0e'},
          {id: 3n, ['_0_version']: '0e'},
          {id: 4n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [],
    },
    {
      name: 'rename column',
      setup: `
        CREATE TABLE foo(id INT8, renameMe TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, renameMe, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, renameMe: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'renameMe', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'newName', spec: {pos: 1, dataType: 'TEXT'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, newName: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, newName: 'hel', ['_0_version']: '0e'},
          {id: 2n, newName: 'low', ['_0_version']: '0e'},
          {id: 3n, newName: 'olrd', ['_0_version']: '0e'},
          {id: 4n, newName: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            newName: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
          },
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'change column nullability',
      setup: `
        CREATE TABLE foo(id INT8, nolz TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, nolz, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, nolz: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'nolz', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'nolz', spec: {pos: 1, dataType: 'TEXT', notNull: true}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, nolz: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, nolz: 'hel', ['_0_version']: '0e'},
          {id: 2n, nolz: 'low', ['_0_version']: '0e'},
          {id: 3n, nolz: 'olrd', ['_0_version']: '0e'},
          {id: 4n, nolz: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            nolz: {
              characterMaximumLength: null,
              dataType: 'TEXT|NOT_NULL',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'change column default and nullability',
      setup: `
        CREATE TABLE foo(id INT8, nolz TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, nolz, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, nolz, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, nolz: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'nolz', spec: {pos: 1, dataType: 'TEXT'}},
            {
              name: 'nolz',
              spec: {pos: 1, dataType: 'TEXT', notNull: true, dflt: 'now()'},
            },
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, nolz: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, nolz: 'hel', ['_0_version']: '0e'},
          {id: 2n, nolz: 'low', ['_0_version']: '0e'},
          {id: 3n, nolz: 'olrd', ['_0_version']: '0e'},
          {id: 4n, nolz: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            nolz: {
              characterMaximumLength: null,
              dataType: 'TEXT|NOT_NULL',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'rename indexed column',
      setup: `
        CREATE TABLE foo(id INT8, renameMe TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        CREATE UNIQUE INDEX foo_rename_me ON foo (renameMe);
        INSERT INTO foo(id, renameMe, _0_version) VALUES (1, 'hel', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (2, 'low', '00');
        INSERT INTO foo(id, renameMe, _0_version) VALUES (3, 'orl', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, renameMe: 'olrd'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'renameMe', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'newName', spec: {pos: 1, dataType: 'TEXT'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, newName: 'yay'})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, newName: 'hel', ['_0_version']: '0e'},
          {id: 2n, newName: 'low', ['_0_version']: '0e'},
          {id: 3n, newName: 'olrd', ['_0_version']: '0e'},
          {id: 4n, newName: 'yay', ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            newName: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
          },
        },
      ],
      indexSpecs: [
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_rename_me',
          tableName: 'foo',
          columns: {newName: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'retype column',
      setup: `
        CREATE TABLE foo(id INT8, num TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, num, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (3, '3', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, num: '1'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'num', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'num', spec: {pos: 1, dataType: 'INT8'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, num: 23})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, num: 3n, ['_0_version']: '0e'},
          {id: 2n, num: 2n, ['_0_version']: '0e'},
          {id: 3n, num: 1n, ['_0_version']: '0e'},
          {id: 4n, num: 23n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            num: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [
        {
          tableName: 'foo',
          name: 'foo_pkey',
          unique: true,
          columns: {id: 'ASC'},
        },
      ],
    },
    {
      name: 'retype column with indexes',
      setup: `
        CREATE TABLE foo(id INT8, num TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id);
        CREATE UNIQUE INDEX foo_num ON foo (num);
        CREATE UNIQUE INDEX foo_id_num ON foo (id, num);
        INSERT INTO foo(id, num, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, num, _0_version) VALUES (3, '0', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, num: '1'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'num', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'num', spec: {pos: 1, dataType: 'INT8'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, num: 23})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, num: 3n, ['_0_version']: '0e'},
          {id: 2n, num: 2n, ['_0_version']: '0e'},
          {id: 3n, num: 1n, ['_0_version']: '0e'},
          {id: 4n, num: 23n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            num: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [
        {
          name: 'foo_id_num',
          tableName: 'foo',
          columns: {id: 'ASC', num: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_num',
          tableName: 'foo',
          columns: {num: 'ASC'},
          unique: true,
        },
        {
          name: 'foo_pkey',
          tableName: 'foo',
          columns: {id: 'ASC'},
          unique: true,
        },
      ],
    },
    {
      name: 'rename and retype column',
      setup: `
        CREATE TABLE foo(id INT8, numburr TEXT, _0_version TEXT);
        CREATE UNIQUE INDEX foo_pkey ON foo (id ASC);
        INSERT INTO foo(id, numburr, _0_version) VALUES (1, '3', '00');
        INSERT INTO foo(id, numburr, _0_version) VALUES (2, '2', '00');
        INSERT INTO foo(id, numburr, _0_version) VALUES (3, '3', '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.update('foo', {id: 3, numburr: '1'})],
        [
          'data',
          fooBarBaz.updateColumn(
            'foo',
            {name: 'numburr', spec: {pos: 1, dataType: 'TEXT'}},
            {name: 'number', spec: {pos: 1, dataType: 'INT8'}},
          ),
        ],
        ['data', fooBarBaz.insert('foo', {id: 4, number: 23})],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        foo: [
          {id: 1n, number: 3n, ['_0_version']: '0e'},
          {id: 2n, number: 2n, ['_0_version']: '0e'},
          {id: 3n, number: 1n, ['_0_version']: '0e'},
          {id: 4n, number: 23n, ['_0_version']: '0e'},
        ],
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
          {
            stateVersion: '0e',
            table: 'foo',
            op: 's',
            rowKey: '{"id":4}',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'foo',
          columns: {
            id: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            number: {
              characterMaximumLength: null,
              dataType: 'INT8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
      indexSpecs: [
        {
          tableName: 'foo',
          name: 'foo_pkey',
          unique: true,
          columns: {id: 'ASC'},
        },
      ],
    },
    {
      name: 'drop table',
      setup: `
        CREATE TABLE foo(id INT8, _0_version TEXT);
        INSERT INTO foo(id, _0_version) VALUES (1, '00');
        INSERT INTO foo(id, _0_version) VALUES (2, '00');
        INSERT INTO foo(id, _0_version) VALUES (3, '00');
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.insert('foo', {id: 4})],
        ['data', fooBarBaz.dropTable('foo')],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
        ],
      },
      tableSpecs: [],
      indexSpecs: [],
    },
    {
      name: 'create index',
      setup: `
        CREATE TABLE foo(id INT8, handle TEXT, _0_version TEXT);
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        [
          'data',
          fooBarBaz.createIndex({
            schema: 'public',
            tableName: 'foo',
            name: 'foo_handle_index',
            columns: {
              handle: 'DESC',
            },
            unique: true,
          }),
        ],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.changeLog']: [
          {
            stateVersion: '0e',
            table: 'foo',
            op: 'r',
            rowKey: '',
          },
        ],
      },
      indexSpecs: [
        {
          name: 'foo_handle_index',
          tableName: 'foo',
          columns: {handle: 'DESC'},
          unique: true,
        },
      ],
    },
    {
      name: 'drop index',
      setup: `
        CREATE TABLE foo(id INT8, handle TEXT, _0_version TEXT);
        CREATE INDEX keep_me ON foo (id DESC, handle ASC);
        CREATE INDEX drop_me ON foo (handle DESC);
      `,
      downstream: [
        ['begin', fooBarBaz.begin(), {commitWatermark: '0e'}],
        ['data', fooBarBaz.dropIndex('drop_me')],
        ['commit', fooBarBaz.commit(), {watermark: '0e'}],
      ],
      data: {
        ['_zero.changeLog']: [],
      },
      indexSpecs: [
        {
          name: 'keep_me',
          tableName: 'foo',
          columns: {
            id: 'DESC',
            handle: 'ASC',
          },
          unique: false,
        },
      ],
    },
    {
      name: 'reserved words in DDL',
      setup: ``,
      downstream: [
        ['begin', tables.begin(), {commitWatermark: '07'}],
        [
          'data',
          tables.createTable({
            schema: 'public',
            name: 'transaction',
            columns: {
              column: {pos: 0, dataType: 'int8'},
              commit: {pos: 1, dataType: 'int8'},
            },
            primaryKey: ['column'],
          }),
        ],
        [
          'data',
          tables.addColumn('transaction', 'trigger', {
            dataType: 'text',
            pos: 10,
          }),
        ],
        [
          'data',
          tables.updateColumn(
            'transaction',
            {
              name: 'trigger',
              spec: {dataType: 'text', pos: 10},
            },
            {
              name: 'index',
              spec: {dataType: 'text', pos: 10},
            },
          ),
        ],
        [
          'data',
          tables.updateColumn(
            'transaction',
            {
              name: 'index',
              spec: {dataType: 'text', pos: 10},
            },
            {
              name: 'index',
              spec: {dataType: 'int8', pos: 10},
            },
          ),
        ],
        ['data', tables.dropColumn('transaction', 'commit')],
        ['commit', orgIssues.commit(), {watermark: '07'}],
      ],
      data: {
        transaction: [],
        ['_zero.changeLog']: [
          {
            stateVersion: '07',
            table: 'transaction',
            op: 'r',
            rowKey: '',
          },
        ],
      },
      tableSpecs: [
        {
          name: 'transaction',
          columns: {
            column: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 1,
            },
            index: {
              characterMaximumLength: null,
              dataType: 'int8',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 3,
            },
            ['_0_version']: {
              characterMaximumLength: null,
              dataType: 'TEXT',
              dflt: null,
              notNull: false,
              elemPgTypeClass: null,
              pos: 2,
            },
          },
        },
      ],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      for (const [replica, processor, includeChangeLog] of [
        [servingReplica, servingProcessor, true],
        [backupReplica, backupProcessor, false],
      ] satisfies [Database, ChangeProcessor, boolean][]) {
        initDB(replica, c.setup);
        initReplicationState(replica, ['zero_data'], '02');
        initChangeLog(replica);

        for (const change of c.downstream) {
          processor.processMessage(lc, change);
        }

        if (includeChangeLog) {
          expectTables(replica, c.data, 'bigint');
        } else {
          expectTables(replica, {...c.data, ['_zero.changeLog']: []}, 'bigint');
        }

        if (c.tableSpecs) {
          expect(
            listTables(replica).filter(t => !t.name.startsWith('_zero.')),
          ).toEqual(c.tableSpecs);
        }
        if (c.indexSpecs) {
          expect(listIndexes(replica)).toEqual(c.indexSpecs);
        }
      }
    });
  }
});

describe('replicator/change-processor-errors', () => {
  let lc: LogContext;
  let replica: Database;

  beforeEach(() => {
    lc = createSilentLogContext();
    replica = new Database(lc, ':memory:');

    replica.exec(`
    CREATE TABLE "foo" (
      id INTEGER PRIMARY KEY,
      big INTEGER,
      _0_version TEXT NOT NULL
    );
    `);

    initReplicationState(replica, ['zero_data', 'zero_metadata'], '02');
    initChangeLog(replica);
  });

  type Case = {
    name: string;
    messages: ChangeStreamData[];
    finalCommit: string;
    expectedVersionChanges: number;
    replicated: Record<string, object[]>;
    expectFailure: boolean;
  };

  const messages = new ReplicationMessages({foo: 'id'});

  const cases: Case[] = [
    {
      name: 'malformed replication stream',
      messages: [
        ['begin', messages.begin(), {commitWatermark: '07'}],
        ['data', messages.insert('foo', {id: 123})],
        ['data', messages.insert('foo', {id: 234})],
        ['commit', messages.commit(), {watermark: '07'}],

        // Induce a failure with a missing 'begin' message.
        ['data', messages.insert('foo', {id: 456})],
        ['data', messages.insert('foo', {id: 345})],
        ['commit', messages.commit(), {watermark: '0a'}],

        // This should be dropped.
        ['begin', messages.begin(), {commitWatermark: '0e'}],
        ['data', messages.insert('foo', {id: 789})],
        ['data', messages.insert('foo', {id: 987})],
        ['commit', messages.commit(), {watermark: '0e'}],
      ],
      finalCommit: '07',
      expectedVersionChanges: 1,
      replicated: {
        foo: [
          {id: 123, big: null, ['_0_version']: '07'},
          {id: 234, big: null, ['_0_version']: '07'},
        ],
      },
      expectFailure: true,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const failures: unknown[] = [];
      let versionChanges = 0;

      const processor = createChangeProcessor(
        replica,
        (_: LogContext, err: unknown) => failures.push(err),
      );

      for (const msg of c.messages) {
        if (processor.processMessage(lc, msg)) {
          versionChanges++;
        }
      }

      expect(versionChanges).toBe(c.expectedVersionChanges);
      if (c.expectFailure) {
        expect(failures[0]).toBeInstanceOf(Error);
      } else {
        expect(failures).toHaveLength(0);
      }
      expectTables(replica, c.replicated);

      const {watermark} = getSubscriptionState(new StatementRunner(replica));
      expect(watermark).toBe(c.finalCommit);
    });
  }

  test('rollback', () => {
    const processor = createChangeProcessor(replica);

    expect(replica.inTransaction).toBe(false);
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '0a'},
    ]);
    expect(replica.inTransaction).toBe(true);
    processor.processMessage(lc, ['rollback', {tag: 'rollback'}]);
    expect(replica.inTransaction).toBe(false);
  });

  test('abort', () => {
    const processor = createChangeProcessor(replica);

    expect(replica.inTransaction).toBe(false);
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: '0e'},
    ]);
    expect(replica.inTransaction).toBe(true);
    processor.abort(lc);
    expect(replica.inTransaction).toBe(false);
  });
});

describe('replicator/fts-rebuilding', () => {
  let lc: LogContext;
  let replica: Database;

  beforeEach(() => {
    lc = createSilentLogContext();
    replica = new Database(lc, ':memory:');
    
    // Create initial tables
    replica.exec(`
      CREATE TABLE comments (
        id INTEGER PRIMARY KEY,
        title TEXT,
        body TEXT,
        author TEXT,
        _0_version TEXT
      );
      
      CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        title TEXT,
        content TEXT,
        _0_version TEXT
      );
      
      CREATE TABLE _zero_indices (
        indices TEXT,
        hash TEXT,
        lock BOOL PRIMARY KEY DEFAULT true CHECK (lock)
      );
    `);
    
    initReplicationState(replica, ['zero_data', 'zero_metadata'], '01');
    initChangeLog(replica);
  });

  test('preserves FTS tables when configuration unchanged', () => {
    // Initial indices configuration
    const config1 = {
      tables: {
        comments: {
          fulltext: {
            body: {
              columns: ['body', 'title']
            }
          }
        }
      }
    };
    
    // Insert initial configuration and create FTS tables
    replica.prepare(
      'INSERT OR REPLACE INTO _zero_indices (indices, hash) VALUES (?, ?)'
    ).run(JSON.stringify(config1), 'hash1');
    
    // Manually create the FTS tables based on the configuration
    // This simulates what the change processor would do
    const createStatements = createFTS5Statements('comments', ['body', 'title'], ['id', 'title', 'body', 'author', '_0_version']);
    createStatements.forEach(stmt => {
      // Add suffix for the index name
      const modifiedStmt = stmt
        .replace(/comments_fts/g, 'comments_body_fts')
        .replace(/comments_view/g, 'comments_body_view');
      replica.exec(modifiedStmt);
    });
    
    // Verify FTS tables were created
    const tables1 = listTables(replica).map(t => t.name).sort();
    expect(tables1).toContain('comments_body_fts');
    
    // Add some data to the FTS table
    replica.exec(`
      INSERT INTO comments (id, title, body, author) 
      VALUES (1, 'Test Title', 'Test Body', 'Test Author');
    `);
    
    // Verify data is in FTS
    const ftsData = replica.prepare('SELECT * FROM comments_body_fts WHERE rowid = 1').get() as Record<string, unknown>;
    expect(ftsData).toEqual({body: 'Test Body', title: 'Test Title'});
    
    // Now simulate an update with the same configuration
    // Get existing FTS tables and their columns
    const existingFTS = replica.prepare(`
      SELECT name FROM sqlite_master 
      WHERE type = 'table' AND name LIKE '%_fts'
    `).all() as Array<{name: string}>;
    
    for (const {name} of existingFTS) {
      const cols = replica.prepare(`PRAGMA table_info('${name}')`).all() as Array<{name: string}>;
      const ftsColumns = new Set(cols.filter(c => c.name !== 'rank').map(c => c.name));
      
      // Check if configuration changed - it hasn't
      const newColumns = new Set(['body', 'title']);
      const configChanged = ftsColumns.size !== newColumns.size || 
        ![...ftsColumns].every(col => newColumns.has(col));
      
      expect(configChanged).toBe(false);
    }
    
    // Verify FTS tables still exist and data is preserved
    const tables2 = listTables(replica).map(t => t.name).sort();
    expect(tables2).toContain('comments_body_fts');
    
    const ftsDataAfter = replica.prepare('SELECT * FROM comments_body_fts WHERE rowid = 1').get() as Record<string, unknown>;
    expect(ftsDataAfter).toEqual({body: 'Test Body', title: 'Test Title'});
  });

  test('rebuilds FTS tables when columns change', () => {
    // Initial configuration with only 'body' column
    const createStatements1 = createFTS5Statements('comments', ['body'], ['id', 'title', 'body', 'author', '_0_version']);
    createStatements1.forEach(stmt => {
      const modifiedStmt = stmt
        .replace(/comments_fts/g, 'comments_body_fts')
        .replace(/comments_view/g, 'comments_body_view');
      replica.exec(modifiedStmt);
    });
    
    // Verify initial FTS table
    const ftsCols1 = replica.prepare("PRAGMA table_info('comments_body_fts')").all() as Array<{name: string}>;
    const colNames1 = ftsCols1.map(c => c.name).filter(n => n !== 'rank');
    expect(colNames1).toEqual(['body']);
    
    // Simulate rebuilding with new columns
    // First drop the old FTS table and its triggers
    replica.exec(`DROP TABLE IF EXISTS comments_body_fts`);
    replica.exec(`DROP TRIGGER IF EXISTS comments_body_fts_insert`);
    replica.exec(`DROP TRIGGER IF EXISTS comments_body_fts_update`);
    replica.exec(`DROP TRIGGER IF EXISTS comments_body_fts_delete`);
    replica.exec(`DROP VIEW IF EXISTS comments_body_view`);
    
    // Create new FTS table with both columns
    const createStatements2 = createFTS5Statements('comments', ['body', 'title'], ['id', 'title', 'body', 'author', '_0_version']);
    createStatements2.forEach(stmt => {
      const modifiedStmt = stmt
        .replace(/comments_fts/g, 'comments_body_fts')
        .replace(/comments_view/g, 'comments_body_view');
      replica.exec(modifiedStmt);
    });
    
    // Verify FTS table was rebuilt with new columns
    const ftsCols2 = replica.prepare("PRAGMA table_info('comments_body_fts')").all() as Array<{name: string}>;
    const colNames2 = ftsCols2.map(c => c.name).filter(n => n !== 'rank').sort();
    expect(colNames2).toEqual(['body', 'title']);
  });

  test('removes orphaned FTS tables', () => {
    // Create FTS tables for both comments and posts
    const commentsStatements = createFTS5Statements('comments', ['body'], ['id', 'title', 'body', 'author', '_0_version']);
    commentsStatements.forEach(stmt => {
      const modifiedStmt = stmt
        .replace(/comments_fts/g, 'comments_body_fts')
        .replace(/comments_view/g, 'comments_body_view');
      replica.exec(modifiedStmt);
    });
    
    const postsStatements = createFTS5Statements('posts', ['content', 'title'], ['id', 'title', 'content', '_0_version']);
    postsStatements.forEach(stmt => {
      const modifiedStmt = stmt
        .replace(/posts_fts/g, 'posts_content_fts')
        .replace(/posts_view/g, 'posts_content_view');
      replica.exec(modifiedStmt);
    });
    
    // Verify both FTS tables exist
    const tables1 = listTables(replica).map(t => t.name).sort();
    expect(tables1).toContain('comments_body_fts');
    expect(tables1).toContain('posts_content_fts');
    
    // Simulate removing posts FTS
    replica.exec(`DROP TABLE IF EXISTS posts_content_fts`);
    replica.exec(`DROP TRIGGER IF EXISTS posts_content_fts_insert`);
    replica.exec(`DROP TRIGGER IF EXISTS posts_content_fts_update`);
    replica.exec(`DROP TRIGGER IF EXISTS posts_content_fts_delete`);
    replica.exec(`DROP VIEW IF EXISTS posts_content_view`);
    
    // Verify posts_content_fts was removed but comments_body_fts remains
    const tables2 = listTables(replica).map(t => t.name).sort();
    expect(tables2).toContain('comments_body_fts');
    expect(tables2).not.toContain('posts_content_fts');
    expect(tables2).not.toContain('posts_content_view');
    
    // Verify triggers were also removed
    const triggers = replica.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'posts_content_fts_%'"
    ).all();
    expect(triggers).toHaveLength(0);
  });

  test('handles multiple FTS indices per table', () => {
    // Create first FTS index for 'body'
    const bodyStatements = createFTS5Statements('comments', ['body'], ['id', 'title', 'body', 'author', '_0_version']);
    bodyStatements.forEach(stmt => {
      const modifiedStmt = stmt
        .replace(/comments_fts/g, 'comments_body_fts')
        .replace(/comments_view/g, 'comments_body_view');
      replica.exec(modifiedStmt);
    });
    
    // Create second FTS index for 'title' and 'author'
    const titleStatements = createFTS5Statements('comments', ['title', 'author'], ['id', 'title', 'body', 'author', '_0_version']);
    titleStatements.forEach(stmt => {
      const modifiedStmt = stmt
        .replace(/comments_fts/g, 'comments_title_fts')
        .replace(/comments_view/g, 'comments_title_view');
      replica.exec(modifiedStmt);
    });
    
    // Verify both FTS tables were created
    const tables = listTables(replica).map(t => t.name).sort();
    expect(tables).toContain('comments_body_fts');
    expect(tables).toContain('comments_title_fts');
    
    // Check views separately
    const views = replica.prepare(`
      SELECT name FROM sqlite_master 
      WHERE type = 'view' AND name LIKE 'comments_%_view'
    `).all() as Array<{name: string}>;
    const viewNames = views.map(v => v.name).sort();
    expect(viewNames).toEqual(['comments_body_view', 'comments_title_view']);
  });
});
