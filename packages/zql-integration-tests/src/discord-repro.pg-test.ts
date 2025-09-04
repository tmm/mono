import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {beforeAll, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {createTableSQL, schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';

const lc = createSilentLogContext();

let pg: PostgresDB;
let sqlite: Database;
type Schema = typeof schema;
let issueQuery: Query<Schema, 'issue'>;
let queryDelegate: QueryDelegate;

beforeAll(async () => {
  pg = await testDBs.create('discord-repro');
  await pg.unsafe(createTableSQL);
  sqlite = new Database(lc, ':memory:');

  await pg.unsafe(/*sql*/ `
    INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES (
      'issue1', 'Test Issue 1', 'Description for issue 1', false, 'user1', TIMESTAMPTZ '2001-02-16T20:38:40.000Z'
    );

    INSERT INTO "users" ("id", "name") VALUES (
      'user1', 'User 1'
    );

    INSERT INTO "comments" ("id", "authorId", "issue_id", text, "createdAt") VALUES (
      'comment1', 'user1', 'issue1', 'Comment 1', TIMESTAMP '2002-03-16 20:38:40'
    );
  `);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'discord_repro', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );

  queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);
  issueQuery = newQuery(queryDelegate, schema, 'issue');
});

test.only('flip', async () => {
  const q = issueQuery
    .whereExists('comments', q => q.where('authorId', '=', 'user1'), {
      flip: true,
    })
    .related('comments');

  const data = await q;
  console.log(data);
});

test('discord report https://discord.com/channels/830183651022471199/1347550174968287233/1347552521865920616', () => {
  /**
   The discord query:
   eb.or(
        eb.cmp('ownerId', '=', authData.sub!),
        eb.and(
            eb.cmp('shared', '=', true),
            eb.exists('states', (q) => q.where('userId', '=', authData.sub!))
        )
    )

    Below is the same form. Using `closed` to stand in for `shared` and `comments` to stand in for `states`.
   */
  const q = issueQuery
    .where('id', 'issue1')
    .where(eb =>
      eb.or(
        eb.cmp('ownerId', '=', 'user1'),
        eb.and(
          eb.cmp('closed', '=', false),
          eb.exists('comments', q => q.where('authorId', '=', 'user1')),
        ),
      ),
    )
    .related('comments');

  const view = q.materialize();

  expect(mapResultToClientNames(view.data, schema, 'issue'))
    .toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "comments": [
            {
              "authorId": "user1",
              "createdAt": 1016311120000,
              "id": "comment1",
              "issueId": "issue1",
              "text": "Comment 1",
            },
          ],
          "createdAt": 982355920000,
          "description": "Description for issue 1",
          "id": "issue1",
          "ownerId": "user1",
          "title": "Test Issue 1",
        },
      ]
    `);

  queryDelegate.getSource('issues')?.push({
    type: 'edit',
    oldRow: {
      id: 'issue1',
      title: 'Test Issue 1',
      description: 'Description for issue 1',
      closed: false,
      ownerId: 'user1',
    },
    row: {
      id: 'issue1',
      title: 'Test Issue 1',
      description: 'Description for issue 1',
      closed: true,
      ownerId: 'user1',
    },
  });

  expect(mapResultToClientNames(view.data, schema, 'issue')).toEqual(
    q.materialize().data,
  );
});
