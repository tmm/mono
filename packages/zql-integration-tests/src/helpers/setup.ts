import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import type {ServerSchema} from '../../../zero-schema/src/server-schema.ts';
import {generateShrinkableQuery} from '../../../zql/src/query/test/query-gen.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';

export async function fillPgAndSync(
  schema: Schema,
  createTableSQL: string,
  testData: Record<string, Row[]>,
  dbName: string,
) {
  const lc = createSilentLogContext();
  const pg = await testDBs.create(dbName, undefined, false);

  await pg.unsafe(createTableSQL);
  const sqlite = new Database(lc, ':memory:');

  const mapper = clientToServer(schema.tables);
  for (const [table, rows] of Object.entries(testData)) {
    const columns = Object.keys(rows[0]);
    const forPg = rows.map(row =>
      columns.reduce(
        (acc, c) => ({
          ...acc,
          [mapper.columnName(table, c)]: row[c as keyof typeof row],
        }),
        {} as Record<string, unknown>,
      ),
    );
    await pg`INSERT INTO ${pg(mapper.tableName(table))} ${pg(forPg)}`;
  }

  await initialSync(
    lc,
    {appID: 'collate_test', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );

  return {pg, sqlite};
}

export function createCase(
  schema: Schema,
  serverSchema: ServerSchema,
  seed?: number | undefined,
) {
  seed = seed ?? Date.now() ^ (Math.random() * 0x100000000);
  const randomizer = generateMersenne53Randomizer(seed);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  return {
    seed,
    query: generateShrinkableQuery(schema, {}, rng, faker, serverSchema),
  };
}
