import type {Faker} from '@faker-js/faker';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {
  Relationship,
  RelationshipsSchema,
  SchemaValue,
  TableSchema,
} from '../../../../zero-schema/src/table-schema.ts';
import {generateUniqueValues, selectRandom, shuffle, type Rng} from './util.ts';

const dbTypes = {
  string: ['text', 'varchar', 'char'],
  number: ['int', 'bigint', 'decimal'],
  boolean: ['bool'],
  json: ['jsonb', 'json'],
} as const;

export function generateSchema(
  rng: Rng,
  faker: Faker,
  numTables?: number,
): Schema {
  const tables = generateUniqueValues(
    faker.word.noun,
    numTables ?? Math.floor(rng() * 10) + 1,
  ).map(name => generateTable(name, rng, faker));
  const relationships = generateRelationships(rng, tables);

  return {
    tables: Object.fromEntries(tables.map(table => [table.name, table])),
    relationships,
  };
}

function generateTable(name: string, rng: Rng, faker: Faker): TableSchema {
  const columns = generateUniqueValues(
    faker.word.noun,
    Math.floor(rng() * 10) + 1,
  ).map(name => generateColumn(name, rng));
  const numPkColumns = Math.min(rng() < 0.5 ? 1 : 2, columns.length);

  return {
    name,
    columns: Object.fromEntries(columns),
    primaryKey:
      numPkColumns === 1 ? [columns[0][0]] : [columns[0][0], columns[1][0]],
  };
}

function generateRelationships(
  rng: Rng,
  tables: TableSchema[],
): {
  [table: string]: RelationshipsSchema;
} {
  const relationships: {[table: string]: RelationshipsSchema} = {};
  for (const table of tables) {
    relationships[table.name] = generateRelationshipsForTable(
      rng,
      table,
      tables,
    );
  }
  return relationships;
}

function generateRelationshipsForTable(
  rng: Rng,
  table: TableSchema,
  tables: TableSchema[],
): RelationshipsSchema {
  const numRelationships = Math.floor(rng() * 3);
  if (numRelationships === 0) {
    return {};
  }

  const shuffledTables = shuffle(rng, tables);

  const relationships: Record<string, Relationship> = {};
  for (let i = 0; i < numRelationships; i++) {
    const destTable = shuffledTables[i];
    const sourceField = selectRandom(rng, Object.keys(table.columns));
    if (table.columns[sourceField].type === 'json') {
      continue;
    }

    const destTargets = Object.entries(destTable.columns).filter(
      ([_, column]) => column.type === table.columns[sourceField].type,
    );
    if (destTargets.length === 0) {
      continue;
    }

    const destField = selectRandom(rng, destTargets);
    const cardinality =
      destField[1].optional || destField[1].type === 'boolean'
        ? 'many'
        : selectRandom(rng, ['one', 'many'] as const);

    relationships[destTable.name] = [
      {
        sourceField: [sourceField],
        destField: [destField[0]],
        destSchema: destTable.name,
        cardinality,
      },
    ];
  }

  return relationships;
}

function generateColumn(name: string, rng: Rng): [string, SchemaValue] {
  return [
    name,
    {
      type: selectRandom(rng, [
        'string',
        'string',
        'string',
        'string',
        'string',
        'boolean',
        'number',
        'number',
        'number',
        'json',
      ]) as keyof typeof dbTypes,
      optional: rng() < 0.5,
      ...(rng() < 0.7
        ? {
            defaultConfig: {
              insert: {
                server: 'db',
              },
              ...(rng() < 0.5
                ? {
                    update: {
                      server: 'db',
                    },
                  }
                : {}),
            },
          }
        : {}),
    },
  ];
}
