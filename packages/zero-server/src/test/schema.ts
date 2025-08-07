import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';

const jsonCols = {
  str: json<string>(),
  num: json<number>(),
  bool: json<boolean>(),
  nil: json<null>(),
  obj: json<{foo: string}>(),
  arr: json<string[]>(),
} as const;

export const schema = createSchema({
  tables: [
    table('basic')
      .columns({
        id: string(),
        a: number(),
        b: string(),
        c: boolean().nullable(),
      })
      .primaryKey('id'),
    table('names')
      .from('divergent_names')
      .columns({
        id: string().from('divergent_id'),
        a: number().from('divergent_a'),
        b: string().from('divergent_b'),
        c: boolean().from('divergent_c').nullable(),
      })
      .primaryKey('id'),
    table('compoundPk')
      .columns({
        a: string(),
        b: number(),
        c: string().nullable(),
      })
      .primaryKey('a', 'b'),
    table('dateTypes')
      .columns({
        ts: number(),
        tstz: number(),
        tswtz: number(),
        tswotz: number(),
        d: number(),
      })
      .primaryKey('ts'),
    table('jsonCases')
      .columns({
        ...jsonCols,
        str: string(),
      })
      .primaryKey('str'),
    table('jsonbCases').columns(jsonCols).primaryKey('str'),
    table('typesWithParams')
      .from('types_with_params')
      .columns({
        id: string(),
        char: string(),
        varchar: string(),
        numeric: number(),
        decimal: number(),
      })
      .primaryKey('id'),
    table('uuidAndEnum')
      .columns({
        id: string(),
        // eslint-disable-next-line @typescript-eslint/naming-convention
        reference_id: string(),
        status: enumeration<'active' | 'inactive' | 'pending'>(),
        type: enumeration<'user' | 'system' | 'admin'>(),
      })
      .primaryKey('id'),
    table('alternate_basic')
      .from('alternate_schema.basic')
      .columns({
        id: string(),
        a: number(),
        b: string(),
        c: boolean().nullable(),
      })
      .primaryKey('id'),
    table('defaults')
      .columns({
        id: string(),
        // eslint-disable-next-line @typescript-eslint/naming-convention
        insert_db_generated: string().default({
          insert: {
            server: 'db',
          },
        }),
        // eslint-disable-next-line @typescript-eslint/naming-convention
        update_db_generated: string()
          .default({
            update: {
              server: 'db',
            },
          })
          .nullable(),
        // eslint-disable-next-line @typescript-eslint/naming-convention
        insert_update_db_generated: string().default({
          insert: {
            server: 'db',
          },
          update: {
            server: 'db',
          },
        }),
      })
      .primaryKey('id'),
  ],
  relationships: [],
});

export const schemaSql = `CREATE TABLE basic (
  id TEXT PRIMARY KEY,
  a INTEGER,
  b TEXT,
  C BOOLEAN
);

CREATE TABLE divergent_names (
  divergent_id TEXT PRIMARY KEY,
  divergent_a INTEGER,
  divergent_b TEXT,
  divergent_c BOOLEAN
);

CREATE TABLE "compoundPk" (
  a TEXT,
  b INTEGER,
  c TEXT,
  PRIMARY KEY (a, b)
);

CREATE TABLE "dateTypes" (
  "ts" TIMESTAMP,
  "tstz" TIMESTAMPTZ,
  "tswtz" TIMESTAMP WITH TIME ZONE,
  "tswotz" TIMESTAMP WITHOUT TIME ZONE,
  "d" DATE,
  PRIMARY KEY ("ts")
);

CREATE TABLE "jsonbCases" (
  "str" JSONB,
  "num" JSONB,
  "bool" JSONB,
  "nil" JSONB,
  "obj" JSONB,
  "arr" JSONB,
  PRIMARY KEY ("str")
);

CREATE TABLE "jsonCases" (
  "str" TEXT,
  "num" JSON,
  "bool" JSON,
  "nil" JSON,
  "obj" JSON,
  "arr" JSON,
  PRIMARY KEY ("str")
);

CREATE TABLE types_with_params (
  id TEXT PRIMARY KEY,
  char CHAR(10),
  varchar VARCHAR(20),
  numeric NUMERIC(8, 3),
  decimal DECIMAL(10, 5)
);


CREATE TYPE "statusEnum" AS ENUM ('active', 'inactive', 'pending');
CREATE TYPE type_enum AS ENUM ('user', 'system', 'admin');

CREATE TABLE "uuidAndEnum" (
  "id" UUID PRIMARY KEY,
  "reference_id" UUID NOT NULL,
  "status" "statusEnum" NOT NULL,
  "type" type_enum NOT NULL
);

CREATE SCHEMA alternate_schema;

CREATE TABLE alternate_schema.basic (
  id TEXT PRIMARY KEY,
  a INTEGER,
  b TEXT,
  C BOOLEAN
);

CREATE TABLE "defaults" (
  id TEXT PRIMARY KEY,
  "insert" TEXT,
  "update" TEXT,
  "insert_update" TEXT,
  "insert_db_generated" TEXT DEFAULT 'db-insert-default-1',
  "update_db_generated" TEXT,
  "insert_update_db_generated" TEXT DEFAULT 'db-insert-update-default-2'
);

CREATE OR REPLACE FUNCTION update_defaults_trigger()
RETURNS TRIGGER AS $$
BEGIN
  NEW."update_db_generated" = 'db-update-default-3';
  NEW."insert_update_db_generated" = 'db-insert-update-default-4';
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER defaults_update_trigger
  BEFORE UPDATE ON "defaults"
  FOR EACH ROW
  EXECUTE FUNCTION update_defaults_trigger();
`;

export const seedDataSql = `
INSERT INTO basic (id, a, b, c) VALUES ('1', 2, 'foo', true);
INSERT INTO divergent_names (divergent_id, divergent_a, divergent_b, divergent_c) VALUES ('2', 3, 'bar', false);
INSERT INTO "compoundPk" (a, b, c) VALUES ('a', 1, 'c');
INSERT INTO "dateTypes" (ts, tstz, tswtz, tswotz) VALUES (
  '2021-01-01 00:00:01',
  '2022-02-02 00:00:02',
  '2023-03-03 00:00:03',
  '2024-04-04 00:00:04'
);
`;
