import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {DefaultConfig, SchemaValue, TableSchema} from '../table-schema.ts';

/* eslint-disable @typescript-eslint/no-explicit-any */
export function table<TName extends string>(name: TName) {
  return new TableBuilder({
    name,
    columns: {},
    primaryKey: [] as any as PrimaryKey,
  });
}

export function string<T extends string = string>() {
  return new ColumnBuilder({
    type: 'string',
    optional: false,
    customType: null as unknown as T,
  });
}

export function number<T extends number = number>() {
  return new ColumnBuilder({
    type: 'number',
    optional: false,
    customType: null as unknown as T,
  });
}

export function boolean<T extends boolean = boolean>() {
  return new ColumnBuilder({
    type: 'boolean',
    optional: false,
    customType: null as unknown as T,
  });
}

export function json<T extends ReadonlyJSONValue = ReadonlyJSONValue>() {
  return new ColumnBuilder({
    type: 'json',
    optional: false,
    customType: null as unknown as T,
  });
}

export function enumeration<T extends string>() {
  return new ColumnBuilder({
    type: 'string',
    optional: false,
    customType: null as unknown as T,
  });
}

export const column = {
  string,
  number,
  boolean,
  json,
  enumeration,
};

export class TableBuilder<TShape extends TableSchema> {
  readonly #schema: TShape;
  constructor(schema: TShape) {
    this.#schema = schema;
  }

  /**
   * Allows the table to be named differently in the database.
   *
   * @param serverName - The name of the table in the database.
   */
  from<ServerName extends string>(serverName: ServerName) {
    return new TableBuilder<TShape>({
      ...this.#schema,
      // Strip the "public." schema if specified, as tables in the upstream
      // "public" schema are created without the schema prefix on the replica.
      // See liteTableName() in zero-cache/src/types/names.ts
      serverName: serverName.startsWith('public.')
        ? serverName.substring('public.'.length)
        : serverName,
    });
  }

  /**
   * Specifies the column definitions for the table.
   *
   * @param columns - The column definitions for the table.
   */
  columns<const TColumns extends Record<string, {schema: SchemaValue}>>(
    columns: TColumns,
  ): TableBuilderWithColumns<{
    name: TShape['name'];
    columns: {[K in keyof TColumns]: TColumns[K]['schema']};
    primaryKey: TShape['primaryKey'];
  }> {
    const columnSchemas = Object.fromEntries(
      Object.entries(columns).map(([k, v]) => [k, v.schema]),
    ) as {[K in keyof TColumns]: TColumns[K]['schema']};
    return new TableBuilderWithColumns({
      ...this.#schema,
      columns: columnSchemas,
    }) as any;
  }
}

export class TableBuilderWithColumns<TShape extends TableSchema> {
  readonly #schema: TShape;

  constructor(schema: TShape) {
    this.#schema = schema;
  }

  /**
   * Specifies the primary key(s) for the table.
   *
   * This cannot include columns that have default values since these are
   * generated separately on the client and server.
   */
  primaryKey<TPKColNames extends (keyof TShape['columns'])[]>(
    ...pkColumnNames: TPKColNames
  ) {
    return new TableBuilderWithColumns({
      ...this.#schema,
      primaryKey: pkColumnNames,
    });
  }

  get schema() {
    return this.#schema;
  }

  build() {
    // We can probably get the type system to throw an error if primaryKey is not called
    // before passing the schema to createSchema
    // Till then --
    if (this.#schema.primaryKey.length === 0) {
      throw new Error(`Table "${this.#schema.name}" is missing a primary key`);
    }
    const names = new Set<string>();
    for (const [col, {serverName}] of Object.entries(this.#schema.columns)) {
      const name = serverName ?? col;
      if (names.has(name)) {
        throw new Error(
          `Table "${
            this.#schema.name
          }" has multiple columns referencing "${name}"`,
        );
      }
      names.add(name);
    }
    return this.#schema;
  }
}

class ColumnBuilder<TShape extends SchemaValue> {
  readonly #schema: TShape;
  constructor(schema: TShape) {
    this.#schema = schema;
  }

  /**
   * Allows the column to be named differently in the database.
   *
   * @param serverName - The name of the column in the database.
   */
  from<ServerName extends string>(serverName: ServerName) {
    return new ColumnBuilder<TShape & {serverName: string}>({
      ...this.#schema,
      serverName,
    });
  }

  /**
   * Allows the column to be `null` or undefined on insert.
   */
  optional(): ColumnBuilder<Omit<TShape, 'optional'> & {optional: true}> {
    return new ColumnBuilder({
      ...this.#schema,
      optional: true,
    });
  }

  /**
   * Allows specifying a database-generated default value for a column on insert and/or update operations.
   *
   * @example
   * ```ts
   * const member = table('member')
   *   .columns({
   *     id: string(),
   *     createdAt: number().default({
   *       insert: {
   *         server: 'db'
   *       }
   *     }),
   *     updatedAt: number().default({
   *       insert: {
   *         server: 'db',
   *       },
   *       update: {
   *         server: 'db',
   *       },
   *     }),
   *   })
   *   .primaryKey('id');
   * ```
   */
  default<TDefaultConfig extends DefaultConfig>(
    config: TDefaultConfig,
  ): ColumnBuilder<
    Omit<TShape, 'defaultConfig'> & {
      defaultConfig: TDefaultConfig;
    }
  > {
    return new ColumnBuilder({
      ...this.#schema,
      defaultConfig: config,
    });
  }

  get schema() {
    return this.#schema;
  }
}

export type {ColumnBuilder};
