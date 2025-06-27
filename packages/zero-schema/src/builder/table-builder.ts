import type {Expand} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {
  DefaultValueFunction,
  SchemaValue,
  SchemaValueWithCustomType,
  TableSchema,
} from '../table-schema.ts';

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
    nullable: false,
    customType: null as unknown as T,
  });
}

export function number<T extends number = number>() {
  return new ColumnBuilder({
    type: 'number',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function boolean<T extends boolean = boolean>() {
  return new ColumnBuilder({
    type: 'boolean',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function json<T extends ReadonlyJSONValue = ReadonlyJSONValue>() {
  return new ColumnBuilder({
    type: 'json',
    nullable: false,
    customType: null as unknown as T,
  });
}

export function enumeration<T extends string>() {
  return new ColumnBuilder({
    type: 'string',
    nullable: false,
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

/**
 * Utility type to exclude columns that have insertDefault or updateDefault properties
 * from being used as primary keys.
 */
type ColumnsWithoutDefaults<TColumns> = {
  [K in keyof TColumns]: TColumns[K] extends
    | {insertDefault: any}
    | {updateDefault: any}
    ? never
    : K;
}[keyof TColumns];

export class TableBuilderWithColumns<TShape extends TableSchema> {
  readonly #schema: TShape;

  constructor(schema: TShape) {
    this.#schema = schema;
  }

  /**
   * Specifies the primary key(s) for the table.
   *
   * This cannot include columns that have an `insertDefault` or `updateDefault`
   * property, since these are generated separately on the client and server.
   */
  primaryKey<
    TPKColNames extends Expand<ColumnsWithoutDefaults<TShape['columns']>>[],
  >(...pkColumnNames: TPKColNames) {
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

class ColumnBuilder<TShape extends SchemaValue<any>> {
  protected readonly _schema: TShape;
  constructor(schema: TShape) {
    this._schema = schema;
  }

  /**
   * Allows the column to be named differently in the database.
   *
   * @param serverName - The name of the column in the database.
   */
  from<ServerName extends string>(serverName: ServerName) {
    return new ColumnBuilder<TShape & {serverName: string}>({
      ...this._schema,
      serverName,
    });
  }

  /**
   * Allows the column to be `null` or undefined on insert.
   *
   * This affects the select model of the table - columns with `nullable` will be
   * nullable on select.
   */
  nullable(): ColumnBuilder<Omit<TShape, 'nullable'> & {nullable: true}> {
    return new ColumnBuilder({
      ...this._schema,
      nullable: true,
    });
  }

  /**
   * Provides a default value for the column when a new row is inserted.
   *
   * **The default value generated by the client will never be sent to the server.**
   * It can **only** run the same function independently on the client and server,
   * which may result in different values being generated on the server. If you want
   * the same value on the client and server (e.g. for an ID column), you should
   * generate values outside of the mutation.
   *
   * By default, the `onInsert` function will run on the server.
   * Use `dbGenerated('insert')` to run the `onInsert` value function only on the client.
   *
   * @example
   * ```ts
   * const member = table('member')
   *   .columns({
   *     id: string(),
   *     createdAt: number().onInsert(() => Date.now()),
   *   })
   *   .primaryKey('id');
   * ```
   */
  onInsert<
    T extends TShape extends SchemaValueWithCustomType<infer V> ? V : never,
  >(
    onInsert: DefaultValueFunction<T>,
  ): ColumnBuilderWithDefault<
    Omit<TShape, 'insertDefault'> & {
      insertDefault: DefaultValueFunction<T>;
    }
  > {
    return new ColumnBuilderWithDefault({
      ...this._schema,
      insertDefault: onInsert,
    });
  }

  /**
   * Provides a default value for the column when a row is updated.
   *
   * **The default value generated by the client will never be sent to the server.**
   * It can **only** run the same function independently on the client and server,
   * which may result in different values being generated on the server. If you want
   * the same value on the client and server (e.g. for an ID column), you should
   * generate values outside of the mutation.
   *
   * By default, the `onUpdate` function will run on the server.
   * Use `dbGenerated('update')` to run the `onUpdate` value function only on the client.
   *
   * @example
   * ```ts
   * const member = table('member')
   *   .columns({
   *     id: string(),
   *     updatedAt: number().onUpdate(() => Date.now()),
   *   })
   *   .primaryKey('id');
   * ```
   */
  onUpdate<
    T extends TShape extends SchemaValueWithCustomType<infer V> ? V : never,
  >(
    onUpdate: DefaultValueFunction<T>,
  ): ColumnBuilderWithDefault<
    Omit<TShape, 'updateDefault'> & {
      updateDefault: DefaultValueFunction<T>;
    }
  > {
    return new ColumnBuilderWithDefault({
      ...this._schema,
      updateDefault: onUpdate,
    });
  }

  get schema() {
    return this._schema;
  }
}

type ClientOnlyOptions<TShape extends SchemaValue<any>> = TShape extends {
  insertDefault: DefaultValueFunction<any>;
  updateDefault: DefaultValueFunction<any>;
}
  ?
      | readonly ['insert', 'update']
      | readonly ['update', 'insert']
      | readonly ['insert']
      | readonly ['update']
  : TShape extends {
        insertDefault: DefaultValueFunction<any>;
      }
    ? readonly ['insert']
    : TShape extends {
          updateDefault: DefaultValueFunction<any>;
        }
      ? readonly ['update']
      : never;

class ColumnBuilderWithDefault<
  TShape extends SchemaValue<any>,
  TClientOnlyOptions extends
    ClientOnlyOptions<TShape> = ClientOnlyOptions<TShape>,
> extends ColumnBuilder<TShape> {
  constructor(schema: TShape) {
    super(schema);
  }

  /**
   * Specifies whether an `onInsert` or `onUpdate` are generated by the database
   * and should not be run on the server.
   *
   * @example
   * ```ts
   * const member = table('member')
   *   .columns({
   *     id: string(),
   *     createdAt: number().onInsert(() => Date.now()).dbGenerated('insert'),
   *     updatedAt: number().onUpdate(() => Date.now()).dbGenerated('update'),
   *   })
   *   .primaryKey('id');
   * ```
   */
  dbGenerated<
    TInsert extends 'insert' extends TClientOnlyOptions[number] ? true : false,
    TUpdate extends 'update' extends TClientOnlyOptions[number] ? true : false,
  >(
    ...dbGeneratedOptions: TClientOnlyOptions
  ): ColumnBuilderWithDefault<
    TShape & {
      insertDefaultClientOnly: TInsert;
      updateDefaultClientOnly: TUpdate;
    }
  > {
    const dbGenerated = new Set(dbGeneratedOptions);
    const insertDefaultClientOnly = dbGenerated.has('insert') as TInsert;
    const updateDefaultClientOnly = dbGenerated.has('update') as TUpdate;

    return new ColumnBuilderWithDefault({
      ...this._schema,
      insertDefaultClientOnly,
      updateDefaultClientOnly,
    });
  }
}

export type {ColumnBuilder, ColumnBuilderWithDefault};
