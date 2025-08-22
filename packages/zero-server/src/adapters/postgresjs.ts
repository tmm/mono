import type {JSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {
  DBConnection,
  DBTransaction,
  Row,
} from '../../../zql/src/mutate/custom.ts';
import postgres from 'postgres';
import {ZQLDatabase} from '../zql-database.ts';

/**
 * Helper type for the wrapped transaction used by postgres.js.
 *
 * @typeParam T - The row-shape context bound to the postgres.js client.
 * @remarks Use with `ServerTransaction` as `ServerTransaction<Schema, PostgresJsTransaction>`.
 */
export type PostgresJsTransaction<
  T extends Record<string, unknown> = Record<string, unknown>,
> = postgres.TransactionSql<T>;

export class PostgresJSConnection<T extends Record<string, unknown>>
  implements DBConnection<postgres.TransactionSql<T>>
{
  readonly #pg: postgres.Sql<T>;
  constructor(pg: postgres.Sql<T>) {
    this.#pg = pg;
  }

  transaction<TRet>(
    fn: (tx: DBTransaction<postgres.TransactionSql<T>>) => Promise<TRet>,
  ): Promise<TRet> {
    return this.#pg.begin(pgTx =>
      fn(new PostgresJsTransactionInternal(pgTx)),
    ) as Promise<TRet>;
  }
}

class PostgresJsTransactionInternal<T extends Record<string, unknown>>
  implements DBTransaction<postgres.TransactionSql<T>>
{
  readonly wrappedTransaction: postgres.TransactionSql<T>;
  constructor(pgTx: postgres.TransactionSql<T>) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}

/**
 * Wrap a `postgres` client for Zero ZQL.
 *
 * Provides ZQL querying plus access to the underlying postgres.js transaction.
 * Use {@link PostgresJsTransaction} to type your server mutator transaction.
 *
 * @param schema - Zero schema.
 * @param pg - `postgres` client or connection string.
 *
 * @example
 * ```ts
 * import postgres from 'postgres';
 *
 * const sql = postgres(process.env.ZERO_UPSTREAM_DB!);
 * const zql = zeroPostgresJS(schema, sql);
 *
 * // Define the server mutator transaction type using the helper
 * type ServerTx = ServerTransaction<
 *   Schema,
 *   PostgresJsTransaction
 * >;
 *
 * async function createUser(
 *   tx: ServerTx,
 *   {id, name}: {id: string; name: string},
 * ) {
 *   await tx.dbTransaction.wrappedTransaction
 *     .unsafe('INSERT INTO "user" (id, name) VALUES ($1, $2)', [id, name]);
 * }
 * ```
 */
export function zeroPostgresJS<
  S extends Schema,
  T extends Record<string, unknown> = Record<string, unknown>,
>(schema: S, pg: postgres.Sql<T> | string) {
  if (typeof pg === 'string') {
    pg = postgres(pg) as postgres.Sql<T>;
  }
  return new ZQLDatabase(new PostgresJSConnection(pg), schema);
}
