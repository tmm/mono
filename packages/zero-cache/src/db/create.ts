import {id, idList} from '../types/sql.ts';
import type {
  ColumnSpec,
  LiteIndexSpec,
  LiteTableSpec,
  TableSpec,
} from './specs.ts';

export function columnDef(spec: ColumnSpec) {
  let def = id(spec.dataType);
  if (spec.characterMaximumLength) {
    def += `(${spec.characterMaximumLength})`;
  }
  if (spec.elemPgTypeClass !== null) {
    def += '[]';
  }
  if (spec.notNull) {
    def += ' NOT NULL';
  }
  if (spec.dflt) {
    def += ` DEFAULT ${spec.dflt}`;
  }
  return def;
}

/**
 * Constructs a `CREATE TABLE` statement for a {@link TableSpec}.
 */
export function createTableStatement(spec: TableSpec | LiteTableSpec): string {
  const defs = Object.entries(spec.columns)
    .sort(([_a, {pos: a}], [_b, {pos: b}]) => a - b)
    .map(([name, columnSpec]) => `${id(name)} ${columnDef(columnSpec)}`);
  if (spec.primaryKey) {
    defs.push(`PRIMARY KEY (${idList(spec.primaryKey)})`);
  }

  const createStmt =
    'schema' in spec
      ? `CREATE TABLE ${id(spec.schema)}.${id(spec.name)} (`
      : `CREATE TABLE ${id(spec.name)} (`;
  return [createStmt, defs.join(',\n'), ');'].join('\n');
}

/**
 * Creates FTS5 virtual table and sync triggers for fulltext search.
 * Returns an array of SQL statements to execute.
 */
export function createFTS5Statements(
  tableName: string,
  columns: string[],
): string[] {
  const ftsTableName = `${tableName}_fts`;
  const columnList = columns.join(', ');
  const statements: string[] = [];

  // Create FTS5 virtual table
  statements.push(
    `CREATE VIRTUAL TABLE IF NOT EXISTS ${id(ftsTableName)} USING fts5(` +
      `${columnList}, ` +
      `content='${tableName}', ` +
      `tokenize='unicode61'` +
      `);`,
  );

  // Create INSERT trigger
  const insertColumns = columns.map(col => `new.${id(col)}`).join(', ');
  statements.push(
    `CREATE TRIGGER IF NOT EXISTS ${id(`${ftsTableName}_insert`)} ` +
      `AFTER INSERT ON ${id(tableName)} BEGIN ` +
      `INSERT INTO ${id(ftsTableName)}(rowid, ${columnList}) ` +
      `VALUES (new.rowid, ${insertColumns}); ` +
      `END;`,
  );

  // Create UPDATE trigger
  const updateSetters = columns
    .map(col => `${id(col)} = new.${id(col)}`)
    .join(', ');
  statements.push(
    `CREATE TRIGGER IF NOT EXISTS ${id(`${ftsTableName}_update`)} ` +
      `AFTER UPDATE ON ${id(tableName)} BEGIN ` +
      `UPDATE ${id(ftsTableName)} SET ${updateSetters} ` +
      `WHERE rowid = new.rowid; ` +
      `END;`,
  );

  // Create DELETE trigger
  statements.push(
    `CREATE TRIGGER IF NOT EXISTS ${id(`${ftsTableName}_delete`)} ` +
      `AFTER DELETE ON ${id(tableName)} BEGIN ` +
      `DELETE FROM ${id(ftsTableName)} WHERE rowid = old.rowid; ` +
      `END;`,
  );

  return statements;
}

export function createIndexStatement(index: LiteIndexSpec): string {
  // Handle fulltext indices by creating FTS5 virtual table
  if (index.indexType === 'fulltext') {
    const columns = Object.keys(index.columns);
    if (columns.length === 0) {
      return `-- Fulltext index ${index.name} has no columns to index`;
    }
    // Return all FTS5 statements joined
    const ftsStatements = createFTS5Statements(index.tableName, columns);
    return ftsStatements.join('\n');
  }

  const columns = Object.entries(index.columns)
    .map(([name, dir]) => `${id(name)} ${dir}`)
    .join(',');
  const unique = index.unique ? 'UNIQUE' : '';
  return `CREATE ${unique} INDEX ${id(index.name)} ON ${id(
    index.tableName,
  )} (${columns});`;
}
