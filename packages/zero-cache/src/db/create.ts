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

export function createIndexStatement(index: LiteIndexSpec): string {
  // TODO: Handle fulltext indices when index.indexType === 'fulltext'
  // For SQLite FTS5, we need to:
  // 1. Create an FTS5 virtual table that mirrors the main table
  // 2. Set up triggers to keep the FTS table in sync with the main table
  // 3. Handle the column mapping (PostgreSQL tsvector -> SQLite FTS5)
  // For now, skip fulltext index creation and log a warning
  if (index.indexType === 'fulltext') {
    // Returning a comment instead of a CREATE INDEX statement
    // This will be logged but won't fail the replication
    return `-- TODO: Fulltext index ${index.name} detected but not yet supported in SQLite`;
  }

  const columns = Object.entries(index.columns)
    .map(([name, dir]) => `${id(name)} ${dir}`)
    .join(',');
  const unique = index.unique ? 'UNIQUE' : '';
  return `CREATE ${unique} INDEX ${id(index.name)} ON ${id(
    index.tableName,
  )} (${columns});`;
}
