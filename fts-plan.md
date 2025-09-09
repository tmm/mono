Simplified Plan for Creating SQLite FTS5 Indices (Column-Based Only) │ │
│ │ │ │
│ │ Scope │ │
│ │ │ │
│ │ Focus on PostgreSQL fulltext indices that directly index columns (e.g., GIN index on a tsvector column), NOT │ │
│ │ expression-based indices like to_tsvector('english', title). │ │
│ │ │ │
│ │ Current State │ │
│ │ │ │
│ │ - Fulltext indices are detected and marked with indexType: 'fulltext' │ │
│ │ - Column names ARE captured for direct column indices (only expression-based ones lose columns due to WHERE │ │
│ │ col.table_pos > 0) │ │
│ │ - The columns field contains the indexed column names │ │
│ │ │ │
│ │ Implementation Plan │ │
│ │ │ │
│ │ Phase 1: Create SQLite FTS5 Virtual Tables │ │
│ │ │ │
│ │ 1. Modify createIndexStatement (src/db/create.ts): │ │
│ │ - When indexType === 'fulltext': │ │
│ │ - Generate FTS5 virtual table creation │ │
│ │ - Generate INSERT/UPDATE/DELETE triggers for sync │ │
│ │ - Return multiple SQL statements (table + triggers) │ │
│ │ 2. FTS5 Structure: │ │
│ │ -- Virtual table for fulltext columns │ │
│ │ CREATE VIRTUAL TABLE IF NOT EXISTS {tableName}\_fts USING fts5( │ │
│ │ {column1}, {column2}, ..., │ │
│ │ content='{tableName}', │ │
│ │ tokenize='unicode61' │ │
│ │ ); │ │
│ │ │ │
│ │ -- Sync triggers │ │
│ │ CREATE TRIGGER IF NOT EXISTS {tableName}\_fts_insert │ │
│ │ AFTER INSERT ON {tableName} BEGIN │ │
│ │ INSERT INTO {tableName}\_fts(rowid, {columns}) │ │
│ │ VALUES (new.rowid, new.{column1}, new.{column2}, ...); │ │
│ │ END; │ │
│ │ │ │
│ │ CREATE TRIGGER IF NOT EXISTS {tableName}\_fts_update │ │
│ │ AFTER UPDATE ON {tableName} BEGIN │ │
│ │ UPDATE {tableName}\_fts │ │
│ │ SET {column1} = new.{column1}, {column2} = new.{column2}, ... │ │
│ │ WHERE rowid = new.rowid; │ │
│ │ END; │ │
│ │ │ │
│ │ CREATE TRIGGER IF NOT EXISTS {tableName}\_fts_delete │ │
│ │ AFTER DELETE ON {tableName} BEGIN │ │
│ │ DELETE FROM {tableName}\_fts WHERE rowid = old.rowid; │ │
│ │ END; │ │
│ │ │ │
│ │ Phase 2: Handle Multiple Fulltext Indices per Table │ │
│ │ │ │
│ │ 1. Consolidation Strategy: │ │
│ │ - If multiple fulltext indices exist on the same table, combine ALL their columns into a single FTS5 table │ │
│ │ - Use a Set to deduplicate column names │ │
│ │ - Create one FTS table per base table, not per index │ │
│ │ 2. Track FTS Tables: │ │
│ │ - Maintain metadata about which tables have FTS virtual tables │ │
│ │ - Use consistent naming: {tableName}\_fts │ │
│ │ │ │
│ │ Phase 3: Update Index Processing │ │
│ │ │ │
│ │ 1. Initial Sync (src/services/change-source/pg/initial-sync.ts): │ │
│ │ - Group fulltext indices by table name │ │
│ │ - Create one FTS table per base table with all fulltext-indexed columns │ │
│ │ - Execute FTS creation after all regular indices │ │
│ │ 2. Ongoing Replication (src/services/replicator/change-processor.ts): │ │
│ │ - In processCreateIndex: Check if FTS table exists, if not create it │ │
│ │ - Handle adding columns to existing FTS table if needed │ │
│ │ │ │
│ │ Phase 4: Handle DDL Operations │ │
│ │ │ │
│ │ 1. Drop Index: │ │
│ │ - When dropping a fulltext index, check if other fulltext indices exist on the table │ │
│ │ - Only drop FTS table if it's the last fulltext index │ │
│ │ 2. Drop Table: │ │
│ │ - Automatically drops associated FTS table and triggers (CASCADE behavior) │ │
│ │ │ │
│ │ Files to Modify │ │
│ │ │ │
│ │ 1. src/db/create.ts - Implement FTS5 table and trigger generation │ │
│ │ 2. src/services/change-source/pg/initial-sync.ts - Group and process FTS indices │ │
│ │ 3. src/services/replicator/change-processor.ts - Handle FTS during ongoing replication │ │
│ │ │ │
│ │ Key Simplifications │ │
│ │ │ │
│ │ 1. No Expression Parsing: Only handle direct column indices │ │
│ │ 2. Simple Column Mapping: Use the columns already captured in the columns field │ │
│ │ 3. One FTS Table per Base Table: Simplifies management and avoids duplication │ │
│ │ │ │
│ │ Example Scenarios │ │
│ │ │ │
│ │ 1. Single tsvector column: │ │
│ │ - PostgreSQL: CREATE INDEX ON articles USING GIN (search_vector); │ │
│ │ - SQLite: Creates articles_fts with search_vector column │ │
│ │ 2. Multiple tsvector columns: │ │
│ │ - PostgreSQL: Two indices on title_vector and body_vector │ │
│ │ - SQLite: Creates articles_fts with both title_vector and body_vector columns │ │
│ │ │ │
│ │ Notes │ │
│ │ │ │
│ │ - Uses SQLite's automatic rowid for content mapping │ │
│ │ - Triggers maintain sync between base table and FTS table │ │
│ │ - FTS5 tokenizer set to 'unicode61' for better language support



Final Plan for Creating FTS Views (Option 2)                                                                         │ │
│ │                                                                                                                      │ │
│ │ Goal                                                                                                                 │ │
│ │                                                                                                                      │ │
│ │ Create views that expose all base table columns plus FTS columns for MATCH queries, avoiding duplicate column names. │ │
│ │                                                                                                                      │ │
│ │ Implementation Strategy                                                                                              │ │
│ │                                                                                                                      │ │
│ │ 1. Update FTS5 Statement Generation (src/db/create.ts)                                                               │ │
│ │                                                                                                                      │ │
│ │ Enhance createFTS5Statements:                                                                                        │ │
│ │ export function createFTS5Statements(                                                                                │ │
│ │   tableName: string,                                                                                                 │ │
│ │   ftsColumns: string[],                                                                                              │ │
│ │   allTableColumns?: string[]  // All columns from base table                                                         │ │
│ │ ): string[]                                                                                                          │ │
│ │                                                                                                                      │ │
│ │ View Creation Logic:                                                                                                 │ │
│ │ 1. If allTableColumns is provided:                                                                                   │ │
│ │   - Filter out FTS columns from base table column list                                                               │ │
│ │   - Select non-FTS columns from base table                                                                           │ │
│ │   - Select FTS columns from FTS table                                                                                │ │
│ │ 2. Build view with explicit column selection:                                                                        │ │
│ │                                                                                                                      │ │
│ │ CREATE VIEW IF NOT EXISTS {tableName}_view AS                                                                        │ │
│ │   SELECT                                                                                                             │ │
│ │     t.{non_fts_col1}, t.{non_fts_col2}, ...,  -- Non-FTS columns from base                                           │ │
│ │     fts.{fts_col1}, fts.{fts_col2}, ...        -- FTS columns for MATCH                                              │ │
│ │   FROM {tableName} t                                                                                                 │ │
│ │   JOIN {tableName}_fts fts ON t.rowid = fts.rowid;                                                                   │ │
│ │                                                                                                                      │ │
│ │ Implementation:                                                                                                      │ │
│ │ // Inside createFTS5Statements                                                                                       │ │
│ │ if (allTableColumns) {                                                                                               │ │
│ │   const ftsColumnSet = new Set(columns);                                                                             │ │
│ │   const nonFtsColumns = allTableColumns.filter(col => !ftsColumnSet.has(col));                                       │ │
│ │                                                                                                                      │ │
│ │   const viewColumns = [                                                                                              │ │
│ │     ...nonFtsColumns.map(col => `t.${id(col)}`),                                                                     │ │
│ │     ...columns.map(col => `fts.${id(col)}`)                                                                          │ │
│ │   ].join(', ');                                                                                                      │ │
│ │                                                                                                                      │ │
│ │   statements.push(                                                                                                   │ │
│ │     `CREATE VIEW IF NOT EXISTS ${id(viewName)} AS ` +                                                                │ │
│ │     `SELECT ${viewColumns} ` +                                                                                       │ │
│ │     `FROM ${id(tableName)} t ` +                                                                                     │ │
│ │     `JOIN ${id(ftsTableName)} fts ON t.rowid = fts.rowid;`                                                           │ │
│ │   );                                                                                                                 │ │
│ │ }                                                                                                                    │ │
│ │                                                                                                                      │ │
│ │ 2. Update Initial Sync (src/services/change-source/pg/initial-sync.ts)                                               │ │
│ │                                                                                                                      │ │
│ │ In createLiteIndices:                                                                                                │ │
│ │ - Build a map of table names to their full column lists from tables parameter                                        │ │
│ │ - Pass column information when creating FTS statements                                                               │ │
│ │                                                                                                                      │ │
│ │ function createLiteIndices(                                                                                          │ │
│ │   lc: LogContext,                                                                                                    │ │
│ │   tx: Database,                                                                                                      │ │
│ │   indices: IndexSpec[],                                                                                              │ │
│ │   tables: PublishedTableSpec[]  // Add tables parameter                                                              │ │
│ │ ) {                                                                                                                  │ │
│ │   // Build table column map                                                                                          │ │
│ │   const tableColumnMap = new Map<string, string[]>();                                                                │ │
│ │   for (const table of tables) {                                                                                      │ │
│ │     const liteTable = mapPostgresToLite(table, ''); // version not needed for columns                                │ │
│ │     tableColumnMap.set(                                                                                              │ │
│ │       liteTable.name,                                                                                                │ │
│ │       Object.keys(liteTable.columns).filter(col => col !== '_0_version')                                             │ │
│ │     );                                                                                                               │ │
│ │   }                                                                                                                  │ │
│ │                                                                                                                      │ │
│ │   // ... existing FTS grouping logic ...                                                                             │ │
│ │                                                                                                                      │ │
│ │   // Create FTS5 tables with views                                                                                   │ │
│ │   for (const [tableName, ftsColumns] of ftsTablesByTable) {                                                          │ │
│ │     if (ftsColumns.size > 0) {                                                                                       │ │
│ │       const allColumns = tableColumnMap.get(tableName);                                                              │ │
│ │       const ftsStatements = createFTS5Statements(                                                                    │ │
│ │         tableName,                                                                                                   │ │
│ │         Array.from(ftsColumns),                                                                                      │ │
│ │         allColumns                                                                                                   │ │
│ │       );                                                                                                             │ │
│ │       // ... execute statements ...                                                                                  │ │
│ │     }                                                                                                                │ │
│ │   }                                                                                                                  │ │
│ │ }                                                                                                                    │ │
│ │                                                                                                                      │ │
│ │ Update the call site to pass tables:                                                                                 │ │
│ │ // Around line 209 in initial-sync.ts                                                                                │ │
│ │ createLiteIndices(lc, tx, indexes, tables);                                                                          │ │
│ │                                                                                                                      │ │
│ │ 3. Update Change Processor (src/services/replicator/change-processor.ts)                                             │ │
│ │                                                                                                                      │ │
│ │ In processCreateIndex:                                                                                               │ │
│ │ if (index.indexType === 'fulltext') {                                                                                │ │
│ │   // ... existing FTS table check ...                                                                                │ │
│ │                                                                                                                      │ │
│ │   if (!existing) {                                                                                                   │ │
│ │     const columns = Object.keys(index.columns);                                                                      │ │
│ │                                                                                                                      │ │
│ │     // Get all table columns from cached spec                                                                        │ │
│ │     const tableSpec = this.#tableSpecs.get(index.tableName);                                                         │ │
│ │     const allColumns = tableSpec                                                                                     │ │
│ │       ? Object.keys(tableSpec.columns).filter(col => col !== '_0_version')                                           │ │
│ │       : undefined;                                                                                                   │ │
│ │                                                                                                                      │ │
│ │     if (columns.length > 0) {                                                                                        │ │
│ │       const ftsStatements = createFTS5Statements(                                                                    │ │
│ │         index.tableName,                                                                                             │ │
│ │         columns,                                                                                                     │ │
│ │         allColumns                                                                                                   │ │
│ │       );                                                                                                             │ │
│ │       for (const stmt of ftsStatements) {                                                                            │ │
│ │         this.#db.db.exec(stmt);                                                                                      │ │
│ │       }                                                                                                              │ │
│ │     }                                                                                                                │ │
│ │   }                                                                                                                  │ │
│ │ }                                                                                                                    │ │
│ │                                                                                                                      │ │
│ │ Example Output                                                                                                       │ │
│ │                                                                                                                      │ │
│ │ For table comments (id, author, created, body, tags) with FTS on body and tags:                                      │ │
│ │                                                                                                                      │ │
│ │ -- FTS5 virtual table                                                                                                │ │
│ │ CREATE VIRTUAL TABLE IF NOT EXISTS comments_fts USING fts5(                                                          │ │
│ │   body, tags,                                                                                                        │ │
│ │   content='comments',                                                                                                │ │
│ │   tokenize='unicode61'                                                                                               │ │
│ │ );                                                                                                                   │ │
│ │                                                                                                                      │ │
│ │ -- Triggers                                                                                                          │ │
│ │ CREATE TRIGGER IF NOT EXISTS comments_fts_insert ...                                                                 │ │
│ │ CREATE TRIGGER IF NOT EXISTS comments_fts_update ...                                                                 │ │
│ │ CREATE TRIGGER IF NOT EXISTS comments_fts_delete ...                                                                 │ │
│ │                                                                                                                      │ │
│ │ -- View with no duplicate columns                                                                                    │ │
│ │ CREATE VIEW IF NOT EXISTS comments_view AS                                                                           │ │
│ │   SELECT                                                                                                             │ │
│ │     t.id, t.author, t.created,  -- Non-FTS columns from base table                                                   │ │
│ │     fts.body, fts.tags           -- FTS columns from FTS table                                                       │ │
│ │   FROM comments t                                                                                                    │ │
│ │   JOIN comments_fts fts ON t.rowid = fts.rowid;                                                                      │ │
│ │                                                                                                                      │ │
│ │ Usage:                                                                                                               │ │
│ │ -- Fulltext search on body                                                                                           │ │
│ │ SELECT * FROM comments_view WHERE body MATCH 'important';                                                            │ │
│ │                                                                                                                      │ │
│ │ -- Fulltext search on tags                                                                                           │ │
│ │ SELECT * FROM comments_view WHERE tags MATCH 'typescript';                                                           │ │
│ │                                                                                                                      │ │
│ │ -- Combined with regular filters                                                                                     │ │
│ │ SELECT * FROM comments_view                                                                                          │ │
│ │ WHERE body MATCH 'bug'                                                                                               │ │
│ │   AND author = 'alice';                                                                                              │ │
│ │                                                                                                                      │ │
│ │ Benefits                                                                                                             │ │
│ │                                                                                                                      │ │
│ │ 1. No Duplicate Columns: Explicitly selects non-overlapping column sets                                              │ │
│ │ 2. Clear Separation: Non-FTS columns from base table, FTS columns from FTS table                                     │ │
│ │ 3. MATCH Support: FTS columns can be used with MATCH operator                                                        │ │
│ │ 4. Cached Table Specs: Uses existing infrastructure, no extra DB queries                                             │ │
│ │                                                                                                                      │ │
│ │ Edge Cases Handled                                                                                                   │ │
│ │                                                                                                                      │ │
│ │ 1. All columns are FTS: View only selects from FTS table                                                             │ │
│ │ 2. No non-FTS columns: View still works (might only have FTS columns)                                                │ │
│ │ 3. Column added to base table: Need to recreate view                                                                 │ │
│ │ 4. FTS column added: Need to recreate view  