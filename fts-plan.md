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
