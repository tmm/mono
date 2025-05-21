import type {LogContext} from '@rocicorp/logger';

const MB = 1024 * 1024;

export type TableSize = {
  rows: number;
  bytes: number;
};

export type TablePart = {
  offset: number;
  limit: number;

  partNum: number; // 1-indexed part number
  totalParts: number; // Total number of parts
};

export function getPartsToDownload<T extends TableSize>(
  lc: LogContext,
  tables: T[],
  numWorkers: number,
  minPartSize = 10 * MB,
): (T & {part: TablePart})[] {
  if (tables.length === 0) {
    return [];
  }
  // Sort the tables in ascending byte size, or number of rows for ties.
  tables.sort((a, b) =>
    a.bytes !== b.bytes ? a.bytes - b.bytes : a.rows - b.rows,
  );

  const largestTableSize = tables[tables.length - 1].bytes;
  const maxPartSize = Math.max(
    Math.ceil(
      // Set the max part size to be largest-table-size / workers.
      // The intent is to achieve a balance of keeping the workers occupied
      // without incurring the cost of computing the query OFFSET too often.
      largestTableSize / numWorkers,
    ),
    // Don't bother creating parts smaller than minPartSize.
    minPartSize,
  );
  lc.info?.(`target download part size: ${(maxPartSize / MB).toFixed(3)} MB`);
  const partitions: (T & {part: TablePart})[] = [];
  for (const table of tables) {
    const totalParts =
      table.bytes > 0
        ? Math.ceil(table.bytes / maxPartSize)
        : // Partitioned tables show up as 0 bytes.
          // For these use up to numWorkers parts, with each part handling
          // at least 10000 rows.
          Math.min(numWorkers, Math.ceil(table.rows / 10000));
    const limit = Math.ceil(table.rows / totalParts);
    let offset = 0;
    let partNum = 1;
    while (offset < table.rows) {
      partitions.push({
        ...table,
        part: {offset, limit, partNum, totalParts},
      });
      offset += limit;
      partNum++;
    }
  }
  return partitions;
}
