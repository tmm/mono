import type {PrimaryKey} from '../constraint.ts';
import {MemorySource} from '../memory-source.ts';
import type {Source} from '../source.ts';
import type {LogContext} from '@rocicorp/logger';
import type {ColumnType} from '../schema.ts';
import type {LogConfig} from '../log.ts';

export type SourceFactory = (
  lc: LogContext,
  logConfig: LogConfig,
  tableName: string,
  columns: Record<string, ColumnType>,
  primaryKey: PrimaryKey,
) => Source;

export const createSource: SourceFactory = (
  lc: LogContext,
  logConfig: LogConfig,
  tableName: string,
  columns: Record<string, ColumnType>,
  primaryKey: PrimaryKey,
): Source => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const {sourceFactory} = globalThis as {
    sourceFactory?: SourceFactory;
  };
  if (sourceFactory) {
    return sourceFactory(lc, logConfig, tableName, columns, primaryKey);
  }

  return new MemorySource(tableName, columns, primaryKey);
};
