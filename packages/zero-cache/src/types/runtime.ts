import type {LogContext} from '@rocicorp/logger';
import {getHeapSpaceStatistics, getHeapStatistics} from 'node:v8';

export function printHeapStats(lc: LogContext) {
  lc.info?.(`Heap Stats`, getHeapStatistics());
  lc.info?.(`Heap Space Stats`, getHeapSpaceStatistics());
}
