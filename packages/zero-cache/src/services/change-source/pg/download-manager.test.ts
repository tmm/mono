import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {getPartsToDownload} from './download-manager.ts';

describe('download-manager', () => {
  const lc = new LogContext('debug', {}, consoleLogSink);
  createSilentLogContext();

  test('multi-part', () => {
    expect(
      getPartsToDownload(
        lc,
        [
          // About 1.2 GB, partitioned into 4 partitions of ~300MB
          {table: 'foo', rows: 123400, bytes: 1234567890},
          // About 420 MB, split into two.
          {table: 'bar', rows: 300000, bytes: 434567890},
          // Small table, not partitioned.
          {table: 'baz', rows: 4000, bytes: 234567},
        ],
        4,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "bytes": 234567,
          "part": {
            "limit": 4000,
            "offset": 0,
            "partNum": 1,
            "totalParts": 1,
          },
          "rows": 4000,
          "table": "baz",
        },
        {
          "bytes": 434567890,
          "part": {
            "limit": 150000,
            "offset": 0,
            "partNum": 1,
            "totalParts": 2,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 434567890,
          "part": {
            "limit": 150000,
            "offset": 150000,
            "partNum": 2,
            "totalParts": 2,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 1234567890,
          "part": {
            "limit": 30850,
            "offset": 0,
            "partNum": 1,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 1234567890,
          "part": {
            "limit": 30850,
            "offset": 30850,
            "partNum": 2,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 1234567890,
          "part": {
            "limit": 30850,
            "offset": 61700,
            "partNum": 3,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 1234567890,
          "part": {
            "limit": 30850,
            "offset": 92550,
            "partNum": 4,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
      ]
    `);

    expect(
      getPartsToDownload(
        lc,
        [
          // About 120 MB, partitioned into 4 partitions of ~20MB
          {table: 'foo', rows: 123400, bytes: 123456789},
          // About 220 MB, partitioned into 6 partitions of ~37MB.
          {table: 'bar', rows: 300000, bytes: 234567890},
          // Small table, not partitioned.
          {table: 'baz', rows: 4000, bytes: 234567},
        ],
        6,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "bytes": 234567,
          "part": {
            "limit": 4000,
            "offset": 0,
            "partNum": 1,
            "totalParts": 1,
          },
          "rows": 4000,
          "table": "baz",
        },
        {
          "bytes": 123456789,
          "part": {
            "limit": 30850,
            "offset": 0,
            "partNum": 1,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 123456789,
          "part": {
            "limit": 30850,
            "offset": 30850,
            "partNum": 2,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 123456789,
          "part": {
            "limit": 30850,
            "offset": 61700,
            "partNum": 3,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 123456789,
          "part": {
            "limit": 30850,
            "offset": 92550,
            "partNum": 4,
            "totalParts": 4,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 50000,
            "offset": 0,
            "partNum": 1,
            "totalParts": 6,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 50000,
            "offset": 50000,
            "partNum": 2,
            "totalParts": 6,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 50000,
            "offset": 100000,
            "partNum": 3,
            "totalParts": 6,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 50000,
            "offset": 150000,
            "partNum": 4,
            "totalParts": 6,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 50000,
            "offset": 200000,
            "partNum": 5,
            "totalParts": 6,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 50000,
            "offset": 250000,
            "partNum": 6,
            "totalParts": 6,
          },
          "rows": 300000,
          "table": "bar",
        },
      ]
    `);
  });

  test('partitioned tables', () => {
    expect(
      getPartsToDownload(
        lc,
        [
          // A Postgres-partitioned table shows up as bytes: 0.
          // Gets the default of 5 parts since each is over 10000
          {table: 'foo', rows: 123400, bytes: 0},
          // About 220 MB, partitioned into 5 partitions of ~44MB.
          {table: 'bar', rows: 300000, bytes: 234567890},
          // Also partitioned, but fewer rows. Two parts based on
          // targeting (but not exceeding) 10000 rows per part.
          {table: 'baz', rows: 14000, bytes: 0},
        ],
        5,
      ),
    ).toMatchInlineSnapshot(`
      [
        {
          "bytes": 0,
          "part": {
            "limit": 7000,
            "offset": 0,
            "partNum": 1,
            "totalParts": 2,
          },
          "rows": 14000,
          "table": "baz",
        },
        {
          "bytes": 0,
          "part": {
            "limit": 7000,
            "offset": 7000,
            "partNum": 2,
            "totalParts": 2,
          },
          "rows": 14000,
          "table": "baz",
        },
        {
          "bytes": 0,
          "part": {
            "limit": 24680,
            "offset": 0,
            "partNum": 1,
            "totalParts": 5,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 0,
          "part": {
            "limit": 24680,
            "offset": 24680,
            "partNum": 2,
            "totalParts": 5,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 0,
          "part": {
            "limit": 24680,
            "offset": 49360,
            "partNum": 3,
            "totalParts": 5,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 0,
          "part": {
            "limit": 24680,
            "offset": 74040,
            "partNum": 4,
            "totalParts": 5,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 0,
          "part": {
            "limit": 24680,
            "offset": 98720,
            "partNum": 5,
            "totalParts": 5,
          },
          "rows": 123400,
          "table": "foo",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 60000,
            "offset": 0,
            "partNum": 1,
            "totalParts": 5,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 60000,
            "offset": 60000,
            "partNum": 2,
            "totalParts": 5,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 60000,
            "offset": 120000,
            "partNum": 3,
            "totalParts": 5,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 60000,
            "offset": 180000,
            "partNum": 4,
            "totalParts": 5,
          },
          "rows": 300000,
          "table": "bar",
        },
        {
          "bytes": 234567890,
          "part": {
            "limit": 60000,
            "offset": 240000,
            "partNum": 5,
            "totalParts": 5,
          },
          "rows": 300000,
          "table": "bar",
        },
      ]
    `);
  });
});
