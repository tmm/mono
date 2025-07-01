/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';
import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {ast} from '../../../zql/src/query/query-impl.ts';
import type {
  AnyQuery,
  AnyStaticQuery,
} from '../../../zql/src/query/test/util.ts';
import {astToZQL} from '../../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../../ast-to-zql/src/format.ts';
import {staticToRunnable} from '../helpers/static.ts';
import {createCase} from '../helpers/setup.ts';

const pgContent = await getChinook();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_hydration',
  zqlSchema: schema,
  pgContent,
});

test.each(
  Array.from({length: 0}, () =>
    createCase(schema, harness.delegates.pg.serverSchema),
  ),
)('fuzz-hydration $seed', runCase);

test('sentinel', () => {
  expect(true).toBe(true);
});

if (REPRO_SEED) {
  // eslint-disable-next-line no-only-tests/no-only-tests
  test.only('repro', async () => {
    const {query} = createCase(
      schema,
      harness.delegates.pg.serverSchema,
      REPRO_SEED,
    );
    console.log(
      'ZQL',
      await formatOutput(ast(query[0]).table + astToZQL(ast(query[0]))),
    );
  });
}

async function runCase({
  query,
  seed,
}: {
  query: [AnyQuery, AnyQuery[]];
  seed: number;
}) {
  try {
    await runAndCompare(
      schema,
      staticToRunnable({
        query: query[0] as AnyStaticQuery,
        schema,
        harness,
      }),
      undefined,
    );
  } catch (e) {
    const zql = await shrink(query[1], seed);
    if (seed === REPRO_SEED) {
      throw e;
    }

    throw new Error('Mismatch. Repro seed: ' + seed + '\nshrunk zql: ' + zql);
  }
}

async function shrink(generations: AnyQuery[], seed: number) {
  console.log('Found failure at seed', seed);
  console.log('Shrinking', generations.length, 'generations');
  let low = 0;
  let high = generations.length;
  let lastFailure = -1;
  while (low < high) {
    const mid = low + ((high - low) >> 1);
    try {
      await runAndCompare(
        schema,
        staticToRunnable({
          query: generations[mid] as AnyStaticQuery,
          schema,
          harness,
        }),
        undefined,
      );
      low = mid + 1;
    } catch (e) {
      lastFailure = mid;
      high = mid;
    }
  }
  if (lastFailure === -1) {
    throw new Error('no failure found');
  }
  const query = generations[lastFailure];
  const ret = formatOutput(ast(query).table + astToZQL(ast(query)));
  console.log('Shrunk to', ret);
  return ret;
}
