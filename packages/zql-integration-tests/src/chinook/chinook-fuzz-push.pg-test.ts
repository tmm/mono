/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {test} from 'vitest';

import '../helpers/comparePg.ts';
import {schema} from './schema.ts';
import {createCase} from '../helpers/setup.ts';
import {staticToRunnable} from '../helpers/static.ts';
import type {AnyQuery} from '../../../zql/src/query/query-impl.ts';
import type {AnyStaticQuery} from '../../../zql/src/query/test/util.ts';

const pgContent = await getChinook();

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_push',
  zqlSchema: schema,
  pgContent,
});

test.each(
  Array.from({length: 0}, () =>
    createCase(schema, harness.delegates.pg.serverSchema),
  ),
)('fuzz-push $seed', runCase);

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
    throw new Error('Mismatch. Repro seed: ' + seed + '\n' + e, {
      cause: e,
    });
  }
}
