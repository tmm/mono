/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, expectTypeOf, test} from 'vitest';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {queries, createBuilder, type NamedQuery} from './named.ts';
import {ast} from './query-impl.ts';
import {StaticQuery} from './static-query.ts';
import {schema} from './test/test-schemas.ts';

test('defining a named query', () => {
  const queryBuilder = createBuilder(schema);
  const x = queries({
    myName: (id: string) => queryBuilder.issue.where('id', id),
  });
  const q = x.myName('123');
  expectTypeOf<ReturnType<typeof q.run>>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
        readonly updatedAt: number;
      }[]
    >
  >();
  check(x.myName);

  // define many at once
  const y = queries({
    myName: (id: string) => queryBuilder.issue.where('id', id),
    myOtherName: (id: string) => queryBuilder.issue.where('id', id),
    myThirdName: (id: string) => queryBuilder.issue.where('id', id),
  });
  check(y.myName, 'myName');
  check(y.myOtherName, 'myOtherName');
  check(y.myThirdName, 'myThirdName');
  const q1 = y.myName('123');
  const q2 = y.myOtherName('123');
  const q3 = y.myThirdName('123');
  expectTypeOf<ReturnType<typeof q1.run>>().toEqualTypeOf<
    ReturnType<typeof q.run>
  >();
  expectTypeOf<ReturnType<typeof q2.run>>().toEqualTypeOf<
    ReturnType<typeof q.run>
  >();
  expectTypeOf<ReturnType<typeof q3.run>>().toEqualTypeOf<
    ReturnType<typeof q.run>
  >();
});

function check(
  named: NamedQuery<[string], any>,
  expectedName: string = 'myName',
) {
  const r = named('123');

  const id = r.customQueryID;
  expect(id?.name).toBe(expectedName);
  expect(id?.args).toEqual(['123']);
  expect(ast(r)).toMatchInlineSnapshot(`
    {
      "table": "issue",
      "where": {
        "left": {
          "name": "id",
          "type": "column",
        },
        "op": "=",
        "right": {
          "type": "literal",
          "value": "123",
        },
        "type": "simple",
      },
    }
  `);

  // see comment on `r.hash()`
  expect(r.hash()).not.toEqual(hashOfNameAndArgs('issue', ['123']));
  expect(r.hash()).toEqual(
    hashOfAST((r as StaticQuery<typeof schema, 'issue'>).ast),
  );
}

test('makeSchemaQuery', () => {
  const builders = createBuilder(schema);
  const q1 = builders.issue.where('id', '123').nameAndArgs('myName', ['123']);
  expect(ast(q1)).toMatchInlineSnapshot(`
    {
      "table": "issue",
      "where": {
        "left": {
          "name": "id",
          "type": "column",
        },
        "op": "=",
        "right": {
          "type": "literal",
          "value": "123",
        },
        "type": "simple",
      },
    }
  `);
});
