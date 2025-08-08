import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import type {StaticQuery} from '../static-query.ts';
import {generateQuery} from './query-gen.ts';
import {generateSchema} from './schema-gen.ts';

test('random generation', () => {
  const randomizer = generateMersenne53Randomizer(
    Date.now() ^ (Math.random() * 0x100000000),
  );
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  const schema = generateSchema(rng, faker);
  expect(() => generateQuery(schema, {}, rng, faker)).not.toThrow();
});

test('stable generation', () => {
  const randomizer = generateMersenne53Randomizer(42);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });
  const schema = generateSchema(rng, faker);

  const q = generateQuery(schema, {}, rng, faker);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  expect((q as StaticQuery<any, any>).ast).toMatchInlineSnapshot(`
    {
      "orderBy": [
        [
          "captain",
          "desc",
        ],
      ],
      "table": "cleaner",
      "where": {
        "conditions": [
          {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "numeric",
                ],
                "parentField": [
                  "final",
                ],
              },
              "subquery": {
                "alias": "zsubq_rawhide",
                "limit": 181,
                "orderBy": [
                  [
                    "chapel",
                    "asc",
                  ],
                  [
                    "hose",
                    "asc",
                  ],
                  [
                    "elver",
                    "asc",
                  ],
                ],
                "table": "rawhide",
                "where": {
                  "conditions": [
                    {
                      "left": {
                        "name": "numeric",
                        "type": "column",
                      },
                      "op": "!=",
                      "right": {
                        "type": "literal",
                        "value": "confero vilitas commodi",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "chapel",
                        "type": "column",
                      },
                      "op": "!=",
                      "right": {
                        "type": "literal",
                        "value": 0.25178229582536416,
                      },
                      "type": "simple",
                    },
                  ],
                  "type": "and",
                },
              },
              "system": "permissions",
            },
            "type": "correlatedSubquery",
          },
          {
            "left": {
              "name": "captain",
              "type": "column",
            },
            "op": "IS",
            "right": {
              "type": "literal",
              "value": "spectaculum temptatio capio",
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
