import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {generateSchema} from './schema-gen.ts';

test('stable generation', () => {
  const rng = generateMersenne53Randomizer(400);
  expect(
    generateSchema(
      () => rng.next(),
      new Faker({
        locale: en,
        randomizer: rng,
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "relationships": {
        "adrenalin": {
          "adrenalin": [
            {
              "cardinality": "one",
              "destField": [
                "curl",
              ],
              "destSchema": "adrenalin",
              "sourceField": [
                "hierarchy",
              ],
            },
          ],
        },
        "chops": {
          "chops": [
            {
              "cardinality": "one",
              "destField": [
                "birth",
              ],
              "destSchema": "chops",
              "sourceField": [
                "lox",
              ],
            },
          ],
          "decongestant": [
            {
              "cardinality": "many",
              "destField": [
                "language",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "outlaw",
              ],
            },
          ],
        },
        "decongestant": {},
        "elevator": {
          "chops": [
            {
              "cardinality": "many",
              "destField": [
                "outlaw",
              ],
              "destSchema": "chops",
              "sourceField": [
                "asset",
              ],
            },
          ],
        },
        "habit": {},
        "sanity": {
          "sanity": [
            {
              "cardinality": "many",
              "destField": [
                "legging",
              ],
              "destSchema": "sanity",
              "sourceField": [
                "legging",
              ],
            },
          ],
        },
        "stranger": {
          "chops": [
            {
              "cardinality": "many",
              "destField": [
                "outlaw",
              ],
              "destSchema": "chops",
              "sourceField": [
                "newsstand",
              ],
            },
          ],
          "decongestant": [
            {
              "cardinality": "many",
              "destField": [
                "amnesty",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "gymnast",
              ],
            },
          ],
        },
      },
      "tables": {
        "adrenalin": {
          "columns": {
            "cemetery": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "json",
              "updateDefault": [Function],
            },
            "curl": {
              "nullable": false,
              "type": "string",
              "updateDefault": [Function],
            },
            "hierarchy": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "string",
            },
          },
          "name": "adrenalin",
          "primaryKey": [
            "curl",
            "hierarchy",
          ],
        },
        "chops": {
          "columns": {
            "birth": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "string",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
            "lox": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "string",
              "updateDefault": [Function],
            },
            "outlaw": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "number",
            },
          },
          "name": "chops",
          "primaryKey": [
            "lox",
            "birth",
          ],
        },
        "decongestant": {
          "columns": {
            "amnesty": {
              "nullable": false,
              "type": "number",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
            "circumference": {
              "nullable": false,
              "type": "string",
            },
            "community": {
              "nullable": false,
              "type": "string",
            },
            "ghost": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "string",
            },
            "language": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "number",
            },
            "lyre": {
              "nullable": false,
              "type": "string",
            },
            "pacemaker": {
              "nullable": true,
              "type": "number",
            },
            "status": {
              "nullable": true,
              "type": "string",
            },
            "traffic": {
              "nullable": false,
              "type": "string",
              "updateDefault": [Function],
            },
          },
          "name": "decongestant",
          "primaryKey": [
            "lyre",
          ],
        },
        "elevator": {
          "columns": {
            "asset": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "number",
            },
            "bonnet": {
              "nullable": true,
              "type": "string",
              "updateDefault": [Function],
            },
            "derby": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "string",
            },
            "metal": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "json",
            },
            "resource": {
              "nullable": true,
              "type": "number",
            },
            "sandbar": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "string",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
          },
          "name": "elevator",
          "primaryKey": [
            "resource",
          ],
        },
        "habit": {
          "columns": {
            "coal": {
              "nullable": true,
              "type": "string",
            },
            "secret": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "string",
              "updateDefault": [Function],
            },
          },
          "name": "habit",
          "primaryKey": [
            "secret",
          ],
        },
        "sanity": {
          "columns": {
            "advancement": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "number",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
            "flame": {
              "nullable": true,
              "type": "string",
              "updateDefault": [Function],
            },
            "hygienic": {
              "nullable": true,
              "type": "number",
              "updateDefault": [Function],
            },
            "lady": {
              "nullable": false,
              "type": "number",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
            "legging": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "string",
            },
            "sesame": {
              "nullable": true,
              "type": "string",
            },
          },
          "name": "sanity",
          "primaryKey": [
            "lady",
          ],
        },
        "stranger": {
          "columns": {
            "airline": {
              "nullable": true,
              "type": "string",
              "updateDefault": [Function],
            },
            "guidance": {
              "nullable": true,
              "type": "string",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
            "gymnast": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "number",
              "updateDefault": [Function],
            },
            "marathon": {
              "insertDefault": [Function],
              "nullable": false,
              "type": "number",
            },
            "mathematics": {
              "nullable": false,
              "type": "string",
              "updateDefault": [Function],
            },
            "newsstand": {
              "nullable": false,
              "type": "number",
              "updateDefault": [Function],
            },
            "someplace": {
              "insertDefault": [Function],
              "nullable": true,
              "type": "number",
              "updateDefault": [Function],
              "updateDefaultClientOnly": true,
            },
          },
          "name": "stranger",
          "primaryKey": [
            "newsstand",
          ],
        },
      },
    }
  `);
});
