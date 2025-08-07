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
          "decongestant": [
            {
              "cardinality": "one",
              "destField": [
                "lyre",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "outlaw",
              ],
            },
          ],
        },
        "chops": {
          "decongestant": [
            {
              "cardinality": "many",
              "destField": [
                "traffic",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "encouragement",
              ],
            },
          ],
        },
        "decongestant": {},
        "elevator": {
          "decongestant": [
            {
              "cardinality": "many",
              "destField": [
                "language",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "impostor",
              ],
            },
          ],
        },
        "habit": {},
        "sanity": {
          "elevator": [
            {
              "cardinality": "many",
              "destField": [
                "range",
              ],
              "destSchema": "elevator",
              "sourceField": [
                "noon",
              ],
            },
          ],
          "habit": [
            {
              "cardinality": "many",
              "destField": [
                "cake",
              ],
              "destSchema": "habit",
              "sourceField": [
                "courtroom",
              ],
            },
          ],
        },
        "stranger": {},
      },
      "tables": {
        "adrenalin": {
          "columns": {
            "birth": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "creator": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "number",
            },
            "cricket": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "string",
            },
            "jellyfish": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "lox": {
              "optional": true,
              "type": "string",
            },
            "outlaw": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "sanity": {
              "optional": true,
              "type": "number",
            },
          },
          "name": "adrenalin",
          "primaryKey": [
            "sanity",
            "jellyfish",
          ],
        },
        "chops": {
          "columns": {
            "cleaner": {
              "optional": false,
              "type": "json",
            },
            "encouragement": {
              "optional": true,
              "type": "number",
            },
            "pension": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "string",
            },
            "secrecy": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
          },
          "name": "chops",
          "primaryKey": [
            "secrecy",
          ],
        },
        "decongestant": {
          "columns": {
            "amnesty": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "number",
            },
            "circumference": {
              "optional": false,
              "type": "string",
            },
            "community": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "json",
            },
            "ghost": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "json",
            },
            "language": {
              "optional": true,
              "type": "string",
            },
            "lyre": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "string",
            },
            "pacemaker": {
              "optional": false,
              "type": "number",
            },
            "status": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "traffic": {
              "optional": true,
              "type": "number",
            },
          },
          "name": "decongestant",
          "primaryKey": [
            "lyre",
          ],
        },
        "elevator": {
          "columns": {
            "appliance": {
              "optional": false,
              "type": "string",
            },
            "impostor": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "string",
            },
            "legend": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "json",
            },
            "range": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
          },
          "name": "elevator",
          "primaryKey": [
            "legend",
            "appliance",
          ],
        },
        "habit": {
          "columns": {
            "baseboard": {
              "optional": true,
              "type": "string",
            },
            "bench": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "boolean",
            },
            "brush": {
              "optional": true,
              "type": "number",
            },
            "cake": {
              "optional": false,
              "type": "string",
            },
            "hygienic": {
              "optional": false,
              "type": "string",
            },
            "legend": {
              "optional": true,
              "type": "string",
            },
            "mechanic": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "string",
            },
            "outlaw": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "string",
            },
          },
          "name": "habit",
          "primaryKey": [
            "brush",
            "mechanic",
          ],
        },
        "sanity": {
          "columns": {
            "courtroom": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "gripper": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "json",
            },
            "kielbasa": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "string",
            },
            "lace": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "lady": {
              "optional": true,
              "type": "string",
            },
            "legging": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "string",
            },
            "noon": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "number",
            },
            "sesame": {
              "optional": true,
              "type": "string",
            },
            "swath": {
              "optional": false,
              "type": "string",
            },
          },
          "name": "sanity",
          "primaryKey": [
            "kielbasa",
          ],
        },
        "stranger": {
          "columns": {
            "bracelet": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": false,
              "type": "json",
            },
            "marathon": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "mathematics": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "newsstand": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": [Function],
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "number",
            },
            "other": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": false,
              "type": "json",
            },
            "unibody": {
              "defaultConfig": {
                "insert": {
                  "client": [Function],
                  "server": "db",
                },
                "update": {
                  "client": [Function],
                  "server": [Function],
                },
              },
              "optional": true,
              "type": "number",
            },
          },
          "name": "stranger",
          "primaryKey": [
            "unibody",
          ],
        },
      },
    }
  `);
});
