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
              "cardinality": "many",
              "destField": [
                "privilege",
              ],
              "destSchema": "adrenalin",
              "sourceField": [
                "fund",
              ],
            },
          ],
        },
        "chops": {},
        "decongestant": {
          "decongestant": [
            {
              "cardinality": "one",
              "destField": [
                "lyre",
              ],
              "destSchema": "decongestant",
              "sourceField": [
                "pacemaker",
              ],
            },
          ],
          "stranger": [
            {
              "cardinality": "many",
              "destField": [
                "bracelet",
              ],
              "destSchema": "stranger",
              "sourceField": [
                "amnesty",
              ],
            },
          ],
        },
        "elevator": {
          "stranger": [
            {
              "cardinality": "many",
              "destField": [
                "mathematics",
              ],
              "destSchema": "stranger",
              "sourceField": [
                "appliance",
              ],
            },
          ],
        },
        "habit": {
          "sanity": [
            {
              "cardinality": "many",
              "destField": [
                "courtroom",
              ],
              "destSchema": "sanity",
              "sourceField": [
                "schedule",
              ],
            },
          ],
        },
        "sanity": {},
        "stranger": {
          "adrenalin": [
            {
              "cardinality": "many",
              "destField": [
                "cricket",
              ],
              "destSchema": "adrenalin",
              "sourceField": [
                "newsstand",
              ],
            },
          ],
          "elevator": [
            {
              "cardinality": "many",
              "destField": [
                "planula",
              ],
              "destSchema": "elevator",
              "sourceField": [
                "character",
              ],
            },
          ],
        },
      },
      "tables": {
        "adrenalin": {
          "columns": {
            "apparatus": {
              "optional": false,
              "type": "string",
            },
            "bin": {
              "optional": false,
              "type": "boolean",
            },
            "cricket": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "fund": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "premise": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "privilege": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "tennis": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
          },
          "name": "adrenalin",
          "primaryKey": [
            "cricket",
            "premise",
          ],
        },
        "chops": {
          "columns": {
            "climb": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "json",
            },
            "corral": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "fort": {
              "optional": false,
              "type": "string",
            },
            "understanding": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
          },
          "name": "chops",
          "primaryKey": [
            "corral",
          ],
        },
        "decongestant": {
          "columns": {
            "amnesty": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "circumference": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "boolean",
            },
            "community": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "number",
            },
            "ghost": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "json",
            },
            "language": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "lyre": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "pacemaker": {
              "optional": true,
              "type": "string",
            },
            "status": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "json",
            },
            "traffic": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "number",
            },
          },
          "name": "decongestant",
          "primaryKey": [
            "lyre",
            "language",
          ],
        },
        "elevator": {
          "columns": {
            "appliance": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "forage": {
              "optional": true,
              "type": "boolean",
            },
            "impostor": {
              "optional": true,
              "type": "number",
            },
            "planula": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "range": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "unique": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
          },
          "name": "elevator",
          "primaryKey": [
            "appliance",
          ],
        },
        "habit": {
          "columns": {
            "fledgling": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "honesty": {
              "optional": true,
              "type": "string",
            },
            "hubris": {
              "optional": true,
              "type": "string",
            },
            "kielbasa": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "produce": {
              "optional": true,
              "type": "string",
            },
            "sarong": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
            "schedule": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
          },
          "name": "habit",
          "primaryKey": [
            "kielbasa",
          ],
        },
        "sanity": {
          "columns": {
            "courtroom": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "gripper": {
              "optional": false,
              "type": "boolean",
            },
            "lace": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "noon": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "number",
            },
            "swath": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
          },
          "name": "sanity",
          "primaryKey": [
            "gripper",
            "courtroom",
          ],
        },
        "stranger": {
          "columns": {
            "bracelet": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "character": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "marathon": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "boolean",
            },
            "markup": {
              "optional": false,
              "type": "number",
            },
            "mathematics": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
                "update": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "string",
            },
            "newsstand": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "other": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": true,
              "type": "number",
            },
            "unibody": {
              "defaultConfig": {
                "insert": {
                  "server": "db",
                },
              },
              "optional": false,
              "type": "string",
            },
          },
          "name": "stranger",
          "primaryKey": [
            "character",
          ],
        },
      },
    }
  `);
});
