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
      "limit": 57,
      "orderBy": [
        [
          "backburn",
          "asc",
        ],
      ],
      "related": [
        {
          "correlation": {
            "childField": [
              "bend",
            ],
            "parentField": [
              "backburn",
            ],
          },
          "subquery": {
            "alias": "cleaner",
            "limit": 167,
            "orderBy": [
              [
                "sailor",
                "asc",
              ],
            ],
            "table": "cleaner",
            "where": {
              "op": "NOT EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "safe",
                  ],
                  "parentField": [
                    "safe",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_cleaner",
                  "limit": 87,
                  "orderBy": [
                    [
                      "safe",
                      "desc",
                    ],
                    [
                      "council",
                      "asc",
                    ],
                    [
                      "formula",
                      "desc",
                    ],
                    [
                      "sailor",
                      "asc",
                    ],
                  ],
                  "table": "cleaner",
                  "where": {
                    "conditions": [
                      {
                        "op": "NOT EXISTS",
                        "related": {
                          "correlation": {
                            "childField": [
                              "safe",
                            ],
                            "parentField": [
                              "safe",
                            ],
                          },
                          "subquery": {
                            "alias": "zsubq_cleaner",
                            "limit": 108,
                            "orderBy": [
                              [
                                "accompanist",
                                "desc",
                              ],
                              [
                                "sailor",
                                "asc",
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
                                        "safe",
                                      ],
                                      "parentField": [
                                        "safe",
                                      ],
                                    },
                                    "subquery": {
                                      "alias": "zsubq_cleaner",
                                      "limit": 140,
                                      "orderBy": [
                                        [
                                          "captain",
                                          "asc",
                                        ],
                                        [
                                          "rationale",
                                          "desc",
                                        ],
                                        [
                                          "formula",
                                          "desc",
                                        ],
                                        [
                                          "sailor",
                                          "desc",
                                        ],
                                      ],
                                      "table": "cleaner",
                                      "where": {
                                        "conditions": [
                                          {
                                            "op": "NOT EXISTS",
                                            "related": {
                                              "correlation": {
                                                "childField": [
                                                  "safe",
                                                ],
                                                "parentField": [
                                                  "safe",
                                                ],
                                              },
                                              "subquery": {
                                                "alias": "zsubq_cleaner",
                                                "limit": 39,
                                                "orderBy": [
                                                  [
                                                    "bend",
                                                    "desc",
                                                  ],
                                                  [
                                                    "captain",
                                                    "desc",
                                                  ],
                                                  [
                                                    "printer",
                                                    "desc",
                                                  ],
                                                  [
                                                    "singing",
                                                    "desc",
                                                  ],
                                                  [
                                                    "sailor",
                                                    "asc",
                                                  ],
                                                ],
                                                "table": "cleaner",
                                                "where": {
                                                  "conditions": [
                                                    {
                                                      "op": "NOT EXISTS",
                                                      "related": {
                                                        "correlation": {
                                                          "childField": [
                                                            "safe",
                                                          ],
                                                          "parentField": [
                                                            "safe",
                                                          ],
                                                        },
                                                        "subquery": {
                                                          "alias": "zsubq_cleaner",
                                                          "limit": 177,
                                                          "orderBy": [
                                                            [
                                                              "formula",
                                                              "desc",
                                                            ],
                                                            [
                                                              "printer",
                                                              "asc",
                                                            ],
                                                            [
                                                              "captain",
                                                              "asc",
                                                            ],
                                                            [
                                                              "sailor",
                                                              "desc",
                                                            ],
                                                          ],
                                                          "table": "cleaner",
                                                          "where": {
                                                            "conditions": [
                                                              {
                                                                "left": {
                                                                  "name": "safe",
                                                                  "type": "column",
                                                                },
                                                                "op": "=",
                                                                "right": {
                                                                  "type": "literal",
                                                                  "value": "similique ater studio",
                                                                },
                                                                "type": "simple",
                                                              },
                                                              {
                                                                "left": {
                                                                  "name": "singing",
                                                                  "type": "column",
                                                                },
                                                                "op": "ILIKE",
                                                                "right": {
                                                                  "type": "literal",
                                                                  "value": "confugo amplitudo vesper",
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
                                                        "name": "safe",
                                                        "type": "column",
                                                      },
                                                      "op": "=",
                                                      "right": {
                                                        "type": "literal",
                                                        "value": "pecto absque ambitus",
                                                      },
                                                      "type": "simple",
                                                    },
                                                    {
                                                      "left": {
                                                        "name": "safe",
                                                        "type": "column",
                                                      },
                                                      "op": "=",
                                                      "right": {
                                                        "type": "literal",
                                                        "value": "stultus solio caelestis",
                                                      },
                                                      "type": "simple",
                                                    },
                                                    {
                                                      "left": {
                                                        "name": "bend",
                                                        "type": "column",
                                                      },
                                                      "op": "<",
                                                      "right": {
                                                        "type": "literal",
                                                        "value": 0.6496328990472147,
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
                                              "name": "accompanist",
                                              "type": "column",
                                            },
                                            "op": "=",
                                            "right": {
                                              "type": "literal",
                                              "value": 8470897860098338,
                                            },
                                            "type": "simple",
                                          },
                                          {
                                            "left": {
                                              "name": "captain",
                                              "type": "column",
                                            },
                                            "op": "ILIKE",
                                            "right": {
                                              "type": "literal",
                                              "value": null,
                                            },
                                            "type": "simple",
                                          },
                                          {
                                            "left": {
                                              "name": "captain",
                                              "type": "column",
                                            },
                                            "op": "IS",
                                            "right": {
                                              "type": "literal",
                                              "value": "ultio claustrum creo",
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
                                    "name": "accompanist",
                                    "type": "column",
                                  },
                                  "op": "IS",
                                  "right": {
                                    "type": "literal",
                                    "value": 0.9132405525564713,
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
                          "name": "rationale",
                          "type": "column",
                        },
                        "op": "=",
                        "right": {
                          "type": "literal",
                          "value": "contigo antepono enim",
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
          },
          "system": "permissions",
        },
      ],
      "table": "unit",
      "where": {
        "conditions": [
          {
            "op": "NOT EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "bend",
                ],
                "parentField": [
                  "backburn",
                ],
              },
              "subquery": {
                "alias": "zsubq_cleaner",
                "orderBy": [
                  [
                    "sailor",
                    "asc",
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
                            "safe",
                          ],
                          "parentField": [
                            "safe",
                          ],
                        },
                        "subquery": {
                          "alias": "zsubq_cleaner",
                          "limit": 134,
                          "orderBy": [
                            [
                              "printer",
                              "desc",
                            ],
                            [
                              "bend",
                              "desc",
                            ],
                            [
                              "accompanist",
                              "asc",
                            ],
                            [
                              "council",
                              "asc",
                            ],
                            [
                              "sailor",
                              "asc",
                            ],
                          ],
                          "table": "cleaner",
                          "where": {
                            "conditions": [
                              {
                                "op": "NOT EXISTS",
                                "related": {
                                  "correlation": {
                                    "childField": [
                                      "safe",
                                    ],
                                    "parentField": [
                                      "safe",
                                    ],
                                  },
                                  "subquery": {
                                    "alias": "zsubq_cleaner",
                                    "limit": 125,
                                    "orderBy": [
                                      [
                                        "sailor",
                                        "asc",
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
                                                "safe",
                                              ],
                                              "parentField": [
                                                "safe",
                                              ],
                                            },
                                            "subquery": {
                                              "alias": "zsubq_cleaner",
                                              "orderBy": [
                                                [
                                                  "council",
                                                  "asc",
                                                ],
                                                [
                                                  "accompanist",
                                                  "desc",
                                                ],
                                                [
                                                  "sailor",
                                                  "asc",
                                                ],
                                              ],
                                              "table": "cleaner",
                                              "where": {
                                                "conditions": [
                                                  {
                                                    "op": "NOT EXISTS",
                                                    "related": {
                                                      "correlation": {
                                                        "childField": [
                                                          "safe",
                                                        ],
                                                        "parentField": [
                                                          "safe",
                                                        ],
                                                      },
                                                      "subquery": {
                                                        "alias": "zsubq_cleaner",
                                                        "limit": 125,
                                                        "orderBy": [
                                                          [
                                                            "sailor",
                                                            "asc",
                                                          ],
                                                        ],
                                                        "table": "cleaner",
                                                        "where": {
                                                          "conditions": [
                                                            {
                                                              "left": {
                                                                "name": "sailor",
                                                                "type": "column",
                                                              },
                                                              "op": "=",
                                                              "right": {
                                                                "type": "literal",
                                                                "value": 7705303934764238,
                                                              },
                                                              "type": "simple",
                                                            },
                                                            {
                                                              "left": {
                                                                "name": "bend",
                                                                "type": "column",
                                                              },
                                                              "op": "<=",
                                                              "right": {
                                                                "type": "literal",
                                                                "value": 4264654580915609,
                                                              },
                                                              "type": "simple",
                                                            },
                                                            {
                                                              "left": {
                                                                "name": "council",
                                                                "type": "column",
                                                              },
                                                              "op": "<=",
                                                              "right": {
                                                                "type": "literal",
                                                                "value": 0.6350936508676438,
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
                                                      "name": "formula",
                                                      "type": "column",
                                                    },
                                                    "op": "IS",
                                                    "right": {
                                                      "type": "literal",
                                                      "value": true,
                                                    },
                                                    "type": "simple",
                                                  },
                                                  {
                                                    "left": {
                                                      "name": "safe",
                                                      "type": "column",
                                                    },
                                                    "op": "=",
                                                    "right": {
                                                      "type": "literal",
                                                      "value": "necessitatibus cresco sed",
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
                                            "name": "formula",
                                            "type": "column",
                                          },
                                          "op": "=",
                                          "right": {
                                            "type": "literal",
                                            "value": true,
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
                                  "name": "bend",
                                  "type": "column",
                                },
                                "op": "IS",
                                "right": {
                                  "type": "literal",
                                  "value": 6760936631577134,
                                },
                                "type": "simple",
                              },
                              {
                                "left": {
                                  "name": "formula",
                                  "type": "column",
                                },
                                "op": "IS NOT",
                                "right": {
                                  "type": "literal",
                                  "value": true,
                                },
                                "type": "simple",
                              },
                              {
                                "left": {
                                  "name": "bend",
                                  "type": "column",
                                },
                                "op": ">",
                                "right": {
                                  "type": "literal",
                                  "value": 0.42899402737501835,
                                },
                                "type": "simple",
                              },
                              {
                                "left": {
                                  "name": "bend",
                                  "type": "column",
                                },
                                "op": "IS",
                                "right": {
                                  "type": "literal",
                                  "value": 0.5052523724478571,
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
                        "name": "bend",
                        "type": "column",
                      },
                      "op": "IS",
                      "right": {
                        "type": "literal",
                        "value": 518443220325734,
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "rationale",
                        "type": "column",
                      },
                      "op": "IS",
                      "right": {
                        "type": "literal",
                        "value": "angelus aranea temptatio",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "name": "safe",
                        "type": "column",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "aequus traho suffoco",
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
              "name": "backburn",
              "type": "column",
            },
            "op": ">",
            "right": {
              "type": "literal",
              "value": 0.9860010638228709,
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "substitution",
              "type": "column",
            },
            "op": "IS",
            "right": {
              "type": "literal",
              "value": "labore curriculum ventus",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "backburn",
              "type": "column",
            },
            "op": "<=",
            "right": {
              "type": "literal",
              "value": 507134822760922,
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "backburn",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": 0.5833687650971596,
            },
            "type": "simple",
          },
        ],
        "type": "and",
      },
    }
  `);
});
