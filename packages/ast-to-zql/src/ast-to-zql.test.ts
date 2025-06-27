import {en, Faker, generateMersenne53Randomizer} from '@faker-js/faker';
import {expect, test} from 'vitest';
import {type AST} from '../../zero-protocol/src/ast.ts';
import {ast} from '../../zql/src/query/query-impl.ts';
import {staticQuery} from '../../zql/src/query/static-query.ts';
import {generateQuery} from '../../zql/src/query/test/query-gen.ts';
import {generateSchema} from '../../zql/src/query/test/schema-gen.ts';
import {astToZQL} from './ast-to-zql.ts';

test('simple table selection', () => {
  const ast: AST = {
    table: 'issue',
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`""`);
});

test('simple where condition with equality', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'id'},
      op: '=',
      right: {type: 'literal', value: 123},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('id', 123)"`);
});

test('where condition with non-equality operator', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'priority'},
      op: '>',
      right: {type: 'literal', value: 2},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('priority', '>', 2)"`);
});

test('not exists over a junction edge', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      op: 'NOT EXISTS',
      related: {
        correlation: {
          childField: ['issueId'],
          parentField: ['id'],
        },
        subquery: {
          alias: 'zsubq_labels',
          orderBy: [
            ['issueId', 'asc'],
            ['labelId', 'asc'],
          ],
          table: 'issueLabel',
          where: {
            op: 'EXISTS',
            related: {
              correlation: {
                childField: ['id'],
                parentField: ['labelId'],
              },
              subquery: {
                alias: 'zsubq_zhidden_labels',
                orderBy: [['id', 'asc']],
                table: 'label',
              },
              system: 'permissions',
            },
            type: 'correlatedSubquery',
          },
        },
        system: 'permissions',
      },
      type: 'correlatedSubquery',
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({exists, not}) => not(exists('labels', q => q.orderBy('id', 'asc'))))"`,
  );
});

test('simple where condition with single AND', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('id', 123)"`);
});

test('simple where condition with single OR', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".where('id', 123)"`);
});

test('AND condition using multiple where clauses', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('id', 123).where('status', 'open')"`,
  );
});

test('only top level AND should be spread into where calls', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 123},
        },
        {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'status'},
              op: '=',
              right: {type: 'literal', value: 'open'},
            },
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'status'},
                  op: '=',
                  right: {type: 'literal', value: 'in-progress'},
                },
                {
                  type: 'simple',
                  left: {type: 'column', name: 'priority'},
                  op: '>=',
                  right: {type: 'literal', value: 3},
                },
              ],
            },
          ],
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('id', 123).where(({and, cmp, or}) => or(cmp('status', 'open'), and(cmp('status', 'in-progress'), cmp('priority', '>=', 3)))).where('status', 'open')"`,
  );
});

test('OR condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'in-progress'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({cmp, or}) => or(cmp('status', 'open'), cmp('status', 'in-progress')))"`,
  );
});

test('with orderBy', () => {
  const ast: AST = {
    table: 'issue',
    orderBy: [
      ['priority', 'desc'],
      ['created_at', 'asc'],
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".orderBy('priority', 'desc').orderBy('created_at', 'asc')"`,
  );
});

test('with limit', () => {
  const ast: AST = {
    table: 'issue',
    limit: 10,
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".limit(10)"`);
});

test('with start', () => {
  const ast: AST = {
    table: 'issue',
    start: {
      row: {id: 5},
      exclusive: false,
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".start({"id":5}, { inclusive: true })"`,
  );
});

test('whereExists condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'zsubq_comments',
        },
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".whereExists('comments')"`);
});

test('whereNotExists condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'zsubq_comments',
        },
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({exists, not}) => not(exists('comments')))"`,
  );
});

test('whereNotExists condition with orderBy in subquery', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      related: {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'zsubq_comments',
          orderBy: [['created_at', 'desc']],
        },
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where(({exists, not}) => not(exists('comments', q => q.orderBy('created_at', 'desc'))))"`,
  );
});

test('NOT LIKE operator', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'title'},
      op: 'NOT LIKE',
      right: {type: 'literal', value: '%urgent%'},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('title', 'NOT LIKE', '%urgent%')"`,
  );
});

test('NOT ILIKE operator', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'title'},
      op: 'NOT ILIKE',
      right: {type: 'literal', value: '%urgent%'},
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('title', 'NOT ILIKE', '%urgent%')"`,
  );
});

test('NOT LIKE in complex condition', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'title'},
          op: 'NOT LIKE',
          right: {type: 'literal', value: '%bug%'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'open'},
        },
      ],
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('title', 'NOT LIKE', '%bug%').where('status', 'open')"`,
  );
});

test('related query', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".related('comments')"`);
});

test('related query with filters', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'is_deleted'},
            op: '=',
            right: {type: 'literal', value: false},
          },
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".related('comments', q => q.where('is_deleted', false))"`,
  );
});

test('nested related query with filters', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'is_deleted'},
            op: '=',
            right: {type: 'literal', value: false},
          },
          related: [
            {
              correlation: {
                parentField: ['authorID'],
                childField: ['id'],
              },
              subquery: {
                table: 'user',
                alias: 'author',
                where: {
                  type: 'simple',
                  left: {type: 'column', name: 'name'},
                  op: '=',
                  right: {type: 'literal', value: 'Bob'},
                },
              },
            },
          ],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".related('comments', q => q.where('is_deleted', false).related('author', q => q.where('name', 'Bob')))"`,
  );
});

test('related query with hidden junction', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issueId'],
        },
        hidden: true,
        subquery: {
          table: 'issueLabel',
          alias: 'labels',
          related: [
            {
              correlation: {
                parentField: ['labelId'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                alias: 'labels',
              },
            },
          ],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(`".related('labels')"`);
});

test('related query with hidden junction with filters', () => {
  const ast: AST = {
    table: 'issue',
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issueId'],
        },
        hidden: true,
        subquery: {
          table: 'issueLabel',
          alias: 'labels',
          related: [
            {
              correlation: {
                parentField: ['labelId'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                alias: 'labels',
                where: {
                  type: 'simple',
                  left: {type: 'column', name: 'name'},
                  op: '=',
                  right: {type: 'literal', value: 'Bob'},
                },
              },
            },
          ],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".related('labels', q => q.where('name', 'Bob'))"`,
  );
});

test('complex query with multiple features', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '!=',
          right: {type: 'literal', value: 'closed'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'priority'},
          op: '>=',
          right: {type: 'literal', value: 3},
        },
      ],
    },
    orderBy: [['created_at', 'desc']],
    limit: 20,
    related: [
      {
        correlation: {
          parentField: ['id'],
          childField: ['issue_id'],
        },
        subquery: {
          table: 'comment',
          alias: 'comments',
          limit: 5,
          orderBy: [['created_at', 'desc']],
        },
      },
    ],
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('status', '!=', 'closed').where('priority', '>=', 3).related('comments', q => q.orderBy('created_at', 'desc').limit(5)).orderBy('created_at', 'desc').limit(20)"`,
  );
});

test('with auth parameter', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'owner_id'},
      op: '=',
      right: {
        type: 'static',
        anchor: 'authData',
        field: 'id',
      },
    },
  };
  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".where('owner_id', authParam('id'))"`,
  );
});

test('EXISTS with order', () => {
  const ast: AST = {
    table: 'users',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        correlation: {parentField: ['recruiterID'], childField: ['id']},
        subquery: {
          table: 'users',
          alias: 'zsubq_recruiter',
          where: {
            type: 'simple',
            left: {type: 'column', name: 'y'},
            op: '>',
            right: {type: 'literal', value: 0},
          },
        },
      },
      op: 'EXISTS',
    },
  };

  expect(astToZQL(ast)).toMatchInlineSnapshot(
    `".whereExists('recruiter', q => q.where('y', '>', 0)).orderBy('id', 'asc')"`,
  );
});

test('round trip', () => {
  const randomizer = generateMersenne53Randomizer(42);
  const rng = () => randomizer.next();
  const faker = new Faker({
    locale: en,
    randomizer,
  });

  const codes: string[] = [];

  for (let i = 0; i < 10; i++) {
    const schema = generateSchema(rng, faker, 10);
    const q = generateQuery(schema, {}, rng, faker);

    const code = astToZQL(ast(q));
    codes.push(code);

    const q2 = new Function(
      'staticQuery',
      'schema',
      'tableName',
      `return staticQuery(schema, tableName)${code}`,
    )(staticQuery, schema, ast(q).table);
    expect(ast(q2)).toEqual(ast(q));
  }

  expect(codes).toMatchInlineSnapshot(`
    [
      ".where('heating', false).where('typeface', 'IS NOT', 'corrupti circumvenio ustilo').limit(2)",
      ".where(({exists, not}) => not(exists('other', q => q.orderBy('morbidity', 'asc').orderBy('guidance', 'desc').orderBy('monasticism', 'asc')))).orderBy('request', 'desc').orderBy('cruelty', 'asc').orderBy('knitting', 'asc').limit(4)",
      ".where(({exists, not}) => not(exists('guide', q => q.where('backbone', 'IS', 'verto derideo vulgivagus').where('cash', false).where('backbone', 'ILIKE', 'texo cogo aggero').orderBy('antelope', 'desc').orderBy('crest', 'asc').limit(121)))).where('backbone', null).orderBy('igloo', 'asc').orderBy('antelope', 'desc').orderBy('crest', 'desc').orderBy('tuba', 'asc')",
      "",
      ".where(({exists, not}) => not(exists('vanadyl', q => q.orderBy('nucleotidase', 'desc').orderBy('information', 'asc').orderBy('vol', 'asc')))).where('phrase', 'ILIKE', 'trepide vorax civitas').where('hyena', 'IS', 'hic arguo iste').where('thigh', 'LIKE', 'decet cognatus caveo').where('scorpion', 'voco labore conor').related('vanadyl', q => q.where('cappelletti', '!=', 1284373868304998).where('cinema', 'IS', 'acies sint repellendus').where('nucleotidase', 'colo spiculum pecto').orderBy('following', 'asc').orderBy('information', 'asc').orderBy('vol', 'asc')).orderBy('rubric', 'desc').orderBy('hyena', 'desc').limit(79)",
      ".where('collectivization', 7807173291138031).where('collectivization', 'IS NOT', 5224975719318282).limit(56)",
      ".whereExists('forager', q => q.where(({exists, not}) => not(exists('help', q => q.where('eggplant', 'terebro infit aeger').orderBy('mantua', 'desc').orderBy('fireplace', 'desc').orderBy('atrium', 'desc').orderBy('omelet', 'desc').orderBy('adrenalin', 'asc').orderBy('pinstripe', 'asc').limit(115)))).where('finding', 'IS', 'balbus combibo carbo').where('finding', 'IS', 'audeo aestus subito').where('finding', 'LIKE', 'cernuus stultus celer').orderBy('finding', 'asc').orderBy('stock', 'asc').limit(64)).where('subsidy', 'LIKE', 'celo somniculosus tot').limit(37)",
      ".where('carnival', 'IS NOT', true).where('hornet', 'IS NOT', 'vigilo attonbitus qui').orderBy('flint', 'asc')",
      ".where('deer', 'IS NOT', 0.07675806188873802).where('disposer', '!=', 4410321756724066).where('teammate', 'altus viscus patruus').where('handover', '!=', 'capillus alo atrox').orderBy('synergy', 'asc').orderBy('designation', 'desc').limit(48)",
      ".where('density', 'IS NOT', null).where('asset', '!=', true).where('sweatshop', '>=', 1771609784252409).where('sweatshop', '>', 0.36250198453151317).orderBy('asset', 'desc')",
    ]
  `);
});
