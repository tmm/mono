import {renderHook, testEffect} from '@solidjs/testing-library';
import {
  createEffect,
  createSignal,
  runWithOwner,
  type Accessor,
  type JSX,
} from 'solid-js';
import {expect, test, vi} from 'vitest';
import {must} from '../../shared/src/must.ts';
import {
  createSchema,
  number,
  relationships,
  string,
  table,
  Zero,
  type Query,
  type Schema,
  type TTL,
} from '../../zero/src/zero.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {idSymbol, refCountSymbol} from '../../zql/src/ivm/view-apply-change.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {useQuery, type UseQueryOptions} from './use-query.ts';
import {ZeroProvider} from './use-zero.ts';

function setupTestEnvironment() {
  const schema = createSchema({
    tables: [
      table('table')
        .columns({
          a: number(),
          b: string(),
        })
        .primaryKey('a'),
    ],
  });
  const ms = new MemorySource(
    schema.tables.table.name,
    schema.tables.table.columns,
    schema.tables.table.primaryKey,
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const queryDelegate = new QueryDelegateImpl({sources: {table: ms}});
  const tableQuery = newQuery(queryDelegate, schema, 'table');

  return {ms, tableQuery, queryDelegate, schema};
}

function newMockZero(clientID: string): Zero<Schema> {
  return {
    clientID,
  } as unknown as Zero<Schema>;
}

function useQueryWithZeroProvider<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  clientIDOrZeroSignal: string | Accessor<Zero<Schema>>,
  querySignal: () => Query<TSchema, TTable, TReturn>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
) {
  const isClientID = typeof clientIDOrZeroSignal === 'string';
  const zero = newMockZero(isClientID ? clientIDOrZeroSignal : 'unused');
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const MockZeroProvider = (props: {children: JSX.Element}) => (
    <ZeroProvider zero={!isClientID ? clientIDOrZeroSignal() : zero}>
      {props.children}
    </ZeroProvider>
  );
  return renderHook(useQuery, {
    initialProps: [querySignal, options],
    wrapper: MockZeroProvider,
  });
}

test('useQuery', async () => {
  const {ms, tableQuery, queryDelegate} = setupTestEnvironment();
  const querySignal = vi.fn(() => tableQuery);

  const {
    result: [rows, resultType],
  } = useQueryWithZeroProvider('useQuery-id', querySignal);

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
  ]);
  expect(resultType()).toEqual({type: 'unknown'});

  must(queryDelegate.gotCallbacks[0])(true);
  await 1;

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});
  queryDelegate.commit();

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
    {a: 3, b: 'c', [refCountSymbol]: 1, [idSymbol]: '3'},
  ]);
  expect(resultType()).toEqual({type: 'complete'});
});

test('useQuery with ttl', () => {
  const {tableQuery, queryDelegate} = setupTestEnvironment();
  const [ttl, setTTL] = createSignal<TTL>('1m');

  const materializeSpy = vi.spyOn(tableQuery, 'materialize');
  const addServerQuerySpy = vi.spyOn(queryDelegate, 'addServerQuery');
  const updateServerQuerySpy = vi.spyOn(queryDelegate, 'updateServerQuery');

  const querySignal = vi.fn(() => tableQuery);

  useQueryWithZeroProvider('useQuery-with-ttl-id', querySignal, () => ({
    ttl: ttl(),
  }));

  expect(querySignal).toHaveBeenCalledTimes(1);
  expect(addServerQuerySpy).toHaveBeenCalledTimes(1);
  expect(updateServerQuerySpy).toHaveBeenCalledTimes(0);

  addServerQuerySpy.mockClear();
  materializeSpy.mockClear();

  setTTL('10m');
  expect(addServerQuerySpy).toHaveBeenCalledTimes(0);
  expect(updateServerQuerySpy).toHaveBeenCalledExactlyOnceWith(
    {
      orderBy: [['a', 'asc']],
      table: 'table',
    },
    '10m',
  );
  expect(materializeSpy).toHaveBeenCalledTimes(0);
});

test('useQuery query deps change', async () => {
  const {tableQuery, queryDelegate} = setupTestEnvironment();

  const [a, setA] = createSignal(1);
  const querySignal = () => tableQuery.where('a', a());

  const renderHookResult = useQueryWithZeroProvider(
    'useQuery-deps-change-id',
    querySignal,
  );

  const [rows, resultDetails] = renderHookResult.result;

  const rowLog: unknown[] = [];
  const resultDetailsLog: unknown[] = [];
  const resetLogs = () => {
    rowLog.length = 0;
    resultDetailsLog.length = 0;
  };

  runWithOwner(renderHookResult.owner, () => {
    createEffect(() => {
      rowLog.push([...rows()]);
    });
    createEffect(() => {
      const details = resultDetails();
      resultDetailsLog.push({...details});
    });
  });

  expect(rowLog).toEqual([
    [{a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'}],
  ]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(rowLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  setA(2);
  expect(rowLog).toEqual([
    [{a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'}],
  ]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(rowLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
});

test('useQuery query deps change, reconcile minimizes reactive updates', async () => {
  const {tableQuery, queryDelegate} = setupTestEnvironment();

  const [querySignal, setQuery] = createSignal(tableQuery.where('a', 1));

  const renderHookResult = useQueryWithZeroProvider(
    'useQuery-deps-change-id',
    querySignal,
  );

  const [rows, resultDetails] = renderHookResult.result;

  const row0Log: unknown[] = [];
  const row1Log: unknown[] = [];
  const resultDetailsLog: unknown[] = [];
  const resetLogs = () => {
    row0Log.length = 0;
    row1Log.length = 0;
    resultDetailsLog.length = 0;
  };

  runWithOwner(renderHookResult.owner, () => {
    createEffect(() => {
      row0Log.push(rows()[0]);
    });
    createEffect(() => {
      row1Log.push(rows()[1]);
    });
    createEffect(() => {
      const details = resultDetails();
      resultDetailsLog.push({...details});
    });
  });

  expect(row0Log).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);
  expect(row1Log).toEqual([undefined]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  setQuery(tableQuery.where(({or, cmp}) => or(cmp('a', 1), cmp('a', 10))));
  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  setQuery(tableQuery.where(({or, cmp}) => or(cmp('a', 1), cmp('a', 2))));
  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([
    {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
  ]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0Log).toEqual([]);
  expect(row1Log).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
});

test('useQuery query deps change, reconcile minimizes reactive updates, tree', async () => {
  const issue = table('issue')
    .columns({
      id: string(),
    })
    .primaryKey('id');
  const comment = table('comment')
    .columns({
      id: string(),
      issueID: string(),
    })
    .primaryKey('id');
  const schema = createSchema({
    tables: [issue, comment],
    relationships: [
      relationships(issue, connect => ({
        comments: connect.many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
      })),
    ],
  });
  const issueSource = new MemorySource(
    schema.tables.issue.name,
    schema.tables.issue.columns,
    schema.tables.issue.primaryKey,
  );
  const commentSource = new MemorySource(
    schema.tables.comment.name,
    schema.tables.comment.columns,
    schema.tables.comment.primaryKey,
  );
  issueSource.push({row: {id: 'i1'}, type: 'add'});
  issueSource.push({row: {id: 'i2'}, type: 'add'});
  commentSource.push({row: {id: 'c1', issueID: 'i1'}, type: 'add'});
  commentSource.push({row: {id: 'c2', issueID: 'i1'}, type: 'add'});

  const queryDelegate = new QueryDelegateImpl({
    sources: {issue: issueSource, comment: commentSource},
  });
  const issueQuery = newQuery(queryDelegate, schema, 'issue');

  const [querySignal, setQuery] = createSignal(
    issueQuery.where('id', 'i1').related('comments'),
  );

  const renderHookResult = useQueryWithZeroProvider(
    'useQuery-deps-change-id',
    querySignal,
  );

  const [rows, resultDetails] = renderHookResult.result;

  expect(rows()).toMatchInlineSnapshot(`
    [
      {
        "comments": [
          {
            "id": "c1",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c1"",
          },
          {
            "id": "c2",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c2"",
          },
        ],
        "id": "i1",
        Symbol(rc): 1,
        Symbol(id): ""i1"",
      },
    ]
  `);

  const row0IDLog: unknown[] = [];
  const row1IDLog: unknown[] = [];
  const row00IDLog: unknown[] = [];
  const row01IDLog: unknown[] = [];
  const resultDetailsLog: unknown[] = [];
  const resetLogs = () => {
    row0IDLog.length = 0;
    row1IDLog.length = 0;
    row00IDLog.length = 0;
    row01IDLog.length = 0;
    resultDetailsLog.length = 0;
  };

  runWithOwner(renderHookResult.owner, () => {
    createEffect(() => {
      row0IDLog.push(rows()[0]?.id);
    });
    createEffect(() => {
      row1IDLog.push(rows()[1]?.id);
    });
    createEffect(() => {
      row00IDLog.push(rows()[0]?.['comments']?.[0]?.id);
    });
    createEffect(() => {
      row01IDLog.push(rows()[0]?.['comments']?.[1]?.id);
    });
    createEffect(() => {
      const details = resultDetails();
      resultDetailsLog.push({...details});
    });
  });

  expect(row0IDLog).toEqual(['i1']);
  expect(row1IDLog).toEqual([undefined]);
  expect(row00IDLog).toEqual(['c1']);
  expect(row01IDLog).toEqual(['c2']);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(row00IDLog).toEqual([]);
  expect(row01IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  setQuery(
    issueQuery
      .where(({or, cmp}) => or(cmp('id', 'i1'), cmp('id', 'i10')))
      .related('comments'),
  );
  expect(rows()).toMatchInlineSnapshot(`
    [
      {
        "comments": [
          {
            "id": "c1",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c1"",
          },
          {
            "id": "c2",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c2"",
          },
        ],
        "id": "i1",
        Symbol(rc): 1,
        Symbol(id): ""i1"",
      },
    ]
  `);

  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(row00IDLog).toEqual([]);
  expect(row01IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(row00IDLog).toEqual([]);
  expect(row01IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  setQuery(
    issueQuery
      .where(({or, cmp}) => or(cmp('id', 'i1'), cmp('id', 'i2')))
      .related('comments'),
  );
  expect(rows()).toMatchInlineSnapshot(`
    [
      {
        "comments": [
          {
            "id": "c1",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c1"",
          },
          {
            "id": "c2",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c2"",
          },
        ],
        "id": "i1",
        Symbol(rc): 1,
        Symbol(id): ""i1"",
      },
      {
        "comments": [],
        "id": "i2",
        Symbol(rc): 1,
        Symbol(id): ""i2"",
      },
    ]
  `);
  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual(['i2']);
  expect(row00IDLog).toEqual([]);
  expect(row01IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
  resetLogs();

  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  setQuery(
    issueQuery
      .where(({or, cmp}) => or(cmp('id', 'i1'), cmp('id', 'i2')))
      .related('comments', q => q.where('id', 'c1')),
  );
  expect(rows()).toMatchInlineSnapshot(`
    [
      {
        "comments": [
          {
            "id": "c1",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c1"",
          },
        ],
        "id": "i1",
        Symbol(rc): 1,
        Symbol(id): ""i1"",
      },
      {
        "comments": [],
        "id": "i2",
        Symbol(rc): 1,
        Symbol(id): ""i2"",
      },
    ]
  `);
  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(row00IDLog).toEqual([]);
  expect(row01IDLog).toEqual([undefined]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);

  resetLogs();

  // eslint-disable-next-line @typescript-eslint/no-floating-promises
  setQuery(
    issueQuery
      .where(({or, cmp}) => or(cmp('id', 'i1'), cmp('id', 'i2')))
      .related('comments', q => q.where('id', 'c2')),
  );
  expect(rows()).toMatchInlineSnapshot(`
    [
      {
        "comments": [
          {
            "id": "c2",
            "issueID": "i1",
            Symbol(rc): 1,
            Symbol(id): ""c2"",
          },
        ],
        "id": "i1",
        Symbol(rc): 1,
        Symbol(id): ""i1"",
      },
      {
        "comments": [],
        "id": "i2",
        Symbol(rc): 1,
        Symbol(id): ""i2"",
      },
    ]
  `);
  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(row00IDLog).toEqual(['c2']);
  expect(row01IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'unknown'}]);
  resetLogs();

  queryDelegate.gotCallbacks.forEach(cb => cb?.(true));
  await 1;

  expect(row0IDLog).toEqual([]);
  expect(row1IDLog).toEqual([]);
  expect(resultDetailsLog).toEqual([{type: 'complete'}]);
});

test('useQuery query deps change testEffect', () => {
  const {ms, tableQuery, queryDelegate} = setupTestEnvironment();

  const [a, setA] = createSignal(1);
  const querySignal = () => tableQuery.where('a', a());

  const renderHookResult = useQueryWithZeroProvider(
    'useQuery-deps-change-testEffect-id',
    querySignal,
  );

  const [rows] = renderHookResult.result;

  return testEffect(done =>
    createEffect((run: number = 0) => {
      if (run === 0) {
        expect(rows()).toEqual([
          {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
        ]);
        ms.push({type: 'edit', oldRow: {a: 1, b: 'a'}, row: {a: 1, b: 'a2'}});
        queryDelegate.commit();
      } else if (run === 1) {
        expect(rows()).toEqual([
          {a: 1, b: 'a2', [refCountSymbol]: 1, [idSymbol]: '1'},
        ]);
        setA(2);
      } else if (run === 2) {
        expect(rows()).toEqual([
          {a: 2, b: 'b', [refCountSymbol]: 1, [idSymbol]: '2'},
        ]);
        done();
      }
      return run + 1;
    }),
  );
});

test('useQuery ttl dep changed', () => {
  const {tableQuery} = setupTestEnvironment();
  const query = tableQuery.where('a', 1);
  const materializeSpy = vi.spyOn(query, 'materialize');
  const querySignal = () => query;

  const [options, setOptions] = createSignal<UseQueryOptions>({ttl: '1d'});

  const clientID = 'useQuery ttl dep changed';
  const {
    result: [rows],
  } = useQueryWithZeroProvider(clientID, querySignal, options);

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);

  expect(materializeSpy).toHaveBeenCalledTimes(1);

  const view = materializeSpy.mock.results[0].value;
  const destroySpy = vi.spyOn(view, 'destroy');
  const updateTTLSpy = vi.spyOn(view, 'updateTTL');

  expect(destroySpy).toHaveBeenCalledTimes(0);
  expect(updateTTLSpy).toHaveBeenCalledTimes(0);

  setOptions({ttl: '2d'});

  expect(destroySpy).toHaveBeenCalledTimes(0);
  expect(updateTTLSpy).toHaveBeenCalledExactlyOnceWith('2d');
});

test('useQuery view disposed when owner cleaned up', () => {
  const {tableQuery} = setupTestEnvironment();
  const query = tableQuery.where('a', 1);
  const materializeSpy = vi.spyOn(query, 'materialize');
  const querySignal = () => query;

  const clientID = 'useQuery-view-disposed-when-owner-cleaned-up';
  const {
    result: [rows],
    cleanup,
  } = useQueryWithZeroProvider(clientID, querySignal);

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);

  expect(materializeSpy).toHaveBeenCalledTimes(1);

  const view = materializeSpy.mock.results[0].value;
  const destroySpy = vi.spyOn(view, 'destroy');

  expect(destroySpy).toHaveBeenCalledTimes(0);

  cleanup();

  expect(destroySpy).toHaveBeenCalledTimes(1);
});

test('useQuery view disposed when zero instance changes, new view created', () => {
  const {tableQuery} = setupTestEnvironment();
  const query = tableQuery.where('a', 1);
  const materializeSpy = vi.spyOn(query, 'materialize');
  const querySignal = () => query;

  const clientID =
    'useQuery view disposed when zero instance changes, new view created';
  const mockZero0 = newMockZero(clientID);
  const [zero, setZero] = createSignal(mockZero0);
  const {
    result: [rows],
  } = useQueryWithZeroProvider(zero, querySignal);

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);

  expect(materializeSpy).toHaveBeenCalledTimes(1);

  const view = materializeSpy.mock.results[0].value;
  const destroySpy = vi.spyOn(view, 'destroy');

  expect(destroySpy).toHaveBeenCalledTimes(0);

  setZero(mockZero0);

  expect(destroySpy).toHaveBeenCalledTimes(0);
  expect(materializeSpy).toHaveBeenCalledTimes(1);

  const mockZero1 = newMockZero(clientID + '1');

  setZero(mockZero1);

  expect(destroySpy).toHaveBeenCalledTimes(1);
  expect(materializeSpy).toHaveBeenCalledTimes(2);
});

test('useQuery view disposed when query changes and new view is created', () => {
  const {tableQuery} = setupTestEnvironment();
  const queries = [
    tableQuery.where('a', 1),
    tableQuery.where('a', 2),
    tableQuery.where('a', 3),
  ];
  const querySpies = queries.map(q => vi.spyOn(q, 'materialize'));
  const [queryIndex, setQueryIndex] = createSignal(0);
  const querySignal = () => queries[queryIndex()];

  const clientID = 'useQuery-view-disposed-when-owner-cleaned-up';
  const {
    result: [rows],
    cleanup,
  } = useQueryWithZeroProvider(clientID, querySignal);

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);

  expect(querySpies[0]).toHaveBeenCalledTimes(1);
  expect(querySpies[1]).toHaveBeenCalledTimes(0);
  expect(querySpies[2]).toHaveBeenCalledTimes(0);

  const view0 = querySpies[0].mock.results[0].value;
  const destroy0Spy = vi.spyOn(view0, 'destroy');

  expect(destroy0Spy).toHaveBeenCalledTimes(0);

  setQueryIndex(1);

  expect(destroy0Spy).toHaveBeenCalledTimes(1);

  expect(querySpies[0]).toHaveBeenCalledTimes(1);
  expect(querySpies[1]).toHaveBeenCalledTimes(1);
  expect(querySpies[2]).toHaveBeenCalledTimes(0);

  const view1 = querySpies[1].mock.results[0].value;
  const destroy1Spy = vi.spyOn(view1, 'destroy');

  expect(destroy1Spy).toHaveBeenCalledTimes(0);

  setQueryIndex(2);

  expect(destroy0Spy).toHaveBeenCalledTimes(1);
  expect(destroy1Spy).toHaveBeenCalledTimes(1);

  expect(querySpies[0]).toHaveBeenCalledTimes(1);
  expect(querySpies[1]).toHaveBeenCalledTimes(1);
  expect(querySpies[2]).toHaveBeenCalledTimes(1);

  const view2 = querySpies[2].mock.results[0].value;
  const destroy2Spy = vi.spyOn(view2, 'destroy');

  expect(destroy2Spy).toHaveBeenCalledTimes(0);

  cleanup();

  expect(destroy0Spy).toHaveBeenCalledTimes(1);
  expect(destroy1Spy).toHaveBeenCalledTimes(1);
  expect(destroy2Spy).toHaveBeenCalledTimes(1);
});

test('useQuery when ZeroProvider is used, view is reused if query instance changes as long as hash does not change', () => {
  const {tableQuery} = setupTestEnvironment();
  const queries = [tableQuery.where('a', 1), tableQuery.where('a', 1)];
  const querySpies = queries.map(q => vi.spyOn(q, 'materialize'));
  const [queryIndex, setQueryIndex] = createSignal(0);
  const querySignal = () => queries[queryIndex()];

  const clientID =
    'useQuery when ZeroProvider is used, view is reused if query instance changes as long as hash does not change';
  const {
    result: [rows],
    cleanup,
  } = useQueryWithZeroProvider(clientID, querySignal);

  expect(rows()).toEqual([
    {a: 1, b: 'a', [refCountSymbol]: 1, [idSymbol]: '1'},
  ]);

  expect(querySpies[0]).toHaveBeenCalledTimes(1);
  expect(querySpies[1]).toHaveBeenCalledTimes(0);

  const view0 = querySpies[0].mock.results[0].value;
  const destroy0Spy = vi.spyOn(view0, 'destroy');

  expect(destroy0Spy).toHaveBeenCalledTimes(0);

  setQueryIndex(1);

  expect(destroy0Spy).toHaveBeenCalledTimes(0);

  expect(querySpies[0]).toHaveBeenCalledTimes(1);
  expect(querySpies[1]).toHaveBeenCalledTimes(0);

  cleanup();

  expect(destroy0Spy).toHaveBeenCalledTimes(1);
});

test('useQuery when ZeroProvider is not-used, view is not-reused if query instance changes even if hash does not change', () => {
  const {tableQuery} = setupTestEnvironment();
  const queries = [tableQuery.where('a', 1), tableQuery.where('a', 1)];
  const [queryIndex] = createSignal(0);
  const querySignal = () => queries[queryIndex()];

  expect(() => {
    renderHook(useQuery, {
      initialProps: [querySignal],
    });
  }).toThrow('useZero must be used within a ZeroProvider');
});
