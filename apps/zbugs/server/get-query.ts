import type {AnyQuery, ReadonlyJSONValue} from '@rocicorp/zero';
import {queries} from '../shared/queries.ts';
import type {AuthData} from '../shared/auth.ts';

export function getQuery(
  context: AuthData | undefined,
  name: string,
  args: readonly ReadonlyJSONValue[],
) {
  let query;
  if (isSharedQuery(name)) {
    query = (
      queries[name] as (
        context: AuthData | undefined,
        ...args: readonly ReadonlyJSONValue[]
      ) => AnyQuery
    )(context, ...args);
  } else {
    throw new Error(`Unknown query: ${name}`);
  }

  return query;
}

function isSharedQuery(key: string): key is keyof typeof queries {
  return key in queries;
}
