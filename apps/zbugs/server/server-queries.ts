import {type Query} from '@rocicorp/zero';
import {schema} from '../shared/schema.ts';
import type {Role} from '../shared/auth.ts';
import {issuePreload as sharedIssuePreload} from '../shared/queries.ts';

type TODO = any;
export function issuePreload(
  serverContext: {
    role: Role;
    db: PostgresDB;
  },
  tx: TODO,
  userID: string,
): Query<typeof schema, 'issue'> {
  const baseQuery = sharedIssuePreload(tx, userID);
  if (serverContext.role !== 'crew') {
    return baseQuery.where('visibility', 'public');
  }

  return baseQuery;
}
