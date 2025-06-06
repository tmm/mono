import {type Query} from '@rocicorp/zero';
import {schema} from '../shared/schema.ts';
import type {Role} from '../shared/auth.ts';
import {issuePreload as sharedIssuePreload} from '../shared/queries.ts';

type TODO = any;
export function issuePreload(
  serverContext: {
    role: Role;
  },
  tx: TODO,
  userID: string,
): Query<typeof schema, 'issue'> {
  // Get the tx object.
  // Call the client version.
  // Augment it with role on existence checks to filter out private issues
  const baseQuery = sharedIssuePreload(tx, userID);
  if (serverContext.role !== 'crew') {
    return baseQuery.where('visibility', 'public');
  }

  return baseQuery;
}
