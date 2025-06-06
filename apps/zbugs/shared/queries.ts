import {query} from '@rocicorp/zero';
import {schema} from './schema.ts';

const q = query.bindTo(schema);

export const allLabels = q('labelsPreload', tx => tx.label);
export const allUsers = q('usersPreload', tx => tx.user);

// Preload all issues and first 10 comments from each.
// location === server check...
// we can do logic in here too...
// but we don't know if data is available for that logic.
export const issuePreload = q('issuePreload', (tx, userID: string) =>
  tx.issue
    .related('labels')
    .related('viewState', q => q.where('userID', userID))
    .related('creator')
    .related('assignee')
    .related('emoji', emoji => emoji.related('creator'))
    .related('comments', comments =>
      comments
        .related('creator')
        .related('emoji', emoji => emoji.related('creator'))
        .limit(10)
        .orderBy('created', 'desc'),
    ),
);
export const user = q('user', (tx, userID: string) =>
  tx.user.where('id', userID).one(),
);
export const userPref = q('userPref', (tx, key: string, userID: string) =>
  tx.userPref.where('key', key).where('userID', userID).one(),
);

/**
 * The problem.
 *
 * Issue page example.
 */
export const issue = q('issue', (tx, issueID: string) => {
  // 1. we could try to run a query against the local store
  // but then we'd be async if we want to know final results.
  // 2. we can check if location is server then do special things.
  // --
  // ZQL in custom query is not runnable against PG at the moment...
  // but it is in custom mutators.
  // --
  // on server, query builder thing vs query runner thing
  return tx.issue
    .where('id', issueID)
    .related('labels')
    .related('viewState')
    .related('creator');
});
