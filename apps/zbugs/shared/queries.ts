import {query} from '@rocicorp/zero';
import {schema} from './schema.ts';

const q = query.bindTo(schema);

export const allLabels = q('allLabels', tx => tx.label);
export const allUsers = q('allUsers', tx => tx.user);

// Preload all issues and first 10 comments from each.
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

export const userPicker = q(
  'userPicker',
  (
    tx,
    disabled: boolean,
    login: string | null,
    filter: 'crew' | 'creators' | null,
  ) => {
    let q = tx.user;
    if (disabled && login) {
      q = q.where('login', login);
    } else if (filter) {
      if (filter === 'crew') {
        q = q.where(({cmp, not, and}) =>
          and(cmp('role', 'crew'), not(cmp('login', 'LIKE', 'rocibot%'))),
        );
      } else if (filter === 'creators') {
        q = q.whereExists('createdIssues');
      } else {
        throw new Error(`Unknown filter: ${filter}`);
      }
    }
    return q;
  },
);

// export const issueDetail = q(
//   'issueDetail',
//   (tx, idField: 'shortID' | 'id', id: string, userID: string) => {},
// );
