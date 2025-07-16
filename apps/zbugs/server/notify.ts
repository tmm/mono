import {type ServerTransaction, type UpdateValue} from '@rocicorp/zero';
import type {TransactionSql} from 'postgres';
import {assert} from '../../../packages/shared/src/asserts.ts';
import {assertIsLoggedIn, type AuthData} from '../shared/auth.ts';
import {schema, type Schema} from '../shared/schema.ts';
import {postToDiscord} from './discord.ts';
import {sendEmail} from './email.ts';
import type {PostCommitTask} from './server-mutators.ts';

type CreateIssueNotification = {
  kind: 'create-issue';
};

type UpdateIssueNotification = {
  kind: 'update-issue';
  update: UpdateValue<typeof schema.tables.issue>;
};

type AddEmojiToIssueNotification = {
  kind: 'add-emoji-to-issue';
  emoji: string;
};

type AddEmojiToCommentNotification = {
  kind: 'add-emoji-to-comment';
  commentID: string;
  emoji: string;
};

type AddCommentNotification = {
  kind: 'add-comment';
  commentID: string;
  comment: string;
};

type EditCommentNotification = {
  kind: 'edit-comment';
  commentID: string;
  comment: string;
};

type NotificationArgs = {issueID: string} & (
  | CreateIssueNotification
  | UpdateIssueNotification
  | AddEmojiToIssueNotification
  | AddEmojiToCommentNotification
  | AddCommentNotification
  | EditCommentNotification
);

export async function notify(
  tx: ServerTransaction<Schema, TransactionSql>,
  authData: AuthData | undefined,
  args: NotificationArgs,
  postCommitTasks: PostCommitTask[],
): Promise<void> {
  assertIsLoggedIn(authData);

  const {issueID, kind} = args;
  const issue = await tx.query.issue.where('id', issueID).one();
  assert(issue);

  const modifierUserID = authData.sub;
  const modifierUser = await tx.query.user.where('id', modifierUserID).one();
  assert(modifierUser);

  const recipients = await gatherRecipients(tx, issueID, modifierUserID);

  // If no recipients, skip notification
  if (recipients.length === 0) {
    console.log('No recipients for notification', issueID);
    return;
  }

  if (issue.shortID === null) {
    console.log('No short ID for issue', issueID);
    return;
  }

  // Only send to Discord for public issues
  const shouldSendToDiscord = issue.visibility === 'public';

  const issueLink = `https://bugs.rocicorp.dev/issue/${issue.shortID}`;
  const getUnsubscribeLink = (email: string) =>
    `https://bugs.rocicorp.dev/api/unsubscribe?id=${issue.shortID}&email=${encodeURIComponent(email)}`;

  switch (kind) {
    case 'create-issue': {
      const payload = {
        title: `${modifierUser.login} reported an issue`,
        message: [issue.title, clip((await issue.description) ?? '')]
          .filter(Boolean)
          .join('\n'),
        link: issueLink,
      };
      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({
            tx,
            email: recipient,
            ...payload,
            issue,
            unsubscribeLink: getUnsubscribeLink(recipient),
          });
        });
      }
      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'update-issue': {
      const {update} = args;
      const changes: string[] = [];

      if (update.open !== undefined) {
        changes.push(`Status changed to ${update.open ? 'open' : 'closed'}`);
      }
      if (update.assigneeID !== undefined) {
        if (update.assigneeID === null) {
          changes.push('Assignee was removed');
        } else {
          const newAssignee = await tx.query.user
            .where('id', update.assigneeID)
            .one();
          if (newAssignee) {
            changes.push(`Assignee changed to ${newAssignee.login}`);
          }
        }
      }
      if (update.visibility !== undefined) {
        changes.push(`Visibility changed to ${update.visibility}`);
      }
      if (update.title !== undefined) {
        changes.push(`Title changed to "${update.title}"`);
      }
      if (update.description !== undefined) {
        changes.push('Description was updated');
      }

      const title = `${modifierUser.login} updated an issue`;
      const message = [
        issue.title,
        ...changes,
        clip((await issue.description) ?? ''),
      ]
        .filter(Boolean)
        .join('\n');

      const payload = {
        recipients,
        title,
        message,
        link: issueLink,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({
            tx,
            email: recipient,
            ...payload,
            issue,
            unsubscribeLink: getUnsubscribeLink(recipient),
          });
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'add-emoji-to-issue': {
      const {emoji} = args;
      const payload = {
        recipients,
        title: `${modifierUser.login} reacted to an issue`,
        message: [issue.title, emoji].join('\n'),
        link: issueLink,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({
            tx,
            email: recipient,
            ...payload,
            issue,
            unsubscribeLink: getUnsubscribeLink(recipient),
          });
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'add-emoji-to-comment': {
      const {commentID, emoji} = args;
      const comment = await tx.query.comment.where('id', commentID).one();
      assert(comment);

      const payload = {
        recipients,
        title: `${modifierUser.login} reacted to a comment`,
        message: [clip(await comment.body), emoji].filter(Boolean).join('\n'),
        link: issueLink,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({
            tx,
            email: recipient,
            ...payload,
            issue,
            unsubscribeLink: getUnsubscribeLink(recipient),
          });
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'add-comment': {
      const {commentID, comment} = args;
      const payload = {
        recipients,
        title: `${modifierUser.login} commented on an issue`,
        message: [issue.title, clip(await comment)].join('\n'),
        link: `${issueLink}#comment-${commentID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({
            tx,
            email: recipient,
            ...payload,
            issue,
            unsubscribeLink: getUnsubscribeLink(recipient),
          });
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }

    case 'edit-comment': {
      const {commentID, comment} = args;

      const payload = {
        recipients,
        title: `${modifierUser.login} edited a comment`,
        message: [issue.title, clip(await comment)].join('\n'),
        link: `${issueLink}#comment-${commentID}`,
      };

      for (const recipient of recipients) {
        postCommitTasks.push(async () => {
          await sendEmail({
            tx,
            email: recipient,
            ...payload,
            issue,
            unsubscribeLink: getUnsubscribeLink(recipient),
          });
        });
      }

      if (shouldSendToDiscord) {
        postCommitTasks.push(() => postToDiscord(payload));
      }
      break;
    }
  }
}

function clip(s: string) {
  return s.length > 255 ? s.slice(0, 252) + '...' : s;
}

export async function gatherRecipients(
  tx: ServerTransaction<Schema, TransactionSql>,
  issueID: string,
  actorID: string,
): Promise<string[]> {
  const sql = tx.dbTransaction.wrappedTransaction;

  // we filter out the actor to not send them notifications on their own actions
  // and filter by issue visibility - only crew members get notifications for internal issues
  const recipientRows = await sql`
    SELECT DISTINCT u.email
    FROM "issueNotifications" n
    JOIN "user" u ON u.id = n."userID"
    JOIN "issue" i ON i.id = n."issueID"
    WHERE n."issueID" = ${issueID} 
      AND n."subscribed" = true
      AND n."userID" != ${actorID}
      AND u.email IS NOT NULL
      AND (
        -- If issue is public, include all candidates
        i.visibility = 'public'
        OR
        -- If issue is not public, only include crew members
        u.role = 'crew'
      );`;

  return recipientRows.map(row => row.email);
}
