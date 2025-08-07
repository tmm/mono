import * as v from '../../shared/src/valita.ts';
import {clientSchemaSchema} from './client-schema.ts';
import {deleteClientsBodySchema} from './delete-clients.ts';
import {upQueriesPatchSchema} from './queries-patch.ts';

/**
 * After opening a websocket the client waits for a `connected` message
 * from the server.  It then sends an `initConnection` message to the
 * server.  The server waits for the `initConnection` message before
 * beginning to send pokes to the newly connected client, so as to avoid
 * syncing lots of queries which are no longer desired by the client.
 */

export const connectedBodySchema = v.object({
  wsid: v.string(),
  timestamp: v.number().optional(),
});

export const connectedMessageSchema = v.tuple([
  v.literal('connected'),
  connectedBodySchema,
]);

const userQueryMutateParamsSchema = v.object({
  /**
   * A client driven URL to send queries or mutations to.
   * This URL must match one of the URLs set in the zero config.
   *
   * E.g., Given the following environment variable:
   * ZERO_QUERY_URL=[https://*.example.com/query]
   *
   * Then this URL could be:
   * https://myapp.example.com/query
   */
  url: v.string().optional(),
  // The query string to use for query or mutation calls.
  queryParams: v.record(v.string()).optional(),
});

const initConnectionBodySchema = v.object({
  desiredQueriesPatch: upQueriesPatchSchema,
  clientSchema: clientSchemaSchema.optional(),
  deleted: deleteClientsBodySchema.optional(),
  // parameters to configure the mutate endpoint
  userPushParams: userQueryMutateParamsSchema.optional(),
  // parameters to configure the query endpoint
  userQueryParams: userQueryMutateParamsSchema.optional(),

  /**
   * `activeClients` is an optional array of client IDs that are currently active
   * in the client group. This is used to inform the server about the clients
   * that are currently active (aka running, aka alive), so it can inactive
   * queries from inactive clients.
   */
  activeClients: v.array(v.string()).optional(),
});

export const initConnectionMessageSchema = v.tuple([
  v.literal('initConnection'),
  initConnectionBodySchema,
]);

export type ConnectedBody = v.Infer<typeof connectedBodySchema>;
export type ConnectedMessage = v.Infer<typeof connectedMessageSchema>;
export type UserMutateParams = v.Infer<typeof userQueryMutateParamsSchema>;
export type UserQueryParams = v.Infer<typeof userQueryMutateParamsSchema>;

export type InitConnectionBody = v.Infer<typeof initConnectionBodySchema>;
export type InitConnectionMessage = v.Infer<typeof initConnectionMessageSchema>;

export function encodeSecProtocols(
  initConnectionMessage: InitConnectionMessage | undefined,
  authToken: string | undefined,
) {
  const protocols = {
    initConnectionMessage,
    authToken,
  };
  // WS sec protocols needs to be URI encoded. To save space, we base64 encode
  // the JSON before URI encoding it. But InitConnectionMessage can contain
  // arbitrary unicode strings, so we need to encode the JSON as UTF-8 first.
  // Phew!
  const bytes = new TextEncoder().encode(JSON.stringify(protocols));
  return encodeURIComponent(btoa(String.fromCharCode(...bytes)));
}

export function decodeSecProtocols(secProtocol: string): {
  initConnectionMessage: InitConnectionMessage | undefined;
  authToken: string | undefined;
} {
  const binString = atob(decodeURIComponent(secProtocol));
  const bytes = Uint8Array.from(binString, c => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}
