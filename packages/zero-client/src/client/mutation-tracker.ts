import {resolver, type Resolver} from '@rocicorp/resolver';
import type {
  EphemeralID,
  MutationTrackingData,
} from '../../../replicache/src/replicache-options.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {emptyObject} from '../../../shared/src/sentinels.ts';
import type {
  MutationError,
  MutationID,
  MutationOk,
  PushError,
  PushResponse,
} from '../../../zero-protocol/src/push.ts';
import type {ZeroLogContext} from './zero-log-context.ts';
import type {MutationPatch} from '../../../zero-protocol/src/mutations-patch.ts';

type ErrorType =
  | MutationError
  | Omit<PushError, 'mutationIDs'>
  | Error
  | unknown;

let currentEphemeralID = 0;
function nextEphemeralID(): EphemeralID {
  return ++currentEphemeralID as EphemeralID;
}

/**
 * Tracks what pushes are in-flight and resolves promises when they're acked.
 */
export class MutationTracker {
  readonly #outstandingMutations: Map<
    EphemeralID,
    {
      mutationID?: number | undefined;
      resolver: Resolver<MutationOk, ErrorType>;
    }
  >;
  readonly #ephemeralIDsByMutationID: Map<number, EphemeralID>;
  readonly #allMutationsAppliedListeners: Set<() => void>;
  readonly #lc: ZeroLogContext;

  // This is only used in the new code path that processes
  // mutation responses that arrive via the `poke` protocol.
  // The old code path will be removed in the release after
  // the one containing mutation-responses-via-poke.
  readonly #ackMutations: (upTo: MutationID) => void;
  #clientID: string | undefined;
  #largestOutstandingMutationID: number;
  #currentMutationID: number;

  constructor(lc: ZeroLogContext, ackMutations: (upTo: MutationID) => void) {
    this.#lc = lc.withContext('MutationTracker');
    this.#outstandingMutations = new Map();
    this.#ephemeralIDsByMutationID = new Map();
    this.#allMutationsAppliedListeners = new Set();
    this.#largestOutstandingMutationID = 0;
    this.#currentMutationID = 0;
    this.#ackMutations = ackMutations;
  }

  set clientID(clientID: string) {
    this.#clientID = clientID;
  }

  trackMutation(): MutationTrackingData {
    const id = nextEphemeralID();
    const mutationResolver = resolver<MutationOk, ErrorType>();

    this.#outstandingMutations.set(id, {
      resolver: mutationResolver,
    });
    return {ephemeralID: id, serverPromise: mutationResolver.promise};
  }

  mutationIDAssigned(id: EphemeralID, mutationID: number): void {
    const entry = this.#outstandingMutations.get(id);
    if (entry) {
      entry.mutationID = mutationID;
      this.#ephemeralIDsByMutationID.set(mutationID, id);
      this.#largestOutstandingMutationID = Math.max(
        this.#largestOutstandingMutationID,
        mutationID,
      );
    }
  }

  /**
   * Reject the mutation due to an unhandled exception on the client.
   * The mutation must not have been persisted to the client store.
   */
  rejectMutation(id: EphemeralID, e: unknown): void {
    const entry = this.#outstandingMutations.get(id);
    if (entry) {
      this.#settleMutation(id, entry, 'reject', e);
    }
  }

  /**
   * Used when zero-cache pokes down mutation results.
   */
  processMutationResponses(patches: MutationPatch[]) {
    try {
      for (const patch of patches) {
        if (patch.mutation.id.clientID !== this.#clientID) {
          // will it be poked to us tho?
          // maybe not ever if it was poked via another client?
          continue; // This mutation is not for this client.
        }

        // Since we only write responses for failed mutations,
        // we could have mutations that need to be resolved as `ok` that come before
        // the current mutation in the response.
        // Each turn through the loop, resolve these earlier mutations as `ok`.
        this.#resolveMutationsAsOk(patch.mutation.id.id - 1);

        if ('error' in patch.mutation.result) {
          this.#processMutationError(patch.mutation.id, patch.mutation.result);
        } else {
          this.#processMutationOk(patch.mutation.id, patch.mutation.result);
        }
      }
    } finally {
      const last = patches[patches.length - 1];
      if (last) {
        // We only ack the last mutation in the batch.
        this.#ackMutations(last.mutation.id);
      }
    }
  }

  processPushResponse(response: PushResponse) {
    if ('error' in response) {
      // do nothing
      return;
    }

    this.processMutationResponses(
      response.mutations.map(mutation => ({
        mutation,
        op: 'put',
      })),
    );
  }

  onConnected(lmid: number) {
    this.lmidAdvanced(lmid);
  }

  /**
   * lmid advance will:
   * 1. notify "allMutationsApplied" listeners if the lastMutationID
   *    is greater than or equal to the largest outstanding mutation ID.
   * 2. resolve all limbo mutations whose mutation ID is less than or equal to
   *    the lastMutationID.
   */
  lmidAdvanced(lastMutationID: number): void {
    assert(
      lastMutationID >= this.#currentMutationID,
      'lmid must be greater than or equal to current lmid',
    );
    if (lastMutationID === this.#currentMutationID) {
      return;
    }

    try {
      this.#currentMutationID = lastMutationID;
      this.#resolveMutationsAsOk(lastMutationID);
    } finally {
      if (lastMutationID >= this.#largestOutstandingMutationID) {
        // this is very important otherwise we hang query de-registration
        this.#notifyAllMutationsAppliedListeners();
      }
    }
  }

  #resolveMutationsAsOk(lastMutationID: number): void {
    for (const [id, entry] of this.#outstandingMutations) {
      if (entry.mutationID && entry.mutationID <= lastMutationID) {
        this.#settleMutation(id, entry, 'resolve', emptyObject);
      } else {
        // #outstandingMutations is added to in-order so we can break
        // once we reach a mutation ID that is greater than the lastMutationID.
        // or does not have a mutation ID assigned.
        break;
      }
    }
  }

  get size() {
    return this.#outstandingMutations.size;
  }

  #processMutationError(
    mid: MutationID,
    error: MutationError | Omit<PushError, 'mutationIDs'>,
  ): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );

    this.#lc.error?.(`Mutation ${mid.id} returned an error`, error);

    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid.id);
    if (!ephemeralID && error.error === 'alreadyProcessed') {
      return;
    }

    // Each tab sends all mutations for the client group
    // and the server responds back to the individual client that actually
    // ran the mutation. This means that N clients can send the same
    // mutation concurrently. If that happens, the promise for the mutation tracked
    // by this class will try to be resolved N times.
    // Every time after the first, the ephemeral ID will not be
    // found in the map. These later times, however, should always have been
    // "mutation already processed" events which we ignore (above).
    assert(
      ephemeralID,
      `ephemeral ID is missing for mutation error: ${error.error}.`,
    );

    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid.id);
    // Resolving the promise with an error was an intentional API decision
    // so the user receives typed errors.
    this.#settleMutation(ephemeralID, entry, 'reject', error);
  }

  #processMutationOk(mid: MutationID, result: MutationOk): void {
    assert(
      mid.clientID === this.#clientID,
      'received mutation for the wrong client',
    );
    const ephemeralID = this.#ephemeralIDsByMutationID.get(mid.id);
    assert(
      ephemeralID,
      'ephemeral ID is missing. This can happen if a mutation response is received twice ' +
        'but it should be impossible to receive a success response twice for the same mutation.',
    );
    const entry = this.#outstandingMutations.get(ephemeralID);
    assert(entry && entry.mutationID === mid.id);
    this.#settleMutation(ephemeralID, entry, 'resolve', result);
  }

  #settleMutation<Type extends 'resolve' | 'reject'>(
    ephemeralID: EphemeralID,
    entry: {
      mutationID?: number | undefined;
      resolver: Resolver<MutationOk, ErrorType>;
    },
    type: Type,
    result: 'resolve' extends Type ? MutationOk : unknown,
  ): void {
    switch (type) {
      case 'resolve':
        entry.resolver.resolve(result as MutationOk);
        break;
      case 'reject':
        entry.resolver.reject(result);
        break;
    }

    this.#outstandingMutations.delete(ephemeralID);
    if (entry.mutationID) {
      this.#ephemeralIDsByMutationID.delete(entry.mutationID);
    }
  }

  /**
   * Be notified when all mutations have been included in the server snapshot.
   *
   * The query manager will not de-register queries from the server until there
   * are no pending mutations.
   *
   * The reason is that a mutation may need to be rebased. We do not want
   * data that was available the first time it was run to not be available
   * on a rebase.
   */
  onAllMutationsApplied(listener: () => void): void {
    this.#allMutationsAppliedListeners.add(listener);
  }

  #notifyAllMutationsAppliedListeners() {
    for (const listener of this.#allMutationsAppliedListeners) {
      listener();
    }
  }
}
