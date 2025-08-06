import {expectTypeOf, test} from 'vitest';
import {mutators, type MutatorProvider, type Transaction} from './custom.ts';
import type {Schema} from '../query/test/test-schemas.ts';

// Check the type gymnastics of the mutators function
test('mutators function', async () => {
  const m = mutators({
    exampleMutator: async (
      _tx: Transaction<Schema>,
      _id: string,
      _name: string,
    ) => {},
    otherMutator: async (
      _tx: Transaction<Schema>,
      _id: string,
      _value: number,
    ) => {},
  });

  const fakeTx = {} as Transaction<Schema>;
  await m.exampleMutator(fakeTx, '1', 'example');
  await m.otherMutator(fakeTx, '1', 42);

  // @ts-expect-error - should not allow non-existent mutators
  // to be called
  m.foo();

  // `Schema` name is used rather than the unrolled `Schema` type.
  // This should prevent type depth issues.
  // vs. the old z.mutate where the entire Schema type is expanded
  // for each mutator.
  expectTypeOf(m).toEqualTypeOf<{
    exampleMutator: (
      _tx: Transaction<Schema> | MutatorProvider,
      _id: string,
      _name: string,
    ) => Promise<void>;
    otherMutator: (
      _tx: Transaction<Schema> | MutatorProvider,
      _id: string,
      _value: number,
    ) => Promise<void>;
  }>();
});
