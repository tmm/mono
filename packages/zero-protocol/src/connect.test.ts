import fc from 'fast-check';
import {expect, test} from 'vitest';
import {decodeSecProtocols, encodeSecProtocols} from './connect.ts';

test('encode/decodeSecProtocols round-trip', () => {
  fc.assert(
    fc.property(
      fc.record({
        initConnectionMessage: fc.tuple(
          fc.constant<'initConnection'>('initConnection'),
          fc.record(
            {
              desiredQueriesPatch: fc.array(
                fc.oneof(
                  fc.record(
                    {
                      op: fc.constant<'put'>('put'),
                      hash: fc.string(),
                      data: fc.fullUnicodeString(),
                      ast: fc.constant({
                        table: 'table',
                      }),
                      ttl: fc.option(
                        fc.double({
                          noDefaultInfinity: true,
                          noNaN: true,
                          min: 0,
                        }),
                        {nil: undefined},
                      ),
                    },
                    {requiredKeys: ['op', 'hash', 'ast']},
                  ),
                  fc.record({
                    op: fc.constant<'del'>('del'),
                    hash: fc.string(),
                  }),
                ),
              ),
              deleted: fc.record(
                {
                  clientIDs: fc.array(fc.string()),
                  clientGroupIDs: fc.array(fc.string()),
                },
                {requiredKeys: []},
              ),
            },
            {requiredKeys: ['desiredQueriesPatch']},
          ),
        ),
        authToken: fc.option(
          fc.stringOf(
            fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789-_.'),
          ),
          {nil: undefined},
        ),
      }),
      ({initConnectionMessage, authToken}) => {
        const encoded = encodeSecProtocols(initConnectionMessage, authToken);
        const {
          initConnectionMessage: decodedInitConnectionMessage,
          authToken: decodedAuthToken,
        } = decodeSecProtocols(encoded);
        expect(decodedInitConnectionMessage).toEqual(initConnectionMessage);
        expect(decodedAuthToken).toEqual(authToken);
      },
    ),
  );
});
