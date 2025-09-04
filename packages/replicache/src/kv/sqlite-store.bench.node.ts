import {afterAll, bench, describe, expect} from 'vitest';
import {withRead, withWrite} from '../with-transactions.ts';
import {getTestSQLiteDatabaseManager} from './sqlite-store-test-util.ts';
import {SQLiteStore} from './sqlite-store.ts';

const walSQLite3DatabaseManager = getTestSQLiteDatabaseManager();
const walStore = new SQLiteStore('bench-wal', walSQLite3DatabaseManager, {
  readPoolSize: 2,
  journalMode: 'WAL',
  synchronous: 'NORMAL',
  readUncommitted: false,
});

const defaultSQLite3DatabaseManager = getTestSQLiteDatabaseManager();
const defaultStore = new SQLiteStore(
  'bench-default',
  defaultSQLite3DatabaseManager,
  {
    readPoolSize: 2,
    synchronous: 'NORMAL',
    readUncommitted: false,
  },
);

afterAll(() => {
  walSQLite3DatabaseManager.clearAllStoresForTesting();
  defaultSQLite3DatabaseManager.clearAllStoresForTesting();
});

describe('sqlite tx', () => {
  bench(
    `default journal mode`,
    async () => {
      await withWrite(defaultStore, async wt => {
        expect(await wt.get('bar')).equal(undefined);
        await wt.put('bar', 'baz');
        expect(await wt.get('bar')).equal('baz');
        await wt.del('bar');
        expect(await wt.get('bar')).equal(undefined);
      });
    },
    {
      throws: true,
    },
  );

  bench(
    `WAL journal mode`,
    async () => {
      await withWrite(walStore, async wt => {
        expect(await wt.get('bar')).equal(undefined);
        await wt.put('bar', 'baz');
        expect(await wt.get('bar')).equal('baz');
        await wt.del('bar');
        expect(await wt.get('bar')).equal(undefined);
      });
    },
    {
      throws: true,
    },
  );
});

describe('sqlite write contention', () => {
  bench(
    `default journal mode`,
    async () => {
      await withWrite(defaultStore, async wt => {
        await wt.put('foo', 'bar');
      });

      const readP1 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const writeP = withWrite(defaultStore, async wt => {
        await wt.put('foo', 'bar2');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5, writeP]);
    },
    {
      throws: true,
      teardown: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.del('foo');
        });
      },
    },
  );

  bench(
    `WAL journal mode`,
    async () => {
      await withWrite(walStore, async wt => {
        await wt.put('foo', 'bar');
      });

      const readP1 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const writeP = withWrite(walStore, async wt => {
        await wt.put('foo', 'bar2');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5, writeP]);
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(walStore, async wt => {
          await wt.del('foo');
        });
      },
    },
  );
});

describe('plain read', () => {
  bench(
    `default journal mode`,
    async () => {
      const readP1 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(defaultStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5]);
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(defaultStore, async wt => {
          await wt.put('foo', 'bar');
        });
      },
    },
  );

  bench(
    `WAL journal mode`,
    async () => {
      const readP1 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP2 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP3 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP4 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });
      const readP5 = withRead(walStore, async rt => {
        expect(await rt.get('foo')).equal('bar');
      });

      await Promise.all([readP1, readP2, readP3, readP4, readP5]);
    },
    {
      throws: true,
      setup: async () => {
        await withWrite(walStore, async wt => {
          await wt.put('foo', 'bar');
        });
      },
    },
  );
});

describe('bulk operations', () => {
  bench(
    `default journal mode - 100 key/value pairs`,
    async () => {
      await withWrite(defaultStore, async wt => {
        // Add 100 key/value pairs in a single transaction
        for (let i = 0; i < 100; i++) {
          await wt.put(`key-${i}`, {
            id: i,
            name: `test-item-${i}`,
            value: Math.random() * 1000,
            metadata: {
              created: new Date().toISOString(),
              type: 'benchmark',
              batch: 'bulk-test',
            },
          });
        }
      });
    },
    {
      throws: true,
      setup: async () => {
        // Clean up any existing test data before this benchmark task runs
        await withWrite(defaultStore, async wt => {
          for (let i = 0; i < 100; i++) {
            await wt.del(`key-${i}`);
          }
        });
      },
    },
  );

  bench(
    `WAL journal mode - 100 key/value pairs`,
    async () => {
      await withWrite(walStore, async wt => {
        // Add 100 key/value pairs in a single transaction
        for (let i = 0; i < 100; i++) {
          await wt.put(`key-${i}`, {
            id: i,
            name: `test-item-${i}`,
            value: Math.random() * 1000,
            metadata: {
              created: new Date().toISOString(),
              type: 'benchmark',
              batch: 'bulk-test',
            },
          });
        }
      });
    },
    {
      throws: true,
      setup: async () => {
        // Clean up any existing test data before this benchmark task runs
        await withWrite(walStore, async wt => {
          for (let i = 0; i < 100; i++) {
            await wt.del(`key-${i}`);
          }
        });
      },
    },
  );

  bench(
    `mixed operations - 50 puts + 25 updates + 25 deletes`,
    async () => {
      await withWrite(walStore, async wt => {
        // First, add 75 items
        for (let i = 0; i < 75; i++) {
          await wt.put(`mixed-key-${i}`, {
            id: i,
            operation: 'initial',
            value: i * 10,
          });
        }

        // Update 25 existing items (0-24)
        for (let i = 0; i < 25; i++) {
          await wt.put(`mixed-key-${i}`, {
            id: i,
            operation: 'updated',
            value: i * 100,
          });
        }

        // Add 50 new items (75-124)
        for (let i = 75; i < 125; i++) {
          await wt.put(`mixed-key-${i}`, {
            id: i,
            operation: 'new',
            value: i * 5,
          });
        }

        // Delete 25 items (50-74)
        for (let i = 50; i < 75; i++) {
          await wt.del(`mixed-key-${i}`);
        }
      });
    },
    {
      throws: true,
      setup: async () => {
        // Clean up any existing test data before this benchmark task runs
        await withWrite(walStore, async wt => {
          for (let i = 0; i < 125; i++) {
            await wt.del(`mixed-key-${i}`);
          }
        });
      },
    },
  );
});
