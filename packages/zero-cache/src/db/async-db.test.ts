import {beforeEach, describe, expect, test} from 'vitest';
import {AsyncDatabase} from './async-db.ts';

describe('async-db', () => {
  let db: AsyncDatabase;

  beforeEach(async () => {
    db = await AsyncDatabase.connect(':memory:');
    await db.exec(/*sql*/ `CREATE TABLE foo(a INT, b TEXT, c JSON)`);
  });

  test('run insert with ?s', async () => {
    await db.run(
      /*sql*/ `INSERT INTO foo VALUES (?, ?, ?)`,
      1,
      'two',
      `"three"`,
    );
    expect(await db.all(/*sql*/ `SELECT * FROM foo`)).toMatchInlineSnapshot(`
      [
        {
          "a": 1,
          "b": "two",
          "c": ""three"",
        },
      ]
    `);
  });

  test('run insert with numbered ?s', async () => {
    await db.run(
      /*sql*/ `INSERT INTO foo VALUES (?3, ?2, ?1)`,
      `"nine"`,
      'eight',
      7,
    );
    expect(await db.all(/*sql*/ `SELECT * FROM foo`)).toMatchInlineSnapshot(`
      [
        {
          "a": 7,
          "b": "eight",
          "c": ""nine"",
        },
      ]
    `);
  });

  test('run insert with named args', async () => {
    await db.run(/*sql*/ `INSERT INTO foo VALUES (@a, @b, @c)`, {
      a: 2,
      b: 'three',
      c: `["four"]`,
    });
    expect(await db.all(/*sql*/ `SELECT * FROM foo`)).toMatchInlineSnapshot(`
      [
        {
          "a": 2,
          "b": "three",
          "c": "["four"]",
        },
      ]
    `);
  });

  test('prepared insert with ?s', async () => {
    const insert = await db.prepare(/*sql*/ `INSERT INTO foo VALUES (?, ?, ?)`);
    await insert.run(0, 'one', `"two"`);
    expect(await db.all(/*sql*/ `SELECT * FROM foo`)).toMatchInlineSnapshot(`
      [
        {
          "a": 0,
          "b": "one",
          "c": ""two"",
        },
      ]
    `);
  });

  test('prepared insert with named args', async () => {
    const insert = await db.prepare(
      /*sql*/ `INSERT INTO foo VALUES (@a, @b, @c)`,
    );
    await insert.run({a: 3, b: 'four', c: `"five"`});
    expect(await db.all(/*sql*/ `SELECT * FROM foo`)).toMatchInlineSnapshot(`
      [
        {
          "a": 3,
          "b": "four",
          "c": ""five"",
        },
      ]
    `);
  });
});
