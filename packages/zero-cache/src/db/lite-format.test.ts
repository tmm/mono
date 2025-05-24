import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';

// Unit test purely for confirming the behavior of better-sqlite3.
test('lite-format', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(
    `CREATE TABLE foo (
        txt1 TEXT,
        txt2 TEXT,
        intAsString INT4,
        intAsNum INT4,
        fltAsString FLOAT8,
        fltAsNum FLOAT8,
        nil TEXT
     );`,
  );
  db.prepare(
    `INSERT INTO foo VALUES (
        format('%s', ?1),
        ?2,  -- unformatted Buffer
        format('%s', ?3),
        format('%d', ?4),
        format('%s', ?5),
        format('%g', ?6),
        iif(?7 NOTNULL, format('%s', ?7), NULL)
      )`,
  ).run({
    1: Buffer.from('formatting a blob stores it as text! やった！'),
    2: Buffer.from('this is an unformatted blob'),
    3: Buffer.from('123456'),
    4: Buffer.from('987654'),
    5: Buffer.from('123.456'),
    6: Buffer.from('987.654'),
    7: null,
  });
  expect(
    db
      .prepare(
        `SELECT 
           txt1, typeof(txt1),
           txt2, typeof(txt2),
           intAsString, typeof(intAsString),
           intAsNum, typeof(intAsNum),
           fltAsString, typeof(fltAsString),
           fltAsNum, typeof(fltAsNum),
           nil, typeof(nil)
           FROM foo`,
      )
      .get(),
  ).toMatchInlineSnapshot(`
    {
      "fltAsNum": 987.654,
      "fltAsString": 123.456,
      "intAsNum": 987654,
      "intAsString": 123456,
      "nil": null,
      "txt1": "formatting a blob stores it as text! やった！",
      "txt2": {
        "data": [
          116,
          104,
          105,
          115,
          32,
          105,
          115,
          32,
          97,
          110,
          32,
          117,
          110,
          102,
          111,
          114,
          109,
          97,
          116,
          116,
          101,
          100,
          32,
          98,
          108,
          111,
          98,
        ],
        "type": "Buffer",
      },
      "typeof(fltAsNum)": "real",
      "typeof(fltAsString)": "real",
      "typeof(intAsNum)": "integer",
      "typeof(intAsString)": "integer",
      "typeof(nil)": "null",
      "typeof(txt1)": "text",
      "typeof(txt2)": "blob",
    }
  `);
});
