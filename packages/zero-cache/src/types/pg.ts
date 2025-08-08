import {PreciseDate} from '@google-cloud/precise-date';
import {OID} from '@postgresql-typed/oids';
import {LogContext} from '@rocicorp/logger';
import postgres, {type Notice, type PostgresType} from 'postgres';
import {randInt} from '../../../shared/src/rand.ts';
import type {ValueType} from '../../../zero-protocol/src/client-schema.ts';
import {BigIntJSON, type JSONValue} from '../../../shared/src/bigint-json.ts';
import {
  DATE,
  JSON,
  JSONB,
  NUMERIC,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
} from './pg-types.ts';

// exported for testing.
export function timestampToFpMillis(timestamp: string): number {
  // Convert from PG's time string, e.g. "1999-01-08 12:05:06+00" to "Z"
  // format expected by PreciseDate.
  timestamp = timestamp.replace(' ', 'T');
  const positiveOffset = timestamp.includes('+');
  const tzSplitIndex = positiveOffset
    ? timestamp.lastIndexOf('+')
    : timestamp.indexOf('-', timestamp.indexOf('T'));
  const timezoneOffset =
    tzSplitIndex === -1 ? undefined : timestamp.substring(tzSplitIndex);
  const tsWithoutTimezone =
    (tzSplitIndex === -1 ? timestamp : timestamp.substring(0, tzSplitIndex)) +
    'Z';

  try {
    // PreciseDate does not return microsecond precision unless the provided
    // timestamp is in UTC time so we need to add the timezone offset back in.
    const fullTime = new PreciseDate(tsWithoutTimezone).getFullTime();
    const millis = Number(fullTime / 1_000_000n);
    const nanos = Number(fullTime % 1_000_000n);
    const ret = millis + nanos * 1e-6; // floating point milliseconds

    // add back in the timezone offset
    if (timezoneOffset) {
      const [hours, minutes] = timezoneOffset.split(':');
      const offset =
        Math.abs(Number(hours)) * 60 + (minutes ? Number(minutes) : 0);
      const offsetMillis = offset * 60 * 1_000;
      // If it is a positive offset, we subtract the offset from the UTC
      // because we passed in the "local time" as if it was UTC.
      // The opposite is true for negative offsets.
      return positiveOffset ? ret - offsetMillis : ret + offsetMillis;
    }
    return ret;
  } catch (e) {
    throw new Error(`Error parsing ${timestamp}`, {cause: e});
  }
}

function serializeTimestamp(val: unknown): string {
  switch (typeof val) {
    case 'string':
      return val; // Let Postgres parse it
    case 'number': {
      if (Number.isInteger(val)) {
        return new PreciseDate(val).toISOString();
      }
      // Convert floating point to bigint nanoseconds.
      const nanoseconds =
        1_000_000n * BigInt(Math.trunc(val)) +
        BigInt(Math.trunc((val % 1) * 1e6));
      return new PreciseDate(nanoseconds).toISOString();
    }
    // Note: Don't support bigint inputs until we decide what the semantics are (e.g. micros vs nanos)
    // case 'bigint':
    //   return new PreciseDate(val).toISOString();
    default:
      if (val instanceof Date) {
        return val.toISOString();
      }
  }
  throw new Error(`Unsupported type "${typeof val}" for timestamp: ${val}`);
}

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;
export function millisecondsToPostgresTime(milliseconds: number): string {
  if (milliseconds < 0) {
    throw new Error('Milliseconds cannot be negative');
  }

  if (milliseconds >= MILLISECONDS_PER_DAY) {
    throw new Error(
      `Milliseconds cannot exceed 24 hours (${MILLISECONDS_PER_DAY}ms)`,
    );
  }

  milliseconds = Math.floor(milliseconds); // Ensure it's an integer

  const totalSeconds = Math.floor(milliseconds / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const ms = milliseconds % 1000;

  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}.${ms.toString().padStart(3, '0')}`;
}

export function postgresTimeToMilliseconds(timeString: string): number {
  // Validate basic format
  if (!timeString || typeof timeString !== 'string') {
    throw new Error('Invalid time string: must be a non-empty string');
  }

  // Regular expression to match HH:MM:SS or HH:MM:SS.mmm format
  const timeRegex = /^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$/;
  const match = timeString.match(timeRegex);

  if (!match) {
    throw new Error(
      `Invalid time format: "${timeString}". Expected HH:MM:SS or HH:MM:SS.mmm`,
    );
  }

  // Extract components
  const hours = parseInt(match[1], 10);
  const minutes = parseInt(match[2], 10);
  const seconds = parseInt(match[3], 10);
  // Handle optional milliseconds, pad right with zeros if needed
  let milliseconds = 0;
  if (match[4]) {
    // Pad microseconds to 6 digits
    const msString = match[4].padEnd(6, '0');
    // slice milliseconds out of the microseconds
    // e.g. 123456 -> 123, 1234 -> 123,
    milliseconds = parseInt(msString.slice(0, 3), 10);
  }

  // Validate ranges
  if (hours < 0 || hours > 24) {
    throw new Error(
      `Invalid hours: ${hours}. Must be between 0 and 24 (24 means end of day)`,
    );
  }

  if (minutes < 0 || minutes >= 60) {
    throw new Error(`Invalid minutes: ${minutes}. Must be between 0 and 59`);
  }

  if (seconds < 0 || seconds >= 60) {
    throw new Error(`Invalid seconds: ${seconds}. Must be between 0 and 59`);
  }

  if (milliseconds < 0 || milliseconds >= 1000) {
    throw new Error(
      `Invalid milliseconds: ${milliseconds}. Must be between 0 and 999`,
    );
  }

  // Special case: PostgreSQL allows 24:00:00 to represent end of day
  if (hours === 24 && (minutes !== 0 || seconds !== 0 || milliseconds !== 0)) {
    throw new Error(
      'Invalid time: when hours is 24, minutes, seconds, and milliseconds must be 0',
    );
  }

  // Calculate total milliseconds
  const totalMs =
    hours * 3600000 + minutes * 60000 + seconds * 1000 + milliseconds;

  return totalMs;
}

function dateToUTCMidnight(date: string): number {
  const d = new Date(date);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

/**
 * The (javascript) types of objects that can be returned by our configured
 * Postgres clients. For initial-sync, these comes from the postgres.js client:
 *
 * https://github.com/porsager/postgres/blob/master/src/types.js
 *
 * and for the replication stream these come from the the node-postgres client:
 *
 * https://github.com/brianc/node-pg-types/blob/master/lib/textParsers.js
 */
export type PostgresValueType = JSONValue | Uint8Array;

/**
 * Configures types for the Postgres.js client library (`postgres`).
 *
 * @param jsonAsString Keep JSON / JSONB values as strings instead of parsing.
 */
export const postgresTypeConfig = (
  jsonAsString?: 'json-as-string' | undefined,
) => ({
  // Type the type IDs as `number` so that Typescript doesn't complain about
  // referencing external types during type inference.
  types: {
    bigint: postgres.BigInt,
    json: {
      to: JSON,
      from: [JSON, JSONB],
      serialize: BigIntJSON.stringify,
      parse: jsonAsString ? (x: string) => x : BigIntJSON.parse,
    },
    // Timestamps are converted to PreciseDate objects.
    timestamp: {
      to: TIMESTAMP,
      from: [TIMESTAMP, TIMESTAMPTZ],
      serialize: serializeTimestamp,
      parse: timestampToFpMillis,
    },
    // Times are converted as strings
    time: {
      to: TIME,
      from: [TIME],
      serialize: (x: unknown) => {
        switch (typeof x) {
          case 'string':
            return x; // Let Postgres parse it
          case 'number':
            return millisecondsToPostgresTime(x);
        }

        throw new Error(`Unsupported type "${typeof x}" for time: ${x}`);
      },
      parse: postgresTimeToMilliseconds,
    },
    // The DATE type is stored directly as the PG normalized date string.
    date: {
      to: DATE,
      from: [DATE],
      serialize: (x: string | Date) =>
        (x instanceof Date ? x : new Date(x)).toISOString(),
      parse: dateToUTCMidnight,
    },
    // Returns a `js` number which can lose precision for large numbers.
    // JS number is 53 bits so this should generally not occur.
    // An API will be provided for users to override this type.
    numeric: {
      to: NUMERIC,
      from: [NUMERIC],
      serialize: (x: number) => String(x), // pg expects a string
      parse: (x: string | number) => Number(x),
    },
  },
});

export type PostgresDB = postgres.Sql<{
  bigint: bigint;
  json: JSONValue;
}>;

export type PostgresTransaction = postgres.TransactionSql<{
  bigint: bigint;
  json: JSONValue;
}>;

export function pgClient(
  lc: LogContext,
  connectionURI: string,
  options?: postgres.Options<{
    bigint: PostgresType<bigint>;
    json: PostgresType<JSONValue>;
  }>,
  jsonAsString?: 'json-as-string',
): PostgresDB {
  const onnotice = (n: Notice) => {
    // https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html#PLPGSQL-STATEMENTS-RAISE
    switch (n.severity) {
      case 'NOTICE':
        return; // silenced
      case 'DEBUG':
        lc.debug?.(n);
        return;
      case 'WARNING':
      case 'EXCEPTION':
        lc.error?.(n);
        return;
      case 'LOG':
      case 'INFO':
      default:
        lc.info?.(n);
    }
  };
  const url = new URL(connectionURI);
  const sslFlag =
    url.searchParams.get('ssl') ?? url.searchParams.get('sslmode') ?? 'prefer';

  let ssl: boolean | 'prefer' | {rejectUnauthorized: boolean};
  if (sslFlag === 'disable' || sslFlag === 'false') {
    ssl = false;
  } else if (sslFlag === 'no-verify') {
    ssl = {rejectUnauthorized: false};
  } else {
    ssl = sslFlag as 'prefer';
  }

  // Set connections to expire between 5 and 10 minutes to free up state on PG.
  const maxLifetimeSeconds = randInt(5 * 60, 10 * 60);
  return postgres(connectionURI, {
    ...postgresTypeConfig(jsonAsString),
    onnotice,
    ['max_lifetime']: maxLifetimeSeconds,
    ssl,
    ...options,
  });
}

export const typeNameByOID: Record<number, string> = Object.freeze(
  Object.fromEntries(
    Object.entries(OID).map(([name, oid]) => [
      oid,
      name.startsWith('_') ? `${name.substring(1)}[]` : name,
    ]),
  ),
);

export const pgToZqlNumericTypeMap = Object.freeze({
  'smallint': 'number',
  'integer': 'number',
  'int': 'number',
  'int2': 'number',
  'int4': 'number',
  'int8': 'number',
  'bigint': 'number',
  'smallserial': 'number',
  'serial': 'number',
  'serial2': 'number',
  'serial4': 'number',
  'serial8': 'number',
  'bigserial': 'number',
  'decimal': 'number',
  'numeric': 'number',
  'real': 'number',
  'double precision': 'number',
  'float': 'number',
  'float4': 'number',
  'float8': 'number',
});

export function isPgNumberType(pgType: string) {
  return Object.hasOwn(pgToZqlNumericTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlStringTypeMap = Object.freeze({
  'bpchar': 'string',
  'character': 'string',
  'character varying': 'string',
  'text': 'string',
  'uuid': 'string',
  'varchar': 'string',
});

export function isPgStringType(pgType: string) {
  return Object.hasOwn(pgToZqlStringTypeMap, formatTypeForLookup(pgType));
}

export const pgToZqlTypeMap = Object.freeze({
  // Numeric types
  ...pgToZqlNumericTypeMap,

  // Date/Time types
  'date': 'number',
  'time': 'string',
  'timestamp': 'number',
  'timestamptz': 'number',
  'timestamp with time zone': 'number',
  'timestamp without time zone': 'number',

  // String types
  ...pgToZqlStringTypeMap,

  // Boolean types
  'bool': 'boolean',
  'boolean': 'boolean',

  'json': 'json',
  'jsonb': 'json',

  // TODO: Add support for these.
  // 'bytea':
});

export function dataTypeToZqlValueType(
  pgType: string,
  isEnum: boolean,
  isArray: boolean,
): ValueType | undefined {
  // We treat pg arrays as JSON values.
  if (isArray) {
    return 'json';
  }

  const valueType = (pgToZqlTypeMap as Record<string, ValueType>)[
    formatTypeForLookup(pgType)
  ];
  if (valueType === undefined) {
    if (isEnum) {
      return 'string';
    }
  }
  return valueType;
}

// Strips args (i.e. (32) in char(32)) and lowercases.
function formatTypeForLookup(pgType: string): string {
  const startOfArgs = pgType.indexOf('(');
  if (startOfArgs === -1) {
    return pgType.toLocaleLowerCase();
  }
  return pgType.toLocaleLowerCase().substring(0, startOfArgs);
}
