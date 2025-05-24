import {Transform} from 'node:stream';

// The lone NULL byte signifies that the column value is `null`.
// (Postgres does not permit NULL bytes in TEXT values).
//
// Note that although NULL bytes can appear in JSON strings,
// those will always be represented within double-quotes,
// and thus never as a lone NULL byte.
export const NULL_BYTE = '\u0000';

export type TextTransformOutput = typeof NULL_BYTE | Buffer;

/**
 * A stream Transform that parses a Postgres `COPY ... TO` text stream into
 * individual text Buffers. The special {@link NULL_BYTE} string is used to
 * indicate a `null` value (as the `null` value itself indicates the end of
 * the stream and cannot be pushed as an element).
 *
 * Note that the transform assumes that the next step of the pipeline
 * understands the cardinality of values per row and does not push any
 * special value when reaching the end of a row.
 */
export class TextTransform extends Transform {
  // Note: null means empty / "no value". #currVal will be a Buffer
  //   containing a lone NULL_BYTE (e.g. NULL_BUFFER.equals(#currVal))
  //   when the NULL_BYTE should be pushed down the pipeline.
  #currVal: null | Buffer | Buffer[] = null;
  #escaped = false;

  constructor() {
    super({objectMode: true});
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (e?: Error) => void,
  ) {
    try {
      let l = 0;
      let r = 0;

      const append = (b: Buffer) => {
        this.#currVal === null
          ? (this.#currVal = b)
          : Array.isArray(this.#currVal)
            ? this.#currVal.push(b)
            : (this.#currVal = [this.#currVal, b]);
      };

      const flushSegment = () => {
        l < r && append(chunk.subarray(l, r));
        l = r + 1;
      };

      for (; r < chunk.length; r++) {
        const ch = chunk[r];
        if (this.#escaped) {
          const escapedChar = ESCAPED_CHARACTERS[ch];
          if (escapedChar === undefined) {
            throw new Error(
              `Unexpected escape character \\${String.fromCharCode(ch)}`,
            );
          }
          append(Buffer.from(escapedChar));
          l = r + 1;
          this.#escaped = false;
          continue;
        }
        switch (ch) {
          case 0x5c: // '\'
            flushSegment();
            this.#escaped = true;
            break;

          case 0x09: // '\t'
          case 0x0a: // '\n'
            flushSegment();

            // Value is done in both cases.
            this.push(outputFrom(this.#currVal));
            this.#currVal = null;
            break;
        }
      }
      flushSegment();
      callback();
    } catch (e) {
      callback(e instanceof Error ? e : new Error(String(e)));
    }
  }
}

const EMPTY = Buffer.alloc(0);
// Note: never expose this because Buffer contents are mutable
const NULL_BUFFER = Buffer.from(NULL_BYTE);

function outputFrom(currVal: Buffer | Buffer[] | null): TextTransformOutput {
  return currVal === null
    ? EMPTY
    : Array.isArray(currVal)
      ? Buffer.concat(currVal)
      : NULL_BUFFER.equals(currVal)
        ? NULL_BYTE
        : currVal;
}

// escaped characters used in https://www.postgresql.org/docs/current/sql-copy.html
const ESCAPED_CHARACTERS: Record<number, string | undefined> = {
  0x4e: NULL_BYTE, // \N signifies the NULL character.
  0x5c: '\\',
  0x62: '\b',
  0x66: '\f',
  0x6e: '\n',
  0x72: '\r',
  0x74: '\t',
  0x76: '\v',
} as const;
