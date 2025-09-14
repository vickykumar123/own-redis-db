import * as fs from "fs";

export interface RDBKey {
  key: string;
  value: any;
  expiry?: number; // Unix timestamp in milliseconds
  type: "string" | "list" | "hash" | "set" | "zset";
}

export class RDBParser {
  private data: Uint8Array;
  private index: number = 0;
  private entries: Map<string, RDBKey> = new Map();

  constructor(buffer: Buffer) {
    this.data = new Uint8Array(buffer);
  }

  // Read encoded integer (handles special encodings)
  private readEncodedInt(): number {
    let length = 0;
    const type = this.data[this.index] >> 6;
    switch (type) {
      case 0:
        length = this.data[this.index++] & 0b00111111;
        break;
      case 1:
        length =
          (this.data[this.index++] & 0b00111111) |
          (this.data[this.index++] << 6);
        break;
      case 2:
        this.index++;
        length =
          this.data[this.index++] |
          (this.data[this.index++] << 8) |
          (this.data[this.index++] << 16) |
          (this.data[this.index++] << 24);
        break;
      case 3: {
        const bitType = this.data[this.index++] & 0b00111111;
        length = this.data[this.index++];
        if (bitType > 0) {
          length |= this.data[this.index++] << 8;
        }
        if (bitType == 2) {
          length |=
            (this.data[this.index++] << 16) | (this.data[this.index++] << 24);
        }
        if (bitType > 2) {
          throw Error("length not implemented");
        }
        break;
      }
    }
    return length;
  }

  // Read encoded string
  private readEncodedString(): string {
    const length = this.readEncodedInt();
    const str = this.bytesToString(this.data.slice(this.index, this.index + length));
    this.index += length;
    return str;
  }

  // Helper method to convert bytes to string
  private bytesToString(arr: Uint8Array): string {
    return Array.from(arr).map((byte) => String.fromCharCode(byte)).join('');
  }

  // Read 32-bit little-endian integer
  private readUint32(): number {
    return (
      this.data[this.index++] +
      (this.data[this.index++] << 8) +
      (this.data[this.index++] << 16) +
      (this.data[this.index++] << 24)
    );
  }

  // Read 64-bit little-endian integer
  private readUint64(): bigint {
    let result = BigInt(0);
    let shift = BigInt(0);
    for (let i = 0; i < 8; i++) {
      result += BigInt(this.data[this.index++]) << shift;
      shift += BigInt(8);
    }
    return result;
  }

  parse(): RDBKey[] {
    // Check header
    if (this.bytesToString(this.data.slice(0, 5)) !== "REDIS") {
      console.log(`Invalid RDB file: missing REDIS header`);
      return [];
    }

    console.log(`Version: ${this.bytesToString(this.data.slice(5, 9))}`);
    this.index = 9; // Skip header and version

    let eof = false;
    while (!eof && this.index < this.data.length) {
      const op = this.data[this.index++];
      switch (op) {
        case 0xFA: {
          // Metadata section
          const key = this.readEncodedString();
          switch (key) {
            case "redis-ver":
              console.log(key, this.readEncodedString());
              break;
            case "redis-bits":
              console.log(key, this.readEncodedInt());
              break;
            case "ctime":
              console.log(key, new Date(this.readEncodedInt() * 1000));
              break;
            case "used-mem":
              console.log(key, this.readEncodedInt());
              break;
            case "aof-preamble":
              console.log(key, this.readEncodedInt());
              break;
            default:
              // Skip unknown metadata
              this.readEncodedString();
              break;
          }
          break;
        }
        case 0xFB:
          console.log("keyspace", this.readEncodedInt());
          console.log("expires", this.readEncodedInt());
          this.readEntries();
          break;
        case 0xFE:
          console.log("db selector", this.readEncodedInt());
          break;
        case 0xFF:
          eof = true;
          break;
        default:
          throw Error("op not implemented: " + op);
      }
    }

    return Array.from(this.entries.values());
  }

  private readEntries() {
    const now = new Date();
    while (this.index < this.data.length) {
      let type = this.data[this.index++];
      let expiration: Date | undefined;

      if (type === 0xFF) {
        this.index--;
        break;
      } else if (type === 0xFC) { // Expire time in milliseconds
        const milliseconds = this.readUint64();
        expiration = new Date(Number(milliseconds));
        type = this.data[this.index++];
      } else if (type === 0xFD) { // Expire time in seconds
        const seconds = this.readUint32();
        expiration = new Date(Number(seconds) * 1000);
        type = this.data[this.index++];
      }

      const key = this.readEncodedString();
      switch (type) {
        case 0: { // string encoding
          const value = this.readEncodedString();
          console.log(key, value, expiration);
          if ((expiration ?? now) >= now) {
            const expiryMs = expiration ? expiration.getTime() : undefined;
            this.entries.set(key, { key, value, expiry: expiryMs, type: "string" });
          }
          break;
        }
        default:
          throw Error("type not implemented: " + type);
      }
    }
  }
}

export function parseRDBFile(filePath: string): RDBKey[] {
  try {
    if (!fs.existsSync(filePath)) {
      console.log(`RDB file ${filePath} does not exist, treating database as empty`);
      return [];
    }

    const buffer = fs.readFileSync(filePath);
    const parser = new RDBParser(buffer);
    return parser.parse();
  } catch (error) {
    console.error(`Error parsing RDB file: ${error}`);
    return [];
  }
}