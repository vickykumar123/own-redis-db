import * as fs from "fs";

export interface RDBKey {
  key: string;
  value: any;
  expiry?: number; // Unix timestamp in milliseconds
  type: "string" | "list" | "hash" | "set" | "zset";
}

export class RDBParser {
  private buffer: Buffer;
  private position: number = 0;

  constructor(buffer: Buffer) {
    this.buffer = buffer;
  }

  // Read size-encoded value
  private readSize(): number {
    const byte = this.buffer[this.position++];
    const firstTwoBits = (byte & 0xC0) >> 6; // Get first 2 bits

    if (firstTwoBits === 0b00) {
      // 6-bit length
      return byte & 0x3F;
    } else if (firstTwoBits === 0b01) {
      // 14-bit length (big-endian)
      const nextByte = this.buffer[this.position++];
      return ((byte & 0x3F) << 8) | nextByte;
    } else if (firstTwoBits === 0b10) {
      // 32-bit length (big-endian)
      const length = this.buffer.readUInt32BE(this.position);
      this.position += 4;
      return length;
    } else {
      // Special encoding (0b11) - not used for size
      throw new Error("Special encoding not expected for size");
    }
  }

  // Read string-encoded value
  private readString(): string {
    const size = this.readSize();
    const str = this.buffer.subarray(this.position, this.position + size).toString();
    this.position += size;
    return str;
  }

  // Read 4-byte little-endian unsigned integer
  private readUInt32LE(): number {
    const value = this.buffer.readUInt32LE(this.position);
    this.position += 4;
    return value;
  }

  // Read 8-byte little-endian unsigned long
  private readUInt64LE(): number {
    const low = this.buffer.readUInt32LE(this.position);
    const high = this.buffer.readUInt32LE(this.position + 4);
    this.position += 8;
    return high * 0x100000000 + low;
  }

  parse(): RDBKey[] {
    const keys: RDBKey[] = [];

    // Skip header (REDIS + version)
    if (this.buffer.subarray(0, 5).toString() !== "REDIS") {
      throw new Error("Invalid RDB file: missing REDIS header");
    }
    this.position = 9; // Skip "REDIS0011"

    while (this.position < this.buffer.length) {
      const opcode = this.buffer[this.position++];

      if (opcode === 0xFF) {
        // EOF marker
        break;
      } else if (opcode === 0xFA) {
        // Metadata subsection - skip
        this.readString(); // metadata name
        this.readString(); // metadata value
      } else if (opcode === 0xFE) {
        // Database subsection
        const dbIndex = this.readSize();
        console.log(`Processing database ${dbIndex}`);
      } else if (opcode === 0xFB) {
        // Hash table sizes
        const keyValueHashTableSize = this.readSize();
        const expiryHashTableSize = this.readSize();
        console.log(`Hash table sizes: keys=${keyValueHashTableSize}, expiry=${expiryHashTableSize}`);
      } else if (opcode === 0xFC) {
        // Expiry in milliseconds
        const expiry = this.readUInt64LE();
        const valueType = this.buffer[this.position++];
        const key = this.readString();
        const value = this.readValue(valueType);
        keys.push({ key, value, expiry, type: this.getValueType(valueType) });
      } else if (opcode === 0xFD) {
        // Expiry in seconds
        const expiry = this.readUInt32LE() * 1000; // Convert to milliseconds
        const valueType = this.buffer[this.position++];
        const key = this.readString();
        const value = this.readValue(valueType);
        keys.push({ key, value, expiry, type: this.getValueType(valueType) });
      } else {
        // Value type (no expiry)
        const valueType = opcode;
        const key = this.readString();
        const value = this.readValue(valueType);
        keys.push({ key, value, type: this.getValueType(valueType) });
      }
    }

    return keys;
  }

  private readValue(valueType: number): any {
    switch (valueType) {
      case 0: // String
        return this.readString();
      default:
        throw new Error(`Unsupported value type: ${valueType}`);
    }
  }

  private getValueType(valueType: number): "string" | "list" | "hash" | "set" | "zset" {
    switch (valueType) {
      case 0: return "string";
      case 1: return "list";
      case 2: return "set";
      case 3: return "zset";
      case 4: return "hash";
      default: return "string";
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