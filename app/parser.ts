export type RESPValue = string | number | RESPValue[] | null;

export interface ParsedCommand {
  command: string;
  args: string[];
}

export class RESPParser {
  private buffer: Buffer;
  private offset: number;

  constructor(data: Buffer) {
    this.buffer = data;
    this.offset = 0;
  }

  parse(): RESPValue | null {
    if (this.offset >= this.buffer.length) {
      return null;
    }

    const type = String.fromCharCode(this.buffer[this.offset]);
    this.offset++;

    switch (type) {
      case "+":
        return this.parseSimpleString();
      case "-":
        return this.parseError();
      case ":":
        return this.parseInteger();
      case "$":
        return this.parseBulkString();
      case "*":
        return this.parseArray();
      default:
        throw new Error(`Unknown RESP type: ${type}`);
    }
  }

  private parseSimpleString(): string {
    const endIndex = this.findCRLF();
    const value = this.buffer.subarray(this.offset, endIndex).toString();
    this.offset = endIndex + 2; // Skip CRLF
    return value;
  }

  private parseError(): string {
    const endIndex = this.findCRLF();
    const value = this.buffer.subarray(this.offset, endIndex).toString();
    this.offset = endIndex + 2; // Skip CRLF
    return value;
  }

  private parseInteger(): number {
    const endIndex = this.findCRLF();
    const value = parseInt(
      this.buffer.subarray(this.offset, endIndex).toString()
    );
    this.offset = endIndex + 2; // Skip CRLF
    return value;
  }

  private parseBulkString(): string | null {
    const lengthEndIndex = this.findCRLF();
    const length = parseInt(
      this.buffer.subarray(this.offset, lengthEndIndex).toString()
    );
    this.offset = lengthEndIndex + 2; // Skip CRLF

    if (length === -1) {
      return null; // Null bulk string
    }

    const value = this.buffer
      .subarray(this.offset, this.offset + length)
      .toString();
    this.offset += length + 2; // Skip string + CRLF
    return value;
  }

  private parseArray(): RESPValue[] | null {
    const lengthEndIndex = this.findCRLF();
    const length = parseInt(
      this.buffer.subarray(this.offset, lengthEndIndex).toString()
    );
    this.offset = lengthEndIndex + 2; // Skip CRLF

    if (length === -1) {
      return null; // Null array
    }

    const array: RESPValue[] = [];
    for (let i = 0; i < length; i++) {
      const element = this.parse();
      if (element === null) {
        throw new Error("Unexpected null element in array");
      }
      array.push(element);
    }
    return array;
  }

  private findCRLF(): number {
    for (let i = this.offset; i < this.buffer.length - 1; i++) {
      if (this.buffer[i] === 0x0d && this.buffer[i + 1] === 0x0a) {
        return i;
      }
    }
    throw new Error("CRLF not found");
  }
}

export function parseRESPCommand(data: Buffer): ParsedCommand | null {
  try {
    const parser = new RESPParser(data);
    const parsed = parser.parse();

    if (!Array.isArray(parsed) || parsed.length === 0) {
      return null;
    }

    const command = String(parsed[0]).toUpperCase();
    const args = parsed.slice(1).map((arg) => String(arg));

    return {command, args};
  } catch (error) {
    console.error("Error parsing RESP command:", error);
    return null;
  }
}

export function encodeBulkString(value: string | null): string {
  if (value === null) {
    return "$-1\r\n"; // Null bulk string: $-1\r\n
  }
  return `$${value.length}\r\n${value}\r\n`; // Regular bulk string: $length\r\nvalue\r\n
}

// Alternative: Separate function for clarity
export function encodeNullBulkString(): string {
  return "$-1\r\n";
}

export function encodeSimpleString(value: string): string {
  return `+${value}\r\n`;
}

export function encodeError(message: string): string {
  return `-${message}\r\n`;
}

export function encodeInteger(value: number): string {
  return `:${value}\r\n`;
}

export function encodeArray(elements: (string | null)[] | null): string {
  if (elements === null) {
    return "*-1\r\n"; // Null array
  }

  let response = `*${elements.length}\r\n`; // Array length

  for (const element of elements) {
    response += encodeBulkString(element); // Each element as bulk string
  }

  return response;
}
