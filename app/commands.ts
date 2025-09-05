import {
  encodeBulkString,
  encodeSimpleString,
  encodeError,
  encodeInteger,
  encodeArray,
} from "./parser";
import * as net from "net";

// Enhanced key-value store with expiry support
export interface KeyValueEntry {
  value: any;
  expiry?: number; // Timestamp when key expires (Date.now() + px)
}

export class RedisCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor() {
    this.kvStore = new Map<string, KeyValueEntry>();
  }

  // ===== COMMAND HANDLERS =====

  executeCommand(command: string, args: string[], socket: net.Socket): void {
    let response: string;

    switch (command.toUpperCase()) {
      case "PING":
        response = this.handlePing(args);
        break;
      case "ECHO":
        response = this.handleEcho(args);
        break;
      case "SET":
        response = this.handleSet(args);
        break;
      case "GET":
        response = this.handleGet(args);
        break;
      case "RPUSH":
        response = this.handleRPush(args);
        break;
      case "LRANGE":
        response = this.handleLRange(args);
        break;
      case "LPUSH":
        response = this.handleLPush(args);
        break;
      case "LLEN":
        response = this.handleLLen(args);
        break;
      case "LPOP":
        response = this.handleLPop(args);
        break;
      default:
        response = encodeError(`ERR unknown command '${command}'`);
    }

    socket.write(response);
  }

  private handlePing(args: string[]): string {
    return encodeSimpleString("PONG");
  }

  private handleEcho(args: string[]): string {
    if (args.length !== 1) {
      return encodeError("ERR wrong number of arguments for 'echo' command");
    }
    return encodeBulkString(args[0]);
  }

  private handleSet(args: string[]): string {
    if (args.length === 2) {
      // SET key value (no expiry)
      this.kvStore.set(args[0], {value: args[1]});
      return encodeSimpleString("OK");
    }

    if (args.length === 4 && args[2].toUpperCase() === "PX") {
      // SET key value PX milliseconds
      const px = parseInt(args[3]);
      if (isNaN(px) || px <= 0) {
        return encodeError("ERR invalid expire time in set");
      }
      const expiry = Date.now() + px;
      this.kvStore.set(args[0], {value: args[1], expiry});
      return encodeSimpleString("OK");
    }

    return encodeError("ERR syntax error");
  }

  private handleGet(args: string[]): string {
    if (args.length !== 1) {
      return encodeError("ERR wrong number of arguments for 'get' command");
    }

    const entry = this.kvStore.get(args[0]);
    if (!entry) {
      return encodeBulkString(null);
    }

    // Check if key has expired
    if (entry.expiry && Date.now() > entry.expiry) {
      this.kvStore.delete(args[0]); // Clean up expired key
      return encodeBulkString(null);
    }

    return encodeBulkString(entry.value);
  }

  private handleRPush(args: string[]): string {
    if (args.length < 2) {
      return encodeError("ERR wrong number of arguments for 'rpush' command");
    }
    const key = args[0];
    const values = args.slice(1); // can be multiple values
    let entry = this.kvStore.get(key);

    if (entry) {
      // If existing value is an array, append to it
      if (Array.isArray(entry.value)) {
        entry.value.push(...values); // Append values to existing array
        this.kvStore.set(key, entry); // Update store
      } else {
        // If not an array, create new array with old value + new values
        const newArray = [entry.value as string, ...values];
        this.kvStore.set(key, {value: newArray, expiry: entry.expiry});
      }
    } else {
      // Create new list
      this.kvStore.set(key, {value: values});
    }

    const currentEntry = this.kvStore.get(key)!;
    const newLength = Array.isArray(currentEntry.value)
      ? currentEntry.value.length
      : 0;
    return encodeInteger(newLength); // RESP Integer for new list length
  }

  private handleLRange(args: string[]): string {
    if (args.length !== 3) {
      return encodeError("ERR wrong number of arguments for 'lrange' command");
    }

    const [key, startStr, stopStr] = args;
    const start = parseInt(startStr);
    const stop = parseInt(stopStr);

    if (isNaN(start) || isNaN(stop)) {
      return encodeError("ERR value is not an integer or out of range");
    }

    const entry = this.kvStore.get(key);
    if (!entry || !Array.isArray(entry.value)) {
      return encodeArray([]); // Case 1: Non-existent list → empty array
    }

    const list = entry.value as string[];
    const listLength = list.length;

    // Handle negative indices (Redis behavior: -1 = last element)
    let normalizedStart = start < 0 ? listLength + start : start;
    let normalizedStop = stop < 0 ? listLength + stop : stop;

    // Clamp start to valid range: 0 to listLength
    normalizedStart = Math.max(0, Math.min(normalizedStart, listLength));

    // Clamp stop: for negative indices allow -1, for positive allow any value
    if (stop < 0) {
      normalizedStop = Math.max(-1, normalizedStop);
    } else {
      normalizedStop = Math.max(normalizedStop, -1); // Allow any positive value
    }

    // Case 4: start > stop → empty array
    if (normalizedStart > normalizedStop) {
      return encodeArray([]);
    }

    // Case 2: start >= list length → empty array
    if (listLength === 0 || normalizedStart >= listLength) {
      return encodeArray([]);
    }

    // Get the slice (slice is inclusive of end index in 2nd param)
    const result = list.slice(normalizedStart, normalizedStop + 1);
    return encodeArray(result);
  }

  private handleLPush(args: string[]): string {
    if (args.length < 2) {
      return encodeError("ERR wrong number of arguments for 'lpush' command");
    }
    const key = args[0];
    const values = args.slice(1).reverse(); // can be multiple values
    let entry = this.kvStore.get(key);
    if (entry) {
      // If existing value is an array, prepend to it
      if (Array.isArray(entry.value)) {
        entry.value.unshift(...values); // Prepend values to existing array
        this.kvStore.set(key, entry); // Update store
      } else {
        // If not an array, create new array with old value + new values
        const newArray = [...values, entry.value as string];
        this.kvStore.set(key, {value: newArray, expiry: entry.expiry});
      }
    } else {
      // Create new list
      this.kvStore.set(key, {value: values});
    }
    return encodeInteger((this.kvStore.get(key)?.value as string[]).length); // RESP Integer for new list length
  }

  private handleLLen(args: string[]): string {
    if (args.length !== 1) {
      return encodeError("ERR wrong number of arguments for 'llen' command");
    }
    const key = args[0];
    const entry = this.kvStore.get(key);
    if (!entry || !Array.isArray(entry.value)) {
      return encodeInteger(0); // Non-existent list or not a list
    }
    return encodeInteger(entry.value.length);
  }

  private handleLPop(args: string[]): string {
    if (args.length < 1) {
      return encodeError("ERR wrong number of arguments for 'lpop' command");
    }
    const key = args[0];
    const entry = this.kvStore.get(key);
    if (!entry || !Array.isArray(entry.value) || entry.value.length === 0) {
      return encodeBulkString(null); // Non-existent list or empty list
    }
    const list = entry.value as string[];
    const hasCountArg = args.length === 2;
    if (hasCountArg) {
      const numberOfElementsToPop = parseInt(args[1]);
      if (isNaN(numberOfElementsToPop) || numberOfElementsToPop <= 0) {
        return encodeError("ERR value is not an integer or out of range");
      }
      const elementsPoped = [];
      for (let i = 0; i < numberOfElementsToPop; i++) {
        if (list.length === 0) break;
        elementsPoped.push(list.shift()!);
      }
      this.kvStore.set(key, {value: list, expiry: entry.expiry});
      return encodeArray(elementsPoped);
    }

    const poppedValue = list.shift()!; // Remove and get the first element
    this.kvStore.set(key, {value: list, expiry: entry.expiry});
    return encodeBulkString(poppedValue);
  }

  // ===== UTILITY METHODS =====

  // For testing or debugging
  getStoreSize(): number {
    return this.kvStore.size;
  }

  clearStore(): void {
    this.kvStore.clear();
  }
}
