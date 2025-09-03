import {
  encodeBulkString,
  encodeSimpleString,
  encodeError,
  encodeInteger,
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

  // ===== UTILITY METHODS =====

  // For testing or debugging
  getStoreSize(): number {
    return this.kvStore.size;
  }

  clearStore(): void {
    this.kvStore.clear();
  }
}
