// String command handlers
import {encodeBulkString, encodeSimpleString, encodeError} from "../parser";
import type {KeyValueEntry} from "../commands";

export class StringCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  handleSet(args: string[]): string {
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

  handleGet(args: string[]): string {
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
}
