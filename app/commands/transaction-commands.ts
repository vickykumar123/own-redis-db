import type {KeyValueEntry} from "../commands";
import {encodeSimpleString, encodeError, encodeArray} from "../parser";

export class TransactionCommands {
  private kvStore: Map<string, KeyValueEntry>;
  private isMulti: boolean = false;
  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  handleMulti(args: string[]): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'multi' command");
    }
    this.isMulti = true;
    return encodeSimpleString("OK");
  }

  handleExec(args: string[]): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'exec' command");
    }
    if (!this.isMulti) {
      return encodeError("ERR EXEC without MULTI");
    }
    this.isMulti = false;
    // For simplicity, we return an empty array as we are not queuing commands
    return encodeArray([]); // RESP for empty array
  }
}
