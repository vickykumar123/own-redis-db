import type {KeyValueEntry} from "../commands";
import {encodeSimpleString, encodeError} from "../parser";

export class TransactionCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  handleMulti(args: string[]): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'multi' command");
    }
    
    return encodeSimpleString("OK");
  }
}