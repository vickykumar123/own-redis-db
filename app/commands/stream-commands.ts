import type {KeyValueEntry} from "../commands";
import {encodeBulkString, encodeError} from "../parser";

export class StreamCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  handleXAdd(args: string[]): string {
    if (args.length < 4 || args.length % 2 !== 0) {
      return encodeError("ERR wrong number of arguments for 'xadd' command");
    }

    const [key, id, ...fieldsValues] = args;

    const entry = this.kvStore.get(key);
    if (!entry) {
      // Create new entry
      this.kvStore.set(key, {value: [], type: "stream"});
    }

    // create the entry
    const fieldMap = new Map<string, string>();
    for (let i = 0; i < fieldsValues.length; i += 2) {
      fieldMap.set(fieldsValues[i], fieldsValues[i + 1]);
    }

    const streamEntry = {
      id: id,
      fields: fieldMap,
    };
    entry?.value.push(streamEntry);

    return encodeBulkString(id);
  }
}
