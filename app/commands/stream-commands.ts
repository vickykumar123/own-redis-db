import type {KeyValueEntry} from "../commands";
import {encodeBulkString, encodeError} from "../parser";

export class StreamCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  private idValidation(entry: KeyValueEntry, incomingId: string): string {
    const lastEntry = entry.value[entry.value.length - 1];
    const lastId = lastEntry.id;
    const [lastMs, lastSeq] = lastId.split("-").map(Number);
    const [newMs, newSeq] = incomingId.split("-").map(Number);
    let errorMsg = "";
    if (newMs === 0 && newSeq === 0) {
      errorMsg = encodeError(
        "ERR The ID specified in XADD must be greater than 0-0"
      );
    } else if (newMs === lastMs && newSeq <= lastSeq) {
      errorMsg = encodeError(
        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
      );
    } else if (newMs < lastMs || isNaN(newMs) || isNaN(newSeq)) {
      errorMsg = encodeError(
        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
      );
    }
    return errorMsg;
  }

  private generateSequenceNumber(entry: KeyValueEntry, timeMs: number): number {
    if (entry.value.length === 0) {
      return timeMs === 0 ? 1 : 0; // Special case: when time=0, seq starts at 1
    }

    // Find highest sequence number for this time part
    let maxSeq = -1;
    for (const streamEntry of entry.value) {
      const [entryMs, entrySeq] = streamEntry.id.split("-").map(Number);
      if (entryMs === timeMs && entrySeq > maxSeq) {
        maxSeq = entrySeq;
      }
    }

    return maxSeq === -1 ? (timeMs === 0 ? 1 : 0) : maxSeq + 1;
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

    const [timeStr, seqStr] = id.split("-");
    const isAutoSeq = seqStr === "*";
    let finalId = id;

    if (isAutoSeq) {
      const timeMs = parseInt(timeStr);
      if (isNaN(timeMs)) {
        return encodeError("ERR Invalid stream ID specified as argument");
      }

      // Get the current entry (now guaranteed to exist after creation above)
      const currentEntry = this.kvStore.get(key)!;

      // Validate time part against existing entries
      if (currentEntry.value.length > 0) {
        const lastEntry = currentEntry.value[currentEntry.value.length - 1];
        const [lastMs] = lastEntry.id.split("-").map(Number);

        if (timeMs < lastMs) {
          return encodeError(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
          );
        }

        // Special case: if timeMs === 0 and we have any entries, it's invalid
        if (timeMs === 0) {
          return encodeError(
            "ERR The ID specified in XADD must be greater than 0-0"
          );
        }
      }

      // Generate sequence number
      const seqNum = this.generateSequenceNumber(currentEntry, timeMs);
      finalId = `${timeMs}-${seqNum}`;
    } else {
      // validation of ID
      if (entry && entry.value.length > 0) {
        const error = this.idValidation(entry, id);
        if (error) return error;
      }
    }

    // create the entry
    const fieldMap = new Map<string, string>();
    for (let i = 0; i < fieldsValues.length; i += 2) {
      fieldMap.set(fieldsValues[i], fieldsValues[i + 1]);
    }

    const streamEntry = {
      id: finalId,
      fields: fieldMap,
    };
    const currentEntry = this.kvStore.get(key)!;
    currentEntry.value.push(streamEntry);

    return encodeBulkString(finalId);
  }
}
