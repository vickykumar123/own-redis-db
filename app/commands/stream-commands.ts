import type {KeyValueEntry} from "../commands";
import {
  encodeArray,
  encodeBulkString,
  encodeError,
  encodeNestedArray,
  encodeRawArray,
  encodeXReadResponse,
} from "../parser";

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

  private parseRangeId(id: string, isEnd: boolean): [number, number] {
    if (id === "-") {
      return [Number.NEGATIVE_INFINITY, Number.NEGATIVE_INFINITY];
    }
    if (id === "+") {
      return [Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY];
    }
    const parts = id.split("-");
    let timeMs = parseInt(parts[0]);
    if (parts.length === 1) {
      //If no sequence number was provided (e.g., just "1526985054069"):
      //- For start range: default sequence to 0 (get from beginning of that time)
      // - For end range: default sequence to MAX_SAFE_INTEGER (get until end of that time)
      return [timeMs, isEnd ? Number.MAX_SAFE_INTEGER : 0];
    }

    return [timeMs, parseInt(parts[1])];
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

    // Ensure stream exists
    if (!this.kvStore.has(key)) {
      this.kvStore.set(key, {value: [], type: "stream"});
    }
    const streamEntry = this.kvStore.get(key)!;

    const [timeStr, seqStr] = id.split("-");
    const isAutoSeq = timeStr !== "*" && seqStr === "*";
    const isFullyAuto = timeStr === "*";
    let finalId = id;

    if (isFullyAuto) {
      const timeMs = Date.now();
      const seqNum = this.generateSequenceNumber(streamEntry, timeMs);
      finalId = `${timeMs}-${seqNum}`;
    } else if (isAutoSeq) {
      const timeMs = parseInt(timeStr);
      if (isNaN(timeMs)) {
        return encodeError("ERR Invalid stream ID specified as argument");
      }

      // Validate time part against existing entries
      if (streamEntry.value.length > 0) {
        const lastEntry = streamEntry.value[streamEntry.value.length - 1];
        const [lastMs] = lastEntry.id.split("-").map(Number);

        if (timeMs < lastMs) {
          return encodeError(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
          );
        }

        if (timeMs === 0) {
          return encodeError(
            "ERR The ID specified in XADD must be greater than 0-0"
          );
        }
      }

      const seqNum = this.generateSequenceNumber(streamEntry, timeMs);
      finalId = `${timeMs}-${seqNum}`;
    } else {
      if (streamEntry.value.length > 0) {
        const error = this.idValidation(streamEntry, id);
        if (error) return error;
      }
    }

    const fieldMap = new Map<string, string>();
    for (let i = 0; i < fieldsValues.length; i += 2) {
      fieldMap.set(fieldsValues[i], fieldsValues[i + 1]);
    }

    const newEntry = {
      id: finalId,
      fields: fieldMap,
    };
    streamEntry.value.push(newEntry);

    return encodeBulkString(finalId);
  }

  handleXRange(args: string[]): string {
    if (args.length !== 3) {
      return encodeError("ERR wrong number of arguments for 'xrange' command");
    }
    const [key, startId, endId] = args;
    const entry = this.kvStore.get(key);
    if (!entry || entry.type !== "stream") {
      return encodeArray([]); // Key does not exist or is not a stream
    }
    const [startMs, startSeq] = this.parseRangeId(startId, false);
    const [endMs, endSeq] = this.parseRangeId(endId, true);
    const results = [];

    for (const streamEntry of entry.value) {
      const [entryMs, entrySeq] = streamEntry.id.split("-").map(Number);
      if (entryMs < startMs || (entryMs === startMs && entrySeq < startSeq)) {
        continue; // Before start range
        // - Skip entries that are before the start range:
        // - Either time is less than start time
        // - OR time equals start time BUT sequence is less than start sequence
      }

      if (entryMs > endMs || (entryMs === endMs && entrySeq > endSeq)) {
        break; // After end range, can stop processing
        // - Stop processing if entry is after end range:
        // - Either time is greater than end time
        // - OR time equals end time BUT sequence is greater than end sequence
      }

      // Format entry as [id, [field1, value1, field2, value2, ...]]
      // Build field array as raw strings (not encoded)
      const fieldArray: string[] = [];
      for (const [field, value] of streamEntry.fields) {
        fieldArray.push(field, value);
      }

      // Push raw values, not encoded strings
      results.push([streamEntry.id, fieldArray]);
    }
    return encodeNestedArray(results);
  }

  handleXRead(args: string[]): string {
    if (args.length < 3 || args.length % 2 === 0) {
      return encodeError("ERR wrong number of arguments for 'xread' command");
    }
    if (args[0].toUpperCase() !== "STREAMS") {
      return encodeError("ERR syntax error");
    }

    // Calculate number of streams: (total_args - 1) / 2
    const numStreams = (args.length - 1) / 2;

    // Extract stream keys and IDs
    const streamKeys = args.slice(1, 1 + numStreams); // [stream_key, other_stream_key]
    const streamIds = args.slice(1 + numStreams); // [0-0, 0-1]

    const allResults: [string, [string, string[]][]][] = [];

    // Process each stream
    for (let i = 0; i < numStreams; i++) {
      const key = streamKeys[i];
      const startId = streamIds[i];

      const entry = this.kvStore.get(key);
      if (!entry || entry.type !== "stream") {
        continue; // Skip non-existent streams
      }

      const [startMs, startSeq] = this.parseRangeId(startId, false);
      const results: [string, string[]][] = [];

      // Process entries for this stream (your existing logic)
      for (const streamEntry of entry.value) {
        const [entryMs, entrySeq] = streamEntry.id.split("-").map(Number);
        if (
          entryMs < startMs ||
          (entryMs === startMs && entrySeq <= startSeq)
        ) {
          continue;
        }

        const fieldArray: string[] = [];
        for (const [field, value] of streamEntry.fields) {
          fieldArray.push(field, value);
        }
        results.push([streamEntry.id, fieldArray]);
      }

      // Only include streams that have results
      if (results.length > 0) {
        allResults.push([key, results]);
      }
    }

    return encodeXReadResponse(allResults);
  }
}
