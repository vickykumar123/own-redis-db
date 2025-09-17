import {encodeError, encodeInteger, encodeBulkString} from "../parser";
import {type KeyValueEntry} from "../commands";

export class SortedSetCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  // Helper to get or create a sorted set
  private getSortedSet(key: string): Map<string, number> {
    const entry = this.kvStore.get(key);

    if (!entry) {
      // Create new sorted set
      const sortedSet = new Map<string, number>();
      this.kvStore.set(key, {value: sortedSet, type: "zset"});
      return sortedSet;
    }

    if (entry.type && entry.type !== "zset") {
      throw new Error(
        `WRONGTYPE Operation against a key holding the wrong kind of value`
      );
    }

    return entry.value as Map<string, number>;
  }

  // Sort and rebuild the map to maintain sorted order
  private rebuildSortedOrder(sortedSet: Map<string, number>): void {
    const sortedEntries = Array.from(sortedSet.entries()).sort((a, b) => {
      // Sort by score first
      if (a[1] !== b[1]) return a[1] - b[1];
      // Then by member name lexicographically
      return a[0].localeCompare(b[0]);
    });

    // Rebuild map in sorted order
    sortedSet.clear();
    for (const [member, score] of sortedEntries) {
      sortedSet.set(member, score);
    }
  }

  // ZADD key score member [score member ...]
  handleZAdd(args: string[]): string {
    if (args.length < 3 || args.length % 2 === 0) {
      return encodeError("ERR wrong number of arguments for 'zadd' command");
    }

    const key = args[0];
    let addedCount = 0;

    try {
      const sortedSet = this.getSortedSet(key);

      // Process score-member pairs
      for (let i = 1; i < args.length; i += 2) {
        const scoreStr = args[i];
        const member = args[i + 1];

        const score = parseFloat(scoreStr);
        if (isNaN(score)) {
          return encodeError("ERR value is not a valid float");
        }

        // Check if member already exists
        const wasNew = !sortedSet.has(member);
        sortedSet.set(member, score);

        if (wasNew) {
          addedCount++;
        }
      }

      // Rebuild sorted order
      this.rebuildSortedOrder(sortedSet);

      return encodeInteger(addedCount);
    } catch (error) {
      if (error instanceof Error) {
        return encodeError(error.message);
      }
      return encodeError("ERR unknown error");
    }
  }

  // ZRANK key member - get the rank (0-based index) of a member
  handleZRank(args: string[]): string {
    if (args.length !== 2) {
      return encodeError("ERR wrong number of arguments for 'zrank' command");
    }

    const key = args[0];
    const member = args[1];

    const entry = this.kvStore.get(key);
    if (!entry) {
      return encodeBulkString(null); // Null for non-existent key
    }

    if (entry.type && entry.type !== "zset") {
      return encodeError("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    const sortedSet = entry.value as Map<string, number>;
    if (!sortedSet.has(member)) {
      return encodeBulkString(null); // Null for non-existent member
    }

    // Find the rank (0-based index) in the sorted set
    // Since our map maintains sorted order (by score, then lexicographically), we iterate and count
    let rank = 0;
    for (const [currentMember] of sortedSet) {
      if (currentMember === member) {
        return encodeInteger(rank);
      }
      rank++;
    }

    // This should never happen if member exists
    return encodeBulkString(null);
  }
}
