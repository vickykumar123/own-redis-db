import {encodeError, encodeInteger} from "../parser";
import {type KeyValueEntry} from "../commands";

export class GeoCommands {
  private kvStore: Map<string, KeyValueEntry>;

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  // Helper to get or create a geo sorted set (which is just a sorted set)
  private getGeoSortedSet(key: string): Map<string, number> {
    const entry = this.kvStore.get(key);

    if (!entry) {
      // Create new geo sorted set (stored as zset)
      const geoSet = new Map<string, number>();
      this.kvStore.set(key, {value: geoSet, type: "zset"});
      return geoSet;
    }

    if (entry.type && entry.type !== "zset") {
      throw new Error(
        `WRONGTYPE Operation against a key holding the wrong kind of value`
      );
    }

    return entry.value as Map<string, number>;
  }

  // Convert longitude/latitude to geohash (simplified implementation)
  private encodeGeohash(longitude: number, latitude: number): number {
    // This is a simplified geohash implementation
    // In Redis, coordinates are encoded as 52-bit geohash and stored as sorted set scores

    // Normalize coordinates to [0, 1] range
    const normalizedLon = (longitude + 180.0) / 360.0;
    const normalizedLat = (latitude + 90.0) / 180.0;

    // Simple interleaving of bits (simplified geohash)
    // This is not the full Redis implementation but works for basic functionality
    let geohash = 0;
    let lonBits = Math.floor(normalizedLon * 0x1fffff); // 21 bits
    let latBits = Math.floor(normalizedLat * 0x1fffff); // 21 bits

    // Interleave the bits (simplified)
    for (let i = 0; i < 21; i++) {
      geohash |=
        ((lonBits & (1 << i)) << i) | ((latBits & (1 << i)) << (i + 1));
    }

    return geohash;
  }

  // Sort and rebuild the map to maintain sorted order (same as sorted sets)
  private rebuildSortedOrder(geoSet: Map<string, number>): void {
    const sortedEntries = Array.from(geoSet.entries()).sort((a, b) => {
      // Sort by geohash score first
      if (a[1] !== b[1]) return a[1] - b[1];
      // Then by member name lexicographically
      return a[0].localeCompare(b[0]);
    });

    // Rebuild map in sorted order
    geoSet.clear();
    for (const [member, geohash] of sortedEntries) {
      geoSet.set(member, geohash);
    }
  }

  // GEOADD key longitude latitude member [longitude latitude member ...]
  handleGeoAdd(args: string[]): string {
    if (args.length < 4 || (args.length - 1) % 3 !== 0) {
      return encodeError("ERR wrong number of arguments for 'geoadd' command");
    }

    const key = args[0];
    let addedCount = 0;

    try {
      const geoSet = this.getGeoSortedSet(key);

      // Process longitude-latitude-member triplets
      for (let i = 1; i < args.length; i += 3) {
        const longitudeStr = args[i];
        const latitudeStr = args[i + 1];
        const member = args[i + 2];

        const longitude = parseFloat(longitudeStr);
        const latitude = parseFloat(latitudeStr);

        if (isNaN(longitude) || isNaN(latitude)) {
          return encodeError("ERR value is not a valid float");
        }

        // Validate coordinate ranges
        if (longitude < -180 || longitude > 180) {
          return encodeError("ERR invalid longitude");
        }
        if (latitude < -85.05112878 || latitude > 85.05112878) {
          return encodeError("ERR invalid latitude");
        }

        // Encode coordinates as geohash
        const geohash = this.encodeGeohash(longitude, latitude);

        // Check if member already exists
        const wasNew = !geoSet.has(member);
        geoSet.set(member, geohash);

        if (wasNew) {
          addedCount++;
        }
      }

      // Rebuild sorted order
      this.rebuildSortedOrder(geoSet);

      return encodeInteger(addedCount);
    } catch (error) {
      if (error instanceof Error) {
        return encodeError(error.message);
      }
      return encodeError("ERR unknown error");
    }
  }
}
