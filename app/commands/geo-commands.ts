import {
  encodeArray,
  encodeBulkString,
  encodeError,
  encodeInteger,
} from "../parser";
import {type KeyValueEntry} from "../commands";

const MIN_LATITUDE = -85.05112878;
const MAX_LATITUDE = 85.05112878;
const MIN_LONGITUDE = -180.0;
const MAX_LONGITUDE = 180.0;

const LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;

class DecodeCoordinates {
  constructor(public latitude: number, public longitude: number) {}
}

function decodeCompactInt64ToInt32(v: bigint): number {
  v = v & 0x5555555555555555n;
  v = (v | (v >> 1n)) & 0x3333333333333333n;
  v = (v | (v >> 2n)) & 0x0f0f0f0f0f0f0f0fn;
  v = (v | (v >> 4n)) & 0x00ff00ff00ff00ffn;
  v = (v | (v >> 8n)) & 0x0000ffff0000ffffn;
  v = (v | (v >> 16n)) & 0x00000000ffffffffn;
  return Number(v);
}

function decodeConvertGridNumbersToCoordinates(
  gridLatitudeNumber: number,
  gridLongitudeNumber: number
): DecodeCoordinates {
  // Calculate the grid boundaries
  const gridLatitudeMin =
    MIN_LATITUDE +
    LATITUDE_RANGE * ((gridLatitudeNumber * 1.0) / Math.pow(2, 26));
  const gridLatitudeMax =
    MIN_LATITUDE +
    LATITUDE_RANGE * (((gridLatitudeNumber + 1) * 1.0) / Math.pow(2, 26));
  const gridLongitudeMin =
    MIN_LONGITUDE +
    LONGITUDE_RANGE * ((gridLongitudeNumber * 1.0) / Math.pow(2, 26));
  const gridLongitudeMax =
    MIN_LONGITUDE +
    LONGITUDE_RANGE * (((gridLongitudeNumber + 1) * 1.0) / Math.pow(2, 26));

  // Calculate the center point of the grid cell
  const latitude = (gridLatitudeMin + gridLatitudeMax) / 2;
  const longitude = (gridLongitudeMin + gridLongitudeMax) / 2;

  return new DecodeCoordinates(latitude, longitude);
}

function decodeGeohash(geoCode: bigint): DecodeCoordinates {
  // Align bits of both latitude and longitude to take even-numbered position
  const y = geoCode >> 1n;
  const x = geoCode;

  // Compact bits back to 32-bit ints
  const gridLatitudeNumber = decodeCompactInt64ToInt32(x);
  const gridLongitudeNumber = decodeCompactInt64ToInt32(y);

  return decodeConvertGridNumbersToCoordinates(
    gridLatitudeNumber,
    gridLongitudeNumber
  );
}

function spread32BitsTo64Bits(v: number): bigint {
  let result = BigInt(v) & 0xffffffffn;
  result = (result | (result << 16n)) & 0x0000ffff0000ffffn;
  result = (result | (result << 8n)) & 0x00ff00ff00ff00ffn;
  result = (result | (result << 4n)) & 0x0f0f0f0f0f0f0f0fn;
  result = (result | (result << 2n)) & 0x3333333333333333n;
  result = (result | (result << 1n)) & 0x5555555555555555n;
  return result;
}

function interleaveBits(x: number, y: number): bigint {
  const xSpread = spread32BitsTo64Bits(x);
  const ySpread = spread32BitsTo64Bits(y);
  const yShifted = ySpread << 1n;
  return xSpread | yShifted;
}

function encodeGeoHash(latitude: number, longitude: number): bigint {
  // Normalize to the range 0-2^26
  const normalizedLatitude =
    (Math.pow(2, 26) * (latitude - MIN_LATITUDE)) / LATITUDE_RANGE;
  const normalizedLongitude =
    (Math.pow(2, 26) * (longitude - MIN_LONGITUDE)) / LONGITUDE_RANGE;

  // Truncate to integers
  const latInt = Math.floor(normalizedLatitude);
  const lonInt = Math.floor(normalizedLongitude);

  return interleaveBits(latInt, lonInt);
}

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
        if (longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE) {
          return encodeError("ERR invalid longitude");
        }
        if (latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
          return encodeError("ERR invalid latitude");
        }

        // Encode coordinates as geohash (returns BigInt, convert to number)
        const geohashBigInt = encodeGeoHash(latitude, longitude);
        const geohash = Number(geohashBigInt);

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

  handleGeoPos(args: string[]): string {
    if (args.length < 2) {
      return encodeError("Invaild arguments length");
    }

    const key = args[0];
    const location = args[1];

    const entry = this.kvStore.get(key);
    if (!entry) {
      return encodeArray(null);
    }

    const geoLoc = entry.value;
    if (!geoLoc.has(location)) {
      return encodeArray(null);
    }
    const geoHash = geoLoc.get(location);
    const decodedHash = decodeGeohash(geoHash);
    const response = [];
    response.push(encodeBulkString(decodedHash.longitude.toString()));
    response.push(encodeBulkString(decodedHash.latitude.toString()));
    return encodeArray(response);
  }
}
