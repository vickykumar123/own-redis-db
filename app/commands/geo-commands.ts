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

  private radians(degree: number) {
    // degrees to radians
    let rad: number = (degree * Math.PI) / 180;

    return rad;
  }

  // Convert distance from meters to specified unit
  private convertFromMeters(meters: number, unit: string): number {
    switch (unit.toLowerCase()) {
      case "m":
        return meters;
      case "km":
        return meters / 1000;
      case "mi":
        return meters / 1609.344;
      case "ft":
        return meters * 3.28084;
      default:
        throw new Error(`Unsupported unit: ${unit}`);
    }
  }

  // Convert distance from specified unit to meters
  private convertToMeters(distance: number, unit: string): number {
    switch (unit.toLowerCase()) {
      case "m":
        return distance;
      case "km":
        return distance * 1000;
      case "mi":
        return distance * 1609.344;
      case "ft":
        return distance / 3.28084;
      default:
        throw new Error(`Unsupported unit: ${unit}`);
    }
  }

  // Validate if unit is supported
  private isValidUnit(unit: string): boolean {
    return ["m", "km", "mi", "ft"].includes(unit.toLowerCase());
  }

  private haversine(lat1: number, lon1: number, lat2: number, lon2: number) {
    // var dlat: number, dlon: number, a: number, c: number, R: number;
    let dlat, dlon, a, c, R: number;

    R = 6372797.560856; // Earth radius in meters (Redis uses this value)
    dlat = this.radians(lat2 - lat1);
    dlon = this.radians(lon2 - lon1);
    lat1 = this.radians(lat1);
    lat2 = this.radians(lat2);
    a =
      Math.sin(dlat / 2) * Math.sin(dlat / 2) +
      Math.sin(dlon / 2) * Math.sin(dlon / 2) * Math.cos(lat1) * Math.cos(lat2);
    c = 2 * Math.asin(Math.sqrt(a));
    return R * c; // Returns distance in meters
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
      return encodeError("ERR wrong number of arguments for 'geopos' command");
    }

    const key = args[0];
    const members = args.slice(1); // Get all members after the key

    const entry = this.kvStore.get(key);

    // Build response array - one entry per member
    const response: (string[] | null)[] = [];

    for (const member of members) {
      if (!entry || entry.type !== "zset") {
        // If key doesn't exist or wrong type, return null for this member
        response.push(null);
      } else {
        const geoSet = entry.value as Map<string, number>;

        if (!geoSet.has(member)) {
          // Member doesn't exist, return null
          response.push(null);
        } else {
          // Decode the geohash to get coordinates
          const geoHash = geoSet.get(member)!;
          const decodedHash = decodeGeohash(BigInt(geoHash));

          // Return [longitude, latitude] as strings
          response.push([
            decodedHash.longitude.toString(),
            decodedHash.latitude.toString(),
          ]);
        }
      }
    }

    // Encode the array of arrays
    let result = `*${response.length}\r\n`;
    for (const item of response) {
      if (item === null) {
        result += "*-1\r\n"; // Null array (not null bulk string)
      } else {
        // Each position is an array of [longitude, latitude]
        result += "*2\r\n";
        result += encodeBulkString(item[0]); // longitude
        result += encodeBulkString(item[1]); // latitude
      }
    }

    return result;
  }

  handleGeoDist(args: string[]): string {
    if (args.length < 3 || args.length > 4) {
      return encodeError("ERR wrong number of arguments for 'geodist' command");
    }

    const key = args[0];
    const member1 = args[1];
    const member2 = args[2];
    const unit = args.length === 4 ? args[3] : "m"; // Default to meters

    // Validate unit
    if (!this.isValidUnit(unit)) {
      return encodeError("ERR unsupported unit provided. please use m, km, mi, or ft");
    }

    const entry = this.kvStore.get(key);
    if (!entry || entry.type !== "zset") {
      return encodeBulkString(null);
    }

    const geoSet = entry.value as Map<string, number>;
    if (!geoSet.has(member1) || !geoSet.has(member2)) {
      return encodeBulkString(null);
    }

    const geoHash1 = geoSet.get(member1)!;
    const geoHash2 = geoSet.get(member2)!;
    const decodedLoc1 = decodeGeohash(BigInt(geoHash1));
    const decodedLoc2 = decodeGeohash(BigInt(geoHash2));

    // Haversine returns distance in meters
    const distanceMeters = this.haversine(
      decodedLoc1.latitude,
      decodedLoc1.longitude,
      decodedLoc2.latitude,
      decodedLoc2.longitude
    );

    // Convert to requested unit using reusable function
    const distance = this.convertFromMeters(distanceMeters, unit);

    return encodeBulkString(distance.toString());
  }

  // GEOSEARCH key FROMLONLAT longitude latitude BYRADIUS radius unit
  handleGeoSearch(args: string[]): string {
    if (args.length !== 7) {
      return encodeError("ERR wrong number of arguments for 'geosearch' command");
    }

    const key = args[0];
    const fromLonLatKeyword = args[1].toUpperCase();
    const centerLon = parseFloat(args[2]);
    const centerLat = parseFloat(args[3]);
    const byRadiusKeyword = args[4].toUpperCase();
    const radius = parseFloat(args[5]);
    const unit = args[6].toLowerCase();

    // Validate keywords
    if (fromLonLatKeyword !== "FROMLONLAT") {
      return encodeError("ERR syntax error: expected FROMLONLAT");
    }
    if (byRadiusKeyword !== "BYRADIUS") {
      return encodeError("ERR syntax error: expected BYRADIUS");
    }

    // Validate numeric inputs
    if (isNaN(centerLon) || isNaN(centerLat) || isNaN(radius)) {
      return encodeError("ERR value is not a valid float");
    }

    // Validate unit
    if (!this.isValidUnit(unit)) {
      return encodeError("ERR unsupported unit provided. please use m, km, mi, or ft");
    }

    // Get the geo set
    const entry = this.kvStore.get(key);
    if (!entry || entry.type !== "zset") {
      return encodeArray([]); // Empty array for non-existent key
    }

    const geoSet = entry.value as Map<string, number>;
    const matchingMembers: string[] = [];

    // Convert radius to meters for consistent calculation
    const radiusInMeters = this.convertToMeters(radius, unit);

    // Check each member's distance from center point
    for (const [member, geoHash] of geoSet) {
      // Decode member's coordinates
      const decodedLocation = decodeGeohash(BigInt(geoHash));

      // Calculate distance from center point to this member
      const distanceInMeters = this.haversine(
        centerLat,
        centerLon,
        decodedLocation.latitude,
        decodedLocation.longitude
      );

      // If within radius, add to results
      if (distanceInMeters <= radiusInMeters) {
        matchingMembers.push(member);
      }
    }

    return encodeArray(matchingMembers);
  }
}
