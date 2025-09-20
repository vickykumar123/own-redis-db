# Redis Clone - TypeScript Implementation

[![progress-banner](https://backend.codecrafters.io/progress/redis/adfae940-d6c4-465a-84d7-77ad47ae7aef)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

A feature-rich Redis clone implementation in TypeScript, supporting core Redis functionality including persistence, replication, transactions, pub/sub, and geospatial commands.

## Features

### Core Commands

#### String Operations
- `PING` - Test server connectivity
- `ECHO <message>` - Echo back the message
- `SET <key> <value> [PX milliseconds]` - Set key with optional expiry
- `GET <key>` - Get value by key
- `INCR <key>` - Increment integer value

#### List Operations
- `LPUSH <key> <value> [value ...]` - Push to list head
- `RPUSH <key> <value> [value ...]` - Push to list tail
- `LPOP <key>` - Pop from list head
- `BLPOP <key> [key ...] <timeout>` - Blocking pop from list head
- `LRANGE <key> <start> <stop>` - Get range of list elements
- `LLEN <key>` - Get list length

#### Sorted Set Operations
- `ZADD <key> <score> <member> [score member ...]` - Add members with scores
- `ZRANGE <key> <start> <stop> [WITHSCORES]` - Get range by rank
- `ZRANK <key> <member>` - Get member rank
- `ZSCORE <key> <member>` - Get member score
- `ZCARD <key>` - Get sorted set cardinality
- `ZREM <key> <member>` - Remove member

#### Geospatial Operations
- `GEOADD <key> <longitude> <latitude> <member> [...]` - Add geo locations
- `GEOPOS <key> <member> [member ...]` - Get positions of members
- `GEODIST <key> <member1> <member2> [unit]` - Distance between members
- `GEOSEARCH <key> FROMLONLAT <lon> <lat> BYRADIUS <radius> <unit>` - Search within radius

#### Stream Operations
- `XADD <key> <id> <field> <value> [field value ...]` - Add stream entry
- `XRANGE <key> <start> <end>` - Get range of stream entries
- `XREAD [BLOCK milliseconds] STREAMS <key> [key ...] <id> [id ...]` - Read from streams

#### Pub/Sub
- `SUBSCRIBE <channel> [channel ...]` - Subscribe to channels
- `UNSUBSCRIBE <channel> [channel ...]` - Unsubscribe from channels
- `PUBLISH <channel> <message>` - Publish message to channel

#### Transactions
- `MULTI` - Start transaction block
- `EXEC` - Execute transaction
- `DISCARD` - Discard transaction

#### Utility Commands
- `TYPE <key>` - Get key data type
- `KEYS <pattern>` - Find keys matching pattern
- `CONFIG GET <parameter>` - Get configuration parameter
- `INFO [section]` - Get server information
- `WAIT <numreplicas> <timeout>` - Wait for replication

### Persistence

#### RDB (Redis Database) Files
- Load existing RDB files on startup
- Support for string, list, and stream data types
- Expiry handling for keys

#### AOF (Append Only File)
- Log all write commands for durability
- Configurable sync policies (always, everysec, no)
- Automatic replay on server restart
- Human-readable RESP format

### Replication
- Master-Slave replication support
- Full resynchronization via PSYNC
- Command propagation to replicas
- WAIT command for synchronous replication
- Replica acknowledgments

### Advanced Features
- **Transactions**: Atomic command execution with MULTI/EXEC
- **Expiry**: Key expiration with millisecond precision
- **Pub/Sub**: Real-time messaging between clients
- **Geospatial**: Location-based operations with geohash encoding
- **Blocking Operations**: BLPOP, XREAD BLOCK for event-driven patterns

## Usage

### Basic Server Start
```bash
./your_program.sh
```

### With Custom Port
```bash
./your_program.sh --port 6380
```

### As a Replica
```bash
./your_program.sh --replicaof localhost 6379
```

### With RDB Persistence
```bash
./your_program.sh --dir /var/redis --dbfilename dump.rdb
```

### With AOF Persistence
```bash
./your_program.sh --appendonly --appendfilename appendonly.aof --aof-dir /var/redis
```

### Combined Options
```bash
./your_program.sh --port 6380 --appendonly --replicaof localhost 6379
```

## Architecture

### Project Structure
```
app/
├── main.ts                 # Server entry point
├── commands.ts             # Main command orchestrator
├── parser.ts               # RESP protocol parser
├── aof-manager.ts          # AOF persistence handling
├── rdb-parser.ts           # RDB file parsing
├── commands/
│   ├── string-commands.ts  # String operations
│   ├── list-commands.ts    # List operations
│   ├── stream-commands.ts  # Stream operations
│   ├── sortedset-commands.ts # Sorted set operations
│   ├── geo-commands.ts     # Geospatial operations
│   ├── pubsub-commands.ts  # Pub/Sub operations
│   └── replication-manager.ts # Replication handling
└── config/
    └── server-config.ts    # Server configuration
```

### Key Components

#### RESP Protocol
Full implementation of Redis Serialization Protocol (RESP):
- Simple strings: `+OK\r\n`
- Errors: `-ERR message\r\n`
- Integers: `:1000\r\n`
- Bulk strings: `$6\r\nfoobar\r\n`
- Arrays: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`

#### Data Storage
- In-memory key-value store using JavaScript Maps
- Type-safe storage with TypeScript interfaces
- Support for multiple data types (string, list, stream, sorted set, geo)

#### Geospatial Implementation
- Geohash encoding for coordinate storage
- Haversine formula for distance calculations
- Efficient radius searches using sorted sets

## Examples

### Basic Operations
```bash
# String operations
redis-cli SET mykey "Hello"
redis-cli GET mykey

# List operations
redis-cli LPUSH mylist "world" "hello"
redis-cli LRANGE mylist 0 -1

# Sorted sets
redis-cli ZADD leaderboard 100 "Alice" 95 "Bob"
redis-cli ZRANGE leaderboard 0 -1 WITHSCORES

# Geo operations
redis-cli GEOADD cities 2.35 48.86 "Paris" 0.13 51.51 "London"
redis-cli GEODIST cities "Paris" "London" km
```

### Transactions
```bash
redis-cli MULTI
redis-cli SET key1 "value1"
redis-cli SET key2 "value2"
redis-cli EXEC
```

### Pub/Sub
```bash
# Terminal 1 - Subscriber
redis-cli SUBSCRIBE news sports

# Terminal 2 - Publisher
redis-cli PUBLISH news "Breaking news!"
```

## Configuration

### Server Configuration
The server accepts various configuration options through command-line arguments:

- `--port <port>`: Server port (default: 6379)
- `--replicaof <host> <port>`: Configure as replica
- `--dir <directory>`: Data directory for RDB files
- `--dbfilename <filename>`: RDB filename
- `--appendonly`: Enable AOF
- `--appendfilename <filename>`: AOF filename
- `--aof-dir <directory>`: AOF directory

### AOF Sync Policies
- `always`: Sync after every write (safest, slowest)
- `everysec`: Sync every second (balanced)
- `no`: Let OS decide (fastest, least safe)

## Testing

Run the test suite:
```bash
bun test
```

Run specific test:
```bash
bun test --grep "ZADD"
```

## Requirements

- Node.js 18+ or Bun 1.2+
- TypeScript 5.0+

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   bun install
   ```
3. Build and run:
   ```bash
   ./your_program.sh
   ```

## Contributing

This is a CodeCrafters challenge implementation. Feel free to fork and enhance!

## License

MIT

## Acknowledgments

Built as part of the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis) on CodeCrafters.