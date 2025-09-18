import {
  encodeBulkString,
  encodeSimpleString,
  encodeError,
  encodeArray,
  encodeInteger,
} from "./parser";
import * as net from "net";
import * as path from "path";
import {parseRDBFile} from "./rdb-parser";
import {StringCommands} from "./commands/string-commands";
import {ListCommands} from "./commands/list-commands";
import {StreamCommands} from "./commands/stream-commands";
import {ReplicationManager} from "./commands/replication-manager";
import {PubSubCommands} from "./commands/pubsub-commands";
import {SortedSetCommands} from "./commands/sortedset-commands";
import {GeoCommands} from "./commands/geo-commands";
import {type ServerConfig, DEFAULT_SERVER_CONFIG} from "./config/server-config";

// Enhanced key-value store with expiry support
export interface KeyValueEntry {
  value: any;
  expiry?: number; // Timestamp when key expires (Date.now() + px)
  type?: "string" | "list" | "stream" | "hash" | "set" | "zset"; // Optional type field
}

// Stream entry interface
export interface StreamEntry {
  id: string; // Entry ID
  fields: Map<string, string>; // Field-value pairs
}

// Queue entry for transactions
export interface QueuedCommand {
  command: string;
  args: string[];
}

export class RedisCommands {
  private kvStore: Map<string, KeyValueEntry>;
  private stringCommands: StringCommands;
  private listCommands: ListCommands;
  private streamCommands: StreamCommands;
  private replicationManager: ReplicationManager;
  private pubsubCommands: PubSubCommands;
  private sortedSetCommands: SortedSetCommands;
  private geoCommands: GeoCommands;
  private transactionState: Map<string, boolean> = new Map(); // Per-connection transaction state
  private commandQueues: Map<string, QueuedCommand[]> = new Map(); // Per-connection command queues
  private executingTransaction = false; // Flag to bypass transaction logic during EXEC
  private rdbFilename?: string;
  private rdbDir?: string;
  private subscriptions: Map<string, Set<string>> = new Map(); // Per-connection subscriptions: connectionId -> Set<channelName>

  constructor(
    config: ServerConfig = DEFAULT_SERVER_CONFIG,
    rdbDir?: string,
    rdbFilename?: string
  ) {
    this.kvStore = new Map<string, KeyValueEntry>();
    this.stringCommands = new StringCommands(this.kvStore);
    this.listCommands = new ListCommands(this.kvStore);
    this.streamCommands = new StreamCommands(this.kvStore);
    this.pubsubCommands = new PubSubCommands(this.subscriptions);
    this.sortedSetCommands = new SortedSetCommands(this.kvStore);
    this.geoCommands = new GeoCommands(this.kvStore);
    this.rdbDir = rdbDir;
    this.rdbFilename = rdbFilename;

    // Load RDB file if specified
    if (rdbDir && rdbFilename) {
      this.loadRDBFile(rdbDir, rdbFilename);
    }

    // Pass a command executor to ReplicationManager so replicas can execute commands locally
    const commandExecutor = async (command: string, args: string[]) => {
      // Use a mock socket for replica command execution (no actual network socket needed)
      const mockSocket = {} as net.Socket;
      return this.executeCommand(command, args, mockSocket);
    };
    this.replicationManager = new ReplicationManager(config, commandExecutor);
  }

  // ===== HELPER METHODS =====

  private getConnectionId(socket: net.Socket): string {
    return `${socket.remoteAddress}:${socket.remotePort}`;
  }

  // Check if this socket is a replica connection (should not receive responses)
  isReplicaSocket(socket: net.Socket): boolean {
    return this.replicationManager.isReplicaSocket(socket);
  }

  // ===== COMMAND HANDLERS =====

  async executeCommand(
    command: string,
    args: string[],
    socket: net.Socket
  ): Promise<any> {
    console.log(
      `[DEBUG] Executing command: ${command} ${args.join(
        " "
      )} (isReplica: ${this.replicationManager.isReplica()})`
    );
    let response: string | undefined;

    if (this.shouldQueue(command, socket)) {
      return this.queueCommand(command, args, socket);
    }

    const isInSubscriptionMode =
      this.pubsubCommands.isInSubscriptionMode(socket);
    if (isInSubscriptionMode) {
      const subscriptionResponse =
        this.pubsubCommands.handleSubscriptionsMode(command);
      if (subscriptionResponse) {
        return subscriptionResponse;
      }
    }

    switch (command.toUpperCase()) {
      case "PING":
        response = this.handlePing(args, isInSubscriptionMode);
        break;
      case "ECHO":
        response = this.handleEcho(args);
        break;
      case "SET":
        response = this.stringCommands.handleSet(args);
        break;
      case "GET":
        response = this.stringCommands.handleGet(args);
        break;
      case "INCR":
        response = this.stringCommands.handleIncr(args);
        break;
      case "EXEC":
        response = await this.handleExec(args, socket);
        break;
      case "RPUSH":
        response = this.listCommands.handleRPush(args);
        break;
      case "LRANGE":
        response = this.listCommands.handleLRange(args);
        break;
      case "LPUSH":
        response = this.listCommands.handleLPush(args);
        break;
      case "LLEN":
        response = this.listCommands.handleLLen(args);
        break;
      case "LPOP":
        response = this.listCommands.handleLPop(args);
        break;
      case "BLPOP":
        this.listCommands.handleBLPop(args, socket);
        return; // Don't write response for blocking commands
      case "TYPE":
        response = this.handleType(args);
        break;
      case "XADD":
        response = this.streamCommands.handleXAdd(args);
        break;
      case "XRANGE":
        response = this.streamCommands.handleXRange(args);
        break;
      case "XREAD":
        response = await this.streamCommands.handleXRead(args);
        break;
      case "MULTI":
        response = this.handleMulti(args, socket);
        break;
      case "DISCARD":
        response = this.handleDiscard(args, socket);
        break;
      case "INFO":
        response = this.handleInfo(args);
        break;
      case "REPLCONF":
        response = this.handleReplConf(args);
        break;
      case "PSYNC":
        response = this.handlePsync(args, socket);
        break;
      case "WAIT":
        response = await this.handleWait(args);
        break;
      case "CONFIG":
        response = this.handleConfig(args);
        break;
      case "KEYS":
        response = this.handleKeys(args);
        break;
      case "SUBSCRIBE":
        response = this.pubsubCommands.handleSubscribe(args, socket);
        break;
      case "PUBLISH":
        response = this.pubsubCommands.handlePublish(args);
        break;
      case "UNSUBSCRIBE":
        response = this.pubsubCommands.handleUnsubscribe(args, socket);
        break;
      case "ZADD":
        response = this.sortedSetCommands.handleZAdd(args);
        break;
      case "ZRANK":
        response = this.sortedSetCommands.handleZRank(args);
        break;
      case "ZRANGE":
        response = this.sortedSetCommands.handleZRange(args);
        break;
      case "ZCARD":
        response = this.sortedSetCommands.handleZCard(args);
        break;
      case "ZSCORE":
        response = this.sortedSetCommands.handleZScore(args);
        break;
      case "ZREM":
        response = this.sortedSetCommands.handleZRem(args);
        break;
      case "GEOADD":
        response = this.geoCommands.handleGeoAdd(args);
        break;
      case "GEOPOS":
        response = this.geoCommands.handleGeoPos(args);
        break;
      case "GEODIST":
        response = this.geoCommands.handleGeoDist(args);
        break;
      default:
        response = encodeError(`ERR unknown command '${command}'`);
    }

    // Propagate write commands to replicas (only if this is the master ,not during transaction execution and not an error response(-) and not a replica itself because replicas don't propagate)
    if (
      response &&
      !this.executingTransaction &&
      this.isWriteCommand(command) &&
      !response.startsWith("-") &&
      !this.replicationManager.isReplica()
    ) {
      console.log(
        `[MASTER] Propagating command to replicas: ${command} ${args.join(" ")}`
      );
      this.replicationManager.propagateCommand(command, args);
    }

    // If this is a replica processing a write command, don't return responses (process commands silently)
    if (this.replicationManager.isReplica() && this.isWriteCommand(command)) {
      return undefined;
    }

    return response;
  }

  private handlePing(args: string[], isInSubscriptionMode: boolean): string {
    if (isInSubscriptionMode) {
      return encodeArray(["pong", ""]);
    }
    return encodeSimpleString("PONG");
  }

  private handleEcho(args: string[]): string {
    if (args.length !== 1) {
      return encodeError("ERR wrong number of arguments for 'echo' command");
    }
    return encodeBulkString(args[0]);
  }

  private handleType(args: string[]): string {
    if (args.length !== 1) {
      return encodeError("ERR wrong number of arguments for 'type' command");
    }
    const key = args[0];
    const entry = this.kvStore.get(key);
    if (!entry) {
      return encodeSimpleString("none");
    }
    if (entry.type === "stream") {
      return encodeSimpleString("stream");
    }

    return encodeSimpleString("string");
  }

  private handleMulti(args: string[], socket: net.Socket): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'multi' command");
    }

    const connectionId = this.getConnectionId(socket);
    this.transactionState.set(connectionId, true);
    this.commandQueues.set(connectionId, []);

    return encodeSimpleString("OK");
  }

  private async handleExec(
    args: string[],
    socket: net.Socket
  ): Promise<string> {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'exec' command");
    }

    const connectionId = this.getConnectionId(socket);
    const isInTransaction = this.transactionState.get(connectionId) || false;

    if (!isInTransaction) {
      return encodeError("ERR EXEC without MULTI");
    }

    // Get queued commands and clear transaction state
    const queuedCommands = this.commandQueues.get(connectionId) || [];
    this.transactionState.set(connectionId, false);
    this.commandQueues.set(connectionId, []);

    // Set flag to bypass transaction logic
    this.executingTransaction = true;

    // Execute each queued command using existing executeCommand logic
    const results: string[] = [];
    // No more mock socket - just call executeCommand and collect responses
    for (const {command, args} of queuedCommands) {
      const result = await this.executeCommand(command, args, socket);
      results.push(result);
    }

    // Reset flag
    this.executingTransaction = false;

    // Return array of results
    return this.encodeResultArray(results);
  }

  private encodeResultArray(results: string[]): string {
    if (results.length === 0) {
      return "*0\r\n"; // Empty array
    }

    let response = `*${results.length}\r\n`;
    for (const result of results) {
      response += result;
    }
    return response;
  }

  private handleDiscard(args: string[], socket: net.Socket): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'discard' command");
    }
    const connectionId = this.getConnectionId(socket);
    const isInTransaction = this.transactionState.get(connectionId) || false;
    if (!isInTransaction) {
      return encodeError("ERR DISCARD without MULTI");
    }
    this.transactionState.set(connectionId, false);
    this.commandQueues.set(connectionId, []);
    return encodeSimpleString("OK");
  }

  private handleInfo(args: string[]): string {
    const isReplicationInfo =
      args.length === 1 && args[0].toLowerCase() === "replication";
    if (isReplicationInfo) {
      return this.buildReplicationInfo();
    }
    return encodeBulkString("");
  }

  private handleReplConf(args: string[]): string {
    // Handle GETACK command - format: REPLCONF GETACK *
    console.log(`[DEBUG] Handling REPLCONF with args: ${args.join(" ")}`);
    if (
      args.length === 2 &&
      args[0].toLowerCase() === "getack" &&
      args[1] === "*"
    ) {
      // Respond with REPLCONF ACK <offset>
      // For now, hardcode offset to 0 as per requirements
      const offset = 0;
      return encodeArray(["REPLCONF", "ACK", offset.toString()]);
    }

    // Handle other REPLCONF commands (listening-port, capa, etc.)
    // Basic validation for key-value pairs
    if (args.length < 2 || args.length % 2 !== 0) {
      return encodeError(
        "ERR wrong number of arguments for 'replconf' command"
      );
    }

    for (let i = 0; i < args.length; i += 2) {
      const option = args[i].toLowerCase();
      const value = args[i + 1];
      if (option === "listening-port") {
        const port = parseInt(value, 10);
        if (isNaN(port) || port <= 0 || port > 65535) {
          return encodeError("ERR invalid port number");
        }
        this.replicationManager.setListeningPort(port);
      } else if (option === "ip-address") {
        this.replicationManager.setIpAddress(value);
      } else if (option === "capa") {
        // Handle capabilities if needed
        this.replicationManager.addCapabilities(value.split(" "));
      } else if (option === "ack") {
        // Handle ACK responses from replicas
        console.log(`[DEBUG] Received ACK from replica with offset: ${value}`);
        // Update ACK count in replication manager
        this.replicationManager.handleAck();
      } else {
        return encodeError(`ERR unknown REPLCONF option '${option}'`);
      }
    }
    return encodeSimpleString("OK");
  }

  private handlePsync(args: string[], socket?: net.Socket): string | undefined {
    if (args.length !== 2) {
      return encodeError("ERR wrong number of arguments for 'psync' command");
    }

    const replicaId = args[0];
    const offset = args[1];

    if (replicaId === "?" && offset === "-1") {
      const replicaInfo = this.replicationManager.getReplicationInfo();
      const masterReplId = replicaInfo.master_replid || "unknown";
      const masterReplOffset = replicaInfo.master_repl_offset || 0;

      // Send FULLRESYNC response first
      const fullresyncResponse = encodeSimpleString(
        `FULLRESYNC ${masterReplId} ${masterReplOffset}`
      );

      // Send empty RDB file after FULLRESYNC response
      if (socket) {
        // Send FULLRESYNC first
        socket.write(fullresyncResponse);

        // Then send empty RDB file
        this.sendEmptyRDBFile(socket);

        // Register this connection as a replica
        console.log(
          `[MASTER] <<<< PSYNC: Adding replica connection from ${socket.remoteAddress}:${socket.remotePort}`
        );
        this.replicationManager.addReplicaConnection(socket);

        return undefined; // Don't return response since we already wrote to socket
      }

      return fullresyncResponse;
    }
    return encodeError("ERR PSYNC not fully implemented");
  }

  private handleConfig(args: string[]): string {
    if (args.length < 1) {
      return encodeError("ERR wrong number of arguments for 'config' command");
    }
    const subcommand = args[0].toUpperCase();
    if (subcommand === "GET") {
      if (args.length !== 2) {
        return encodeError("ERR wrong number of arguments for 'CONFIG GET'");
      }
      const pattern = args[1];
      // For simplicity, only support "dbfilename" and "dir"
      const configEntries: string[] = [];
      if (pattern === "*" || pattern === "dbfilename") {
        configEntries.push("dbfilename");
        configEntries.push(this.rdbFilename || "dump.rdb");
      }
      if (pattern === "*" || pattern === "dir") {
        configEntries.push("dir");
        configEntries.push(this.rdbDir || ".");
      }
      return encodeArray(configEntries);
    }
    return encodeError(`ERR unknown CONFIG subcommand '${subcommand}'`);
  }

  private handleKeys(args: string[]): string {
    if (args.length !== 1) {
      return encodeError("ERR wrong number of arguments for 'keys' command");
    }

    const pattern = args[0];
    const matchingKeys: string[] = [];

    for (const [key] of this.kvStore) {
      if (this.matchesPattern(key, pattern)) {
        matchingKeys.push(key);
      }
    }

    return encodeArray(matchingKeys);
  }

  private matchesPattern(key: string, pattern: string): boolean {
    // Convert Redis glob pattern to regex
    // * matches any sequence of characters
    // ? matches any single character
    // [abc] matches any character in the set
    // [a-z] matches any character in the range
    // \x matches x literally (escape special characters)

    if (pattern === "*") {
      return true; // Match everything
    }

    // Escape regex special characters except our glob characters
    let regexPattern = pattern
      .replace(/[.+^${}()|[\]\\]/g, "\\$&") // Escape regex specials
      .replace(/\\\*/g, ".*") // * becomes .*
      .replace(/\\\?/g, "."); // ? becomes .

    // Add anchors to match entire string
    regexPattern = "^" + regexPattern + "$";

    try {
      const regex = new RegExp(regexPattern);
      return regex.test(key);
    } catch (error) {
      // If pattern is invalid, return false
      return false;
    }
  }

  private sendEmptyRDBFile(socket: net.Socket): void {
    // Empty RDB file hex representation
    const emptyRDBHex =
      "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    // Convert hex to binary
    const rdbData = Buffer.from(emptyRDBHex, "hex");

    // Send RDB file in the format: $<length>\r\n<binary_contents>
    const rdbResponse = `$${rdbData.length}\r\n`;
    socket.write(rdbResponse);
    socket.write(rdbData);
  }

  private buildReplicationInfo(): string {
    const fields = this.replicationManager.getReplicationInfo();

    const info = Object.entries(fields)
      .map(([key, value]) => `${key}:${value}`)
      .join("\r\n");

    return encodeBulkString(info);
  }

  // ========== REPLICATION HANDSHAKE ==========

  async initiateReplicationHandshake(): Promise<void> {
    return this.replicationManager.initiateHandshake();
  }

  getReplicationManager(): ReplicationManager {
    return this.replicationManager;
  }

  // ========== REPLICATION HELPERS ==========

  private isWriteCommand(command: string): boolean {
    const writeCommands = [
      "SET",
      "DEL",
      "INCR",
      "DECR",
      "INCRBY",
      "DECRBY",
      "RPUSH",
      "LPUSH",
      "LPOP",
      "RPOP",
      "LREM",
      "LSET",
      "LTRIM",
      "XADD",
      "XDEL",
      "XTRIM",
      "HSET",
      "HDEL",
      "HINCRBY",
      "HINCRBYFLOAT",
      "SADD",
      "SREM",
      "SPOP",
      "SMOVE",
      "ZADD",
      "ZREM",
      "ZINCRBY",
      "GEOADD",
    ];
    return writeCommands.includes(command.toUpperCase());
  }

  // ========== TRANSACTION HELPERS ==========
  private shouldQueue(command: string, socket: net.Socket): boolean {
    const controlCommands = ["MULTI", "EXEC", "DISCARD"];
    return (
      !this.executingTransaction &&
      this.isInTransaction(socket) &&
      !controlCommands.includes(command.toUpperCase())
    );
  }

  private isInTransaction(socket: net.Socket): boolean {
    const connectionId = this.getConnectionId(socket);
    return this.transactionState.get(connectionId) || false;
  }

  private queueCommand(
    command: string,
    args: string[],
    socket: net.Socket
  ): string {
    const connectionId = this.getConnectionId(socket);
    const isInTransaction = this.transactionState.get(connectionId) || false;

    if (!isInTransaction) {
      return encodeError("ERR command not in transaction");
    }

    const queue = this.commandQueues.get(connectionId) || [];
    queue.push({command, args});
    this.commandQueues.set(connectionId, queue);

    return encodeSimpleString("QUEUED");
  }

  // ========== WAIT COMMAND ==========
  private async handleWait(args: string[]): Promise<string> {
    if (args.length !== 2) {
      return encodeError("ERR wrong number of arguments for 'wait' command");
    }

    const numReplicas = parseInt(args[0], 10);
    const timeout = parseInt(args[1], 10);

    if (isNaN(numReplicas) || isNaN(timeout)) {
      return encodeError("ERR invalid arguments for 'wait' command");
    }

    if (numReplicas < 0) {
      return encodeError("ERR negative number of replicas");
    }

    console.log(
      `[DEBUG] WAIT command: requesting ${numReplicas} replicas, timeout ${timeout}ms`
    );

    // If no replicas are needed, return immediately
    if (numReplicas === 0) {
      return encodeInteger(0);
    }

    // Get the current number of connected replicas
    const connectedReplicas = this.replicationManager.getReplicaCount();
    console.log(
      `[DEBUG] Currently have ${connectedReplicas} connected replicas`
    );

    // If no replicas are connected, return 0 immediately
    if (connectedReplicas === 0) {
      return encodeInteger(0);
    }

    // Delegate to replication manager to handle the actual WAIT logic
    const acknowledgedReplicas = await this.replicationManager.waitForReplicas(
      numReplicas,
      timeout
    );

    console.log(
      `[DEBUG] WAIT completed: ${acknowledgedReplicas} replicas acknowledged`
    );
    return encodeInteger(acknowledgedReplicas);
  }

  // ========== RDB LOADING ==========
  private loadRDBFile(dir: string, filename: string): void {
    const filePath = path.join(dir, filename);
    const rdbKeys = parseRDBFile(filePath);

    for (const rdbKey of rdbKeys) {
      const now = Date.now();

      // Skip expired keys
      if (rdbKey.expiry && rdbKey.expiry <= now) {
        continue;
      }

      this.kvStore.set(rdbKey.key, {
        value: rdbKey.value,
        expiry: rdbKey.expiry,
        type: rdbKey.type,
      });
    }

    console.log(`Loaded ${rdbKeys.length} keys from RDB file`);
  }

  // Clean up connection when client disconnects
  cleanupConnection(socket: net.Socket): void {
    const connectionId = this.getConnectionId(socket);
    this.pubsubCommands.cleanupConnection(socket);
    this.transactionState.delete(connectionId);
    this.commandQueues.delete(connectionId);
    console.log(`[CLEANUP] Removed connection ${connectionId}`);
  }

  // For testing or debugging
  getStoreSize(): number {
    return this.kvStore.size;
  }

  clearStore(): void {
    this.kvStore.clear();
  }
}
