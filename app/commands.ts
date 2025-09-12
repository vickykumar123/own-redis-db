import {encodeBulkString, encodeSimpleString, encodeError} from "./parser";
import * as net from "net";
import {StringCommands} from "./commands/string-commands";
import {ListCommands} from "./commands/list-commands";
import {StreamCommands} from "./commands/stream-commands";
import {ReplicationManager} from "./commands/replication-manager";
import {type ServerConfig, DEFAULT_SERVER_CONFIG} from "./config/server-config";

// Enhanced key-value store with expiry support
export interface KeyValueEntry {
  value: any;
  expiry?: number; // Timestamp when key expires (Date.now() + px)
  type?: "string" | "list" | "stream"; // Optional type field
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
  private transactionState: Map<string, boolean> = new Map(); // Per-connection transaction state
  private commandQueues: Map<string, QueuedCommand[]> = new Map(); // Per-connection command queues
  private executingTransaction = false; // Flag to bypass transaction logic during EXEC

  constructor(config: ServerConfig = DEFAULT_SERVER_CONFIG) {
    this.kvStore = new Map<string, KeyValueEntry>();
    this.stringCommands = new StringCommands(this.kvStore);
    this.listCommands = new ListCommands(this.kvStore);
    this.streamCommands = new StreamCommands(this.kvStore);
    this.replicationManager = new ReplicationManager(config);
  }

  // ===== HELPER METHODS =====

  private getConnectionId(socket: net.Socket): string {
    return `${socket.remoteAddress}:${socket.remotePort}`;
  }

  // ===== COMMAND HANDLERS =====

  async executeCommand(
    command: string,
    args: string[],
    socket: net.Socket
  ): Promise<any> {
    let response: string;

    if (this.shouldQueue(command, socket)) {
      return this.queueCommand(command, args, socket);
    }

    switch (command.toUpperCase()) {
      case "PING":
        response = this.handlePing(args);
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
      default:
        response = encodeError(`ERR unknown command '${command}'`);
    }

    return response;
  }

  private handlePing(args: string[]): string {
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

  // For testing or debugging
  getStoreSize(): number {
    return this.kvStore.size;
  }

  clearStore(): void {
    this.kvStore.clear();
  }
}
