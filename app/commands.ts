import {encodeBulkString, encodeSimpleString, encodeError} from "./parser";
import * as net from "net";
import {StringCommands} from "./commands/string-commands";
import {ListCommands} from "./commands/list-commands";
import {StreamCommands} from "./commands/stream-commands";
import {TransactionCommands} from "./commands/transaction-commands";

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
  private transactionCommands: TransactionCommands;

  constructor() {
    this.kvStore = new Map<string, KeyValueEntry>();
    this.stringCommands = new StringCommands(this.kvStore);
    this.listCommands = new ListCommands(this.kvStore);
    this.streamCommands = new StreamCommands(this.kvStore);
    this.transactionCommands = new TransactionCommands(this.kvStore);
  }

  // ===== COMMAND HANDLERS =====

  async executeCommand(
    command: string,
    args: string[],
    socket: net.Socket
  ): Promise<void> {
    let response: string;

    if (this.shouldQueue(command)) {
      response = this.transactionCommands.queueCommand(command, args);
      socket.write(response);
      return;
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
        response = this.transactionCommands.handleExec(args);
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
        response = this.transactionCommands.handleMulti(args);
        break;
      default:
        response = encodeError(`ERR unknown command '${command}'`);
    }

    socket.write(response);
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

  private shouldQueue(command: string): boolean {
    const controlCommands = ["MULTI", "EXEC", "DISCARD"];
    return (
      this.transactionCommands.isInTransaction() &&
      !controlCommands.includes(command.toUpperCase())
    );
  }

  // For testing or debugging
  getStoreSize(): number {
    return this.kvStore.size;
  }

  clearStore(): void {
    this.kvStore.clear();
  }
}
