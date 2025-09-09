import {
  encodeBulkString,
  encodeSimpleString,
  encodeError,
  encodeInteger,
  encodeArray,
} from "./parser";
import * as net from "net";
import {StringCommands} from "./commands/string-commands";
import {ListCommands} from "./commands/list-commands";
import {StreamCommands} from "./commands/stream-commands";

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

export class RedisCommands {
  private kvStore: Map<string, KeyValueEntry>;
  private stringCommands: StringCommands;
  private listCommands: ListCommands;
  private streamCommands: StreamCommands;

  constructor() {
    this.kvStore = new Map<string, KeyValueEntry>();
    this.stringCommands = new StringCommands(this.kvStore);
    this.listCommands = new ListCommands(this.kvStore);
    this.streamCommands = new StreamCommands(this.kvStore);
  }

  // ===== COMMAND HANDLERS =====

  executeCommand(command: string, args: string[], socket: net.Socket): void {
    let response: string;

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

  // For testing or debugging
  getStoreSize(): number {
    return this.kvStore.size;
  }

  clearStore(): void {
    this.kvStore.clear();
  }
}
