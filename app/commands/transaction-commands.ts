import type {KeyValueEntry, QueuedCommand} from "../commands";
import {encodeSimpleString, encodeError, encodeArray} from "../parser";
import * as net from "net";

export class TransactionCommands {
  private kvStore: Map<string, KeyValueEntry>;
  private transactionState: Map<string, boolean> = new Map(); // Per-connection transaction state
  private commandQueues: Map<string, QueuedCommand[]> = new Map(); // Per-connection command queues

  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  private getConnectionId(socket: net.Socket): string {
    return `${socket.remoteAddress}:${socket.remotePort}`;
  }

  handleMulti(args: string[], socket: net.Socket): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'multi' command");
    }

    const connectionId = this.getConnectionId(socket);
    this.transactionState.set(connectionId, true);
    this.commandQueues.set(connectionId, []);

    return encodeSimpleString("OK");
  }

  handleExec(args: string[], socket: net.Socket): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'exec' command");
    }

    const connectionId = this.getConnectionId(socket);
    const isInTransaction = this.transactionState.get(connectionId) || false;

    if (!isInTransaction) {
      return encodeError("ERR EXEC without MULTI");
    }

    // Clear transaction state for this connection
    this.transactionState.set(connectionId, false);
    const queuedCommands = this.commandQueues.get(connectionId) || [];
    this.commandQueues.set(connectionId, []);

    // For now, we return an empty array since we'll execute commands later
    return encodeArray([]); // RESP for empty array
  }

  // ---------Helpers for transaction state management---------
  isInTransaction(socket: net.Socket): boolean {
    const connectionId = this.getConnectionId(socket);
    return this.transactionState.get(connectionId) || false;
  }

  queueCommand(command: string, args: string[], socket: net.Socket): string {
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

  getQueuedCommands(socket: net.Socket): QueuedCommand[] {
    const connectionId = this.getConnectionId(socket);
    return this.commandQueues.get(connectionId)?.slice() || []; // Return a copy
  }

  clearQueue(socket: net.Socket): void {
    const connectionId = this.getConnectionId(socket);
    this.transactionState.set(connectionId, false);
    this.commandQueues.set(connectionId, []);
  }
}
