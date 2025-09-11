import type {KeyValueEntry, QueuedCommand} from "../commands";
import {encodeSimpleString, encodeError, encodeArray} from "../parser";

export class TransactionCommands {
  private kvStore: Map<string, KeyValueEntry>;
  private isMulti: boolean = false;
  private commandQueue: QueuedCommand[] = [];
  
  constructor(kvStore: Map<string, KeyValueEntry>) {
    this.kvStore = kvStore;
  }

  handleMulti(args: string[]): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'multi' command");
    }
    this.isMulti = true;
    return encodeSimpleString("OK");
  }

  handleExec(args: string[]): string {
    if (args.length !== 0) {
      return encodeError("ERR wrong number of arguments for 'exec' command");
    }
    if (!this.isMulti) {
      return encodeError("ERR EXEC without MULTI");
    }
    this.isMulti = false;
    // For now, we return an empty array since we'll execute commands later
    const queuedCommands = this.commandQueue.slice(); // Copy the queue
    this.commandQueue = []; // Clear the queue
    return encodeArray([]); // RESP for empty array
  }

  isInTransaction(): boolean {
    return this.isMulti;
  }

  queueCommand(command: string, args: string[]): string {
    if (!this.isMulti) {
      return encodeError("ERR command not in transaction");
    }
    
    this.commandQueue.push({ command, args });
    return encodeSimpleString("QUEUED");
  }

  getQueuedCommands(): QueuedCommand[] {
    return this.commandQueue.slice(); // Return a copy
  }

  clearQueue(): void {
    this.commandQueue = [];
    this.isMulti = false;
  }
}
