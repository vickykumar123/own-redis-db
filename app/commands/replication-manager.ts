import * as net from "net";
import {type ServerConfig} from "../config/server-config";
import {encodeRESPCommand, parseRESPCommand, encodeArray, encodeSimpleString} from "../parser";

interface ReplicaInfo {
  socket: net.Socket;
  port: number;
  host: string;
}

export class ReplicationManager {
  private config: ServerConfig;
  // Replica-specific properties
  private masterConnection: net.Socket | null = null;
  private buffer: Buffer = Buffer.alloc(0);
  private replicationOffset: number = 0; // Track bytes processed from master
  // Master-specific properties  
  private replicaConnections: ReplicaInfo[] = [];

  constructor(config: ServerConfig) {
    this.config = config;
  }

  // ========== PUBLIC METHODS (ROLE-AGNOSTIC) ==========

  isReplica(): boolean {
    return this.config.role === "slave";
  }

  getReplicationInfo(): Record<string, any> {
    if (this.config.role === "master") {
      return this.getMasterReplicationInfo();
    } else {
      return this.getReplicaReplicationInfo();
    }
  }

  // ========== SHARED HELPER METHODS ==========

  private getMasterReplicationInfo(): Record<string, any> {
    return {
      role: "master",
      connected_slaves: this.getConnectedSlaves().length,
      master_replid: this.generateReplicationId(),
      master_repl_offset: this.config.replicationOffset || 0,
    };
  }

  private getReplicaReplicationInfo(): Record<string, any> {
    const fields: Record<string, any> = {
      role: this.config.role,
    };

    if (this.config.role === "slave") {
      if (this.config.masterHost) fields.master_host = this.config.masterHost;
      if (this.config.masterPort) fields.master_port = this.config.masterPort;
      // Use the tracked replication offset
      fields.master_repl_offset = this.replicationOffset;
    }

    return fields;
  }

  // ========== REPLICA METHODS ==========

  async initiateHandshake(): Promise<void> {
    if (
      this.config.role !== "slave" ||
      !this.config.masterHost ||
      !this.config.masterPort
    ) {
      return; // Only replicas initiate handshake
    }

    try {
      console.log(
        `Starting replication handshake with master ${this.config.masterHost}:${this.config.masterPort}`
      );

      // Stage 1: Connect and send PING
      this.masterConnection = await this.connectToMaster(
        this.config.masterHost,
        this.config.masterPort
      );
      await this.sendPingToMaster();

      //Stage 2: Send REPLCONF commands
      await this.sendReplconfCommands();

      // Stage 3: Send PSYNC command and handle RDB properly (this includes initial buffer processing)
      await this.sendPsyncCommand();

      // Stage 4: Start listening for propagated commands from master (after RDB is processed)
      this.setupPropagationListener();

      console.log("Replication handshake completed successfully");
    } catch (error) {
      console.error("Replication handshake failed:", error);
      this.cleanup();
    }
  }

  getMasterConnection(): net.Socket | null {
    return this.masterConnection;
  }

  // ========== REPLICA PRIVATE METHODS ==========

  private async connectToMaster(
    host: string,
    port: number
  ): Promise<net.Socket> {
    return new Promise((resolve, reject) => {
      const socket = new net.Socket();

      socket.connect(port, host, () => {
        console.log(`Connected to master at ${host}:${port}`);
        resolve(socket);
      });

      socket.on("error", (error) => {
        console.error(`Failed to connect to master: ${error.message}`);
        reject(error);
      });

      socket.on("close", () => {
        console.log("Connection to master closed");
        this.masterConnection = null;
      });

      // Set timeout for connection
      socket.setTimeout(5000, () => {
        socket.destroy();
        reject(new Error("Connection to master timed out"));
      });
    });
  }

  private async sendCommandToMaster(
    command: string,
    args: string[]
  ): Promise<string> {
    if (!this.masterConnection) {
      throw new Error("No connection to master");
    }

    return new Promise((resolve, reject) => {
      // Use RESP encoding logic
      const respCommand = encodeRESPCommand(command, args);

      const onData = (data: Buffer) => {
        const response = data.toString();
        if (this.masterConnection) {
          this.masterConnection.off("data", onData);
        }
        resolve(response);
      };

      if (this.masterConnection) {
        this.masterConnection.on("data", onData);
        this.masterConnection.write(respCommand);
      } else {
        reject(new Error("No connection to master"));
      }

      setTimeout(() => {
        if (this.masterConnection) {
          this.masterConnection.off("data", onData);
        }
        reject(new Error(`${command} to master timed out`));
      }, 3000);
    });
  }

  private async sendPingToMaster(): Promise<void> {
    console.log("Sending PING to master");
    const response = await this.sendCommandToMaster("PING", []);

    if (response !== "+PONG\r\n") {
      throw new Error(`Unexpected PING response: ${response}`);
    }

    console.log("PING handshake successful");
  }

  private async sendReplconfCommands(): Promise<void> {
    if (!this.masterConnection) {
      throw new Error("No connection to master");
    }

    console.log("Sending REPLCONF commands to master");

    // Send listening port (use configured port or default)
    const listeningPort = this.config.port || 6379;
    console.log(`Sending REPLCONF listening-port ${listeningPort}`);
    const portResponse = await this.sendCommandToMaster("REPLCONF", [
      "listening-port",
      listeningPort.toString(),
    ]);
    if (portResponse !== "+OK\r\n") {
      throw new Error(
        `Unexpected REPLCONF listening-port response: ${portResponse}`
      );
    }

    // Send capabilities
    console.log("Sending REPLCONF capa psync2");
    const capaResponse = await this.sendCommandToMaster("REPLCONF", [
      "capa",
      "psync2",
    ]);
    if (capaResponse !== "+OK\r\n") {
      throw new Error(`Unexpected REPLCONF capa response: ${capaResponse}`);
    }

    console.log("REPLCONF commands sent successfully");
  }
  private async sendPsyncCommand(): Promise<void> {
    if (!this.masterConnection) {
      throw new Error("No connection to master");
    }
    console.log("Sending PSYNC command to master");

    return new Promise((resolve, reject) => {
      let handshakeComplete = false;
      
      // Set up special handler for PSYNC response and RDB file
      const psyncHandler = (data: Buffer) => {
        if (handshakeComplete) return; // Ignore if already processed
        
        console.log(`[DEBUG] PSYNC response received: ${data.length} bytes`);
        
        // Append to buffer for processing
        this.buffer = Buffer.concat([this.buffer, data]);
        
        const bufferStr = this.buffer.toString();
        console.log(`[DEBUG] Buffer string:`, JSON.stringify(bufferStr.substring(0, 100)));
        
        // Check for FULLRESYNC response and process RDB
        if (bufferStr.includes("FULLRESYNC")) {
          console.log("PSYNC FULLRESYNC response detected");
          
          // Find the end of the FULLRESYNC line
          const fullresyncEndIndex = this.buffer.indexOf("\r\n");
          if (fullresyncEndIndex === -1) return; // Wait for complete line
          
          // Skip past the FULLRESYNC line
          this.buffer = this.buffer.subarray(fullresyncEndIndex + 2);
          
          // Process RDB file if present
          if (this.buffer.length > 0 && this.buffer[0] === 36) { // '$'
            const rdbLengthEndIndex = this.buffer.indexOf("\r\n");
            if (rdbLengthEndIndex === -1) return; // Wait for complete RDB length line
            
            const rdbLengthStr = this.buffer.subarray(1, rdbLengthEndIndex).toString();
            const rdbLength = parseInt(rdbLengthStr, 10);
            console.log(`[DEBUG] RDB file length: ${rdbLength}`);
            
            // Check if we have the complete RDB file
            const rdbDataStart = rdbLengthEndIndex + 2;
            const rdbDataEnd = rdbDataStart + rdbLength;
            
            if (this.buffer.length < rdbDataEnd) {
              console.log(`[DEBUG] Waiting for complete RDB file`);
              return; // Wait for complete RDB file
            }
            
            // Skip past the RDB file data
            this.buffer = this.buffer.subarray(rdbDataEnd);
            console.log(`[DEBUG] RDB file processed, remaining buffer: ${this.buffer.length} bytes`);
            console.log(`[DEBUG] Remaining buffer hex:`, this.buffer.toString('hex'));
          }
          
          // Mark handshake as complete and remove this handler
          handshakeComplete = true;
          if (this.masterConnection) {
            this.masterConnection.off('data', psyncHandler);
          }
          console.log("PSYNC handshake completed");
          
          // Process any remaining commands in the buffer immediately
          if (this.buffer.length > 0) {
            console.log("[DEBUG] Processing remaining buffer after PSYNC");
            this.handleBuffer();
          }
          
          resolve();
        }
      };
      
      // Attach the PSYNC handler
      if (this.masterConnection) {
        this.masterConnection.on('data', psyncHandler);
        
        // Send PSYNC command
        const respCommand = encodeRESPCommand("PSYNC", ["?", "-1"]);
        this.masterConnection.write(respCommand);
      } else {
        reject(new Error("No connection to master"));
        return;
      }
      
      // Set timeout for PSYNC response
      setTimeout(() => {
        if (this.masterConnection && !handshakeComplete) {
          this.masterConnection.off('data', psyncHandler);
        }
        if (!handshakeComplete) {
          reject(new Error("PSYNC command timed out"));
        }
      }, 5000);
    });
  }

  private cleanup(): void {
    if (this.masterConnection) {
      this.masterConnection.destroy();
      this.masterConnection = null;
    }
  }


  // ========== SHARED CONFIGURATION METHODS ==========

  private generateReplicationId(): string {
    return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  }

  setListeningPort(port: number): void {
    this.config.port = port;
  }

  setIpAddress(ipAddress: string): void {
    this.config.ipAddress = ipAddress;
  }

  addCapabilities(capabilities: string[]): void {
    if (!this.config.capabilities) {
      this.config.capabilities = [];
    }
    this.config.capabilities.push(...capabilities);
  }

  private getConnectedSlaves(): any[] {
    // Return the replica info objects
    return this.replicaConnections.map((r) => ({
      host: r.host,
      port: r.port,
    }));
  }

  // ========== MASTER METHODS ==========

  addReplicaConnection(socket: net.Socket): void {
    // Check if socket is already tracked to prevent duplicates
    const existingReplica = this.replicaConnections.find(
      (r) => r.socket === socket
    );
    if (existingReplica) {
      return; // Already tracking this socket
    }

    // Get replica info from the socket or use defaults
    const host = socket.remoteAddress || "127.0.0.1";
    const port = this.config.port || 6380; // Use the port from REPLCONF listening-port

    const replicaInfo: ReplicaInfo = {
      socket,
      host,
      port,
    };

    this.replicaConnections.push(replicaInfo);

    // Clean up when replica disconnects
    socket.on("close", () => {
      this.removeReplicaConnection(socket);
    });

    socket.on("error", () => {
      this.removeReplicaConnection(socket);
    });
  }

  private removeReplicaConnection(socket: net.Socket): void {
    const index = this.replicaConnections.findIndex((r) => r.socket === socket);
    if (index > -1) {
      this.replicaConnections.splice(index, 1);
    }
  }

  propagateCommand(command: string, args: string[]): void {
    // For simplicity, we'll connect to replicas as clients
    // In a real implementation, this would use the replication connection
    // but for the test, connecting as a client works fine

    // Since we don't have replica addresses stored, we'll use the replication connection approach
    if (this.replicaConnections.length === 0) {
      return; // No replicas to propagate to
    }

    const respCommand = encodeRESPCommand(command, args);
    console.log(
      `[DEBUG] Propagating command to ${
        this.replicaConnections.length
      } replicas: ${command} ${args.join(" ")}`
    );

    // Send to all connected replicas over their replication connections
    for (const replicaInfo of this.replicaConnections) {
      try {
        console.log(`[DEBUG] Sending command to replica:`, respCommand);
        replicaInfo.socket.write(respCommand);
      } catch (error) {
        console.error("Failed to propagate command to replica:", error);
        // The replica will be removed on socket close/error event
      }
    }
  }

  getReplicaCount(): number {
    return this.replicaConnections.length;
  }

  // ========== WAIT COMMAND IMPLEMENTATION ==========
  async waitForReplicas(numReplicas: number, timeoutMs: number): Promise<number> {
    console.log(`[DEBUG] waitForReplicas: need ${numReplicas}, timeout ${timeoutMs}ms`);
    
    const startTime = Date.now();
    const connectedReplicas = this.replicaConnections.length;
    
    // If we don't have enough replicas, return what we have
    if (connectedReplicas === 0) {
      return 0;
    }
    
    // Send GETACK to all replicas using the propagation mechanism
    console.log(`[DEBUG] Sending GETACK to ${connectedReplicas} replicas`);
    this.propagateCommand("REPLCONF", ["GETACK", "*"]);
    
    // For now, return the number of connected replicas immediately
    // TODO: Implement proper ACK tracking
    console.log(`[DEBUG] Returning ${Math.min(connectedReplicas, numReplicas)} immediately`);
    return Math.min(connectedReplicas, numReplicas);
  }

  handlePsync(args: string[], socket?: net.Socket): string | undefined {
    if (args.length !== 2) {
      return encodeSimpleString("ERR wrong number of arguments for 'psync' command");
    }

    const replicaId = args[0];
    const offset = args[1];

    if (replicaId === "?" && offset === "-1") {
      const replicaInfo = this.getMasterReplicationInfo();
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
        this.addReplicaConnection(socket);

        return undefined; // Don't return response since we already wrote to socket
      }

      return fullresyncResponse;
    }
    return encodeSimpleString("ERR PSYNC not fully implemented");
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

  private setupPropagationListener(): void {
    if (!this.masterConnection) {
      console.error(
        "[DEBUG] Cannot setup propagation listener: no master connection"
      );
      return;
    }

    console.log(
      "[DEBUG] Setting up propagation listener on replication connection"
    );
    console.log(
      "[DEBUG] Master connection state:",
      this.masterConnection.readyState
    );

    // Handle data received from master (propagated commands)
    this.masterConnection.on("data", (data: Buffer) => {
      console.log(
        `[DEBUG] Received propagated data from master: ${data.length} bytes`
      );
      console.log(`[DEBUG] Raw data hex:`, data.toString("hex"));
      console.log(`[DEBUG] Data as string:`, JSON.stringify(data.toString()));

      // Append new data to buffer
      this.buffer = Buffer.concat([this.buffer, data]);

      this.handleBuffer();
    });
  }

  private handleBuffer(): void {
    // First, check if we need to skip RDB file data
    if (this.buffer.length > 0 && this.buffer[0] === 36) { // '$' - RDB file marker
      console.log("[DEBUG] Detected RDB file in buffer, parsing...");
      
      const rdbLengthEndIndex = this.buffer.indexOf("\r\n");
      if (rdbLengthEndIndex === -1) {
        console.log("[DEBUG] Waiting for complete RDB length line");
        return; // Wait for complete RDB length line
      }
      
      const rdbLengthStr = this.buffer.subarray(1, rdbLengthEndIndex).toString();
      const rdbLength = parseInt(rdbLengthStr, 10);
      console.log(`[DEBUG] RDB file length: ${rdbLength}`);
      
      // Check if we have the complete RDB file
      const rdbDataStart = rdbLengthEndIndex + 2;
      const rdbDataEnd = rdbDataStart + rdbLength;
      
      if (this.buffer.length < rdbDataEnd) {
        console.log(`[DEBUG] Waiting for complete RDB file: have ${this.buffer.length}, need ${rdbDataEnd}`);
        return; // Wait for complete RDB file
      }
      
      // Skip past the RDB file data
      this.buffer = this.buffer.subarray(rdbDataEnd);
      console.log(`[DEBUG] RDB file skipped, remaining buffer: ${this.buffer.length} bytes`);
      console.log(`[DEBUG] Remaining buffer hex:`, this.buffer.toString('hex'));
    }

    // Process all complete commands in the buffer
    while (this.buffer.length > 0) {
      try {
        // Try to parse a complete RESP command from the buffer
        const parsedCommand = parseRESPCommand(this.buffer);

        if (!parsedCommand) {
          // If we can't parse a command, wait for more data
          console.log(
            "[DEBUG] Incomplete command in buffer, waiting for more data"
          );
          break;
        }

        const {command, args} = parsedCommand;
        console.log(`[DEBUG] Parsed command: ${command} ${args.join(" ")}`);

        // Calculate how many bytes this command consumed to update buffer
        const commandBytes = this.calculateCommandBytes(command, args);
        this.buffer = this.buffer.subarray(commandBytes);

        // Handle REPLCONF GETACK command directly
        if (
          command.toUpperCase() === "REPLCONF" &&
          args.length >= 2 &&
          args[0].toUpperCase() === "GETACK"
        ) {
          console.log(
            "[DEBUG] *** DETECTED REPLCONF GETACK - Responding directly ***"
          );
          // Respond with current offset (BEFORE processing this GETACK)
          const currentOffset = this.replicationOffset;
          const ackResponse = encodeArray(["REPLCONF", "ACK", currentOffset.toString()]);
          console.log(
            `[DEBUG] Sending ACK response with offset ${currentOffset}:`,
            JSON.stringify(ackResponse)
          );
          if (this.masterConnection) {
            this.masterConnection.write(ackResponse);
          }
          console.log("[DEBUG] *** ACK response sent to master ***");
          
          // NOW update offset to include this GETACK command
          this.replicationOffset += commandBytes;
          console.log(`[DEBUG] Updated offset to ${this.replicationOffset} after processing GETACK`);
          continue;
        }

        // For other commands (PING, SET, etc.), process and update offset
        console.log(`[DEBUG] Processing command: ${command} ${args.join(' ')}`);
        
        // Update offset for all commands (including PING)
        this.replicationOffset += commandBytes;
        console.log(`[DEBUG] Updated offset to ${this.replicationOffset} after processing ${command}`);
        
        // Apply the command directly to avoid TCP overhead
        console.log("[DEBUG] Applying replicated command directly");
        try {
          // Create a mock request to the RedisCommands system
          const respCommand = encodeRESPCommand(command, args);
          // For now, we'll still forward via TCP but with better error handling
          const client = net.createConnection(
            {port: this.config.port || 6379, host: "127.0.0.1"},
            () => {
              console.log("[DEBUG] Connected to own server to process propagated command");
              client.write(respCommand);
              client.end(); // Close immediately since no response needed
            }
          );

          client.on("error", (error) => {
            console.error("[DEBUG] Error connecting to own server:", error);
          });

          client.on("close", () => {
            console.log("[DEBUG] Connection closed after processing command");
          });
        } catch (error) {
          console.error("[DEBUG] Error processing replicated command:", error);
        }
      } catch (error) {
        console.error("[DEBUG] Error parsing buffer:", error);
        // Clear buffer on parse error to avoid infinite loop
        this.buffer = Buffer.alloc(0);
        break;
      }
    }
  }

  private calculateCommandBytes(command: string, args: string[]): number {
    // Calculate the exact number of bytes for a RESP command
    const parts = [command, ...args];
    let totalBytes = 0;

    // Array header: *<count>\r\n
    totalBytes += 1 + parts.length.toString().length + 2; // "*" + count + "\r\n"

    // Each part: $<length>\r\n<data>\r\n
    for (const part of parts) {
      totalBytes += 1; // "$"
      totalBytes += part.length.toString().length; // length digits
      totalBytes += 2; // "\r\n"
      totalBytes += part.length; // actual data
      totalBytes += 2; // "\r\n"
    }

    return totalBytes;
  }
}
