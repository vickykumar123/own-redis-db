import * as net from "net";
import {type ServerConfig} from "../config/server-config";
import {encodeRESPCommand, parseRESPCommand, encodeArray} from "../parser";

interface ReplicaInfo {
  socket: net.Socket;
  port: number;
  host: string;
}

export class ReplicationManager {
  private config: ServerConfig;
  private masterConnection: net.Socket | null = null;
  private replicaConnections: ReplicaInfo[] = []; // Track connected replicas with their info
  private buffer: Buffer = Buffer.alloc(0); // Buffer for handling partial RESP commands

  constructor(config: ServerConfig) {
    this.config = config;
  }

  // ========== PUBLIC METHODS ==========

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

  isReplica(): boolean {
    return this.config.role === "slave";
  }

  getMasterConnection(): net.Socket | null {
    return this.masterConnection;
  }

  // ========== PRIVATE HANDSHAKE METHODS ==========

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
      // Set up special handler for PSYNC response and RDB file
      const psyncHandler = (data: Buffer) => {
        console.log(`[DEBUG] PSYNC response received: ${data.length} bytes`);
        console.log(`[DEBUG] PSYNC data hex:`, data.toString('hex'));
        console.log(`[DEBUG] PSYNC data string:`, JSON.stringify(data.toString()));
        
        // Append to buffer for processing
        this.buffer = Buffer.concat([this.buffer, data]);
        
        const bufferStr = this.buffer.toString();
        
        // Check for FULLRESYNC response
        if (bufferStr.includes("FULLRESYNC")) {
          console.log("PSYNC FULLRESYNC response detected");
          
          // Find the end of the FULLRESYNC line
          const fullresyncEndIndex = this.buffer.indexOf("\r\n");
          if (fullresyncEndIndex === -1) {
            console.log("[DEBUG] Waiting for complete FULLRESYNC line");
            return; // Wait for complete line
          }
          
          // Skip past the FULLRESYNC line
          this.buffer = this.buffer.subarray(fullresyncEndIndex + 2);
          console.log(`[DEBUG] After FULLRESYNC line, buffer length: ${this.buffer.length}`);
          console.log(`[DEBUG] After FULLRESYNC line, buffer hex:`, this.buffer.toString('hex'));
          
          // Continue processing to handle any additional data in this same packet
          // Don't remove the handler yet - let it process RDB and subsequent commands
        }
        
        // Process RDB file if we have one in the buffer
        if (this.buffer.length > 0 && this.buffer[0] === 36) { // '$'
          const rdbLengthEndIndex = this.buffer.indexOf("\r\n");
          if (rdbLengthEndIndex === -1) {
            console.log("[DEBUG] Waiting for RDB length");
            return; // Wait for complete RDB length
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
          
          // Skip past the RDB file data (no trailing \r\n after RDB data)
          this.buffer = this.buffer.subarray(rdbDataEnd);
          console.log(`[DEBUG] RDB file parsed successfully, remaining buffer: ${this.buffer.length} bytes`);
          console.log(`[DEBUG] Remaining buffer hex:`, this.buffer.toString('hex'));
          
          // If we processed FULLRESYNC and RDB, we're ready for commands
          if (bufferStr.includes("FULLRESYNC")) {
            // Remove the PSYNC handler and resolve
            if (this.masterConnection) {
              this.masterConnection.off('data', psyncHandler);
            }
            console.log("PSYNC command sent successfully");
            resolve();
            
            // Process any remaining data (like GETACK commands) in the buffer
            if (this.buffer.length > 0) {
              console.log("[DEBUG] Processing remaining buffer data after RDB");
              this.handleBuffer();
            }
          }
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
        if (this.masterConnection) {
          this.masterConnection.off('data', psyncHandler);
        }
        reject(new Error("PSYNC command timed out"));
      }, 5000);
    });
  }

  private cleanup(): void {
    if (this.masterConnection) {
      this.masterConnection.destroy();
      this.masterConnection = null;
    }
  }

  // ========== REPLICATION INFO ==========

  getReplicationInfo(): Record<string, any> {
    const fields: Record<string, any> = {
      role: this.config.role,
    };

    // Add role-specific fields
    if (this.config.role === "slave") {
      if (this.config.masterHost) fields.master_host = this.config.masterHost;
      if (this.config.masterPort) fields.master_port = this.config.masterPort;
      // TODO: master_link_status, master_last_io_seconds_ago, etc.
    }

    if (this.config.role === "master") {
      fields.connected_slaves = this.getConnectedSlaves().length;
      fields.master_replid = this.generateReplicationId();
      fields.master_repl_offset = 0;
    }

    // Always include replication offset
    if (this.config.replicationOffset !== undefined) {
      fields.master_repl_offset = this.config.replicationOffset;
    }

    return fields;
  }

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

  // ========== REPLICA CONNECTION MANAGEMENT ==========

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
          const ackResponse = encodeArray(["REPLCONF", "ACK", "0"]);
          console.log(
            "[DEBUG] Sending ACK response:",
            JSON.stringify(ackResponse)
          );
          if (this.masterConnection) {
            this.masterConnection.write(ackResponse);
          }
          console.log("[DEBUG] *** ACK response sent to master ***");
          continue;
        }

        // For other commands (SET, etc.), forward to own server for processing
        console.log("[DEBUG] Forwarding non-GETACK command to own server");
        const respCommand = encodeRESPCommand(command, args);
        const client = net.createConnection(
          {port: this.config.port || 6379, host: "127.0.0.1"},
          () => {
            console.log(
              "[DEBUG] Connected to own server to process propagated command"
            );
            client.write(respCommand);
            client.end(); // Close immediately since no response needed
          }
        );

        client.on("error", (error) => {
          console.error("[DEBUG] Error connecting to own server:", error);
        });
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
