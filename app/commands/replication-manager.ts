import * as net from "net";
import {type ServerConfig} from "../config/server-config";
import {encodeRESPCommand} from "../parser";

export class ReplicationManager {
  private config: ServerConfig;
  private masterConnection: net.Socket | null = null;

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

      // TODO: Stage 3: Send PSYNC command
      // await this.sendPsyncCommand();

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
        this.masterConnection!.off("data", onData);
        resolve(response);
      };

      this.masterConnection!.on("data", onData);
      this.masterConnection!.write(respCommand);

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
    const portResponse = await this.sendCommandToMaster("REPLCONF", ["listening-port", listeningPort.toString()]);
    if (portResponse !== "+OK\r\n") {
      throw new Error(`Unexpected REPLCONF listening-port response: ${portResponse}`);
    }
    
    // Send capabilities
    console.log("Sending REPLCONF capa psync2");
    const capaResponse = await this.sendCommandToMaster("REPLCONF", ["capa", "psync2"]);
    if (capaResponse !== "+OK\r\n") {
      throw new Error(`Unexpected REPLCONF capa response: ${capaResponse}`);
    }
    
    console.log("REPLCONF commands sent successfully");
  }
  // private async sendPsyncCommand(): Promise<void> { }

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
    // TODO: In future, this will return actual connected replica information
    return [];
  }
}
