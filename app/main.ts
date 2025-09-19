// Redis server implementing RESP protocol with expiry support
import * as net from "net";
import {
  parseRESPCommand,
  encodeError,
  calculateRESPCommandBytes,
} from "./parser";
import {RedisCommands} from "./commands";
import {type ServerConfig, createServerConfig} from "./config/server-config";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Parse command line arguments
function parseServerArgs(): {
  port: number;
  config: ServerConfig;
  rdbDirValue?: string;
  rdbFilename?: string;
} {
  let port = 6379;
  let configOverrides: Partial<ServerConfig> = {};

  // Parse --port flag
  const portIndex = process.argv.indexOf("--port");
  if (portIndex !== -1 && process.argv.length > portIndex + 1) {
    port = parseInt(process.argv[portIndex + 1]);
  }

  // Parse --replicaof flag
  const replicaofIndex = process.argv.indexOf("--replicaof");
  if (replicaofIndex !== -1 && process.argv.length > replicaofIndex + 1) {
    const replicaofValue = process.argv[replicaofIndex + 1];
    const [masterHost, masterPortStr] = replicaofValue.split(" ");

    if (masterHost && masterPortStr) {
      const masterPort = parseInt(masterPortStr);
      if (!isNaN(masterPort)) {
        configOverrides = {
          role: "slave",
          masterHost: masterHost,
          masterPort: masterPort,
        };
      }
    }
  }

  const rdbDirIndex = process.argv.indexOf("--dir");
  let rdbDirValue;
  if (rdbDirIndex !== -1 && process.argv.length > rdbDirIndex + 1) {
    rdbDirValue = process.argv[rdbDirIndex + 1];
  }

  const rdbFilenameIndex = process.argv.indexOf("--dbfilename");
  let rdbFilename;
  if (rdbFilenameIndex !== -1 && process.argv.length > rdbFilenameIndex + 1) {
    const rdbFilenameValue = process.argv[rdbFilenameIndex + 1];
    rdbFilename = rdbFilenameValue;
  }

  // Parse AOF options
  const appendonlyIndex = process.argv.indexOf("--appendonly");
  const aofEnabled = appendonlyIndex !== -1;

  const aofFilenameIndex = process.argv.indexOf("--appendfilename");
  const aofFilename = aofFilenameIndex !== -1 && process.argv.length > aofFilenameIndex + 1
    ? process.argv[aofFilenameIndex + 1]
    : "appendonly.aof";

  const aofDirIndex = process.argv.indexOf("--aof-dir");
  const aofDir = aofDirIndex !== -1 && process.argv.length > aofDirIndex + 1
    ? process.argv[aofDirIndex + 1]
    : rdbDirValue || ".";

  // Add AOF config to overrides if enabled
  if (aofEnabled) {
    configOverrides.aof = {
      enabled: true,
      filename: aofFilename,
      dir: aofDir,
      syncPolicy: "everysec"
    };
  }

  return {
    port,
    config: createServerConfig({
      ...configOverrides,
      port: port, // Include the server port in config
    }),
    rdbDirValue,
    rdbFilename,
  };
}

const {port, config, rdbDirValue, rdbFilename} = parseServerArgs();
const redisCommands = new RedisCommands(config, rdbDirValue, rdbFilename);

// Initialize from AOF if enabled
async function initializeServer() {
  try {
    // Replay AOF commands if AOF is enabled
    if (config.aof?.enabled) {
      console.log("[MAIN] Initializing from AOF file...");
      await redisCommands.initializeFromAOF();
    }

    // Start replication handshake if this server is a replica
    if (config.role === "slave") {
      // Initiate handshake in background, don't block server startup
      redisCommands.initiateReplicationHandshake().catch((error) => {
        console.error("Failed to complete replication handshake:", error);
      });
    }
  } catch (error) {
    console.error("[MAIN] Failed to initialize server:", error);
    process.exit(1);
  }
}

// Initialize the server
initializeServer();

const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on("data", async (data: Buffer) => {
    try {
      // Handle multiple commands in a single buffer
      let offset = 0;
      while (offset < data.length) {
        try {
          const remainingData = data.subarray(offset);
          const parsedCommand = parseRESPCommand(remainingData);

          if (!parsedCommand) {
            // If we can't parse a command, it might be incomplete
            // For now, we'll treat it as an error
            socket.write(encodeError("ERR Invalid command format"));
            break;
          }

          const {command, args} = parsedCommand;

          // Calculate how many bytes this command consumed
          const commandBytes = calculateRESPCommandBytes(command, args);
          offset += commandBytes;

          const response = await redisCommands.executeCommand(
            command,
            args,
            socket
          );
          if (response !== undefined) {
            // Check if this is a replica socket - replicas should not receive responses
            if (redisCommands.isReplicaSocket(socket)) {
              console.log(
                `[MAIN] SKIPPING response to replica ${socket.remoteAddress}:${
                  socket.remotePort
                }: ${JSON.stringify(response.substring(0, 20))}`
              );
            } else {
              console.log(
                `[MAIN] Sending response to client ${socket.remoteAddress}:${
                  socket.remotePort
                }: ${JSON.stringify(response.substring(0, 20))}`
              );
              socket.write(response);
            }
          }
        } catch (parseError) {
          // If we can't parse more commands, break the loop
          break;
        }
      }
    } catch (error) {
      console.error("Error processing command:", error);
      socket.write(encodeError("ERR Internal server error"));
    }
  });
});

server.listen(port, "127.0.0.1");

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[MAIN] Shutting down server...');
  redisCommands.shutdown();
  server.close(() => {
    console.log('[MAIN] Server closed');
    process.exit(0);
  });
});

process.on('SIGTERM', () => {
  console.log('[MAIN] Shutting down server...');
  redisCommands.shutdown();
  server.close(() => {
    console.log('[MAIN] Server closed');
    process.exit(0);
  });
});
