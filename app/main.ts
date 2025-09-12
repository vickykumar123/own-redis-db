// Redis server implementing RESP protocol with expiry support
import * as net from "net";
import {parseRESPCommand, encodeError} from "./parser";
import {RedisCommands} from "./commands";
import {type ServerConfig, createServerConfig} from "./config/server-config";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Parse command line arguments
function parseServerArgs(): {port: number; config: ServerConfig} {
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

  return {
    port,
    config: createServerConfig(configOverrides),
  };
}

const {port, config} = parseServerArgs();
const redisCommands = new RedisCommands(config);

const server: net.Server = net.createServer((socket: net.Socket) => {
  // Handle connection
  socket.on("data", async (data: Buffer) => {
    try {
      const parsedCommand = parseRESPCommand(data);

      if (!parsedCommand) {
        socket.write(encodeError("ERR Invalid command format"));
        return;
      }

      const {command, args} = parsedCommand;
      const response = await redisCommands.executeCommand(
        command,
        args,
        socket
      );
      if (response !== undefined) {
        socket.write(response);
      }
    } catch (error) {
      console.error("Error processing command:", error);
      socket.write(encodeError("ERR Internal server error"));
    }
  });
});

server.listen(port, "127.0.0.1");
