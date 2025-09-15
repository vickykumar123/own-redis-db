import {
  encodeBulkString,
  encodeError,
  encodeInteger,
} from "../parser";
import * as net from "net";

export class PubSubCommands {
  private subscriptions: Map<string, Set<string>>; // Per-connection subscriptions: connectionId -> Set<channelName>
  private connectionSockets: Map<string, net.Socket> = new Map(); // Track sockets by connection ID

  constructor(subscriptions: Map<string, Set<string>>) {
    this.subscriptions = subscriptions;
  }

  private getConnectionId(socket: net.Socket): string {
    return `${socket.remoteAddress}:${socket.remotePort}`;
  }

  // Check if connection is in subscription mode
  isInSubscriptionMode(socket: net.Socket): boolean {
    return this.subscriptions.has(this.getConnectionId(socket));
  }

  // Handle subscription mode command validation
  handleSubscriptionsMode(command: string): string | null {
    const allowedCommands = new Set([
      "PING",
      "SUBSCRIBE",
      "UNSUBSCRIBE",
      "PSUBSCRIBE",
      "PUNSUBSCRIBE",
      "QUIT",
    ]);

    if (!allowedCommands.has(command.toUpperCase())) {
      return encodeError(`ERR Can't execute '${command}' in subscription mode`);
    }
    return null;
  }

  // Handle SUBSCRIBE command
  handleSubscribe(args: string[], socket: net.Socket): string {
    if (args.length < 1) {
      return encodeError(
        "ERR wrong number of arguments for 'subscribe' command"
      );
    }

    const connectionId = this.getConnectionId(socket);

    // Track the socket for this connection
    this.connectionSockets.set(connectionId, socket);

    // Get or create subscription set for this connection
    if (!this.subscriptions.has(connectionId)) {
      this.subscriptions.set(connectionId, new Set<string>());
    }
    const connectionSubscriptions = this.subscriptions.get(connectionId)!;

    // For SUBSCRIBE command, we should return a response for each channel
    // Redis sends one response per channel subscribed to
    let response = "";

    for (const channel of args) {
      // Add channel to subscriptions (Set automatically handles duplicates)
      connectionSubscriptions.add(channel);

      // Get current subscription count for this connection
      const subscriptionCount = connectionSubscriptions.size;

      // Manual RESP encoding: *3\r\n$9\r\nsubscribe\r\n$<channel_length>\r\n<channel>\r\n:<count>\r\n
      response += "*3\r\n"; // Array with 3 elements
      response += encodeBulkString("subscribe"); // First element: "subscribe" as bulk string
      response += encodeBulkString(channel); // Second element: channel name as bulk string
      response += encodeInteger(subscriptionCount); // Third element: count as integer
    }

    return response;
  }

  // Handle UNSUBSCRIBE command
  handleUnsubscribe(args: string[], socket: net.Socket): string {
    if (args.length < 1) {
      return encodeError(
        "ERR wrong number of arguments for 'unsubscribe' command"
      );
    }
    const connectionId = this.getConnectionId(socket);
    const connectionSubscriptions = this.subscriptions.get(connectionId);
    if (!connectionSubscriptions) {
      return encodeError("ERR not subscribed to any channels");
    }
    let response = "";
    for (const channel of args) {
      connectionSubscriptions.delete(channel);
      const subscriptionCount = connectionSubscriptions.size;
      response += "*3\r\n"; // Array with 3 elements
      response += encodeBulkString("unsubscribe");
      response += encodeBulkString(channel);
      response += encodeInteger(subscriptionCount);
    }
    // If no more subscriptions, remove the entry
    if (connectionSubscriptions.size === 0) {
      this.subscriptions.delete(connectionId);
      this.connectionSockets.delete(connectionId);
    }
    return response;
  }

  // Handle PUBLISH command
  handlePublish(args: string[]): string {
    if (args.length !== 2) {
      return encodeError("ERR wrong number of arguments for 'publish' command");
    }
    const channel = args[0];
    const message = args[1];
    let receivers = 0;

    // Deliver message to all subscribed clients
    for (const [connectionId, channels] of this.subscriptions) {
      if (channels.has(channel)) {
        receivers++;
        const socket = this.connectionSockets.get(connectionId);
        if (socket && !socket.destroyed) {
          // Send message in format: ["message", "channel", "message_content"]
          const messageResponse = this.encodePublishMessage(channel, message);
          socket.write(messageResponse);
          console.log(
            `[PUBLISH] Sent message to ${connectionId} on channel '${channel}': ${message}`
          );
        }
      }
    }
    return encodeInteger(receivers);
  }

  // Clean up connection when client disconnects
  cleanupConnection(socket: net.Socket): void {
    const connectionId = this.getConnectionId(socket);
    this.subscriptions.delete(connectionId);
    this.connectionSockets.delete(connectionId);
    console.log(`[PUBSUB] Cleaned up connection ${connectionId}`);
  }

  private encodePublishMessage(channel: string, message: string): string {
    // Format: *3\r\n$7\r\nmessage\r\n$<channel_length>\r\n<channel>\r\n$<message_length>\r\n<message>\r\n
    let response = "*3\r\n"; // Array with 3 elements
    response += encodeBulkString("message"); // First element: "message" as bulk string
    response += encodeBulkString(channel); // Second element: channel name as bulk string
    response += encodeBulkString(message); // Third element: message content as bulk string
    return response;
  }
}