import * as fs from "fs";
import * as path from "path";
import { encodeRESPCommand } from "./parser";
import { RESPParser, parseRESPCommand } from "./parser";

export interface AOFConfig {
  enabled: boolean;
  filename: string;
  dir: string;
  syncPolicy: "always" | "everysec" | "no";
}

export class AOFManager {
  private aofFilePath: string;
  private writeStream?: fs.WriteStream;
  private config: AOFConfig;
  private lastSync: number = Date.now();

  constructor(config: AOFConfig) {
    this.config = config;
    this.aofFilePath = path.join(config.dir, config.filename);

    if (config.enabled) {
      this.initializeWriteStream();
    }
  }

  private initializeWriteStream(): void {
    try {
      // Ensure directory exists
      const dir = path.dirname(this.aofFilePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      // Open file for appending
      this.writeStream = fs.createWriteStream(this.aofFilePath, {
        flags: 'a',
        encoding: 'utf8'
      });

      this.writeStream.on('error', (error) => {
        console.error('[AOF] Write stream error:', error);
      });

      console.log(`[AOF] Initialized AOF file: ${this.aofFilePath}`);
    } catch (error) {
      console.error('[AOF] Failed to initialize write stream:', error);
      throw error;
    }
  }

  // Append a command to the AOF file
  appendCommand(command: string, args: string[]): void {
    if (!this.config.enabled || !this.writeStream) {
      return;
    }

    try {
      // Encode command in RESP format
      const respCommand = encodeRESPCommand(command, args);

      // Write to file
      this.writeStream.write(respCommand);

      // Handle sync policy
      this.handleSync();

      console.log(`[AOF] Logged command: ${command} ${args.join(' ')}`);
    } catch (error) {
      console.error('[AOF] Failed to append command:', error);
      // Don't throw - AOF failure shouldn't break the command execution
    }
  }

  private handleSync(): void {
    if (!this.writeStream) return;

    switch (this.config.syncPolicy) {
      case "always":
        // Force sync after every write
        this.writeStream.write('', () => {
          // Callback ensures write is flushed
        });
        break;

      case "everysec":
        // Sync every second
        const now = Date.now();
        if (now - this.lastSync >= 1000) {
          this.writeStream.write('', () => {
            // Callback ensures write is flushed
          });
          this.lastSync = now;
        }
        break;

      case "no":
        // Let OS decide when to sync
        break;
    }
  }

  // Replay all commands from AOF file on startup
  async replayCommands(executeCommand: (command: string, args: string[]) => Promise<void>): Promise<number> {
    if (!this.config.enabled || !fs.existsSync(this.aofFilePath)) {
      console.log('[AOF] No AOF file to replay');
      return 0;
    }

    try {
      console.log(`[AOF] Starting AOF replay from: ${this.aofFilePath}`);

      const aofContent = fs.readFileSync(this.aofFilePath);
      let commandCount = 0;
      let offset = 0;

      // Parse and execute commands one by one
      while (offset < aofContent.length) {
        try {
          // Create parser for this command
          const remainingBuffer = aofContent.subarray(offset);
          const parser = new RESPParser(remainingBuffer);
          const parsed = parser.parse();

          if (!Array.isArray(parsed) || parsed.length === 0) {
            console.warn('[AOF] Invalid command format, skipping');
            break;
          }

          // Extract command and args
          const command = String(parsed[0]).toUpperCase();
          const args = parsed.slice(1).map(arg => String(arg));

          // Execute the command
          await executeCommand(command, args);
          commandCount++;

          // Calculate how many bytes we consumed
          const commandBuffer = Buffer.from(encodeRESPCommand(command, args));
          offset += commandBuffer.length;

          if (commandCount % 1000 === 0) {
            console.log(`[AOF] Replayed ${commandCount} commands...`);
          }

        } catch (parseError) {
          console.error('[AOF] Error parsing command at offset', offset, parseError);
          break; // Stop on first parse error to avoid corruption
        }
      }

      console.log(`[AOF] Successfully replayed ${commandCount} commands`);
      return commandCount;

    } catch (error) {
      console.error('[AOF] Failed to replay AOF file:', error);
      throw error;
    }
  }

  // Check if AOF file exists
  fileExists(): boolean {
    return fs.existsSync(this.aofFilePath);
  }

  // Get AOF file size
  getFileSize(): number {
    try {
      const stats = fs.statSync(this.aofFilePath);
      return stats.size;
    } catch {
      return 0;
    }
  }

  // Close AOF file (cleanup)
  close(): void {
    if (this.writeStream) {
      this.writeStream.end();
      this.writeStream = undefined;
      console.log('[AOF] Closed AOF file');
    }
  }

  // Truncate AOF file to given size (for corruption recovery)
  truncate(size: number): void {
    if (!this.config.enabled) return;

    try {
      fs.truncateSync(this.aofFilePath, size);
      console.log(`[AOF] Truncated AOF file to ${size} bytes`);

      // Reinitialize write stream
      this.close();
      this.initializeWriteStream();
    } catch (error) {
      console.error('[AOF] Failed to truncate AOF file:', error);
    }
  }

  // Get current configuration
  getConfig(): AOFConfig {
    return { ...this.config };
  }

  // Update configuration (for runtime changes)
  updateConfig(newConfig: Partial<AOFConfig>): void {
    const wasEnabled = this.config.enabled;

    // Update config
    this.config = { ...this.config, ...newConfig };

    // Handle enable/disable
    if (!wasEnabled && this.config.enabled) {
      this.initializeWriteStream();
    } else if (wasEnabled && !this.config.enabled) {
      this.close();
    }
  }
}