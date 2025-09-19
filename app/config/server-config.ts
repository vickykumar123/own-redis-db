export type ServerRole = "master" | "slave";

export interface ServerConfig {
  // Server configuration
  port?: number; // Server listening port (single port)
  
  // Replication configuration
  role: ServerRole;
  masterHost?: string;
  masterPort?: number;

  // Replica configuration (sent via REPLCONF)
  ipAddress?: string; // IP address for REPLCONF
  capabilities?: string[]; // Replica capabilities

  // Future extensibility for replication features
  replicationId?: string; // Master replication ID
  replicationOffset?: number; // Current replication offset

  // Future: WAIT command and ACK support
  minReplicas?: number; // Minimum replicas for WAIT
  replicaTimeout?: number; // Timeout for replica responses

  // AOF (Append Only File) configuration
  aof?: {
    enabled: boolean;
    filename: string;
    dir: string;
    syncPolicy: "always" | "everysec" | "no";
  };

  // Future: Additional server configurations can be added here
  // cluster?: ClusterConfig;
  // security?: SecurityConfig;
}

export const DEFAULT_SERVER_CONFIG: ServerConfig = {
  role: "master",
  replicationOffset: 0,
  minReplicas: 0,
  replicaTimeout: 1000, // 1 second default
  aof: {
    enabled: false,
    filename: "appendonly.aof",
    dir: ".",
    syncPolicy: "everysec"
  }
};

// Helper function to create server config
export function createServerConfig(
  overrides: Partial<ServerConfig> = {}
): ServerConfig {
  return {
    ...DEFAULT_SERVER_CONFIG,
    ...overrides,
  };
}
