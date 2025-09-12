export type ServerRole = 'master' | 'slave';

export interface ServerConfig {
  // Replication configuration
  role: ServerRole;
  masterHost?: string;
  masterPort?: number;
  
  // Future extensibility for replication features
  replicationId?: string;           // Master replication ID  
  replicationOffset?: number;       // Current replication offset
  
  // Future: WAIT command and ACK support
  minReplicas?: number;            // Minimum replicas for WAIT
  replicaTimeout?: number;         // Timeout for replica responses
  
  // Future: Additional server configurations can be added here
  // persistence?: PersistenceConfig;
  // cluster?: ClusterConfig;
  // security?: SecurityConfig;
}

export const DEFAULT_SERVER_CONFIG: ServerConfig = {
  role: 'master',
  replicationOffset: 0,
  minReplicas: 0,
  replicaTimeout: 1000, // 1 second default
};

// Helper function to create server config
export function createServerConfig(overrides: Partial<ServerConfig> = {}): ServerConfig {
  return {
    ...DEFAULT_SERVER_CONFIG,
    ...overrides
  };
}