export type ClusterConfig = {
  id: string;
  name: string;
  brokers: string[];
  ssl?: boolean;
  sasl?: {
    mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
    username: string;
    password: string;
  };
};

export type AuditEvent = {
  id: string;
  actor: string;
  action: string;
  clusterId?: string;
  resourceType?: string;
  resourceName?: string;
  details?: Record<string, unknown>;
  createdAt: string;
};

export type CollectorHeartbeat = {
  collectorId: string;
  clusterId: string;
  brokerId: string;
  hostname: string;
  version: string;
  startedAt: string;
  observedAt: string;
};

export type CollectorSnapshot = CollectorHeartbeat & {
  brokerCount: number;
  topicCount: number;
  controllerId?: number;
  kafkaClusterId?: string;
  brokers: Array<{
    nodeId: number;
    host: string;
    port: number;
  }>;
};

export type CollectorState = {
  heartbeat: CollectorHeartbeat;
  lastSnapshot?: CollectorSnapshot;
};

export type TopicSummary = {
  name: string;
  partitions: number;
  replicas: number;
  isInternal: boolean;
};

export type ConsumerGroupSummary = {
  groupId: string;
  protocolType: string;
  state?: string;
  members?: number;
};
