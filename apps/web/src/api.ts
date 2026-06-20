export type Cluster = {
  id: string;
  name: string;
  brokers: string[];
};

export type CollectorState = {
  heartbeat: {
    collectorId: string;
    clusterId: string;
    brokerId: string;
    hostname: string;
    version: string;
    startedAt: string;
    observedAt: string;
  };
  lastSnapshot?: {
    brokerCount: number;
    topicCount: number;
    controllerId?: number;
    kafkaClusterId?: string;
    disk?: DiskTelemetry;
    partitions?: PartitionLogSize[];
  };
};

export type DiskTelemetry = {
  path: string;
  totalBytes: number;
  freeBytes: number;
  usedBytes: number;
  usedPercent: number;
  pressure: "normal" | "watch" | "high" | "critical";
  sampledAt: string;
};

export type PartitionLogSize = {
  topic: string;
  partition: number;
  sizeBytes: number;
  logDir: string;
};

export type Overview = {
  clusterId: string;
  clusterName: string;
  kafkaClusterId?: string;
  controllerId?: number;
  brokerCount: number;
  topicCount: number;
  internalTopicCount: number;
  consumerGroupCount: number;
  brokers: Array<{ nodeId: number; host: string; port: number }>;
  collectors: CollectorState[];
};

export type Topic = {
  name: string;
  partitions: number;
  replicas: number;
  isInternal: boolean;
};

export type ConsumerGroup = {
  groupId: string;
  protocolType: string;
  state?: string;
  members?: number;
};

export type MessageRecord = {
  topic: string;
  partition: number;
  offset: string;
  key?: string;
  value?: string;
  timestamp: string;
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

export type SecurityStatus = {
  authMode: "dev" | "token";
  apiTokenConfigured: boolean;
  collectorTokenConfigured: boolean;
  corsOrigin: string;
  writeConfirmationRequired: boolean;
};

export type AgentSeverity = "critical" | "high" | "medium" | "low" | "info";

export type AdvisorAgent = {
  id: string;
  name: string;
  role: "ux" | "security" | "sre" | "governance" | "maintainer";
  mission: string;
  cadence: string;
  score: number;
  findings: AgentFinding[];
};

export type AgentFinding = {
  id: string;
  agentId: string;
  severity: AgentSeverity;
  title: string;
  summary: string;
  recommendation: string;
  resourceType?: string;
  resourceName?: string;
};

export type AgentRun = {
  clusterId: string;
  generatedAt: string;
  agents: AdvisorAgent[];
  findings: AgentFinding[];
};

export type RebalancePlan = {
  id: string;
  clusterId: string;
  generatedAt: string;
  strategy: "disk-pressure";
  executable: boolean;
  executionBlockedReason?: string;
  brokerPressure: Array<{
    brokerId: number;
    host?: string;
    port?: number;
    replicaCount: number;
    leaderCount: number;
    logBytes?: number;
    disk?: DiskTelemetry;
  }>;
  summary: {
    movements: number;
    partitionsEvaluated: number;
    sourceBrokerIds: number[];
    targetBrokerIds: number[];
    maxUsedPercent?: number;
    minUsedPercent?: number;
    estimatedBytesMoved?: number;
  };
  movements: Array<{
    topic: string;
    partition: number;
    sourceBrokerId: number;
    targetBrokerId: number;
    currentReplicas: number[];
    proposedReplicas: number[];
    leaderMove: boolean;
    estimatedSizeBytes?: number;
    reason: string;
  }>;
  reassignment: {
    version: 1;
    partitions: Array<{
      topic: string;
      partition: number;
      replicas: number[];
      log_dirs: string[];
    }>;
  };
  kafkaJsRequest: Array<{
    topic: string;
    partitionAssignment: Array<{
      partition: number;
      replicas: number[];
    }>;
  }>;
  warnings: string[];
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:18080";
const apiToken = import.meta.env.VITE_EVENTHELM_API_TOKEN ?? import.meta.env.VITE_BROKARA_API_TOKEN;
const actor = import.meta.env.VITE_EVENTHELM_ACTOR ?? import.meta.env.VITE_BROKARA_ACTOR ?? "web-console";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    ...(init?.headers as Record<string, string> | undefined)
  };

  if (apiToken) {
    headers.authorization = `Bearer ${apiToken}`;
  }

  const response = await fetch(`${apiBaseUrl}${path}`, {
    ...init,
    headers
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  return response.json() as Promise<T>;
}

export const api = {
  clusters: () => request<Cluster[]>("/api/clusters"),
  security: () => request<SecurityStatus>("/api/security/status"),
  overview: (clusterId: string) => request<Overview>(`/api/clusters/${clusterId}/overview`),
  topics: (clusterId: string) => request<Topic[]>(`/api/clusters/${clusterId}/topics`),
  consumerGroups: (clusterId: string) =>
    request<ConsumerGroup[]>(`/api/clusters/${clusterId}/consumer-groups`),
  collectors: () => request<CollectorState[]>("/api/collectors"),
  audit: () => request<AuditEvent[]>("/api/audit"),
  agents: (clusterId: string) => request<AgentRun>(`/api/clusters/${clusterId}/agents`),
  runAgents: (clusterId: string) =>
    request<AgentRun>(`/api/clusters/${clusterId}/agents/run`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify({})
    }),
  planRebalance: (
    clusterId: string,
    body: {
      maxMovements: number;
      includeInternal: boolean;
      highWatermarkPercent: number;
      minBrokerGapPercent: number;
      sourceBrokerId?: number;
      targetBrokerIds?: number[];
    }
  ) =>
    request<RebalancePlan>(`/api/clusters/${clusterId}/rebalance/plan`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor
      },
      body: JSON.stringify(body)
    }),
  executeRebalance: (clusterId: string, planId: string) =>
    request<{ accepted: boolean; planId: string }>(`/api/clusters/${clusterId}/rebalance/execute`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify({ planId })
    }),
  createTopic: (
    clusterId: string,
    body: {
      name: string;
      partitions: number;
      replicationFactor: number;
      retentionMs?: number;
      cleanupPolicy?: "delete" | "compact";
    }
  ) =>
    request<{ created: boolean }>(`/api/clusters/${clusterId}/topics`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify(body)
    }),
  produceMessage: (
    clusterId: string,
    body: {
      topic: string;
      key?: string;
      value: string;
    }
  ) =>
    request<{ result: unknown }>(`/api/clusters/${clusterId}/messages/produce`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify(body)
    }),
  browseMessages: (clusterId: string, topic: string, limit = 25, fromBeginning = false) =>
    request<MessageRecord[]>(
      `/api/clusters/${clusterId}/messages?topic=${encodeURIComponent(topic)}&limit=${limit}&fromBeginning=${fromBeginning}&timeoutMs=3000`
    )
};
