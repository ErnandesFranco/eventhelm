export type Cluster = {
  id: string;
  name: string;
  brokers: string[];
  ssl?: boolean;
  saslConfigured?: boolean;
  saslPasswordSource?: "inline" | "environment";
  source?: "environment" | "api";
  createdAt?: string;
  updatedAt?: string;
};

export type ClusterRegistration = {
  id: string;
  name: string;
  brokers: string[];
  ssl?: boolean;
  sasl?: {
    mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
    username: string;
    password?: string;
    passwordEnv?: string;
  };
};

export type ClusterChangeReview = {
  id: string;
  clusterId: string;
  action: "upsert" | "delete";
  status: "pending" | "approved" | "rejected" | "applied";
  actor: string;
  request: {
    action: "upsert" | "delete";
    clusterId: string;
    cluster?: {
      id: string;
      name: string;
      brokers: string[];
      ssl: boolean;
      saslConfigured: boolean;
      saslPasswordSource?: "inline" | "environment";
    };
  };
  current?: Cluster;
  proposed?: Cluster;
  warnings: string[];
  createdAt: string;
  reviewedBy?: string;
  reviewedAt?: string;
  reviewComment?: string;
  appliedBy?: string;
  appliedAt?: string;
};

export type ClusterChangeReviewRequest =
  | {
      action: "upsert";
      cluster: ClusterRegistration;
    }
  | {
      action: "delete";
      clusterId: string;
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
    host?: HostTelemetry;
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

export type HostTelemetry = {
  cpuCount: number;
  loadAverage1m: number;
  loadAverage5m: number;
  loadAverage15m: number;
  totalMemoryBytes: number;
  freeMemoryBytes: number;
  usedMemoryBytes: number;
  usedMemoryPercent: number;
  memoryPressure: "normal" | "watch" | "high" | "critical";
  uptimeSeconds: number;
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

export type TopicConfigEntry = {
  name: string;
  value: string;
  isDefault: boolean;
  source: number;
  isSensitive: boolean;
  readOnly: boolean;
};

export type TopicConfig = {
  topic: string;
  generatedAt: string;
  entries: TopicConfigEntry[];
  editable: string[];
};

export type TopicConfigUpdateRequest = {
  configs: Array<{
    name: string;
    value: string;
  }>;
};

export type TopicConfigUpdatePreview = {
  topic: string;
  generatedAt: string;
  executable: boolean;
  reviewToken: string;
  warnings: string[];
  changes: Array<{
    name: string;
    currentValue?: string;
    newValue: string;
    blockedReason?: string;
  }>;
};

export type ConsumerGroup = {
  groupId: string;
  protocolType: string;
  state?: string;
  members?: number;
  lag?: {
    total: number;
    topics: number;
    partitions: number;
    unknownOffsets: number;
  };
};

export type ConsumerGroupLag = {
  groupId: string;
  generatedAt: string;
  state?: string;
  members?: number;
  protocolType: string;
  totalLag: number;
  unknownOffsets: number;
  topics: Array<{
    topic: string;
    totalLag: number;
    partitions: Array<{
      partition: number;
      currentOffset?: string;
      logEndOffset: string;
      lowOffset: string;
      lag?: number;
      metadata?: string | null;
    }>;
  }>;
};

export type ConsumerOffsetResetMode = "earliest" | "latest" | "absolute";

export type ConsumerOffsetResetRequest = {
  topic: string;
  partitions?: number[];
  mode: ConsumerOffsetResetMode;
  offset?: string;
};

export type ConsumerOffsetResetPreview = {
  groupId: string;
  generatedAt: string;
  state?: string;
  members: number;
  protocolType: string;
  request: ConsumerOffsetResetRequest;
  executable: boolean;
  reviewToken: string;
  warnings: string[];
  summary: {
    partitions: number;
    executablePartitions: number;
    lagBefore: string;
    lagAfter: string;
    messagesSkipped: string;
    messagesToReplay: string;
  };
  topics: Array<{
    topic: string;
    partitions: Array<{
      partition: number;
      currentOffset?: string;
      lowOffset: string;
      logEndOffset: string;
      proposedOffset: string;
      lagBefore?: string;
      lagAfter?: string;
      delta?: string;
      blockedReason?: string;
    }>;
  }>;
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

export type AuditFilters = {
  clusterId?: string;
  actor?: string;
  action?: string;
  resourceType?: string;
  resourceName?: string;
  query?: string;
  limit?: number;
};

export type SecurityStatus = {
  authMode: "dev" | "token";
  apiTokenConfigured: boolean;
  apiTokenCount: number;
  configuredScopes: string[];
  collectorTokenConfigured: boolean;
  corsOrigin: string;
  readAuthRequired: boolean;
  writeConfirmationRequired: boolean;
  writeRateLimitPerMinute: number;
};

export type AgentSeverity = "critical" | "high" | "medium" | "low" | "info";
export type AgentRunTrigger = "automatic" | "manual";

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

export type AgentRunSummary = {
  score: number;
  findings: number;
  critical: number;
  high: number;
  medium: number;
  low: number;
  info: number;
};

export type AgentRun = {
  id: string;
  clusterId: string;
  generatedAt: string;
  actor?: string;
  trigger?: AgentRunTrigger;
  summary: AgentRunSummary;
  agents: AdvisorAgent[];
  findings: AgentFinding[];
};

export type AgentRunRecord = {
  id: string;
  clusterId: string;
  actor: string;
  trigger: AgentRunTrigger;
  generatedAt: string;
  createdAt: string;
  summary: AgentRunSummary;
  findingsPreview: AgentFinding[];
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

export type RebalancePlanStatus = "planned" | "approved" | "rejected" | "executed";

export type RebalancePlanRecord = {
  id: string;
  clusterId: string;
  actor: string;
  status: RebalancePlanStatus;
  createdAt: string;
  executedAt?: string;
  reviewedBy?: string;
  reviewedAt?: string;
  reviewComment?: string;
  plan: RebalancePlan;
};

export type RebalancePlanSummaryRecord = Omit<RebalancePlanRecord, "plan"> & {
  strategy: RebalancePlan["strategy"];
  executable: boolean;
  executionBlockedReason?: string;
  summary: RebalancePlan["summary"];
  warnings: string[];
};

export type RebalancePartitionReassignment = {
  topic: string;
  partition: number;
  replicas: number[];
  addingReplicas: number[];
  removingReplicas: number[];
};

export type RebalanceExecutionStatus = {
  clusterId: string;
  checkedAt: string;
  active: boolean;
  activeTopicCount: number;
  activePartitionCount: number;
  reassignments: RebalancePartitionReassignment[];
};

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:18080";
const apiToken = import.meta.env.VITE_EVENTHELM_API_TOKEN ?? import.meta.env.VITE_BROKARA_API_TOKEN;
const actor = import.meta.env.VITE_EVENTHELM_ACTOR ?? import.meta.env.VITE_BROKARA_ACTOR ?? "web-console";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = {
    ...(init?.headers as Record<string, string> | undefined)
  };

  if (init?.body !== undefined && !Object.keys(headers).some((header) => header.toLowerCase() === "content-type")) {
    headers["content-type"] = "application/json";
  }

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
  clusterReviews: (limit = 12) => request<ClusterChangeReview[]>(`/api/clusters/reviews?limit=${limit}`),
  createClusterReview: (body: ClusterChangeReviewRequest) =>
    request<ClusterChangeReview>("/api/clusters/reviews", {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify(body)
    }),
  approveClusterReview: (reviewId: string, comment?: string) =>
    request<ClusterChangeReview>(`/api/clusters/reviews/${encodeURIComponent(reviewId)}/approve`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify({ comment })
    }),
  rejectClusterReview: (reviewId: string, comment?: string) =>
    request<ClusterChangeReview>(`/api/clusters/reviews/${encodeURIComponent(reviewId)}/reject`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify({ comment })
    }),
  applyClusterReview: (reviewId: string) =>
    request<{ applied: true; review: ClusterChangeReview; cluster: Cluster }>(
      `/api/clusters/reviews/${encodeURIComponent(reviewId)}/apply`,
      {
        method: "POST",
        headers: {
          "x-eventhelm-actor": actor,
          "x-eventhelm-confirm": "true"
        },
        body: JSON.stringify({})
      }
    ),
  upsertCluster: (body: ClusterRegistration) =>
    request<Cluster>("/api/clusters", {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify(body)
    }),
  deleteCluster: (clusterId: string) =>
    request<{ deleted: true; cluster: Cluster }>(`/api/clusters/${encodeURIComponent(clusterId)}`, {
      method: "DELETE",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      }
    }),
  security: () => request<SecurityStatus>("/api/security/status"),
  overview: (clusterId: string) => request<Overview>(`/api/clusters/${clusterId}/overview`),
  topics: (clusterId: string) => request<Topic[]>(`/api/clusters/${clusterId}/topics`),
  topicConfig: (clusterId: string, topic: string) =>
    request<TopicConfig>(`/api/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/config`),
  previewTopicConfig: (clusterId: string, topic: string, body: TopicConfigUpdateRequest) =>
    request<TopicConfigUpdatePreview>(`/api/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/config/preview`, {
      method: "POST",
      body: JSON.stringify(body)
    }),
  applyTopicConfig: (clusterId: string, topic: string, body: TopicConfigUpdateRequest & { reviewToken: string }) =>
    request<{ accepted: true; reviewToken: string; changes: TopicConfigUpdatePreview["changes"] }>(
      `/api/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/config/apply`,
      {
        method: "POST",
        headers: {
          "x-eventhelm-actor": actor,
          "x-eventhelm-confirm": "true"
        },
        body: JSON.stringify(body)
      }
    ),
  consumerGroups: (clusterId: string) =>
    request<ConsumerGroup[]>(`/api/clusters/${clusterId}/consumer-groups`),
  consumerGroupLag: (clusterId: string, groupId: string) =>
    request<ConsumerGroupLag>(`/api/clusters/${clusterId}/consumer-groups/${encodeURIComponent(groupId)}/lag`),
  previewOffsetReset: (clusterId: string, groupId: string, body: ConsumerOffsetResetRequest) =>
    request<ConsumerOffsetResetPreview>(
      `/api/clusters/${clusterId}/consumer-groups/${encodeURIComponent(groupId)}/offset-reset/preview`,
      {
        method: "POST",
        body: JSON.stringify(body)
      }
    ),
  executeOffsetReset: (clusterId: string, groupId: string, body: ConsumerOffsetResetRequest & { reviewToken: string }) =>
    request<{ accepted: true; reviewToken: string; summary: ConsumerOffsetResetPreview["summary"] }>(
      `/api/clusters/${clusterId}/consumer-groups/${encodeURIComponent(groupId)}/offset-reset/execute`,
      {
        method: "POST",
        headers: {
          "x-eventhelm-actor": actor,
          "x-eventhelm-confirm": "true"
        },
        body: JSON.stringify(body)
      }
    ),
  collectors: () => request<CollectorState[]>("/api/collectors"),
  audit: (filters: AuditFilters = {}) => {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(filters)) {
      if (value !== undefined && value !== "") {
        query.set(key, String(value));
      }
    }
    const serialized = query.toString();
    const suffix = serialized ? `?${serialized}` : "";
    return request<AuditEvent[]>(`/api/audit${suffix}`);
  },
  agents: (clusterId: string) => request<AgentRun>(`/api/clusters/${clusterId}/agents`),
  agentRuns: (clusterId: string, limit = 12) =>
    request<AgentRunRecord[]>(`/api/clusters/${clusterId}/agents/runs?limit=${limit}`),
  agentRun: (clusterId: string, runId: string) =>
    request<AgentRun>(`/api/clusters/${clusterId}/agents/runs/${encodeURIComponent(runId)}`),
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
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify(body)
    }),
  rebalancePlans: (clusterId: string, limit = 12) =>
    request<RebalancePlanSummaryRecord[]>(`/api/clusters/${clusterId}/rebalance/plans?limit=${limit}`),
  rebalanceStatus: (clusterId: string) =>
    request<RebalanceExecutionStatus>(`/api/clusters/${clusterId}/rebalance/status`),
  rebalancePlan: (clusterId: string, planId: string) =>
    request<RebalancePlanRecord>(`/api/clusters/${clusterId}/rebalance/plans/${encodeURIComponent(planId)}`),
  approveRebalancePlan: (clusterId: string, planId: string, comment?: string) =>
    request<RebalancePlanRecord>(`/api/clusters/${clusterId}/rebalance/plans/${encodeURIComponent(planId)}/approve`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify({ comment })
    }),
  rejectRebalancePlan: (clusterId: string, planId: string, comment?: string) =>
    request<RebalancePlanRecord>(`/api/clusters/${clusterId}/rebalance/plans/${encodeURIComponent(planId)}/reject`, {
      method: "POST",
      headers: {
        "x-eventhelm-actor": actor,
        "x-eventhelm-confirm": "true"
      },
      body: JSON.stringify({ comment })
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
