export type ClusterConfig = {
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

export type DiskPressure = "normal" | "watch" | "high" | "critical";

export type DiskTelemetry = {
  path: string;
  totalBytes: number;
  freeBytes: number;
  usedBytes: number;
  usedPercent: number;
  pressure: DiskPressure;
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
  memoryPressure: DiskPressure;
  uptimeSeconds: number;
  sampledAt: string;
};

export type PartitionLogSize = {
  topic: string;
  partition: number;
  sizeBytes: number;
  logDir: string;
};

export type CollectorSnapshot = CollectorHeartbeat & {
  brokerCount: number;
  topicCount: number;
  controllerId?: number;
  kafkaClusterId?: string;
  disk?: DiskTelemetry;
  host?: HostTelemetry;
  partitions?: PartitionLogSize[];
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

export type PartitionPlacement = {
  topic: string;
  partition: number;
  leader: number;
  replicas: number[];
  isr: number[];
  offlineReplicas: number[];
  isInternal: boolean;
};

export type RebalanceMovement = {
  topic: string;
  partition: number;
  sourceBrokerId: number;
  targetBrokerId: number;
  currentReplicas: number[];
  proposedReplicas: number[];
  leaderMove: boolean;
  estimatedSizeBytes?: number;
  reason: string;
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
  movements: RebalanceMovement[];
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

export type RebalancePreflightCheck = {
  id: string;
  label: string;
  status: "pass" | "warn" | "fail";
  detail: string;
  evidence?: Record<string, unknown>;
};

export type RebalancePreflight = {
  planId: string;
  clusterId: string;
  checkedAt: string;
  executable: boolean;
  blockedReasons: string[];
  warnings: string[];
  staleMovementCount: number;
  unknownSizeMovementCount: number;
  missingTelemetryBrokerIds: number[];
  staleTelemetryBrokerIds: number[];
  checks: RebalancePreflightCheck[];
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

export type ConsumerGroupSummary = {
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

export type SecurityMode = "dev" | "token";
export type AuthScope =
  | "read"
  | "write"
  | "admin"
  | "cluster:write"
  | "cluster:breakglass"
  | "topic:write"
  | "message:write"
  | "consumer:write"
  | "rebalance:plan"
  | "rebalance:review"
  | "rebalance:execute"
  | "agent:run";

export type ApiTokenConfig = {
  token: string;
  actor?: string;
  scopes: AuthScope[];
};

export type SecurityStatus = {
  authMode: SecurityMode;
  apiTokenConfigured: boolean;
  apiTokenCount: number;
  configuredScopes: AuthScope[];
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
  agents: Array<
    AdvisorAgent & {
      findings: AgentFinding[];
      score: number;
    }
  >;
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
