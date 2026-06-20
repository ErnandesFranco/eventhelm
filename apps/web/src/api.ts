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
  };
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

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:18080";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBaseUrl}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...init?.headers
    }
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  return response.json() as Promise<T>;
}

export const api = {
  clusters: () => request<Cluster[]>("/api/clusters"),
  overview: (clusterId: string) => request<Overview>(`/api/clusters/${clusterId}/overview`),
  topics: (clusterId: string) => request<Topic[]>(`/api/clusters/${clusterId}/topics`),
  consumerGroups: (clusterId: string) =>
    request<ConsumerGroup[]>(`/api/clusters/${clusterId}/consumer-groups`),
  collectors: () => request<CollectorState[]>("/api/collectors"),
  audit: () => request<AuditEvent[]>("/api/audit"),
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
      body: JSON.stringify(body)
    }),
  browseMessages: (clusterId: string, topic: string, limit = 25) =>
    request<MessageRecord[]>(
      `/api/clusters/${clusterId}/messages?topic=${encodeURIComponent(topic)}&limit=${limit}&fromBeginning=true&timeoutMs=3000`
    )
};
