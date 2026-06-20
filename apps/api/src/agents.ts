import type {
  AdvisorAgent,
  AgentFinding,
  AgentRun,
  AuditEvent,
  CollectorState,
  ConsumerGroupSummary,
  SecurityStatus,
  TopicSummary
} from "./types.js";

type AgentContext = {
  clusterId: string;
  brokerCount: number;
  topics: TopicSummary[];
  consumerGroups: ConsumerGroupSummary[];
  collectors: CollectorState[];
  auditEvents: AuditEvent[];
  security: SecurityStatus;
};

const advisorAgents: AdvisorAgent[] = [
  {
    id: "navigator",
    name: "Navigator",
    role: "ux",
    mission: "Keeps operator workflows obvious, focused, and safe to use under pressure.",
    cadence: "Runs on every console refresh"
  },
  {
    id: "sentinel",
    name: "Sentinel",
    role: "security",
    mission: "Finds unsafe defaults, missing identity checks, and risky write paths.",
    cadence: "Runs before every production rollout"
  },
  {
    id: "operator",
    name: "Operator",
    role: "sre",
    mission: "Watches broker coverage, collector freshness, and operational health drift.",
    cadence: "Runs every collector heartbeat cycle"
  },
  {
    id: "steward",
    name: "Steward",
    role: "governance",
    mission: "Reviews topic hygiene, naming consistency, and self-service guardrails.",
    cadence: "Runs when inventory changes"
  },
  {
    id: "scribe",
    name: "Scribe",
    role: "maintainer",
    mission: "Protects documentation, release readiness, and project maintainability.",
    cadence: "Runs before each release"
  }
];

export function listAdvisorAgents(): AdvisorAgent[] {
  return advisorAgents;
}

export function runAdvisorAgents(context: AgentContext): AgentRun {
  const findings = [
    ...runNavigator(context),
    ...runSentinel(context),
    ...runOperator(context),
    ...runSteward(context),
    ...runScribe(context)
  ];

  return {
    clusterId: context.clusterId,
    generatedAt: new Date().toISOString(),
    agents: advisorAgents.map((agent) => {
      const agentFindings = findings.filter((finding) => finding.agentId === agent.id);
      return {
        ...agent,
        findings: agentFindings,
        score: scoreFindings(agentFindings)
      };
    }),
    findings: findings.sort((left, right) => severityRank(right.severity) - severityRank(left.severity))
  };
}

function runNavigator(context: AgentContext): AgentFinding[] {
  const findings: AgentFinding[] = [];

  if (context.auditEvents.length === 0) {
    findings.push(
      finding(
        "navigator",
        "medium",
        "No recent activity trail in the console",
        "Operators cannot quickly understand what changed in this cluster from the landing view.",
        "Keep the latest audit events visible on the overview and link every mutation result back to its audit record.",
        "audit"
      )
    );
  }

  if (context.topics.length > 12) {
    findings.push(
      finding(
        "navigator",
        "low",
        "Topic inventory needs stronger filtering",
        "Large topic lists become scanning work without search, type filters, and detail drawers.",
        "Keep search and internal-topic filters pinned above the topic table.",
        "topic"
      )
    );
  }

  return findings;
}

function runSentinel(context: AgentContext): AgentFinding[] {
  const findings: AgentFinding[] = [];

  if (context.security.authMode === "dev") {
    findings.push(
      finding(
        "sentinel",
        "high",
        "API is running in dev auth mode",
        "Mutating endpoints are intentionally open for the local lab.",
        "Set BROKARA_AUTH_MODE=token and BROKARA_API_TOKEN before exposing the API outside localhost.",
        "security"
      )
    );
  }

  if (!context.security.collectorTokenConfigured) {
    findings.push(
      finding(
        "sentinel",
        "medium",
        "Collectors are not authenticated",
        "Any workload that can reach the API could submit collector heartbeats or snapshots.",
        "Set BROKARA_COLLECTOR_TOKEN on the API and collectors for shared-secret identity until mTLS is added.",
        "collector"
      )
    );
  }

  if (context.security.corsOrigin === "*") {
    findings.push(
      finding(
        "sentinel",
        "medium",
        "CORS is open",
        "The API currently accepts browser requests from any origin.",
        "Set BROKARA_CORS_ORIGIN to the deployed console origin in shared environments.",
        "security"
      )
    );
  }

  return findings;
}

function runOperator(context: AgentContext): AgentFinding[] {
  const findings: AgentFinding[] = [];
  const freshCollectors = context.collectors.filter((collector) => collectorFreshness(collector) === "online");
  const staleCollectors = context.collectors.filter((collector) => collectorFreshness(collector) === "stale");

  if (freshCollectors.length < context.brokerCount) {
    findings.push(
      finding(
        "operator",
        freshCollectors.length === 0 ? "critical" : "high",
        "Broker collector coverage is incomplete",
        `${freshCollectors.length} of ${context.brokerCount} brokers have fresh collector telemetry.`,
        "Run one collector per broker host and alert when heartbeat age exceeds 30 seconds.",
        "collector"
      )
    );
  }

  if (staleCollectors.length > 0) {
    findings.push(
      finding(
        "operator",
        "medium",
        "Some collectors are stale",
        `${staleCollectors.length} collectors have not reported recently.`,
        "Surface stale collectors in the overview and restart or redeploy the affected agent.",
        "collector"
      )
    );
  }

  if (context.consumerGroups.length === 0) {
    findings.push(
      finding(
        "operator",
        "info",
        "No active consumer groups detected",
        "The cluster has no described consumer groups right now.",
        "Once lag APIs are added, use this as a health signal rather than a warning.",
        "consumer-group"
      )
    );
  }

  return findings;
}

function runSteward(context: AgentContext): AgentFinding[] {
  const findings: AgentFinding[] = [];
  const userTopics = context.topics.filter((topic) => !topic.isInternal);
  const looseNames = userTopics.filter((topic) => !/^[a-z0-9]+([._-][a-z0-9]+)+$/.test(topic.name));

  if (looseNames.length > 0) {
    findings.push(
      finding(
        "steward",
        "medium",
        "Topic names need a policy",
        `${looseNames.length} user topics do not match the suggested lowercase domain.event pattern.`,
        "Adopt a naming policy before self-service topic creation expands.",
        "topic",
        looseNames[0]?.name
      )
    );
  }

  if (userTopics.some((topic) => topic.replicas < Math.min(3, context.brokerCount))) {
    findings.push(
      finding(
        "steward",
        "high",
        "Some topics have weak replication",
        "At least one user topic has fewer replicas than the broker count allows.",
        "Default production topics to replication factor 3 where cluster size permits.",
        "topic"
      )
    );
  }

  return findings;
}

function runScribe(context: AgentContext): AgentFinding[] {
  const findings: AgentFinding[] = [];

  findings.push(
    finding(
      "scribe",
      "medium",
      "Control-plane state is still in memory",
      "Audit events, collectors, and advisor findings disappear when the API restarts.",
      "Add Postgres persistence before this becomes a real multi-user control plane.",
      "platform"
    )
  );

  if (context.auditEvents.length > 0) {
    findings.push(
      finding(
        "scribe",
        "info",
        "Audit events are being captured",
        `${context.auditEvents.length} audit events are available in memory.`,
        "Persist them and add filters by actor, action, resource, and cluster.",
        "audit"
      )
    );
  }

  return findings;
}

function collectorFreshness(collector: CollectorState): "online" | "stale" {
  const lastSeen = new Date(collector.heartbeat.observedAt).getTime();
  return Date.now() - lastSeen <= 30_000 ? "online" : "stale";
}

function finding(
  agentId: string,
  severity: AgentFinding["severity"],
  title: string,
  summary: string,
  recommendation: string,
  resourceType?: string,
  resourceName?: string
): AgentFinding {
  return {
    id: `${agentId}-${title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`,
    agentId,
    severity,
    title,
    summary,
    recommendation,
    resourceType,
    resourceName
  };
}

function scoreFindings(findings: AgentFinding[]): number {
  const penalty = findings.reduce((total, finding) => total + severityPenalty(finding.severity), 0);
  return Math.max(0, 100 - penalty);
}

function severityRank(severity: AgentFinding["severity"]): number {
  return {
    critical: 5,
    high: 4,
    medium: 3,
    low: 2,
    info: 1
  }[severity];
}

function severityPenalty(severity: AgentFinding["severity"]): number {
  return {
    critical: 35,
    high: 22,
    medium: 12,
    low: 6,
    info: 1
  }[severity];
}
