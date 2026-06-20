import { nanoid } from "nanoid";
import type {
  AdvisorAgent,
  AgentFinding,
  AgentRunSummary,
  AgentRunTrigger,
  AgentRun,
  AuditEvent,
  CollectorState,
  ConsumerGroupSummary,
  RebalancePlanSummaryRecord,
  SecurityStatus,
  TopicSummary
} from "./types.js";
import type { PublicCluster, PublicClusterChangeReview } from "./clusterRegistry.js";

type AgentContext = {
  clusterId: string;
  brokerCount: number;
  topics: TopicSummary[];
  consumerGroups: ConsumerGroupSummary[];
  collectors: CollectorState[];
  auditEvents: AuditEvent[];
  clusters: PublicCluster[];
  clusterChangeReviews: PublicClusterChangeReview[];
  rebalancePlans: RebalancePlanSummaryRecord[];
  security: SecurityStatus;
  persistenceMode: "postgres" | "memory";
};

type AgentRunOptions = {
  actor?: string;
  trigger?: AgentRunTrigger;
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

export function runAdvisorAgents(context: AgentContext, options: AgentRunOptions = {}): AgentRun {
  const findings = [
    ...runNavigator(context),
    ...runSentinel(context),
    ...runOperator(context),
    ...runSteward(context),
    ...runScribe(context)
  ];

  const sortedFindings = findings.sort((left, right) => severityRank(right.severity) - severityRank(left.severity));
  const agents = advisorAgents.map((agent) => {
    const agentFindings = sortedFindings.filter((finding) => finding.agentId === agent.id);
    return {
      ...agent,
      findings: agentFindings,
      score: scoreFindings(agentFindings)
    };
  });

  return {
    id: nanoid(),
    clusterId: context.clusterId,
    generatedAt: new Date().toISOString(),
    actor: options.actor,
    trigger: options.trigger,
    summary: summarizeFindings(sortedFindings, agents.map((agent) => agent.score)),
    agents,
    findings: sortedFindings
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

  const pendingClusterReviews = context.clusterChangeReviews.filter((review) => review.status === "pending");
  if (pendingClusterReviews.length > 0) {
    findings.push(
      finding(
        "navigator",
        "low",
        "Cluster change reviews are waiting",
        `${pendingClusterReviews.length} cluster registry change${pendingClusterReviews.length === 1 ? " is" : "s are"} pending operator review.`,
        "Open the Clusters view and clear the review queue before handoff or production rollout.",
        "cluster",
        pendingClusterReviews[0]?.clusterId
      )
    );
  }

  const plannedRebalanceReviews = context.rebalancePlans.filter((plan) => plan.status === "planned" && plan.summary.movements > 0);
  if (plannedRebalanceReviews.length > 0) {
    findings.push(
      finding(
        "navigator",
        "medium",
        "Rebalance plans need an operator decision",
        `${plannedRebalanceReviews.length} generated rebalance plan${
          plannedRebalanceReviews.length === 1 ? " has" : "s have"
        } partition movements but no approval or rejection.`,
        "Open the Rebalance history, load each plan, run preflight, then approve or reject it explicitly.",
        "rebalance",
        plannedRebalanceReviews[0]?.id
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
        "Set EVENTHELM_AUTH_MODE=token and EVENTHELM_API_TOKEN before exposing the API outside localhost.",
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
        "Set EVENTHELM_COLLECTOR_TOKEN on the API and collectors for shared-secret identity until mTLS is added.",
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
        "Set EVENTHELM_CORS_ORIGIN to the deployed console origin in shared environments.",
        "security"
      )
    );
  }

  const inlineSaslClusters = context.clusters.filter((cluster) => cluster.saslPasswordSource === "inline");
  if (inlineSaslClusters.length > 0) {
    findings.push(
      finding(
        "sentinel",
        "medium",
        "Kafka credentials are stored inline",
        `${inlineSaslClusters.length} cluster registry entr${inlineSaslClusters.length === 1 ? "y uses" : "ies use"} an inline SASL password.`,
        "Move Kafka SASL passwords to passwordEnv references backed by deployment secrets before sharing the control plane.",
        "cluster",
        inlineSaslClusters[0]?.id
      )
    );
  }

  const inlineCredentialReviews = context.clusterChangeReviews.filter((review) =>
    review.warnings.some((warning) => warning.toLowerCase().includes("inline sasl"))
  );
  if (inlineCredentialReviews.length > 0) {
    findings.push(
      finding(
        "sentinel",
        "medium",
        "Cluster review contains inline credentials",
        `${inlineCredentialReviews.length} retained cluster change review${
          inlineCredentialReviews.length === 1 ? " warns" : "s warn"
        } about inline SASL password storage.`,
        "Reject or replace those reviews with passwordEnv-backed registrations before applying them.",
        "cluster",
        inlineCredentialReviews[0]?.clusterId
      )
    );
  }

  const directClusterMutations = context.auditEvents.filter((event) => ["cluster.upsert", "cluster.delete"].includes(event.action));
  if (directClusterMutations.length > 0) {
    findings.push(
      finding(
        "sentinel",
        "medium",
        "Direct cluster registry mutations detected",
        `${directClusterMutations.length} recent cluster registry mutation${
          directClusterMutations.length === 1 ? " bypassed" : "s bypassed"
        } the review queue.`,
        "Use cluster change reviews for registrations and removals; keep direct write routes for break-glass automation only.",
        "cluster",
        directClusterMutations[0]?.resourceName
      )
    );
  }

  return findings;
}

function runOperator(context: AgentContext): AgentFinding[] {
  const findings: AgentFinding[] = [];
  const freshCollectors = context.collectors.filter((collector) => collectorFreshness(collector) === "online");
  const staleCollectors = context.collectors.filter((collector) => collectorFreshness(collector) === "stale");
  const pressuredCollectors = context.collectors.filter((collector) =>
    ["high", "critical"].includes(collector.lastSnapshot?.disk?.pressure ?? "")
  );
  const memoryPressuredCollectors = context.collectors.filter((collector) =>
    ["high", "critical"].includes(collector.lastSnapshot?.host?.memoryPressure ?? "")
  );

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

  const highestLagGroup = context.consumerGroups
    .filter((group) => (group.lag?.total ?? 0) > 0)
    .sort((left, right) => (right.lag?.total ?? 0) - (left.lag?.total ?? 0))[0];
  if (highestLagGroup) {
    const lag = highestLagGroup.lag?.total ?? 0;
    findings.push(
      finding(
        "operator",
        lag >= 10_000 ? "high" : "medium",
        "Consumer group lag detected",
        `${highestLagGroup.groupId} is ${lag.toLocaleString()} records behind across ${highestLagGroup.lag?.partitions ?? 0} partitions.`,
        "Open the Consumers view, inspect lag by topic, and confirm whether consumers need scaling or recovery.",
        "consumer-group",
        highestLagGroup.groupId
      )
    );
  }

  const unknownOffsetGroups = context.consumerGroups.filter((group) => (group.lag?.unknownOffsets ?? 0) > 0);
  if (unknownOffsetGroups.length > 0) {
    findings.push(
      finding(
        "operator",
        "low",
        "Some consumer offsets are not committed",
        `${unknownOffsetGroups.length} consumer groups include partitions with unknown committed offsets.`,
        "Treat unknown offsets as a visibility gap before using lag as an SLO signal.",
        "consumer-group",
        unknownOffsetGroups[0]?.groupId
      )
    );
  }

  if (pressuredCollectors.length > 0) {
    const worst = pressuredCollectors.sort(
      (left, right) => (right.lastSnapshot?.disk?.usedPercent ?? 0) - (left.lastSnapshot?.disk?.usedPercent ?? 0)
    )[0];
    findings.push(
      finding(
        "operator",
        worst?.lastSnapshot?.disk?.pressure === "critical" ? "critical" : "high",
        "Broker disk pressure needs partition movement",
        `Broker ${worst?.heartbeat.brokerId ?? "unknown"} is at ${worst?.lastSnapshot?.disk?.usedPercent ?? 0}% disk usage.`,
        "Open the Rebalance view and generate a disk-pressure reassignment plan before the broker reaches log-dir exhaustion.",
        "broker",
        worst?.heartbeat.brokerId
      )
    );
  }

  if (memoryPressuredCollectors.length > 0) {
    const worst = memoryPressuredCollectors.sort(
      (left, right) => (right.lastSnapshot?.host?.usedMemoryPercent ?? 0) - (left.lastSnapshot?.host?.usedMemoryPercent ?? 0)
    )[0];
    findings.push(
      finding(
        "operator",
        worst?.lastSnapshot?.host?.memoryPressure === "critical" ? "high" : "medium",
        "Broker host memory pressure detected",
        `Broker ${worst?.heartbeat.brokerId ?? "unknown"} host memory is at ${worst?.lastSnapshot?.host?.usedMemoryPercent ?? 0}% used.`,
        "Review broker host pressure before rebalancing or scaling consumers on the affected node.",
        "broker",
        worst?.heartbeat.brokerId
      )
    );
  }

  const approvedRebalancePlans = context.rebalancePlans.filter((plan) => plan.status === "approved" && plan.summary.movements > 0);
  if (approvedRebalancePlans.length > 0) {
    findings.push(
      finding(
        "operator",
        "medium",
        "Approved rebalance plans are waiting",
        `${approvedRebalancePlans.length} approved rebalance plan${
          approvedRebalancePlans.length === 1 ? " is" : "s are"
        } waiting for execution preflight or operator cleanup.`,
        "Load the approved plan, run preflight against live Kafka state, then execute, reject, or supersede it.",
        "rebalance",
        approvedRebalancePlans[0]?.id
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

  if (context.persistenceMode === "memory") {
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
  } else {
    findings.push(
      finding(
        "scribe",
        "info",
        "Postgres persistence is enabled",
        "Cluster configs, audit events, broker collector state, rebalance plans, and advisor runs survive API restarts.",
        "Add retention policies and backup guidance before production rollout.",
        "platform"
      )
    );
  }

  if (context.auditEvents.length > 0) {
    const auditStore = context.persistenceMode === "postgres" ? "stored in Postgres" : "available in memory";
    findings.push(
      finding(
        "scribe",
        "info",
        "Audit events are being captured",
        `${context.auditEvents.length} audit events are ${auditStore}.`,
        "Add filters by actor, action, resource, and cluster so operators can investigate changes quickly.",
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

function summarizeFindings(findings: AgentFinding[], scores: number[]): AgentRunSummary {
  const summary: AgentRunSummary = {
    score: scores.length ? Math.round(scores.reduce((total, score) => total + score, 0) / scores.length) : 100,
    findings: findings.length,
    critical: 0,
    high: 0,
    medium: 0,
    low: 0,
    info: 0
  };

  for (const finding of findings) {
    summary[finding.severity] += 1;
  }

  return summary;
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
