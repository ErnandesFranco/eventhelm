import {
  Activity,
  AlertTriangle,
  ArrowRightLeft,
  Bot,
  Boxes,
  CheckCircle2,
  ChevronRight,
  CircleDot,
  ClipboardCopy,
  Clock3,
  Command,
  Database,
  FileClock,
  Gauge,
  HardDrive,
  KeyRound,
  Layers3,
  LockKeyhole,
  MessageSquareText,
  Network,
  Plus,
  RadioTower,
  RefreshCw,
  Search,
  Send,
  Server,
  ShieldAlert,
  ShieldCheck,
  Users,
  X
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type {
  AdvisorAgent,
  AgentFinding,
  AgentRun,
  AuditEvent,
  Cluster,
  CollectorState,
  ConsumerGroup,
  MessageRecord,
  Overview,
  RebalancePlan,
  SecurityStatus,
  Topic
} from "./api";
import { api } from "./api";
import eventhelmMark from "./assets/eventhelm-mark.svg";

type Tab = "command" | "agents" | "rebalance" | "topics" | "messages" | "consumers" | "collectors" | "audit";

const tabs: Array<{ id: Tab; label: string; icon: LucideIcon; group: "Operate" | "Inspect" }> = [
  { id: "command", label: "Command", icon: Command, group: "Operate" },
  { id: "agents", label: "Agents", icon: Bot, group: "Operate" },
  { id: "rebalance", label: "Rebalance", icon: ArrowRightLeft, group: "Operate" },
  { id: "topics", label: "Topics", icon: Database, group: "Inspect" },
  { id: "messages", label: "Messages", icon: MessageSquareText, group: "Inspect" },
  { id: "consumers", label: "Consumers", icon: Users, group: "Inspect" },
  { id: "collectors", label: "Collectors", icon: RadioTower, group: "Inspect" },
  { id: "audit", label: "Audit", icon: FileClock, group: "Inspect" }
];

export function App() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState("local");
  const [activeTab, setActiveTab] = useState<Tab>("command");
  const [overview, setOverview] = useState<Overview | null>(null);
  const [topics, setTopics] = useState<Topic[]>([]);
  const [groups, setGroups] = useState<ConsumerGroup[]>([]);
  const [collectors, setCollectors] = useState<CollectorState[]>([]);
  const [audit, setAudit] = useState<AuditEvent[]>([]);
  const [messages, setMessages] = useState<MessageRecord[]>([]);
  const [agentRun, setAgentRun] = useState<AgentRun | null>(null);
  const [security, setSecurity] = useState<SecurityStatus | null>(null);
  const [selectedTopic, setSelectedTopic] = useState("");
  const [messageLimit, setMessageLimit] = useState(25);
  const [fromBeginning, setFromBeginning] = useState(false);
  const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const selectedCluster = useMemo(
    () => clusters.find((cluster) => cluster.id === selectedClusterId),
    [clusters, selectedClusterId]
  );

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [loadedClusters, loadedSecurity] = await Promise.all([api.clusters(), api.security()]);
      setClusters(loadedClusters);
      setSecurity(loadedSecurity);
      const nextClusterId = selectedClusterId || loadedClusters[0]?.id;
      if (!nextClusterId) {
        return;
      }
      setSelectedClusterId(nextClusterId);
      const [loadedOverview, loadedTopics, loadedGroups, loadedCollectors, loadedAudit, loadedAgents] =
        await Promise.all([
          api.overview(nextClusterId),
          api.topics(nextClusterId),
          api.consumerGroups(nextClusterId),
          api.collectors(),
          api.audit(),
          api.agents(nextClusterId)
        ]);
      setOverview(loadedOverview);
      setTopics(loadedTopics);
      setGroups(loadedGroups);
      setCollectors(loadedCollectors);
      setAudit(loadedAudit);
      setAgentRun(loadedAgents);
      setSelectedTopic((current) => current || loadedTopics.find((topic) => !topic.isInternal)?.name || loadedTopics[0]?.name || "");
      setLastRefreshed(new Date());
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }, [selectedClusterId]);

  useEffect(() => {
    void load();
  }, [load]);

  const browse = async () => {
    if (!selectedTopic) {
      return;
    }
    setError(null);
    try {
      setMessages(await api.browseMessages(selectedClusterId, selectedTopic, messageLimit, fromBeginning));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  };

  const runAgents = async () => {
    setError(null);
    try {
      setAgentRun(await api.runAgents(selectedClusterId));
      setAudit(await api.audit());
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  };

  const posture = summarizePosture(overview, agentRun);

  return (
    <div className="appShell">
      <aside className="sidebar">
        <div className="brand">
          <img src={eventhelmMark} alt="" />
          <div>
            <strong>EventHelm</strong>
            <span>Kafka Operations</span>
          </div>
        </div>

        <div className="clusterSwitch">
          <label htmlFor="cluster">Cluster</label>
          <select id="cluster" value={selectedClusterId} onChange={(event) => setSelectedClusterId(event.target.value)}>
            {clusters.map((cluster) => (
              <option value={cluster.id} key={cluster.id}>
                {cluster.name}
              </option>
            ))}
          </select>
        </div>

        <nav className="navGroups">
          {(["Operate", "Inspect"] as const).map((group) => (
            <section key={group}>
              <h2>{group}</h2>
              {tabs
                .filter((tab) => tab.group === group)
                .map((tab) => {
                  const Icon = tab.icon;
                  return (
                    <button
                      className={activeTab === tab.id ? "navItem active" : "navItem"}
                      key={tab.id}
                      type="button"
                      onClick={() => setActiveTab(tab.id)}
                    >
                      <Icon size={17} />
                      <span>{tab.label}</span>
                      <ChevronRight size={15} />
                    </button>
                  );
                })}
            </section>
          ))}
        </nav>

        <div className={`postureBlock ${posture.tone}`}>
          <span />
          <div>
            <strong>{posture.label}</strong>
            <small>{posture.detail}</small>
          </div>
        </div>
      </aside>

      <main className="workspace">
        <header className="commandBar">
          <div>
            <p>Mission Control</p>
            <h1>{selectedCluster?.name ?? "Kafka cluster"}</h1>
          </div>
          <div className="commandTools">
            {security ? (
              <StatusPill tone={security.authMode === "dev" ? "warning" : "good"} icon={KeyRound}>
                {security.authMode === "dev" ? "Dev auth" : "Token auth"}
              </StatusPill>
            ) : null}
            {lastRefreshed ? <span className="lastRefresh">Updated {lastRefreshed.toLocaleTimeString()}</span> : null}
            <button className="iconButton" type="button" onClick={() => void load()} title="Refresh">
              <RefreshCw size={18} />
            </button>
          </div>
        </header>

        {error ? <div className="notice error">{error}</div> : null}
        {loading ? <div className="notice loading">Loading live cluster state...</div> : null}

        {!loading && activeTab === "command" && overview ? (
          <CommandCenter overview={overview} agentRun={agentRun} audit={audit} security={security} onRunAgents={runAgents} />
        ) : null}
        {!loading && activeTab === "agents" && agentRun ? <AgentsView agentRun={agentRun} onRunAgents={runAgents} /> : null}
        {!loading && activeTab === "rebalance" && overview ? (
          <RebalanceView clusterId={selectedClusterId} overview={overview} onAuditChanged={() => api.audit().then(setAudit)} />
        ) : null}
        {!loading && activeTab === "topics" ? (
          <TopicsView clusterId={selectedClusterId} topics={topics} brokerCount={overview?.brokerCount ?? 1} onChanged={() => void load()} />
        ) : null}
        {!loading && activeTab === "messages" ? (
          <MessagesView
            topics={topics}
            selectedTopic={selectedTopic}
            setSelectedTopic={setSelectedTopic}
            browse={browse}
            messages={messages}
            clusterId={selectedClusterId}
            onProduced={() => void browse()}
            limit={messageLimit}
            setLimit={setMessageLimit}
            fromBeginning={fromBeginning}
            setFromBeginning={setFromBeginning}
          />
        ) : null}
        {!loading && activeTab === "consumers" ? <ConsumersView groups={groups} /> : null}
        {!loading && activeTab === "collectors" ? <CollectorsView collectors={collectors} brokerCount={overview?.brokerCount ?? 0} /> : null}
        {!loading && activeTab === "audit" ? <AuditView audit={audit} /> : null}
      </main>
    </div>
  );
}

function CommandCenter({
  overview,
  agentRun,
  audit,
  security,
  onRunAgents
}: {
  overview: Overview;
  agentRun: AgentRun | null;
  audit: AuditEvent[];
  security: SecurityStatus | null;
  onRunAgents: () => Promise<void>;
}) {
  const freshCollectors = overview.collectors.filter((collector) => collectorFreshness(collector) === "online").length;
  const highestFinding = agentRun?.findings[0];

  return (
    <div className="commandLayout">
      <section className="statusBanner">
        <div>
          <StatusPill tone={highestFinding?.severity === "critical" ? "bad" : highestFinding?.severity === "high" ? "warning" : "good"} icon={Gauge}>
            {highestFinding ? `${highestFinding.severity} finding` : "No critical findings"}
          </StatusPill>
          <h2>{highestFinding?.title ?? "Cluster posture is clear"}</h2>
          <p>{highestFinding?.recommendation ?? "EventHelm is showing live state from Kafka and broker-local collectors."}</p>
        </div>
        <button className="primaryButton" type="button" onClick={() => void onRunAgents()}>
          <Bot size={17} />
          Run advisor sweep
        </button>
      </section>

      <section className="metricStrip">
        <Metric icon={Server} label="Brokers" value={overview.brokerCount} detail={`controller ${overview.controllerId ?? "unknown"}`} />
        <Metric icon={Database} label="Topics" value={overview.topicCount} detail={`${overview.internalTopicCount} internal hidden`} />
        <Metric icon={Users} label="Consumer groups" value={overview.consumerGroupCount} detail="reported by Kafka" />
        <Metric icon={RadioTower} label="Collectors" value={`${freshCollectors}/${overview.brokerCount}`} detail="fresh broker agents" />
        <Metric icon={ShieldAlert} label="Findings" value={agentRun?.findings.length ?? 0} detail="advisor checks" />
      </section>

      <div className="contentGrid">
        <section className="surface topologySurface">
          <SurfaceHeader icon={Network} title="Live Topology" meta={`${overview.brokers.length} brokers`} />
          <TopologyPanel overview={overview} />
        </section>

        <section className="surface">
          <SurfaceHeader icon={Bot} title="Advisor Queue" meta={agentRun ? `${agentRun.findings.length} open` : "not run"} />
          <FindingList findings={agentRun?.findings.slice(0, 5) ?? []} />
        </section>

        <section className="surface">
          <SurfaceHeader icon={LockKeyhole} title="Security Envelope" meta={security?.authMode ?? "unknown"} />
          <SecurityEnvelope security={security} />
        </section>

        <section className="surface">
          <SurfaceHeader icon={FileClock} title="Operations Ledger" meta={`${audit.length} events`} />
          <AuditTimeline audit={audit.slice(0, 6)} />
        </section>
      </div>
    </div>
  );
}

function TopologyPanel({ overview }: { overview: Overview }) {
  const collectorByBroker = new Map(overview.collectors.map((collector) => [collector.heartbeat.brokerId, collector]));
  const density = overview.brokers.length > 8 ? "dense" : overview.brokers.length > 4 ? "compact" : "standard";
  const onlineCollectors = overview.collectors.filter((collector) => collectorFreshness(collector) === "online").length;
  return (
    <div className={`topologyMap ${density}`}>
      <div className="topologyHub">
        <div className="hubMark">
          <img src={eventhelmMark} alt="" />
        </div>
        <strong>EventHelm API</strong>
        <div className="hubStats">
          <span>
            <Server size={13} />
            {overview.brokers.length} brokers
          </span>
          <span>
            <RadioTower size={13} />
            {onlineCollectors}/{overview.brokers.length} collectors
          </span>
          <span>
            <CircleDot size={13} />
            controller {overview.controllerId ?? "unknown"}
          </span>
        </div>
      </div>
      <div className={`brokerLane ${density}`}>
        {overview.brokers.map((broker) => {
          const collector = collectorByBroker.get(String(broker.nodeId));
          const freshness = collector ? collectorFreshness(collector) : "missing";
          const disk = collector?.lastSnapshot?.disk;
          return (
            <article className={`brokerNode ${freshness}`} key={broker.nodeId}>
              <div className="brokerNodeHeader">
                <span className="brokerIcon">
                  <Server size={17} />
                </span>
                <div>
                  <strong>Broker {broker.nodeId}</strong>
                  <span className="mono">{broker.host}:{broker.port}</span>
                </div>
                {overview.controllerId === broker.nodeId ? <span className="nodeRole">controller</span> : null}
              </div>
              <div className="brokerSignalGrid">
                <span>
                  <RadioTower size={13} />
                  {collector ? `${freshness} ${formatAge(collector.heartbeat.observedAt)}` : "collector missing"}
                </span>
                <span>
                  <HardDrive size={13} />
                  {disk ? `${disk.usedPercent.toFixed(1)}% used` : "disk unknown"}
                </span>
              </div>
              <div className="brokerDiskMeter" aria-label={`Broker ${broker.nodeId} disk usage`}>
                <span className={disk?.pressure ?? "unknown"} style={{ width: `${Math.min(100, disk?.usedPercent ?? 0)}%` }} />
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}

function SecurityEnvelope({ security }: { security: SecurityStatus | null }) {
  if (!security) {
    return <EmptyState title="Security posture unavailable" />;
  }
  const rows = [
    ["API mode", security.authMode],
    ["API token", security.apiTokenConfigured ? "configured" : "not configured"],
    ["Collector token", security.collectorTokenConfigured ? "configured" : "not configured"],
    ["CORS origin", security.corsOrigin],
    ["Write confirmation", security.writeConfirmationRequired ? "required" : "not required"]
  ];
  return (
    <div className="keyValueList">
      {rows.map(([label, value]) => (
        <div key={label}>
          <span>{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}

function AgentsView({ agentRun, onRunAgents }: { agentRun: AgentRun; onRunAgents: () => Promise<void> }) {
  return (
    <div className="viewStack">
      <div className="viewHeader">
        <div>
          <h2>Advisor Agents</h2>
          <p>Policy checks against live cluster, collector, security, and governance state.</p>
        </div>
        <button className="primaryButton" type="button" onClick={() => void onRunAgents()}>
          <Activity size={17} />
          Run checks
        </button>
      </div>

      <div className="agentBoard">
        {agentRun.agents.map((agent) => (
          <AgentCard key={agent.id} agent={agent} />
        ))}
      </div>

      <section className="surface">
        <SurfaceHeader icon={ShieldAlert} title="Open Findings" meta={`${agentRun.findings.length} findings`} />
        <FindingTable findings={agentRun.findings} />
      </section>
    </div>
  );
}

function AgentCard({ agent }: { agent: AdvisorAgent }) {
  const finding = agent.findings[0];
  return (
    <article className="agentPanel">
      <div className="agentTopline">
        <Bot size={19} />
        <span className={`scoreBadge ${scoreTone(agent.score)}`}>{agent.score}</span>
      </div>
      <h3>{agent.name}</h3>
      <p>{agent.mission}</p>
      <small>{agent.cadence}</small>
      {finding ? (
        <div className="agentFinding">
          <Severity severity={finding.severity} />
          <strong>{finding.title}</strong>
        </div>
      ) : (
        <div className="agentFinding clear">
          <CheckCircle2 size={16} />
          No open findings
        </div>
      )}
    </article>
  );
}

function RebalanceView({
  clusterId,
  overview,
  onAuditChanged
}: {
  clusterId: string;
  overview: Overview;
  onAuditChanged: () => Promise<void>;
}) {
  const [maxMovements, setMaxMovements] = useState(12);
  const [highWatermarkPercent, setHighWatermarkPercent] = useState(85);
  const [minBrokerGapPercent, setMinBrokerGapPercent] = useState(10);
  const [includeInternal, setIncludeInternal] = useState(false);
  const [sourceBrokerId, setSourceBrokerId] = useState("auto");
  const [selectedTargetIds, setSelectedTargetIds] = useState<number[]>([]);
  const [plan, setPlan] = useState<RebalancePlan | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const brokerIds = overview.brokers.map((broker) => broker.nodeId).sort((left, right) => left - right);

  function resetPlan() {
    setPlan(null);
    setCopied(false);
  }

  function toggleTargetBroker(brokerId: number, checked: boolean) {
    resetPlan();
    setSelectedTargetIds((current) =>
      checked ? [...new Set([...current, brokerId])].sort((left, right) => left - right) : current.filter((id) => id !== brokerId)
    );
  }

  async function generatePlan() {
    setBusy(true);
    setError(null);
    try {
      const nextPlan = await api.planRebalance(clusterId, {
        maxMovements,
        includeInternal,
        highWatermarkPercent,
        minBrokerGapPercent,
        sourceBrokerId: sourceBrokerId === "auto" ? undefined : Number(sourceBrokerId),
        targetBrokerIds: selectedTargetIds.length > 0 ? selectedTargetIds : undefined
      });
      setPlan(nextPlan);
      await onAuditChanged();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function executePlan() {
    if (!plan?.executable) {
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await api.executeRebalance(clusterId, plan.kafkaJsRequest);
      await onAuditChanged();
      await generatePlan();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
      setBusy(false);
    }
  }

  async function copyPayload() {
    if (!plan) {
      return;
    }
    await navigator.clipboard.writeText(JSON.stringify(plan.reassignment, null, 2));
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1600);
  }

  const brokerPressure = plan?.brokerPressure ?? brokerPressureFromOverview(overview);
  const placementComputed = Boolean(plan);

  return (
    <div className="viewStack">
      <div className="viewHeader">
        <div>
          <h2>Partition Rebalance</h2>
          <p>Disk-pressure reassignment planning from broker-local collector telemetry.</p>
        </div>
        <button className="primaryButton" type="button" disabled={busy} onClick={() => void generatePlan()}>
          <ArrowRightLeft size={17} />
          Generate plan
        </button>
      </div>

      {error ? <div className="notice error">{error}</div> : null}

      <section className="surface rebalanceControls">
        <SurfaceHeader icon={HardDrive} title="Planning Controls" meta="dry-run first" />
        <div className="rebalanceControlGrid">
          <label>
            Max movements
            <input
              type="number"
              min="1"
              max="100"
              value={maxMovements}
              onChange={(event) => {
                setMaxMovements(Number(event.target.value));
                resetPlan();
              }}
            />
          </label>
          <label>
            High-water mark %
            <input
              type="number"
              min="50"
              max="99"
              value={highWatermarkPercent}
              onChange={(event) => {
                setHighWatermarkPercent(Number(event.target.value));
                resetPlan();
              }}
            />
          </label>
          <label>
            Imbalance trigger %
            <input
              type="number"
              min="1"
              max="60"
              value={minBrokerGapPercent}
              onChange={(event) => {
                setMinBrokerGapPercent(Number(event.target.value));
                resetPlan();
              }}
            />
          </label>
          <label>
            Source broker
            <select
              value={sourceBrokerId}
              onChange={(event) => {
                setSourceBrokerId(event.target.value);
                setSelectedTargetIds((current) => current.filter((id) => String(id) !== event.target.value));
                resetPlan();
              }}
            >
              <option value="auto">Auto by disk pressure</option>
              {brokerIds.map((brokerId) => (
                <option value={brokerId} key={brokerId}>
                  Broker {brokerId}
                </option>
              ))}
            </select>
          </label>
          <label className="switch">
            <input
              checked={includeInternal}
              onChange={(event) => {
                setIncludeInternal(event.target.checked);
                resetPlan();
              }}
              type="checkbox"
            />
            Include internal topics
          </label>
          <fieldset className="targetBrokerSelector">
            <legend>Allowed target brokers</legend>
            <div>
              {brokerIds.map((brokerId) => {
                const disabled = sourceBrokerId === String(brokerId);
                return (
                  <label className="targetBrokerOption" key={brokerId}>
                    <input
                      checked={selectedTargetIds.includes(brokerId)}
                      disabled={disabled}
                      onChange={(event) => toggleTargetBroker(brokerId, event.target.checked)}
                      type="checkbox"
                    />
                    Broker {brokerId}
                  </label>
                );
              })}
            </div>
            <small>{selectedTargetIds.length === 0 ? "Any eligible broker" : `${selectedTargetIds.length} selected`}</small>
          </fieldset>
        </div>
      </section>

      <div className="brokerPressureGrid">
        {brokerPressure.map((broker) => (
          <article className="brokerPressureCard" key={broker.brokerId}>
            <div>
              <strong>Broker {broker.brokerId}</strong>
              <StatusPill tone={pressureTone(broker.disk?.pressure)} icon={HardDrive}>
                {broker.disk ? `${broker.disk.usedPercent.toFixed(1)}% used` : "no disk telemetry"}
              </StatusPill>
            </div>
            <div className="diskMeter" aria-label={`Broker ${broker.brokerId} disk usage`}>
              <span style={{ width: `${Math.min(100, broker.disk?.usedPercent ?? 0)}%` }} />
            </div>
            <div className="brokerPressureMeta">
              <span>{broker.disk ? formatBytes(broker.disk.freeBytes) : "unknown"} free</span>
              <span>{broker.disk ? `sampled ${formatAge(broker.disk.sampledAt)}` : "no disk sample"}</span>
              <span>{placementComputed ? `${broker.replicaCount} replicas / ${broker.leaderCount} leaders` : "placement after plan"}</span>
            </div>
          </article>
        ))}
      </div>

      {plan ? (
        <>
          <section className="metricStrip rebalanceSummary">
            <Metric icon={ArrowRightLeft} label="Movements" value={plan.summary.movements} detail="planned replica moves" />
            <Metric icon={HardDrive} label="Max disk" value={formatPercent(plan.summary.maxUsedPercent)} detail="highest broker usage" />
            <Metric icon={HardDrive} label="Min disk" value={formatPercent(plan.summary.minUsedPercent)} detail="lowest broker usage" />
            <Metric icon={Database} label="Partitions" value={plan.summary.partitionsEvaluated} detail="evaluated for placement" />
            <Metric icon={ShieldCheck} label="Execution" value={plan.executable ? "Ready" : "Locked"} detail={plan.executable ? "backend accepts apply" : "requires opt-in"} />
          </section>

          {plan.warnings.length > 0 || plan.executionBlockedReason ? (
            <section className="notice rebalanceWarnings">
              {plan.warnings.map((warning) => (
                <span key={warning}>{warning}</span>
              ))}
              {plan.executionBlockedReason ? <span>{plan.executionBlockedReason}</span> : null}
            </section>
          ) : null}

          <section className="surface">
            <SurfaceHeader icon={ArrowRightLeft} title="Planned Movements" meta={`${plan.movements.length} moves`} />
            <DataTable className="movementTable">
              <div className="tableRow tableHead">
                <span>Partition</span>
                <span>Source</span>
                <span>Target</span>
                <span>Replicas</span>
                <span>Risk</span>
              </div>
              {plan.movements.map((movement) => (
                <div className="tableRow" key={`${movement.topic}-${movement.partition}`}>
                  <span>
                    <strong className="mono">{movement.topic}</strong>
                    <small>partition {movement.partition}</small>
                  </span>
                  <span>Broker {movement.sourceBrokerId}</span>
                  <span>Broker {movement.targetBrokerId}</span>
                  <span className="mono">
                    [{movement.currentReplicas.join(",")}] {"->"} [{movement.proposedReplicas.join(",")}]
                  </span>
                  <StatusPill tone={movement.leaderMove ? "warning" : "good"} icon={CircleDot}>
                    {movement.leaderMove ? "leader move" : "follower move"}
                  </StatusPill>
                </div>
              ))}
            </DataTable>
            {plan.movements.length === 0 ? <EmptyState title="No partition movements generated" /> : null}
          </section>

          <section className="surface reassignmentPayload">
            <SurfaceHeader icon={ClipboardCopy} title="Reassignment Payload" meta="Kafka-compatible JSON" />
            <pre>{JSON.stringify(plan.reassignment, null, 2)}</pre>
            <footer>
              <button className="secondaryButton" type="button" onClick={() => void copyPayload()}>
                <ClipboardCopy size={17} />
                {copied ? "Copied" : "Copy JSON"}
              </button>
              <button className="primaryButton" type="button" disabled={!plan.executable || busy} onClick={() => void executePlan()}>
                <ArrowRightLeft size={17} />
                {plan.executable ? "Apply reassignment" : "Execution locked"}
              </button>
            </footer>
          </section>
        </>
      ) : (
        <section className="surface">
          <EmptyState title="Generate a plan to see proposed replica movements" />
        </section>
      )}
    </div>
  );
}

function TopicsView({
  clusterId,
  topics,
  brokerCount,
  onChanged
}: {
  clusterId: string;
  topics: Topic[];
  brokerCount: number;
  onChanged: () => void;
}) {
  const [query, setQuery] = useState("");
  const [showInternal, setShowInternal] = useState(false);
  const [creating, setCreating] = useState(false);
  const filteredTopics = topics.filter(
    (topic) => (showInternal || !topic.isInternal) && topic.name.toLowerCase().includes(query.toLowerCase())
  );

  return (
    <div className="viewStack">
      <div className="viewHeader">
        <div>
          <h2>Topics</h2>
          <p>{filteredTopics.length} live topics from Kafka metadata.</p>
        </div>
        <button className="primaryButton" type="button" onClick={() => setCreating(true)}>
          <Plus size={17} />
          Create topic
        </button>
      </div>

      <div className="filterBar">
        <SearchField value={query} onChange={setQuery} placeholder="Search topics" />
        <label className="switch">
          <input checked={showInternal} onChange={(event) => setShowInternal(event.target.checked)} type="checkbox" />
          Show internal
        </label>
      </div>

      <section className="surface">
        <DataTable className="topicTable">
          <div className="tableRow tableHead">
            <span>Topic</span>
            <span>Partitions</span>
            <span>Replicas</span>
            <span>Class</span>
          </div>
          {filteredTopics.map((topic) => (
            <div className="tableRow" key={topic.name}>
              <span className="mono strongText">{topic.name}</span>
              <span>{topic.partitions}</span>
              <span>{topic.replicas}</span>
              <StatusPill tone={topic.isInternal ? "neutral" : "good"} icon={Layers3}>
                {topic.isInternal ? "internal" : "user"}
              </StatusPill>
            </div>
          ))}
        </DataTable>
        {filteredTopics.length === 0 ? <EmptyState title="No topics match this filter" /> : null}
      </section>

      {creating ? (
        <CreateTopicDialog
          brokerCount={brokerCount}
          clusterId={clusterId}
          onClose={() => setCreating(false)}
          onCreated={() => {
            setCreating(false);
            onChanged();
          }}
        />
      ) : null}
    </div>
  );
}

function CreateTopicDialog({
  clusterId,
  brokerCount,
  onClose,
  onCreated
}: {
  clusterId: string;
  brokerCount: number;
  onClose: () => void;
  onCreated: () => void;
}) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState(3);
  const [replicationFactor, setReplicationFactor] = useState(Math.min(3, brokerCount));
  const [retentionMs, setRetentionMs] = useState("");
  const [cleanupPolicy, setCleanupPolicy] = useState<"delete" | "compact">("delete");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api.createTopic(clusterId, {
        name,
        partitions,
        replicationFactor,
        cleanupPolicy,
        retentionMs: retentionMs ? Number(retentionMs) : undefined
      });
      onCreated();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modalScrim">
      <form className="modalSurface" onSubmit={(event) => void submit(event)}>
        <header>
          <div>
            <h2>Create topic</h2>
            <p>Validated against broker count and EventHelm naming policy.</p>
          </div>
          <button className="iconButton" type="button" onClick={onClose} title="Close">
            <X size={18} />
          </button>
        </header>
        {error ? <div className="notice error">{error}</div> : null}
        <label>
          Topic name
          <input value={name} onChange={(event) => setName(event.target.value)} placeholder="orders.created" required />
        </label>
        <div className="formGrid">
          <label>
            Partitions
            <input type="number" min="1" max="200" value={partitions} onChange={(event) => setPartitions(Number(event.target.value))} />
          </label>
          <label>
            Replication factor
            <input
              type="number"
              min="1"
              max={Math.max(1, brokerCount)}
              value={replicationFactor}
              onChange={(event) => setReplicationFactor(Number(event.target.value))}
            />
          </label>
        </div>
        <div className="formGrid">
          <label>
            Cleanup policy
            <select value={cleanupPolicy} onChange={(event) => setCleanupPolicy(event.target.value as "delete" | "compact")}>
              <option value="delete">delete</option>
              <option value="compact">compact</option>
            </select>
          </label>
          <label>
            Retention ms
            <input value={retentionMs} onChange={(event) => setRetentionMs(event.target.value)} placeholder="604800000" />
          </label>
        </div>
        <div className="reviewBand">
          <ShieldCheck size={18} />
          <span>
            Creates <strong className="mono">{name || "topic.name"}</strong> with {partitions} partitions and RF {replicationFactor}.
          </span>
        </div>
        <footer>
          <button className="secondaryButton" type="button" onClick={onClose}>
            Cancel
          </button>
          <button className="primaryButton" disabled={busy} type="submit">
            <Plus size={17} />
            Create
          </button>
        </footer>
      </form>
    </div>
  );
}

function MessagesView({
  topics,
  selectedTopic,
  setSelectedTopic,
  browse,
  messages,
  clusterId,
  onProduced,
  limit,
  setLimit,
  fromBeginning,
  setFromBeginning
}: {
  topics: Topic[];
  selectedTopic: string;
  setSelectedTopic: (topic: string) => void;
  browse: () => Promise<void>;
  messages: MessageRecord[];
  clusterId: string;
  onProduced: () => void;
  limit: number;
  setLimit: (limit: number) => void;
  fromBeginning: boolean;
  setFromBeginning: (value: boolean) => void;
}) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [formatJson, setFormatJson] = useState(true);

  async function produce(event: FormEvent) {
    event.preventDefault();
    await api.produceMessage(clusterId, { topic: selectedTopic, key: key || undefined, value });
    onProduced();
  }

  return (
    <div className="viewStack">
      <div className="filterBar">
        <select value={selectedTopic} onChange={(event) => setSelectedTopic(event.target.value)}>
          {topics
            .filter((topic) => !topic.isInternal)
            .map((topic) => (
              <option key={topic.name} value={topic.name}>
                {topic.name}
              </option>
            ))}
        </select>
        <input type="number" min="1" max="100" value={limit} onChange={(event) => setLimit(Number(event.target.value))} title="Limit" />
        <label className="switch">
          <input checked={fromBeginning} onChange={(event) => setFromBeginning(event.target.checked)} type="checkbox" />
          From beginning
        </label>
        <button className="secondaryButton" type="button" onClick={() => void browse()}>
          <Activity size={17} />
          Browse
        </button>
      </div>

      <div className="messageGrid">
        <section className="surface">
          <SurfaceHeader icon={MessageSquareText} title="Records" meta={`${messages.length} loaded`} />
          <label className="inlineSwitch">
            <input checked={formatJson} onChange={(event) => setFormatJson(event.target.checked)} type="checkbox" />
            Format JSON
          </label>
          <div className="recordList">
            {messages.map((message) => (
              <article className="recordBlock" key={`${message.partition}-${message.offset}`}>
                <div>
                  <span className="mono">partition {message.partition}</span>
                  <span className="mono">offset {message.offset}</span>
                  <span>{new Date(message.timestamp).toLocaleString()}</span>
                </div>
                <pre>{formatJson ? prettyJson(message.value) : message.value}</pre>
              </article>
            ))}
            {messages.length === 0 ? <EmptyState title="No records loaded" /> : null}
          </div>
        </section>

        <form className="surface produceSurface" onSubmit={(event) => void produce(event)}>
          <SurfaceHeader icon={Send} title="Produce Record" meta="audited" />
          <label>
            Optional key
            <input value={key} onChange={(event) => setKey(event.target.value)} placeholder="message-key" />
          </label>
          <label>
            Value
            <textarea value={value} onChange={(event) => setValue(event.target.value)} placeholder='{"type":"event","source":"console"}' required />
          </label>
          <button className="primaryButton" type="submit">
            <Send size={17} />
            Produce
          </button>
        </form>
      </div>
    </div>
  );
}

function ConsumersView({ groups }: { groups: ConsumerGroup[] }) {
  const [query, setQuery] = useState("");
  const filtered = groups.filter((group) => group.groupId.toLowerCase().includes(query.toLowerCase()));
  return (
    <TableView
      title="Consumer Groups"
      subtitle={`${filtered.length} consumer groups reported by Kafka`}
      search={<SearchField value={query} onChange={setQuery} placeholder="Search groups" />}
      empty="No consumer groups found"
      rows={filtered}
      render={() => (
        <DataTable className="consumerTable">
          <div className="tableRow tableHead">
            <span>Group</span>
            <span>Protocol</span>
            <span>State</span>
            <span>Members</span>
          </div>
          {filtered.map((group) => (
            <div className="tableRow" key={group.groupId}>
              <span className="mono strongText">{group.groupId}</span>
              <span>{group.protocolType || "n/a"}</span>
              <StatusPill tone="neutral" icon={CircleDot}>
                {group.state ?? "unknown"}
              </StatusPill>
              <span>{group.members ?? 0}</span>
            </div>
          ))}
        </DataTable>
      )}
    />
  );
}

function CollectorsView({ collectors, brokerCount }: { collectors: CollectorState[]; brokerCount: number }) {
  const [query, setQuery] = useState("");
  const filtered = collectors.filter((collector) => collector.heartbeat.collectorId.toLowerCase().includes(query.toLowerCase()));
  return (
    <TableView
      title="Broker Collectors"
      subtitle={`${collectors.length} registered for ${brokerCount} brokers`}
      search={<SearchField value={query} onChange={setQuery} placeholder="Search collectors" />}
      empty="No collectors found"
      rows={filtered}
      render={() => (
        <DataTable className="collectorTable">
          <div className="tableRow tableHead">
            <span>Collector</span>
            <span>Broker</span>
            <span>Status</span>
            <span>Last seen</span>
            <span>Disk</span>
            <span>Snapshot</span>
          </div>
          {filtered.map((collector) => {
            const freshness = collectorFreshness(collector);
            return (
              <div className="tableRow" key={collector.heartbeat.collectorId}>
                <span className="mono strongText">{collector.heartbeat.collectorId}</span>
                <span>{collector.heartbeat.brokerId}</span>
                <StatusPill tone={freshness === "online" ? "good" : "warning"} icon={CircleDot}>
                  {freshness}
                </StatusPill>
                <span>{formatAge(collector.heartbeat.observedAt)}</span>
                <StatusPill tone={pressureTone(collector.lastSnapshot?.disk?.pressure)} icon={HardDrive}>
                  {collector.lastSnapshot?.disk ? `${collector.lastSnapshot.disk.usedPercent.toFixed(1)}%` : "unknown"}
                </StatusPill>
                <span>
                  {collector.lastSnapshot?.brokerCount ?? 0} brokers / {collector.lastSnapshot?.topicCount ?? 0} topics
                </span>
              </div>
            );
          })}
        </DataTable>
      )}
    />
  );
}

function AuditView({ audit }: { audit: AuditEvent[] }) {
  const [query, setQuery] = useState("");
  const filtered = audit.filter((event) =>
    `${event.action} ${event.resourceName ?? ""} ${event.actor}`.toLowerCase().includes(query.toLowerCase())
  );
  return (
    <TableView
      title="Audit"
      subtitle={`${filtered.length} in-memory audit events`}
      search={<SearchField value={query} onChange={setQuery} placeholder="Search audit" />}
      empty="No audit events found"
      rows={filtered}
      render={() => (
        <DataTable className="auditTable">
          <div className="tableRow tableHead">
            <span>Time</span>
            <span>Action</span>
            <span>Resource</span>
            <span>Actor</span>
            <span>Details</span>
          </div>
          {filtered.map((event) => (
            <div className="tableRow" key={event.id}>
              <span>{new Date(event.createdAt).toLocaleString()}</span>
              <span className="mono strongText">{event.action}</span>
              <span>{event.resourceName ?? event.resourceType ?? "n/a"}</span>
              <span>{event.actor}</span>
              <span className="mono detailText">{event.details ? JSON.stringify(event.details) : "n/a"}</span>
            </div>
          ))}
        </DataTable>
      )}
    />
  );
}

function TableView<T>({
  title,
  subtitle,
  search,
  empty,
  rows,
  render
}: {
  title: string;
  subtitle: string;
  search: ReactNode;
  empty: string;
  rows: T[];
  render: () => ReactNode;
}) {
  return (
    <div className="viewStack">
      <div className="viewHeader">
        <div>
          <h2>{title}</h2>
          <p>{subtitle}</p>
        </div>
      </div>
      <div className="filterBar">{search}</div>
      <section className="surface">
        {render()}
        {rows.length === 0 ? <EmptyState title={empty} /> : null}
      </section>
    </div>
  );
}

function Metric({ icon: Icon, label, value, detail }: { icon: LucideIcon; label: string; value: number | string; detail: string }) {
  return (
    <article className="metric">
      <Icon size={18} />
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

function SurfaceHeader({ icon: Icon, title, meta }: { icon: LucideIcon; title: string; meta: string }) {
  return (
    <header className="surfaceHeader">
      <div>
        <Icon size={18} />
        <h2>{title}</h2>
      </div>
      <span>{meta}</span>
    </header>
  );
}

function DataTable({ className, children }: { className: string; children: ReactNode }) {
  return <div className={`dataTable ${className}`}>{children}</div>;
}

function FindingList({ findings }: { findings: AgentFinding[] }) {
  if (findings.length === 0) {
    return <EmptyState title="No advisor findings" />;
  }
  return (
    <div className="findingList">
      {findings.map((finding) => (
        <article key={finding.id}>
          <Severity severity={finding.severity} />
          <div>
            <strong>{finding.title}</strong>
            <p>{finding.recommendation}</p>
          </div>
        </article>
      ))}
    </div>
  );
}

function FindingTable({ findings }: { findings: AgentFinding[] }) {
  return (
    <DataTable className="findingTable">
      <div className="tableRow tableHead">
        <span>Severity</span>
        <span>Agent</span>
        <span>Finding</span>
        <span>Recommendation</span>
      </div>
      {findings.map((finding) => (
        <div className="tableRow" key={finding.id}>
          <Severity severity={finding.severity} />
          <span className="mono">{finding.agentId}</span>
          <span>
            <strong>{finding.title}</strong>
            <small>{finding.summary}</small>
          </span>
          <span>{finding.recommendation}</span>
        </div>
      ))}
    </DataTable>
  );
}

function AuditTimeline({ audit }: { audit: AuditEvent[] }) {
  if (audit.length === 0) {
    return <EmptyState title="No audit events yet" />;
  }
  return (
    <div className="timeline">
      {audit.map((event) => (
        <article key={event.id}>
          <Clock3 size={16} />
          <div>
            <strong>{event.action}</strong>
            <span>
              {event.resourceName ?? event.resourceType ?? "platform"} by {event.actor}
            </span>
          </div>
        </article>
      ))}
    </div>
  );
}

function StatusPill({ tone, icon: Icon, children }: { tone: "good" | "warning" | "bad" | "neutral"; icon: LucideIcon; children: ReactNode }) {
  return (
    <span className={`statusPill ${tone}`}>
      <Icon size={13} />
      {children}
    </span>
  );
}

function Severity({ severity }: { severity: AgentFinding["severity"] }) {
  return <span className={`severity ${severity}`}>{severity}</span>;
}

function SearchField({ value, onChange, placeholder }: { value: string; onChange: (value: string) => void; placeholder: string }) {
  return (
    <div className="searchField">
      <Search size={16} />
      <input value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} />
    </div>
  );
}

function EmptyState({ title }: { title: string }) {
  return (
    <div className="emptyState">
      <AlertTriangle size={18} />
      <span>{title}</span>
    </div>
  );
}

function brokerPressureFromOverview(overview: Overview): RebalancePlan["brokerPressure"] {
  const collectorsByBroker = new Map(overview.collectors.map((collector) => [Number(collector.heartbeat.brokerId), collector]));
  return overview.brokers.map((broker) => {
    const collector = collectorsByBroker.get(broker.nodeId);
    return {
      brokerId: broker.nodeId,
      host: broker.host,
      port: broker.port,
      replicaCount: 0,
      leaderCount: 0,
      disk: collector?.lastSnapshot?.disk
    };
  });
}

function pressureTone(pressure?: "normal" | "watch" | "high" | "critical"): "good" | "warning" | "bad" | "neutral" {
  if (pressure === "critical" || pressure === "high") {
    return "bad";
  }
  if (pressure === "watch") {
    return "warning";
  }
  if (pressure === "normal") {
    return "good";
  }
  return "neutral";
}

function formatBytes(value?: number) {
  if (value === undefined) {
    return "unknown";
  }
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let next = value;
  let unit = 0;
  while (next >= 1024 && unit < units.length - 1) {
    next /= 1024;
    unit += 1;
  }
  return `${next.toFixed(next >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatPercent(value?: number) {
  return value === undefined ? "n/a" : `${value.toFixed(1)}%`;
}

function summarizePosture(overview: Overview | null, agentRun: AgentRun | null) {
  const critical = agentRun?.findings.some((finding) => finding.severity === "critical");
  const high = agentRun?.findings.some((finding) => finding.severity === "high");
  const freshCollectors = overview?.collectors.filter((collector) => collectorFreshness(collector) === "online").length ?? 0;
  if (critical) {
    return { label: "Critical", detail: "Immediate action required", tone: "bad" };
  }
  if (high) {
    return { label: "Attention", detail: "High-priority findings open", tone: "warning" };
  }
  return { label: "Operational", detail: `${freshCollectors} collectors fresh`, tone: "good" };
}

function collectorFreshness(collector: CollectorState): "online" | "stale" {
  return Date.now() - new Date(collector.heartbeat.observedAt).getTime() <= 30_000 ? "online" : "stale";
}

function formatAge(iso: string) {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  return `${Math.round(seconds / 60)}m ago`;
}

function prettyJson(value?: string) {
  if (!value) {
    return "";
  }
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function scoreTone(score: number) {
  if (score >= 85) {
    return "good";
  }
  if (score >= 65) {
    return "warning";
  }
  return "bad";
}
