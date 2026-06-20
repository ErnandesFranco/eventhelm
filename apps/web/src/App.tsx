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
import type { CSSProperties, ReactNode } from "react";
import type {
  AdvisorAgent,
  AgentFinding,
  AgentRun,
  AgentRunRecord,
  AuditEvent,
  Cluster,
  CollectorState,
  ConsumerGroup,
  ConsumerGroupLag,
  ConsumerOffsetResetMode,
  ConsumerOffsetResetPreview,
  MessageRecord,
  Overview,
  RebalancePlan,
  SecurityStatus,
  Topic,
  TopicConfig,
  TopicConfigUpdatePreview
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
  const [agentHistory, setAgentHistory] = useState<AgentRunRecord[]>([]);
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
      const [loadedOverview, loadedTopics, loadedGroups, loadedCollectors, loadedAudit] = await Promise.all([
        api.overview(nextClusterId),
        api.topics(nextClusterId),
        api.consumerGroups(nextClusterId),
        api.collectors(),
        api.audit()
      ]);
      const loadedAgents = await api.agents(nextClusterId);
      const loadedAgentHistory = await api.agentRuns(nextClusterId);
      setOverview(loadedOverview);
      setTopics(loadedTopics);
      setGroups(loadedGroups);
      setCollectors(loadedCollectors);
      setAudit(loadedAudit);
      setAgentRun(loadedAgents);
      setAgentHistory(loadedAgentHistory);
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
      setAgentHistory(await api.agentRuns(selectedClusterId));
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
        {!loading && activeTab === "agents" && agentRun ? (
          <AgentsView agentRun={agentRun} history={agentHistory} onRunAgents={runAgents} />
        ) : null}
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
        {!loading && activeTab === "consumers" ? (
          <ConsumersView clusterId={selectedClusterId} groups={groups} onChanged={() => void load()} />
        ) : null}
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
  const brokerCards = [...overview.brokers]
    .sort((left, right) => left.nodeId - right.nodeId)
    .map((broker) => {
      const collector = collectorByBroker.get(String(broker.nodeId));
      const freshness = collector ? collectorFreshness(collector) : "missing";
      return {
        broker,
        collector,
        disk: collector?.lastSnapshot?.disk,
        freshness,
        isController: overview.controllerId === broker.nodeId
      };
    });
  const onlineCollectors = brokerCards.filter((card) => card.freshness === "online").length;
  const diskValues = brokerCards
    .map((card) => card.disk?.usedPercent)
    .filter((value): value is number => value !== undefined);
  const averageDisk = diskValues.length ? diskValues.reduce((total, value) => total + value, 0) / diskValues.length : undefined;
  const hottestBroker = brokerCards
    .filter((card) => card.disk)
    .sort((left, right) => (right.disk?.usedPercent ?? 0) - (left.disk?.usedPercent ?? 0))[0];
  const laneCount = overview.brokers.length > 14 ? 4 : overview.brokers.length > 6 ? 3 : overview.brokers.length > 3 ? 2 : 1;
  const brokerLanes = Array.from({ length: laneCount }, (_, laneIndex) =>
    brokerCards.filter((_, brokerIndex) => brokerIndex % laneCount === laneIndex)
  );
  const collectorTone = onlineCollectors === overview.brokers.length ? "good" : onlineCollectors === 0 ? "bad" : "warning";
  const storageTone = pressureTone(hottestBroker?.disk?.pressure);
  const laneStyle = { "--topology-lanes": brokerLanes.length } as CSSProperties;

  return (
    <div className={`topologyMap ${density}`}>
      <div className="topologyHub">
        <div className="hubTopline">
          <div className="hubMark">
            <img src={eventhelmMark} alt="" />
          </div>
          <span className="hubStatus">
            <Activity size={13} />
            live
          </span>
        </div>
        <div>
          <strong>EventHelm API</strong>
          <p>{overview.kafkaClusterId ?? overview.clusterName}</p>
        </div>
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
        <div className="topologyMiniMap" aria-label="Broker status overview">
          {brokerCards.map((card) => (
            <span
              className={`miniBroker ${card.freshness} ${card.isController ? "controller" : ""}`}
              key={card.broker.nodeId}
              title={`Broker ${card.broker.nodeId}: ${card.freshness}`}
            />
          ))}
        </div>
      </div>

      <div className="topologyBackplane">
        <div className="topologyVitals">
          <div className={`topologyVital ${collectorTone}`}>
            <RadioTower size={15} />
            <span>Collectors</span>
            <strong>
              {onlineCollectors}/{overview.brokers.length}
            </strong>
          </div>
          <div className={`topologyVital ${storageTone}`}>
            <HardDrive size={15} />
            <span>Disk</span>
            <strong>{averageDisk === undefined ? "n/a" : `${averageDisk.toFixed(1)}% avg`}</strong>
          </div>
          <div className="topologyVital neutral">
            <Gauge size={15} />
            <span>Hottest</span>
            <strong>{hottestBroker ? `broker ${hottestBroker.broker.nodeId}` : "n/a"}</strong>
          </div>
          <div className="topologyVital neutral">
            <Database size={15} />
            <span>Topics</span>
            <strong>{overview.topicCount}</strong>
          </div>
        </div>

        <div className="topologyRail">
          <span>
            <Network size={14} />
            Kafka fabric
          </span>
          <i />
          <span>
            <Layers3 size={14} />
            {brokerLanes.length} lanes
          </span>
        </div>

        <div className={`brokerFleet ${density}`} style={laneStyle}>
          {brokerLanes.map((lane, laneIndex) => (
            <section className="brokerBank" key={laneIndex}>
              <div className="brokerBankHeader">
                <span>Lane {String.fromCharCode(65 + laneIndex)}</span>
                <small>{lane.length} brokers</small>
              </div>
              <div className={`brokerLane ${density}`}>
                {lane.map((card) => (
                  <article className={`brokerNode ${card.freshness} ${card.isController ? "controller" : ""}`} key={card.broker.nodeId}>
                    <div className="brokerNodeHeader">
                      <span className="brokerIcon">
                        <Server size={17} />
                      </span>
                      <div>
                        <strong>Broker {card.broker.nodeId}</strong>
                        <span className="mono">
                          {card.broker.host}:{card.broker.port}
                        </span>
                      </div>
                      <span className={`nodeHealth ${card.freshness}`} title={`Collector ${card.freshness}`} />
                    </div>
                    {card.isController ? (
                      <span className="nodeRole">
                        <CircleDot size={11} />
                        controller
                      </span>
                    ) : null}
                    <div className="brokerSignalGrid">
                      <span>
                        <RadioTower size={13} />
                        {card.collector ? `${card.freshness} ${formatAge(card.collector.heartbeat.observedAt)}` : "collector missing"}
                      </span>
                      <span>
                        <HardDrive size={13} />
                        {card.disk ? `${card.disk.usedPercent.toFixed(1)}% used` : "disk unknown"}
                      </span>
                    </div>
                    <div className="brokerDiskFooter">
                      <div className="brokerDiskMeter" aria-label={`Broker ${card.broker.nodeId} disk usage`}>
                        <span
                          className={card.disk?.pressure ?? "unknown"}
                          style={{ width: `${Math.min(100, card.disk?.usedPercent ?? 0)}%` }}
                        />
                      </div>
                      <span>{card.disk ? formatBytes(card.disk.usedBytes) : "n/a"}</span>
                    </div>
                  </article>
                ))}
              </div>
            </section>
          ))}
        </div>
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

function AgentsView({
  agentRun,
  history,
  onRunAgents
}: {
  agentRun: AgentRun;
  history: AgentRunRecord[];
  onRunAgents: () => Promise<void>;
}) {
  const highestFinding = agentRun.findings[0];

  return (
    <div className="viewStack">
      <div className="viewHeader">
        <div>
          <h2>Advisor Agents</h2>
          <p>
            Last sweep {formatAge(agentRun.generatedAt)} by {agentRun.actor ?? "system"} across live cluster, collector,
            security, and governance state.
          </p>
        </div>
        <button className="primaryButton" type="button" onClick={() => void onRunAgents()}>
          <Activity size={17} />
          Run checks
        </button>
      </div>

      <section className="agentRunStrip" aria-label="Advisor run summary">
        <div>
          <span>Posture score</span>
          <strong>{agentRun.summary.score}</strong>
        </div>
        <div>
          <span>Open findings</span>
          <strong>{agentRun.summary.findings}</strong>
        </div>
        <div>
          <span>High priority</span>
          <strong>{agentRun.summary.critical + agentRun.summary.high}</strong>
        </div>
        <div>
          <span>Top evidence</span>
          <strong>{highestFinding?.title ?? "Clear"}</strong>
        </div>
      </section>

      <div className="agentBoard">
        {agentRun.agents.map((agent) => (
          <AgentCard key={agent.id} agent={agent} />
        ))}
      </div>

      <section className="surface">
        <SurfaceHeader icon={ShieldAlert} title="Open Findings" meta={`${agentRun.findings.length} findings`} />
        <FindingTable findings={agentRun.findings} />
      </section>

      <section className="surface">
        <SurfaceHeader icon={FileClock} title="Run History" meta={`${history.length} retained`} />
        <AgentRunHistory history={history} />
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
      await api.executeRebalance(clusterId, plan.id);
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
          <label htmlFor="rebalance-max-movements">
            Max movements
            <input
              id="rebalance-max-movements"
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
          <label htmlFor="rebalance-high-watermark">
            High-water mark %
            <input
              id="rebalance-high-watermark"
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
          <label htmlFor="rebalance-min-gap">
            Imbalance trigger %
            <input
              id="rebalance-min-gap"
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
          <div className="fieldControl">
            <label htmlFor="rebalance-source-broker">Source broker</label>
            <select
              id="rebalance-source-broker"
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
          </div>
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
              <span>{broker.logBytes !== undefined ? `${formatBytes(broker.logBytes)} logs` : "no log sizes"}</span>
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
            <Metric icon={Database} label="Data moved" value={formatBytes(plan.summary.estimatedBytesMoved)} detail="estimated log bytes" />
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
                <span>Size</span>
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
                  <span>{formatBytes(movement.estimatedSizeBytes)}</span>
                  <StatusPill tone={movement.leaderMove ? "warning" : "good"} icon={CircleDot}>
                    {movement.leaderMove ? "leader move" : "follower move"}
                  </StatusPill>
                </div>
              ))}
            </DataTable>
            {plan.movements.length === 0 ? <EmptyState title="No partition movements generated" /> : null}
          </section>

          <section className="surface reassignmentPayload">
            <SurfaceHeader icon={ClipboardCopy} title="Reassignment Payload" meta={`Plan ${plan.id.slice(0, 8)}`} />
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
  const [selectedConfig, setSelectedConfig] = useState<TopicConfig | null>(null);
  const [configBusyTopic, setConfigBusyTopic] = useState<string | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const filteredTopics = topics.filter(
    (topic) => (showInternal || !topic.isInternal) && topic.name.toLowerCase().includes(query.toLowerCase())
  );

  async function inspectTopicConfig(topic: string) {
    setConfigBusyTopic(topic);
    setConfigError(null);
    try {
      setSelectedConfig(await api.topicConfig(clusterId, topic));
    } catch (caught) {
      setConfigError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setConfigBusyTopic(null);
    }
  }

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
            <span>Config</span>
          </div>
          {filteredTopics.map((topic) => (
            <div className="tableRow" key={topic.name}>
              <span className="mono strongText">{topic.name}</span>
              <span>{topic.partitions}</span>
              <span>{topic.replicas}</span>
              <StatusPill tone={topic.isInternal ? "neutral" : "good"} icon={Layers3}>
                {topic.isInternal ? "internal" : "user"}
              </StatusPill>
              <button
                className="secondaryButton compactButton"
                type="button"
                disabled={configBusyTopic === topic.name}
                onClick={() => void inspectTopicConfig(topic.name)}
              >
                <Search size={15} />
                {configBusyTopic === topic.name ? "Loading" : "Config"}
              </button>
            </div>
          ))}
        </DataTable>
        {filteredTopics.length === 0 ? <EmptyState title="No topics match this filter" /> : null}
      </section>

      {configError ? <div className="notice error">{configError}</div> : null}

      {selectedConfig ? (
        <TopicConfigPanel
          clusterId={clusterId}
          config={selectedConfig}
          onChanged={async () => {
            setSelectedConfig(await api.topicConfig(clusterId, selectedConfig.topic));
            onChanged();
          }}
        />
      ) : null}

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

function TopicConfigPanel({
  clusterId,
  config,
  onChanged
}: {
  clusterId: string;
  config: TopicConfig;
  onChanged: () => Promise<void>;
}) {
  const editableEntries = config.editable
    .map((name) => config.entries.find((entry) => entry.name === name))
    .filter((entry): entry is TopicConfig["entries"][number] => Boolean(entry));
  const [draft, setDraft] = useState<Record<string, string>>(() =>
    Object.fromEntries(editableEntries.map((entry) => [entry.name, entry.value]))
  );
  const [preview, setPreview] = useState<TopicConfigUpdatePreview | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setDraft(Object.fromEntries(editableEntries.map((entry) => [entry.name, entry.value])));
    setPreview(null);
    setNotice(null);
    setError(null);
  }, [config.generatedAt, config.topic]);

  const effectiveConfigs = editableEntries
    .map((entry) => ({ name: entry.name, value: draft[entry.name] ?? entry.value, current: entry.value }))
    .filter((entry) => entry.value !== entry.current)
    .map(({ name, value }) => ({ name, value }));

  async function previewConfig(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      setPreview(await api.previewTopicConfig(clusterId, config.topic, { configs: effectiveConfigs }));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  async function applyConfig() {
    if (!preview) {
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const result = await api.applyTopicConfig(clusterId, config.topic, {
        configs: preview.changes.map((change) => ({ name: change.name, value: change.newValue })),
        reviewToken: preview.reviewToken
      });
      await onChanged();
      setNotice(`Applied ${result.changes.length} config changes.`);
      setPreview(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="surface topicConfigSurface">
      <SurfaceHeader icon={ShieldCheck} title={config.topic} meta={`${config.entries.length} configs`} />
      {error ? <div className="notice error">{error}</div> : null}
      {notice ? <div className="notice success">{notice}</div> : null}
      <form className="topicConfigEditor" onSubmit={(event) => void previewConfig(event)}>
        {editableEntries.map((entry) => (
          <label key={entry.name}>
            {entry.name}
            <input
              value={draft[entry.name] ?? entry.value}
              onChange={(event) => {
                setDraft((current) => ({ ...current, [entry.name]: event.target.value }));
                setPreview(null);
              }}
            />
          </label>
        ))}
        <button className="primaryButton" type="submit" disabled={busy || effectiveConfigs.length === 0}>
          <Search size={17} />
          {busy ? "Reviewing" : "Preview changes"}
        </button>
      </form>

      <DataTable className="topicConfigTable">
        <div className="tableRow tableHead">
          <span>Config</span>
          <span>Value</span>
          <span>Source</span>
          <span>Mode</span>
        </div>
        {config.entries
          .filter((entry) => config.editable.includes(entry.name) || !entry.isDefault)
          .map((entry) => (
            <div className="tableRow" key={entry.name}>
              <span className="mono strongText">{entry.name}</span>
              <span className="mono detailText">{entry.isSensitive ? "sensitive" : entry.value}</span>
              <span>{topicConfigSource(entry.source)}</span>
              <StatusPill tone={entry.readOnly ? "neutral" : entry.isDefault ? "neutral" : "good"} icon={CircleDot}>
                {entry.readOnly ? "read-only" : entry.isDefault ? "default" : "override"}
              </StatusPill>
            </div>
          ))}
      </DataTable>

      {preview ? (
        <div className="topicConfigPreview">
          {preview.warnings.length > 0 ? (
            <div className="resetWarnings">
              {preview.warnings.map((warning) => (
                <span key={warning}>
                  <AlertTriangle size={14} />
                  {warning}
                </span>
              ))}
            </div>
          ) : null}

          <DataTable className="topicConfigChangeTable">
            <div className="tableRow tableHead">
              <span>Config</span>
              <span>Current</span>
              <span>Next</span>
              <span>Status</span>
            </div>
            {preview.changes.map((change) => (
              <div className="tableRow" key={change.name}>
                <span className="mono strongText">{change.name}</span>
                <span className="mono">{change.currentValue ?? "unknown"}</span>
                <span className="mono">{change.newValue}</span>
                {change.blockedReason ? (
                  <StatusPill tone="bad" icon={AlertTriangle}>
                    blocked
                  </StatusPill>
                ) : (
                  <StatusPill tone="good" icon={CheckCircle2}>
                    ready
                  </StatusPill>
                )}
              </div>
            ))}
          </DataTable>

          <footer className="offsetResetFooter">
            <span className="mono">{preview.reviewToken.slice(0, 12)}</span>
            <button className="primaryButton" type="button" disabled={!preview.executable || busy} onClick={() => void applyConfig()}>
              <ShieldCheck size={17} />
              Apply reviewed config
            </button>
          </footer>
        </div>
      ) : null}
    </section>
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

function ConsumersView({ clusterId, groups, onChanged }: { clusterId: string; groups: ConsumerGroup[]; onChanged: () => void }) {
  const [query, setQuery] = useState("");
  const [selectedLag, setSelectedLag] = useState<ConsumerGroupLag | null>(null);
  const [busyGroup, setBusyGroup] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [resetTopic, setResetTopic] = useState("");
  const [resetMode, setResetMode] = useState<ConsumerOffsetResetMode>("latest");
  const [resetOffset, setResetOffset] = useState("");
  const [resetPartitions, setResetPartitions] = useState("");
  const [resetPreview, setResetPreview] = useState<ConsumerOffsetResetPreview | null>(null);
  const [resetBusy, setResetBusy] = useState(false);
  const [resetNotice, setResetNotice] = useState<string | null>(null);
  const filtered = groups.filter((group) => group.groupId.toLowerCase().includes(query.toLowerCase()));
  const totalLag = groups.reduce((total, group) => total + (group.lag?.total ?? 0), 0);
  const laggingGroups = groups.filter((group) => (group.lag?.total ?? 0) > 0).length;

  async function inspectLag(groupId: string) {
    setBusyGroup(groupId);
    setError(null);
    try {
      const lag = await api.consumerGroupLag(clusterId, groupId);
      setSelectedLag(lag);
      setResetTopic(lag.topics[0]?.topic ?? "");
      setResetPreview(null);
      setResetNotice(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setBusyGroup(null);
    }
  }

  async function previewOffsetReset(event: FormEvent) {
    event.preventDefault();
    if (!selectedLag || !resetTopic) {
      return;
    }
    setResetBusy(true);
    setError(null);
    setResetNotice(null);
    try {
      setResetPreview(
        await api.previewOffsetReset(clusterId, selectedLag.groupId, {
          topic: resetTopic,
          mode: resetMode,
          partitions: parseResetPartitions(resetPartitions),
          offset: resetMode === "absolute" ? resetOffset : undefined
        })
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setResetBusy(false);
    }
  }

  async function executeOffsetReset() {
    if (!resetPreview) {
      return;
    }
    setResetBusy(true);
    setError(null);
    setResetNotice(null);
    try {
      const result = await api.executeOffsetReset(clusterId, resetPreview.groupId, {
        ...resetPreview.request,
        reviewToken: resetPreview.reviewToken
      });
      setResetPreview(null);
      await inspectLag(resetPreview.groupId);
      setResetNotice(`Offset reset accepted for ${result.summary.partitions} partitions.`);
      onChanged();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setResetBusy(false);
    }
  }

  return (
    <div className="viewStack">
      <div className="viewHeader">
        <div>
          <h2>Consumer Groups</h2>
          <p>{filtered.length} consumer groups reported by Kafka</p>
        </div>
      </div>

      <section className="metricStrip">
        <Metric icon={Users} label="Groups" value={groups.length} detail="known coordinators" />
        <Metric icon={Gauge} label="Total lag" value={totalLag.toLocaleString()} detail="records behind log end" />
        <Metric icon={AlertTriangle} label="Lagging" value={laggingGroups} detail="groups above zero" />
        <Metric
          icon={CircleDot}
          label="Unknown offsets"
          value={groups.reduce((total, group) => total + (group.lag?.unknownOffsets ?? 0), 0)}
          detail="uncommitted partitions"
        />
      </section>

      <div className="filterBar">
        <SearchField value={query} onChange={setQuery} placeholder="Search groups" />
      </div>
      {error ? <div className="notice error">{error}</div> : null}
      {resetNotice ? <div className="notice success">{resetNotice}</div> : null}

      <section className="surface">
        <DataTable className="consumerTable">
          <div className="tableRow tableHead">
            <span>Group</span>
            <span>State</span>
            <span>Lag</span>
            <span>Topics</span>
            <span>Members</span>
            <span>Inspect</span>
          </div>
          {filtered.map((group) => (
            <div className="tableRow" key={group.groupId}>
              <span>
                <strong className="mono">{group.groupId}</strong>
                <small>{group.protocolType || "n/a"}</small>
              </span>
              <StatusPill tone={group.state === "Stable" ? "good" : "neutral"} icon={CircleDot}>
                {group.state ?? "unknown"}
              </StatusPill>
              <StatusPill tone={lagTone(group.lag?.total ?? 0)} icon={Gauge}>
                {(group.lag?.total ?? 0).toLocaleString()}
              </StatusPill>
              <span>
                <strong>{group.lag?.topics ?? 0}</strong>
                <small>{group.lag?.partitions ?? 0} partitions</small>
              </span>
              <span>{group.members ?? 0}</span>
              <button className="secondaryButton compactButton" type="button" onClick={() => void inspectLag(group.groupId)}>
                <Search size={15} />
                {busyGroup === group.groupId ? "Loading" : "Lag"}
              </button>
            </div>
          ))}
        </DataTable>
        {filtered.length === 0 ? <EmptyState title="No consumer groups found" /> : null}
      </section>

      {selectedLag ? (
        <div className="consumerDetailGrid">
          <section className="surface">
            <SurfaceHeader
              icon={Gauge}
              title={selectedLag.groupId}
              meta={`${selectedLag.totalLag.toLocaleString()} records behind`}
            />
            <DataTable className="lagTable">
              <div className="tableRow tableHead">
                <span>Topic</span>
                <span>Partition</span>
                <span>Committed</span>
                <span>Log end</span>
                <span>Lag</span>
              </div>
              {selectedLag.topics.flatMap((topic) =>
                topic.partitions.map((partition) => (
                  <div className="tableRow" key={`${topic.topic}-${partition.partition}`}>
                    <span className="mono strongText">{topic.topic}</span>
                    <span>{partition.partition}</span>
                    <span className="mono">{partition.currentOffset ?? "unknown"}</span>
                    <span className="mono">{partition.logEndOffset}</span>
                    <StatusPill tone={lagTone(partition.lag ?? 0)} icon={Gauge}>
                      {partition.lag === undefined ? "unknown" : partition.lag.toLocaleString()}
                    </StatusPill>
                  </div>
                ))
              )}
            </DataTable>
            {selectedLag.topics.length === 0 ? <EmptyState title="No committed offsets for this group" /> : null}
          </section>

          <section className="surface offsetResetSurface">
            <SurfaceHeader icon={ArrowRightLeft} title="Offset Reset" meta="review required" />
            <form className="offsetResetControls" onSubmit={(event) => void previewOffsetReset(event)}>
              <label>
                Topic
                <select
                  value={resetTopic}
                  onChange={(event) => {
                    setResetTopic(event.target.value);
                    setResetPreview(null);
                  }}
                >
                  {selectedLag.topics.map((topic) => (
                    <option value={topic.topic} key={topic.topic}>
                      {topic.topic}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Mode
                <select
                  value={resetMode}
                  onChange={(event) => {
                    setResetMode(event.target.value as ConsumerOffsetResetMode);
                    setResetPreview(null);
                  }}
                >
                  <option value="latest">Latest</option>
                  <option value="earliest">Earliest</option>
                  <option value="absolute">Absolute</option>
                </select>
              </label>
              <label>
                Offset
                <input
                  disabled={resetMode !== "absolute"}
                  inputMode="numeric"
                  min="0"
                  value={resetOffset}
                  onChange={(event) => {
                    setResetOffset(event.target.value);
                    setResetPreview(null);
                  }}
                  placeholder="0"
                />
              </label>
              <label>
                Partitions
                <input
                  value={resetPartitions}
                  onChange={(event) => {
                    setResetPartitions(event.target.value);
                    setResetPreview(null);
                  }}
                  placeholder="all"
                />
              </label>
              <button className="primaryButton" type="submit" disabled={!resetTopic || resetBusy}>
                <Search size={17} />
                {resetBusy ? "Reviewing" : "Preview"}
              </button>
            </form>

            {resetPreview ? (
              <div className="offsetResetReview">
                <div className="offsetResetSummary">
                  <Metric icon={Gauge} label="Before" value={formatIntegerString(resetPreview.summary.lagBefore)} detail="lag" />
                  <Metric icon={Gauge} label="After" value={formatIntegerString(resetPreview.summary.lagAfter)} detail="projected lag" />
                  <Metric
                    icon={ArrowRightLeft}
                    label="Skipped"
                    value={formatIntegerString(resetPreview.summary.messagesSkipped)}
                    detail="records"
                  />
                  <Metric
                    icon={RefreshCw}
                    label="Replay"
                    value={formatIntegerString(resetPreview.summary.messagesToReplay)}
                    detail="records"
                  />
                </div>

                {resetPreview.warnings.length > 0 ? (
                  <div className="resetWarnings">
                    {resetPreview.warnings.map((warning) => (
                      <span key={warning}>
                        <AlertTriangle size={14} />
                        {warning}
                      </span>
                    ))}
                  </div>
                ) : null}

                <DataTable className="offsetResetTable">
                  <div className="tableRow tableHead">
                    <span>Topic</span>
                    <span>Partition</span>
                    <span>Current</span>
                    <span>Target</span>
                    <span>Delta</span>
                    <span>Status</span>
                  </div>
                  {resetPreview.topics.flatMap((topic) =>
                    topic.partitions.map((partition) => (
                      <div className="tableRow" key={`${topic.topic}-${partition.partition}`}>
                        <span className="mono strongText">{topic.topic}</span>
                        <span>{partition.partition}</span>
                        <span className="mono">{partition.currentOffset ?? "unset"}</span>
                        <span className="mono">{partition.proposedOffset}</span>
                        <span className="mono">{formatSignedIntegerString(partition.delta)}</span>
                        {partition.blockedReason ? (
                          <StatusPill tone="bad" icon={AlertTriangle}>
                            blocked
                          </StatusPill>
                        ) : (
                          <StatusPill tone="good" icon={CheckCircle2}>
                            ready
                          </StatusPill>
                        )}
                      </div>
                    ))
                  )}
                </DataTable>

                <footer className="offsetResetFooter">
                  <span className="mono">{resetPreview.reviewToken.slice(0, 12)}</span>
                  <button
                    className="primaryButton"
                    type="button"
                    disabled={!resetPreview.executable || resetBusy}
                    onClick={() => void executeOffsetReset()}
                  >
                    <ArrowRightLeft size={17} />
                    Execute reviewed reset
                  </button>
                </footer>
              </div>
            ) : null}
          </section>
        </div>
      ) : null}
    </div>
  );
}

function parseResetPartitions(value: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  return trimmed.split(",").map((part) => {
    const normalized = part.trim();
    if (!normalized) {
      throw new Error("Partitions must be comma-separated non-negative integers.");
    }
    const partition = Number(normalized);
    if (!Number.isInteger(partition) || partition < 0) {
      throw new Error("Partitions must be comma-separated non-negative integers.");
    }
    return partition;
  });
}

function formatIntegerString(value: string) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed.toLocaleString() : value;
}

function formatSignedIntegerString(value?: string) {
  if (!value) {
    return "n/a";
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    return value.startsWith("-") ? value : `+${value}`;
  }
  return parsed > 0 ? `+${parsed.toLocaleString()}` : parsed.toLocaleString();
}

function topicConfigSource(source: number) {
  const sources: Record<number, string> = {
    1: "topic",
    2: "broker",
    3: "broker default",
    4: "static broker",
    5: "default",
    6: "logger"
  };
  return sources[source] ?? "unknown";
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
        <span>Resource</span>
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
          <span>
            <strong>{finding.resourceName ?? finding.resourceType ?? "platform"}</strong>
            <small>{finding.resourceType ?? "control plane"}</small>
          </span>
          <span>{finding.recommendation}</span>
        </div>
      ))}
    </DataTable>
  );
}

function AgentRunHistory({ history }: { history: AgentRunRecord[] }) {
  if (history.length === 0) {
    return <EmptyState title="No advisor runs retained yet" />;
  }

  return (
    <DataTable className="runHistoryTable">
      <div className="tableRow tableHead">
        <span>Time</span>
        <span>Trigger</span>
        <span>Actor</span>
        <span>Score</span>
        <span>Findings</span>
        <span>Top evidence</span>
      </div>
      {history.map((run) => {
        const topFinding = run.findingsPreview[0];
        return (
          <div className="tableRow" key={run.id}>
            <span>
              <strong>{formatDateTime(run.generatedAt)}</strong>
              <small className="mono">{run.id.slice(0, 8)}</small>
            </span>
            <span>{run.trigger}</span>
            <span className="mono">{run.actor}</span>
            <span className={`scoreBadge ${scoreTone(run.summary.score)}`}>{run.summary.score}</span>
            <span>
              <strong>{run.summary.findings}</strong>
              <small>
                {run.summary.critical} critical, {run.summary.high} high
              </small>
            </span>
            <span>
              {topFinding ? (
                <>
                  <Severity severity={topFinding.severity} />
                  <strong>{topFinding.title}</strong>
                  <small>{topFinding.resourceName ?? topFinding.resourceType ?? "platform"}</small>
                </>
              ) : (
                <strong>Clear</strong>
              )}
            </span>
          </div>
        );
      })}
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
      logBytes: collector?.lastSnapshot?.partitions?.reduce((total, partition) => total + partition.sizeBytes, 0),
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

function lagTone(lag: number): "good" | "warning" | "bad" | "neutral" {
  if (lag >= 10_000) {
    return "bad";
  }
  if (lag > 0) {
    return "warning";
  }
  return "good";
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

function formatDateTime(iso: string) {
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(iso));
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
