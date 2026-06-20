import {
  Activity,
  AlertTriangle,
  Bot,
  Boxes,
  CheckCircle2,
  Clock3,
  Database,
  FileClock,
  Gauge,
  KeyRound,
  MessageSquareText,
  Plus,
  RadioTower,
  RefreshCw,
  Search,
  Send,
  ShieldCheck,
  Users,
  X
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
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
  SecurityStatus,
  Topic
} from "./api";
import { api } from "./api";

type Tab = "overview" | "agents" | "topics" | "messages" | "consumers" | "collectors" | "audit";

const tabs: Array<{ id: Tab; label: string; icon: LucideIcon }> = [
  { id: "overview", label: "Overview", icon: Gauge },
  { id: "agents", label: "Agents", icon: Bot },
  { id: "topics", label: "Topics", icon: Database },
  { id: "messages", label: "Messages", icon: MessageSquareText },
  { id: "consumers", label: "Consumers", icon: Users },
  { id: "collectors", label: "Collectors", icon: RadioTower },
  { id: "audit", label: "Audit", icon: FileClock }
];

export function App() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState("local");
  const [activeTab, setActiveTab] = useState<Tab>("overview");
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

  const health = summarizeHealth(overview, agentRun);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brandMark">B</span>
          <div>
            <strong>Brokara</strong>
            <span>Kafka operations cockpit</span>
          </div>
        </div>

        <label className="fieldLabel" htmlFor="cluster">
          Cluster
        </label>
        <select id="cluster" value={selectedClusterId} onChange={(event) => setSelectedClusterId(event.target.value)}>
          {clusters.map((cluster) => (
            <option value={cluster.id} key={cluster.id}>
              {cluster.name}
            </option>
          ))}
        </select>

        <nav className="tabs">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                className={activeTab === tab.id ? "tab active" : "tab"}
                key={tab.id}
                type="button"
                onClick={() => setActiveTab(tab.id)}
                title={tab.label}
              >
                <Icon size={18} />
                <span>{tab.label}</span>
              </button>
            );
          })}
        </nav>

        <div className="sidebarStatus">
          <span className={`statusDot ${health.tone}`} />
          <div>
            <strong>{health.label}</strong>
            <span>{health.detail}</span>
          </div>
        </div>
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <p className="eyebrow">Mission control</p>
            <h1>{selectedCluster?.name ?? "Kafka cluster"}</h1>
          </div>
          <div className="topbarActions">
            {security ? (
              <span className={`chip ${security.authMode === "dev" ? "warning" : "success"}`}>
                <KeyRound size={14} />
                {security.authMode === "dev" ? "Dev auth" : "Token auth"}
              </span>
            ) : null}
            {lastRefreshed ? <span className="muted">Refreshed {lastRefreshed.toLocaleTimeString()}</span> : null}
            <button className="iconButton" type="button" onClick={() => void load()} title="Refresh">
              <RefreshCw size={18} />
            </button>
          </div>
        </header>

        {error ? <div className="error">{error}</div> : null}
        {loading ? <div className="loading">Loading Brokara state...</div> : null}

        {!loading && activeTab === "overview" && overview ? (
          <OverviewView overview={overview} agentRun={agentRun} audit={audit} onRunAgents={runAgents} />
        ) : null}
        {!loading && activeTab === "agents" && agentRun ? (
          <AgentsView agentRun={agentRun} onRunAgents={runAgents} />
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

function OverviewView({
  overview,
  agentRun,
  audit,
  onRunAgents
}: {
  overview: Overview;
  agentRun: AgentRun | null;
  audit: AuditEvent[];
  onRunAgents: () => Promise<void>;
}) {
  const topFindings = agentRun?.findings.slice(0, 4) ?? [];
  const freshCollectors = overview.collectors.filter((collector) => collectorFreshness(collector) === "online").length;

  return (
    <section className="stack">
      <div className="heroPanel">
        <div>
          <span className="chip success">
            <CheckCircle2 size={14} />
            {overview.brokerCount} brokers reachable
          </span>
          <h2>Fleet posture</h2>
          <p>
            Controller broker {overview.controllerId ?? "unknown"} is serving a cluster with {overview.topicCount} user topics,{" "}
            {overview.consumerGroupCount} consumer groups, and {freshCollectors} fresh broker collectors.
          </p>
        </div>
        <button className="primary" type="button" onClick={() => void onRunAgents()}>
          <Bot size={16} />
          <span>Run agents</span>
        </button>
      </div>

      <div className="statGrid">
        <Stat icon={Boxes} label="Brokers" value={overview.brokerCount} detail="Kafka nodes" />
        <Stat icon={Database} label="Topics" value={overview.topicCount} detail={`${overview.internalTopicCount} internal`} />
        <Stat icon={Users} label="Consumer groups" value={overview.consumerGroupCount} detail="Active or known" />
        <Stat icon={RadioTower} label="Collectors" value={freshCollectors} detail={`${overview.collectors.length} registered`} />
      </div>

      <div className="splitGrid">
        <section className="panel">
          <div className="panelHeader">
            <h2>Agent Findings</h2>
            <span className="muted">{agentRun?.generatedAt ? new Date(agentRun.generatedAt).toLocaleTimeString() : "not run"}</span>
          </div>
          <FindingList findings={topFindings} />
        </section>
        <section className="panel">
          <div className="panelHeader">
            <h2>Recent Activity</h2>
            <span className="muted">{audit.length} events</span>
          </div>
          <AuditTimeline audit={audit.slice(0, 5)} />
        </section>
      </div>

      <section className="panel">
        <div className="panelHeader">
          <h2>Broker Inventory</h2>
          <span className="muted">Controller {overview.controllerId ?? "unknown"}</span>
        </div>
        <div className="table brokerTable">
          <div className="row header">
            <span>Broker</span>
            <span>Host</span>
            <span>Port</span>
          </div>
          {overview.brokers.map((broker) => (
            <div className="row" key={broker.nodeId}>
              <span className="mono">{broker.nodeId}</span>
              <span className="mono">{broker.host}</span>
              <span>{broker.port}</span>
            </div>
          ))}
        </div>
      </section>
    </section>
  );
}

function AgentsView({ agentRun, onRunAgents }: { agentRun: AgentRun; onRunAgents: () => Promise<void> }) {
  return (
    <section className="stack">
      <div className="sectionBar">
        <div>
          <h2>Advisor Agents</h2>
          <p className="muted">Rules-based now, model-ready later.</p>
        </div>
        <button className="primary" type="button" onClick={() => void onRunAgents()}>
          <Activity size={16} />
          <span>Run checks</span>
        </button>
      </div>

      <div className="agentGrid">
        {agentRun.agents.map((agent) => (
          <AgentCard key={agent.id} agent={agent} />
        ))}
      </div>

      <section className="panel">
        <div className="panelHeader">
          <h2>Open Findings</h2>
          <span className="muted">{agentRun.findings.length} findings</span>
        </div>
        <FindingTable findings={agentRun.findings} />
      </section>
    </section>
  );
}

function AgentCard({ agent }: { agent: AdvisorAgent }) {
  const topFinding = agent.findings[0];
  return (
    <article className="agentCard">
      <div className="agentCardTop">
        <div className="agentIcon">
          <Bot size={18} />
        </div>
        <span className={`score ${scoreTone(agent.score)}`}>{agent.score}</span>
      </div>
      <h3>{agent.name}</h3>
      <p>{agent.mission}</p>
      <span className="muted">{agent.cadence}</span>
      {topFinding ? (
        <div className="agentFinding">
          <span className={`severity ${topFinding.severity}`}>{topFinding.severity}</span>
          <strong>{topFinding.title}</strong>
        </div>
      ) : (
        <div className="agentFinding calm">
          <CheckCircle2 size={16} />
          No findings
        </div>
      )}
    </article>
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
    <section className="stack">
      <div className="sectionBar">
        <div>
          <h2>Topics</h2>
          <p className="muted">{filteredTopics.length} visible topics</p>
        </div>
        <button className="primary" type="button" onClick={() => setCreating(true)}>
          <Plus size={16} />
          <span>Create topic</span>
        </button>
      </div>

      <div className="filterBar">
        <div className="searchBox">
          <Search size={16} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search topics" />
        </div>
        <label className="toggle">
          <input checked={showInternal} onChange={(event) => setShowInternal(event.target.checked)} type="checkbox" />
          Internal topics
        </label>
      </div>

      <section className="panel">
        <div className="table topicTable">
          <div className="row header">
            <span>Name</span>
            <span>Partitions</span>
            <span>Replicas</span>
            <span>Type</span>
          </div>
          {filteredTopics.map((topic) => (
            <div className="row" key={topic.name}>
              <span className="mono">{topic.name}</span>
              <span>{topic.partitions}</span>
              <span>{topic.replicas}</span>
              <span className={`chip ${topic.isInternal ? "neutral" : "success"}`}>{topic.isInternal ? "internal" : "user"}</span>
            </div>
          ))}
        </div>
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
      <form className="modal" onSubmit={(event) => void submit(event)}>
        <div className="modalHeader">
          <div>
            <h2>Create Topic</h2>
            <p className="muted">Validated against broker count and naming policy.</p>
          </div>
          <button className="iconButton" type="button" onClick={onClose} title="Close">
            <X size={18} />
          </button>
        </div>
        {error ? <div className="error compact">{error}</div> : null}
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
            Replication
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
            Cleanup
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
        <div className="reviewBox">
          <strong>Review</strong>
          <span>
            Brokara will create <span className="mono">{name || "topic.name"}</span> with {partitions} partitions and RF{" "}
            {replicationFactor}.
          </span>
        </div>
        <div className="modalActions">
          <button className="secondary" type="button" onClick={onClose}>
            Cancel
          </button>
          <button className="primary" disabled={busy} type="submit">
            <Plus size={16} />
            Create
          </button>
        </div>
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
  const [value, setValue] = useState('{"hello":"world"}');
  const [formatJson, setFormatJson] = useState(true);

  async function produce(event: FormEvent) {
    event.preventDefault();
    await api.produceMessage(clusterId, { topic: selectedTopic, key: key || undefined, value });
    onProduced();
  }

  return (
    <section className="stack">
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
        <label className="toggle">
          <input checked={fromBeginning} onChange={(event) => setFromBeginning(event.target.checked)} type="checkbox" />
          From beginning
        </label>
        <button className="secondary" type="button" onClick={() => void browse()}>
          <Activity size={16} />
          <span>Browse</span>
        </button>
      </div>

      <div className="splitGrid wide">
        <section className="panel">
          <div className="panelHeader">
            <h2>Browse Records</h2>
            <label className="toggle">
              <input checked={formatJson} onChange={(event) => setFormatJson(event.target.checked)} type="checkbox" />
              Format JSON
            </label>
          </div>
          <div className="records">
            {messages.map((message) => (
              <article className="record" key={`${message.partition}-${message.offset}`}>
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

        <form className="panel produceTool" onSubmit={(event) => void produce(event)}>
          <div className="panelHeader">
            <h2>Produce Test Message</h2>
            <span className="chip warning">audited</span>
          </div>
          <label>
            Optional key
            <input value={key} onChange={(event) => setKey(event.target.value)} placeholder="message-key" />
          </label>
          <label>
            Value
            <textarea value={value} onChange={(event) => setValue(event.target.value)} />
          </label>
          <button className="primary" type="submit">
            <Send size={16} />
            <span>Produce</span>
          </button>
        </form>
      </div>
    </section>
  );
}

function ConsumersView({ groups }: { groups: ConsumerGroup[] }) {
  const [query, setQuery] = useState("");
  const filtered = groups.filter((group) => group.groupId.toLowerCase().includes(query.toLowerCase()));
  return (
    <section className="stack">
      <div className="filterBar">
        <div className="searchBox">
          <Search size={16} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search consumer groups" />
        </div>
      </div>
      <section className="panel">
        <div className="table consumerTable">
          <div className="row header">
            <span>Group</span>
            <span>Protocol</span>
            <span>State</span>
            <span>Members</span>
          </div>
          {filtered.map((group) => (
            <div className="row" key={group.groupId}>
              <span className="mono">{group.groupId}</span>
              <span>{group.protocolType || "n/a"}</span>
              <span className="chip neutral">{group.state ?? "unknown"}</span>
              <span>{group.members ?? 0}</span>
            </div>
          ))}
        </div>
        {filtered.length === 0 ? <EmptyState title="No consumer groups found" /> : null}
      </section>
    </section>
  );
}

function CollectorsView({ collectors, brokerCount }: { collectors: CollectorState[]; brokerCount: number }) {
  const [query, setQuery] = useState("");
  const filtered = collectors.filter((collector) => collector.heartbeat.collectorId.toLowerCase().includes(query.toLowerCase()));
  return (
    <section className="stack">
      <div className="sectionBar">
        <div>
          <h2>Broker Collectors</h2>
          <p className="muted">
            {collectors.length} registered for {brokerCount} brokers
          </p>
        </div>
      </div>
      <div className="filterBar">
        <div className="searchBox">
          <Search size={16} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search collectors" />
        </div>
      </div>
      <section className="panel">
        <div className="table collectorTable">
          <div className="row header">
            <span>Collector</span>
            <span>Broker</span>
            <span>Status</span>
            <span>Last seen</span>
            <span>Snapshot</span>
          </div>
          {filtered.map((collector) => {
            const freshness = collectorFreshness(collector);
            return (
              <div className="row" key={collector.heartbeat.collectorId}>
                <span className="mono">{collector.heartbeat.collectorId}</span>
                <span>{collector.heartbeat.brokerId}</span>
                <span className={`chip ${freshness === "online" ? "success" : "warning"}`}>{freshness}</span>
                <span>{formatAge(collector.heartbeat.observedAt)}</span>
                <span>
                  {collector.lastSnapshot?.brokerCount ?? 0} brokers / {collector.lastSnapshot?.topicCount ?? 0} topics
                </span>
              </div>
            );
          })}
        </div>
        {filtered.length === 0 ? <EmptyState title="No collectors found" /> : null}
      </section>
    </section>
  );
}

function AuditView({ audit }: { audit: AuditEvent[] }) {
  const [query, setQuery] = useState("");
  const filtered = audit.filter((event) =>
    `${event.action} ${event.resourceName ?? ""} ${event.actor}`.toLowerCase().includes(query.toLowerCase())
  );
  return (
    <section className="stack">
      <div className="filterBar">
        <div className="searchBox">
          <Search size={16} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search audit log" />
        </div>
      </div>
      <section className="panel">
        <div className="table auditTable">
          <div className="row header">
            <span>Time</span>
            <span>Action</span>
            <span>Resource</span>
            <span>Actor</span>
            <span>Details</span>
          </div>
          {filtered.map((event) => (
            <div className="row" key={event.id}>
              <span>{new Date(event.createdAt).toLocaleString()}</span>
              <span className="mono">{event.action}</span>
              <span>{event.resourceName ?? event.resourceType ?? "n/a"}</span>
              <span>{event.actor}</span>
              <span className="mono detailText">{event.details ? JSON.stringify(event.details) : "n/a"}</span>
            </div>
          ))}
        </div>
        {filtered.length === 0 ? <EmptyState title="No audit events found" /> : null}
      </section>
    </section>
  );
}

function Stat({ icon: Icon, label, value, detail }: { icon: LucideIcon; label: string; value: number; detail: string }) {
  return (
    <div className="stat">
      <Icon size={20} />
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function FindingList({ findings }: { findings: AgentFinding[] }) {
  if (findings.length === 0) {
    return <EmptyState title="No agent findings" />;
  }

  return (
    <div className="findingList">
      {findings.map((finding) => (
        <article key={finding.id}>
          <span className={`severity ${finding.severity}`}>{finding.severity}</span>
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
    <div className="table findingTable">
      <div className="row header">
        <span>Severity</span>
        <span>Agent</span>
        <span>Finding</span>
        <span>Recommendation</span>
      </div>
      {findings.map((finding) => (
        <div className="row" key={finding.id}>
          <span className={`severity ${finding.severity}`}>{finding.severity}</span>
          <span className="mono">{finding.agentId}</span>
          <span>
            <strong>{finding.title}</strong>
            <small>{finding.summary}</small>
          </span>
          <span>{finding.recommendation}</span>
        </div>
      ))}
    </div>
  );
}

function AuditTimeline({ audit }: { audit: AuditEvent[] }) {
  if (audit.length === 0) {
    return <EmptyState title="No recent activity" />;
  }
  return (
    <div className="timeline">
      {audit.map((event) => (
        <article key={event.id}>
          <Clock3 size={15} />
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

function EmptyState({ title }: { title: string }) {
  return (
    <div className="emptyState">
      <AlertTriangle size={18} />
      <span>{title}</span>
    </div>
  );
}

function summarizeHealth(overview: Overview | null, agentRun: AgentRun | null) {
  const hasCritical = agentRun?.findings.some((finding) => finding.severity === "critical");
  const hasHigh = agentRun?.findings.some((finding) => finding.severity === "high");
  const freshCollectors = overview?.collectors.filter((collector) => collectorFreshness(collector) === "online").length ?? 0;
  if (hasCritical) {
    return { label: "Critical", detail: "Agent intervention needed", tone: "critical" };
  }
  if (hasHigh) {
    return { label: "Needs attention", detail: "High-priority findings open", tone: "warning" };
  }
  return { label: "Operational", detail: `${freshCollectors} collectors fresh`, tone: "success" };
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
    return "success";
  }
  if (score >= 65) {
    return "warning";
  }
  return "critical";
}
