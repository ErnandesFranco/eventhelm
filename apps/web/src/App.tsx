import {
  Activity,
  Boxes,
  Database,
  FileClock,
  Gauge,
  MessageSquareText,
  Plus,
  RadioTower,
  RefreshCw,
  Send,
  ShieldCheck,
  Users
} from "lucide-react";
import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import type { AuditEvent, Cluster, CollectorState, ConsumerGroup, MessageRecord, Overview, Topic } from "./api";
import { api } from "./api";

type Tab = "overview" | "topics" | "messages" | "consumers" | "collectors" | "audit";

const tabs: Array<{ id: Tab; label: string; icon: typeof Gauge }> = [
  { id: "overview", label: "Overview", icon: Gauge },
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
  const [selectedTopic, setSelectedTopic] = useState("");
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
      const loadedClusters = await api.clusters();
      setClusters(loadedClusters);
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
      setOverview(loadedOverview);
      setTopics(loadedTopics);
      setGroups(loadedGroups);
      setCollectors(loadedCollectors);
      setAudit(loadedAudit);
      setSelectedTopic((current) => current || loadedTopics.find((topic) => !topic.isInternal)?.name || loadedTopics[0]?.name || "");
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
      setMessages(await api.browseMessages(selectedClusterId, selectedTopic, 25));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    }
  };

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brandMark">OK</span>
          <div>
            <strong>OpenKCP</strong>
            <span>Kafka control plane</span>
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
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <p className="eyebrow">Cluster operations</p>
            <h1>{selectedCluster?.name ?? "Kafka cluster"}</h1>
          </div>
          <button className="iconButton" type="button" onClick={() => void load()} title="Refresh">
            <RefreshCw size={18} />
          </button>
        </header>

        {error ? <div className="error">{error}</div> : null}
        {loading ? <div className="loading">Loading cluster state...</div> : null}

        {!loading && activeTab === "overview" && overview ? <OverviewView overview={overview} /> : null}
        {!loading && activeTab === "topics" ? (
          <TopicsView clusterId={selectedClusterId} topics={topics} onChanged={() => void load()} />
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
          />
        ) : null}
        {!loading && activeTab === "consumers" ? <ConsumersView groups={groups} /> : null}
        {!loading && activeTab === "collectors" ? <CollectorsView collectors={collectors} /> : null}
        {!loading && activeTab === "audit" ? <AuditView audit={audit} /> : null}
      </main>
    </div>
  );
}

function OverviewView({ overview }: { overview: Overview }) {
  return (
    <section className="stack">
      <div className="statGrid">
        <Stat icon={Boxes} label="Brokers" value={overview.brokerCount} />
        <Stat icon={Database} label="Topics" value={overview.topicCount} />
        <Stat icon={Users} label="Consumer groups" value={overview.consumerGroupCount} />
        <Stat icon={RadioTower} label="Collectors" value={overview.collectors.length} />
      </div>
      <div className="panel">
        <div className="panelHeader">
          <h2>Broker Inventory</h2>
          <span className="muted">Controller {overview.controllerId ?? "unknown"}</span>
        </div>
        <div className="table">
          <div className="row header">
            <span>Broker</span>
            <span>Host</span>
            <span>Port</span>
          </div>
          {overview.brokers.map((broker) => (
            <div className="row" key={broker.nodeId}>
              <span>{broker.nodeId}</span>
              <span>{broker.host}</span>
              <span>{broker.port}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function TopicsView({
  clusterId,
  topics,
  onChanged
}: {
  clusterId: string;
  topics: Topic[];
  onChanged: () => void;
}) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState(3);
  const [replicationFactor, setReplicationFactor] = useState(3);
  const [cleanupPolicy, setCleanupPolicy] = useState<"delete" | "compact">("delete");
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      await api.createTopic(clusterId, { name, partitions, replicationFactor, cleanupPolicy });
      setName("");
      onChanged();
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="stack">
      <form className="toolbar" onSubmit={(event) => void submit(event)}>
        <input value={name} onChange={(event) => setName(event.target.value)} placeholder="topic.name" required />
        <input
          type="number"
          min="1"
          value={partitions}
          onChange={(event) => setPartitions(Number(event.target.value))}
          title="Partitions"
        />
        <input
          type="number"
          min="1"
          value={replicationFactor}
          onChange={(event) => setReplicationFactor(Number(event.target.value))}
          title="Replication factor"
        />
        <select value={cleanupPolicy} onChange={(event) => setCleanupPolicy(event.target.value as "delete" | "compact")}>
          <option value="delete">delete</option>
          <option value="compact">compact</option>
        </select>
        <button className="primary" disabled={busy} type="submit">
          <Plus size={16} />
          <span>Create</span>
        </button>
      </form>

      <div className="panel">
        <div className="panelHeader">
          <h2>Topics</h2>
          <span className="muted">{topics.length} total</span>
        </div>
        <div className="table">
          <div className="row header">
            <span>Name</span>
            <span>Partitions</span>
            <span>Replicas</span>
            <span>Type</span>
          </div>
          {topics.map((topic) => (
            <div className="row" key={topic.name}>
              <span className="mono">{topic.name}</span>
              <span>{topic.partitions}</span>
              <span>{topic.replicas}</span>
              <span>{topic.isInternal ? "internal" : "user"}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function MessagesView({
  topics,
  selectedTopic,
  setSelectedTopic,
  browse,
  messages,
  clusterId,
  onProduced
}: {
  topics: Topic[];
  selectedTopic: string;
  setSelectedTopic: (topic: string) => void;
  browse: () => Promise<void>;
  messages: MessageRecord[];
  clusterId: string;
  onProduced: () => void;
}) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState('{"hello":"world"}');

  async function produce(event: FormEvent) {
    event.preventDefault();
    await api.produceMessage(clusterId, { topic: selectedTopic, key: key || undefined, value });
    onProduced();
  }

  return (
    <section className="stack">
      <div className="toolbar">
        <select value={selectedTopic} onChange={(event) => setSelectedTopic(event.target.value)}>
          {topics
            .filter((topic) => !topic.isInternal)
            .map((topic) => (
              <option key={topic.name} value={topic.name}>
                {topic.name}
              </option>
            ))}
        </select>
        <button className="secondary" type="button" onClick={() => void browse()}>
          <Activity size={16} />
          <span>Browse</span>
        </button>
      </div>

      <form className="producePanel" onSubmit={(event) => void produce(event)}>
        <input value={key} onChange={(event) => setKey(event.target.value)} placeholder="optional key" />
        <textarea value={value} onChange={(event) => setValue(event.target.value)} />
        <button className="primary" type="submit">
          <Send size={16} />
          <span>Produce</span>
        </button>
      </form>

      <div className="panel">
        <div className="panelHeader">
          <h2>Records</h2>
          <span className="muted">{messages.length} loaded</span>
        </div>
        <div className="records">
          {messages.map((message) => (
            <article className="record" key={`${message.partition}-${message.offset}`}>
              <div>
                <span className="mono">partition {message.partition}</span>
                <span className="mono">offset {message.offset}</span>
                <span>{new Date(message.timestamp).toLocaleString()}</span>
              </div>
              <pre>{message.value}</pre>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

function ConsumersView({ groups }: { groups: ConsumerGroup[] }) {
  return (
    <section className="panel">
      <div className="panelHeader">
        <h2>Consumer Groups</h2>
        <span className="muted">{groups.length} groups</span>
      </div>
      <div className="table">
        <div className="row header">
          <span>Group</span>
          <span>Protocol</span>
          <span>State</span>
          <span>Members</span>
        </div>
        {groups.map((group) => (
          <div className="row" key={group.groupId}>
            <span className="mono">{group.groupId}</span>
            <span>{group.protocolType || "n/a"}</span>
            <span>{group.state ?? "unknown"}</span>
            <span>{group.members ?? 0}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function CollectorsView({ collectors }: { collectors: CollectorState[] }) {
  return (
    <section className="panel">
      <div className="panelHeader">
        <h2>Broker Collectors</h2>
        <span className="muted">{collectors.length} online</span>
      </div>
      <div className="table">
        <div className="row header">
          <span>Collector</span>
          <span>Broker</span>
          <span>Host</span>
          <span>Last seen</span>
        </div>
        {collectors.map((collector) => (
          <div className="row" key={collector.heartbeat.collectorId}>
            <span className="mono">{collector.heartbeat.collectorId}</span>
            <span>{collector.heartbeat.brokerId}</span>
            <span>{collector.heartbeat.hostname}</span>
            <span>{new Date(collector.heartbeat.observedAt).toLocaleTimeString()}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function AuditView({ audit }: { audit: AuditEvent[] }) {
  return (
    <section className="panel">
      <div className="panelHeader">
        <h2>Audit Log</h2>
        <ShieldCheck size={18} />
      </div>
      <div className="table">
        <div className="row header">
          <span>Time</span>
          <span>Action</span>
          <span>Resource</span>
          <span>Actor</span>
        </div>
        {audit.map((event) => (
          <div className="row" key={event.id}>
            <span>{new Date(event.createdAt).toLocaleString()}</span>
            <span className="mono">{event.action}</span>
            <span>{event.resourceName ?? event.resourceType ?? "n/a"}</span>
            <span>{event.actor}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function Stat({ icon: Icon, label, value }: { icon: typeof Gauge; label: string; value: number }) {
  return (
    <div className="stat">
      <Icon size={20} />
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}
