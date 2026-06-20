import { persistenceMode, query } from "./db.js";
import type { AgentFinding, AgentRun, AgentRunRecord, AgentRunTrigger } from "./types.js";

type AgentRunRow = {
  id: string;
  cluster_id: string;
  actor: string;
  trigger: AgentRunTrigger;
  run: AgentRun;
  summary: AgentRun["summary"];
  generated_at: Date;
  created_at: Date;
};

type AgentFindingRow = {
  run_id: string;
  finding: AgentFinding;
};

const runStore = new Map<string, AgentRun>();
const runRecords: AgentRunRecord[] = [];
const maxStoredRunsPerCluster = 250;

export async function saveAgentRun(run: AgentRun, actor = "system", trigger: AgentRunTrigger = "automatic"): Promise<AgentRun> {
  const storedRun: AgentRun = {
    ...run,
    actor,
    trigger
  };

  if (persistenceMode() === "postgres") {
    await query(
      `insert into agent_runs (id, cluster_id, actor, trigger, run, summary, generated_at)
       values ($1, $2, $3, $4, $5, $6, $7)
       on conflict (id) do update set
        actor = excluded.actor,
        trigger = excluded.trigger,
        run = excluded.run,
        summary = excluded.summary`,
      [storedRun.id, storedRun.clusterId, actor, trigger, storedRun, storedRun.summary, storedRun.generatedAt]
    );

    for (const finding of storedRun.findings) {
      await query(
        `insert into agent_findings
          (id, run_id, cluster_id, agent_id, severity, resource_type, resource_name, finding)
         values ($1, $2, $3, $4, $5, $6, $7, $8)
         on conflict (id) do update set finding = excluded.finding`,
        [
          `${storedRun.id}:${finding.id}`,
          storedRun.id,
          storedRun.clusterId,
          finding.agentId,
          finding.severity,
          finding.resourceType ?? null,
          finding.resourceName ?? null,
          finding
        ]
      );
    }
    await pruneStoredRuns(storedRun.clusterId);

    return storedRun;
  }

  runStore.set(storedRun.id, storedRun);
  runRecords.unshift(toRecord(storedRun, actor, trigger, storedRun.generatedAt));
  pruneMemoryRuns(storedRun.clusterId);
  return storedRun;
}

export async function listAgentRuns(clusterId: string, limit = 25): Promise<AgentRunRecord[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<AgentRunRow>(
      `select id, cluster_id, actor, trigger, run, summary, generated_at, created_at
       from agent_runs
       where cluster_id = $1
       order by created_at desc
       limit $2`,
      [clusterId, limit]
    );
    const rows = result.rows;
    const findings = await listFindingsForRuns(rows.map((row) => row.id));
    return rows.map((row) => rowToRecord(row, findings.get(row.id) ?? []));
  }

  return runRecords.filter((record) => record.clusterId === clusterId).slice(0, limit);
}

export async function getAgentRun(runId: string): Promise<AgentRun | undefined> {
  if (persistenceMode() === "postgres") {
    const result = await query<AgentRunRow>(
      `select id, cluster_id, actor, trigger, run, summary, generated_at, created_at
       from agent_runs
       where id = $1`,
      [runId]
    );
    return result.rows[0]?.run;
  }

  return runStore.get(runId);
}

async function listFindingsForRuns(runIds: string[]): Promise<Map<string, AgentFinding[]>> {
  const findingsByRun = new Map<string, AgentFinding[]>();
  if (runIds.length === 0) {
    return findingsByRun;
  }

  const result = await query<AgentFindingRow>(
    `select run_id, finding
     from agent_findings
     where run_id = any($1::text[])
     order by
      case severity
        when 'critical' then 5
        when 'high' then 4
        when 'medium' then 3
        when 'low' then 2
        else 1
      end desc,
      created_at asc`,
    [runIds]
  );

  for (const row of result.rows) {
    const findings = findingsByRun.get(row.run_id) ?? [];
    findings.push(row.finding);
    findingsByRun.set(row.run_id, findings);
  }

  return findingsByRun;
}

async function pruneStoredRuns(clusterId: string): Promise<void> {
  await query(
    `delete from agent_runs
     where id in (
      select id
      from agent_runs
      where cluster_id = $1
      order by created_at desc
      offset $2
     )`,
    [clusterId, maxStoredRunsPerCluster]
  );
}

function pruneMemoryRuns(clusterId: string): void {
  const retained: AgentRunRecord[] = [];
  const removedIds = new Set<string>();
  let clusterCount = 0;

  for (const record of runRecords) {
    if (record.clusterId !== clusterId) {
      retained.push(record);
      continue;
    }

    if (clusterCount < maxStoredRunsPerCluster) {
      retained.push(record);
      clusterCount += 1;
    } else {
      removedIds.add(record.id);
    }
  }

  runRecords.splice(0, runRecords.length, ...retained);
  for (const runId of removedIds) {
    runStore.delete(runId);
  }
}

function rowToRecord(row: AgentRunRow, findings: AgentFinding[]): AgentRunRecord {
  return {
    id: row.id,
    clusterId: row.cluster_id,
    actor: row.actor,
    trigger: row.trigger,
    generatedAt: row.generated_at.toISOString(),
    createdAt: row.created_at.toISOString(),
    summary: row.summary,
    findingsPreview: findings.slice(0, 3)
  };
}

function toRecord(run: AgentRun, actor: string, trigger: AgentRunTrigger, createdAt: string): AgentRunRecord {
  return {
    id: run.id,
    clusterId: run.clusterId,
    actor,
    trigger,
    generatedAt: run.generatedAt,
    createdAt,
    summary: run.summary,
    findingsPreview: run.findings.slice(0, 3)
  };
}
