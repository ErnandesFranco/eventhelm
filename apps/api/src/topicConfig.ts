import { createHash } from "node:crypto";
import type { TopicConfig, TopicConfigEntry, TopicConfigUpdatePreview, TopicConfigUpdateRequest } from "./types.js";

export const editableTopicConfigs = [
  "cleanup.policy",
  "retention.ms",
  "delete.retention.ms",
  "min.insync.replicas",
  "max.message.bytes",
  "segment.bytes"
];

type TopicConfigUpdatePlan = {
  preview: TopicConfigUpdatePreview;
  configEntries: Array<{ name: string; value: string }>;
};

export function buildTopicConfigUpdatePlan(current: TopicConfig, request: TopicConfigUpdateRequest): TopicConfigUpdatePlan {
  const entryByName = new Map(current.entries.map((entry) => [entry.name, entry]));
  const normalized = normalizeConfigChanges(request);
  const changes = normalized.map((change) => {
    const currentEntry = entryByName.get(change.name);
    const validationError = validateTopicConfigValue(change.name, change.value);
    let blockedReason: string | undefined;

    if (!editableTopicConfigs.includes(change.name)) {
      blockedReason = "Config is not in the EventHelm editable allowlist.";
    } else if (!currentEntry) {
      blockedReason = "Config is not reported by Kafka for this topic.";
    } else if (currentEntry.readOnly || currentEntry.isSensitive) {
      blockedReason = "Config is read-only or sensitive.";
    } else if (validationError) {
      blockedReason = validationError;
    }

    return {
      name: change.name,
      currentValue: currentEntry?.value,
      newValue: change.value,
      blockedReason
    };
  });

  const dynamicEntries = current.entries
    .filter((entry) => !entry.isDefault && !entry.readOnly && !entry.isSensitive)
    .map((entry) => ({ name: entry.name, value: entry.value }));
  const configEntryByName = new Map(dynamicEntries.map((entry) => [entry.name, entry.value]));
  for (const change of changes) {
    if (!change.blockedReason && change.currentValue !== change.newValue) {
      configEntryByName.set(change.name, change.newValue);
    }
  }

  const effectiveChanges = changes.filter((change) => change.currentValue !== change.newValue);
  const warnings = warningsFor(effectiveChanges);
  const executable =
    effectiveChanges.length > 0 &&
    effectiveChanges.every((change) => !change.blockedReason) &&
    Array.from(configEntryByName).length > 0;

  const previewWithoutToken = {
    topic: current.topic,
    generatedAt: new Date().toISOString(),
    executable,
    reviewToken: "",
    warnings,
    changes: effectiveChanges
  };

  return {
    preview: {
      ...previewWithoutToken,
      reviewToken: hashTopicConfigPreview(previewWithoutToken, configEntryByName)
    },
    configEntries: Array.from(configEntryByName)
      .map(([name, value]) => ({ name, value }))
      .sort((left, right) => left.name.localeCompare(right.name))
  };
}

export function toTopicConfig(topic: string, entries: TopicConfigEntry[]): TopicConfig {
  return {
    topic,
    generatedAt: new Date().toISOString(),
    entries: entries.sort((left, right) => left.name.localeCompare(right.name)),
    editable: editableTopicConfigs
  };
}

function normalizeConfigChanges(request: TopicConfigUpdateRequest) {
  const configByName = new Map<string, string>();
  for (const config of request.configs) {
    configByName.set(config.name.trim(), config.value.trim());
  }
  return Array.from(configByName)
    .map(([name, value]) => ({ name, value }))
    .sort((left, right) => left.name.localeCompare(right.name));
}

function validateTopicConfigValue(name: string, value: string) {
  if (name === "cleanup.policy" && !["delete", "compact", "compact,delete"].includes(value)) {
    return "Cleanup policy must be delete, compact, or compact,delete.";
  }
  if (name === "retention.ms") {
    return validateInteger(value, { min: -1 });
  }
  if (["delete.retention.ms", "min.insync.replicas", "max.message.bytes", "segment.bytes"].includes(name)) {
    return validateInteger(value, { min: 1 });
  }
  return undefined;
}

function validateInteger(value: string, options: { min: number }) {
  if (!/^-?\d+$/.test(value)) {
    return "Value must be an integer.";
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < options.min) {
    return `Value must be at least ${options.min}.`;
  }
  return undefined;
}

function warningsFor(changes: TopicConfigUpdatePreview["changes"]) {
  const warnings: string[] = [];
  if (changes.length === 0) {
    warnings.push("No effective config changes were requested.");
  }
  if (changes.some((change) => change.blockedReason)) {
    warnings.push("One or more topic config changes are blocked.");
  }
  if (changes.some((change) => change.name === "cleanup.policy" || change.name === "retention.ms")) {
    warnings.push("Retention and cleanup changes can delete data or alter compaction behavior.");
  }
  return warnings;
}

function hashTopicConfigPreview(
  preview: Omit<TopicConfigUpdatePreview, "reviewToken">,
  configEntryByName: Map<string, string>
) {
  const configEntries = Array.from(configEntryByName)
    .map(([name, value]) => ({ name, value }))
    .sort((left, right) => left.name.localeCompare(right.name));
  return createHash("sha256").update(JSON.stringify({ ...preview, generatedAt: undefined, configEntries })).digest("hex");
}
