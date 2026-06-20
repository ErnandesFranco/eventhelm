import { z } from "zod";
import type { ClusterConfig } from "./types.js";

const clusterSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  brokers: z.array(z.string().min(1)).min(1),
  ssl: z.boolean().optional(),
  sasl: z
    .object({
      mechanism: z.enum(["plain", "scram-sha-256", "scram-sha-512"]),
      username: z.string(),
      password: z.string()
    })
    .optional()
});

const defaultClusters: ClusterConfig[] = [
  {
    id: "local",
    name: "Local Kafka Lab",
    brokers: ["localhost:19092", "localhost:29092", "localhost:39092"]
  }
];

export function loadClusters(): ClusterConfig[] {
  const raw = process.env.PLATFORM_CLUSTERS_JSON;
  if (!raw) {
    return defaultClusters;
  }

  const parsed = z.array(clusterSchema).safeParse(JSON.parse(raw));
  if (!parsed.success) {
    throw new Error(`Invalid PLATFORM_CLUSTERS_JSON: ${parsed.error.message}`);
  }

  return parsed.data;
}

export function getPort(): number {
  return Number(process.env.API_PORT ?? 18080);
}
