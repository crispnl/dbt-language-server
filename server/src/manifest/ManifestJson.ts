import { Dag } from '../dag/Dag';

export interface ManifestNode {
  uniqueId: string;
  originalFilePath: string;
  name: string;
  packageName: string;
}

export interface ManifestModel extends ManifestNode {
  database: string;
  schema: string;
  rawCode: string;
  compiledCode: string;
  dependsOn: {
    nodes: string[];
  };
  refs: (string[] | { name: string; package?: string })[];
  alias?: string;
  config?: {
    sqlHeader?: string;
    materialized?: string;
    schema?: string;
  };
  description: string;
  columns: Record<string, { name: string; description: string }>;
  patchPath: string;
}

export type ManifestMacro = ManifestNode;

export interface ManifestSource extends ManifestNode {
  database: string;
  schema: string;
  sourceName: string;
  description: string;
  columns: Record<string, { name: string; description: string }>;
}

export interface ManifestSeed extends ManifestNode {
  database: string;
  schema: string;
  sourceName: string;
  description: string;
  columns: Record<string, { name: string; description: string }>;
}

export interface ManifestJson {
  macros: ManifestMacro[];
  sources: ManifestSource[];
  seeds: ManifestSeed[];
  dag: Dag;
}
