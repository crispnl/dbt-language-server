import { homedir } from 'node:os';
import { TextDocument, Uri } from 'vscode';
import path = require('node:path');

export const SQL_LANG_ID = 'sql';
export const SNOWFLAKE_SQL_LANG_ID = 'snowflake-sql';
export const SUPPORTED_LANG_IDS = [SQL_LANG_ID, 'jinja-sql', SNOWFLAKE_SQL_LANG_ID, 'sql-bigquery'];
export const PACKAGES_YML = 'packages.yml';
export const PROFILES_YML = 'profiles.yml';
export const PROFILES_YML_DEFAULT_URI = Uri.file(path.join(homedir(), '.dbt', PROFILES_YML));
export const DBT_PROJECT_YML = 'dbt_project.yml';
export const DBT_ADAPTERS = [
  'dbt-postgres',
  'dbt-redshift',
  'dbt-bigquery',
  'dbt-snowflake',
  'dbt-spark',
  'dbt-clickhouse',
  'dbt-databricks',
  'dbt-firebolt',
  'dbt-impala',
  'dbt-iomete',
  'dbt-layer-bigquery',
  'dbt-materialize',
  'dbt-mindsdb',
  'dbt-oracle',
  'dbt-rockset',
  'dbt-singlestore',
  'dbt-trino',
  'dbt-teradata',
  'dbt-tidb',
  'dbt-sqlserver',
  'dbt-synapse',
  'dbt-exasol',
  'dbt-dremio',
  'dbt-vertica',
  'dbt-glue',
  'dbt-greenplum',
  'dbt-duckdb',
  'dbt-sqlite',
  'dbt-mysql',
  'dbt-ibmdb2',
  'dbt-hive',
  'dbt-athena-community',
  'dbt-doris',
  'dbt-infer',
  'dbt-databend-cloud',
  'dbt-fal',
  'dbt-decodable',
];

export function isDocumentSupported(document: TextDocument): boolean {
  return (
    (SUPPORTED_LANG_IDS.includes(document.languageId) || document.fileName.endsWith(PACKAGES_YML) || document.fileName.endsWith(DBT_PROJECT_YML)) &&
    document.uri.scheme === 'file'
  );
}
export function wait(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}
