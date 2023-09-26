import { DBTProjectConfiguration } from '../../ExtensionClient';
import * as fs from 'node:fs';
import { BigQuery } from '@google-cloud/bigquery';
import { Command } from '../CommandManager';
import { DbtLanguageClientManager } from '../../DbtLanguageClientManager';
import { ProgressLocation, window } from 'vscode';
import { wait } from '../../Utils';

// Currently largely unused but nice to have
interface DryRunMetadata {
  kind: string;
  etag: string;
  id: string;
  selfLink: string;
  user_email: string;
  configuration: {
    query: {
      query: string;
      destinationTable: {
        projectId: string;
        datasetId: string;
        tableId: string;
      };
      writeDisposition: string;
      defaultDataset: {
        datasetId: string;
        projectId: string;
      };
      priority: string;
      useLegacySql: boolean;
    };
    dryRun: boolean;
    jobType: string;
  };
  jobReference: {
    projectId: string;
    location: string;
  };
  statistics: {
    creationTime: string;
    totalBytesProcessed: string;
    query: {
      totalBytesProcessed: string;
      totalBytesBilled: string;
      cacheHit: boolean;
      referencedTables: Array<{
        projectId: string;
        datasetId: string;
        tableId: string;
      }>;
      schema: {
        fields: Array<{
          name: string;
          type: string;
          mode: string;
        }>;
      };
      statementType: string;
      totalBytesProcessedAccuracy: string;
    };
  };
  status: {
    state: string;
  };
  principal_subject: string;
}

export abstract class DryRun implements Command {
  abstract readonly id: string;

  constructor(
    private readonly dbtProjectConfiguration: DBTProjectConfiguration,
    private dbtLanguageClientManager: DbtLanguageClientManager,
  ) {}

  async dryRun(environment: 'dev' | 'staging' | 'prod'): Promise<void> {
    const activeDocument = this.dbtLanguageClientManager.getActiveDocument();
    if (!activeDocument) {
      await window.showErrorMessage('Active document does not support dry-runs');
      return;
    }

    const target = this.dbtProjectConfiguration.dbt_crisp_dwh?.outputs[environment];
    if (!target) {
      await window.showErrorMessage(`Failed to find environment ${environment} in DBT configuration file`);
      return;
    }

    const languageClient = await this.dbtLanguageClientManager.getClientForActiveDocument();
    if (!languageClient) {
      await window.showErrorMessage('Failed to connect to language server');
      return;
    }

    await window.withProgress(
      {
        location: ProgressLocation.Notification,
      },
      async (progress, token) => {
        let totalProgress = 0;
        const report = (message: string, newTotalProgress: number): void => {
          progress.report({
            message,
            increment: newTotalProgress - totalProgress,
          });
          totalProgress = newTotalProgress;
        };

        report('Compiling SQL', 0);

        const compiledSql = await languageClient.sendRequest<string | null>('WizardForDbtCore(TM)/getCompiledSql', activeDocument.uri.toString());
        if (compiledSql === null) {
          progress.report({
            message: 'Failed: unable to get compiled SQL for file',
          });
          await wait(5000);
          return;
        }

        if (token.isCancellationRequested) {
          return;
        }

        report(`Starting dry-run job on environment ${environment}`, 10);

        const bigquery = new BigQuery();
        bigquery.projectId = target.project;
        bigquery.location = target.location;
        // Has an async call signature too but it's typed wrongly so let's just use this.
        await new Promise<void>(resolve => {
          bigquery.createQueryJob(
            {
              dryRun: true,
              projectId: target.project,
              defaultDataset: {
                datasetId: target.dataset,
                projectId: target.project,
              },
              location: target.location,
              query: compiledSql,
            },
            async (err, job) => {
              if (err) {
                report(`Failed: ${err.name} - ${err.message}`, 10);
                await wait(5000);
                report(`Failed: ${err.name} - ${err.message}`, 100);
                return;
              }

              const metadata = job!.metadata as DryRunMetadata;
              report(`Completed: processed ${metadata.statistics.totalBytesProcessed} bytes`, 100);
              await wait(5000);
              resolve();
            },
          );
        });
      },
    );
  }

  abstract execute(): Promise<void>;
}

export function tryReadFile(filePath: string): string | null {
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch {
    return null;
  }
}
