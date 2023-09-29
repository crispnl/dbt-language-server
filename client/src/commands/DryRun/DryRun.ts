import { BigQuery } from '@google-cloud/bigquery';
import * as fs from 'node:fs';
import { ProgressLocation, window } from 'vscode';
import { DbtLanguageClientManager } from '../../DbtLanguageClientManager';
import { DBTProjectConfiguration } from '../../ExtensionClient';
import { humanFileSize, wait } from '../../Utils';
import { Command } from '../CommandManager';

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
    totalBytesProcessed: number;
    query: {
      totalBytesProcessed: number;
      totalBytesBilled: number;
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

  async dryRun(target: string): Promise<void> {
    const activeDocument = this.dbtLanguageClientManager.getActiveDocument();
    if (!activeDocument) {
      await window.showErrorMessage('Active document does not support dry-runs');
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
        cancellable: true,
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

        const projectName = await languageClient.sendRequest<string | null>('WizardForDbtCore(TM)/getProjectName');

        if (!projectName) {
          progress.report({
            message: 'Failed: unable to retrieve DBT project name',
          });
          await wait(5000);
          return;
        }

        const output = this.dbtProjectConfiguration[projectName].outputs[target];
        if (!output) {
          await window.showErrorMessage(`Failed to find target ${target} in DBT profiles file`);
          return;
        }

        report('Compiling SQL', 0);

        const compiledSql = await languageClient.sendRequest<string | null>('WizardForDbtCore(TM)/getCompiledSql', {
          uri: activeDocument.uri.toString(),
          target,
        });
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

        report(`Starting dry-run job on target ${target}`, 10);

        const bigquery = new BigQuery();
        bigquery.projectId = output.project;
        bigquery.location = output.location;

        try {
          const [jobResponse] = await bigquery.createQueryJob({
            dryRun: true,
            projectId: output.project,
            defaultDataset: {
              datasetId: output.dataset,
              projectId: output.project,
            },
            location: output.location,
            query: compiledSql,
          });
          const metadata = jobResponse.metadata as DryRunMetadata;
          report(`Completed: query processes ${humanFileSize(metadata.statistics.totalBytesProcessed)}`, 100);
          await wait(5000);
        } catch (e) {
          if (e instanceof Error) {
            report(`Failed: ${e.name} - ${e.message}`, 80);
            await wait(5000);
          }
        }
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
