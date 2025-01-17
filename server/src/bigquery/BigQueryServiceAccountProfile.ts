import { BigQuery, BigQueryOptions } from '@google-cloud/bigquery';
import { Result, err, ok } from 'neverthrow';
import { DbtDestinationClient } from '../DbtDestinationClient';
import { DbtProfile, TargetConfig } from '../DbtProfile';
import { YamlUtils } from '../YamlUtils';
import { BigQueryClient } from './BigQueryClient';

export class BigQueryServiceAccountProfile implements DbtProfile {
  static readonly BQ_SERVICE_ACCOUNT_FILE_DOCS =
    '[Service Account File configuration](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#service-account-file).';

  getDocsUrl(): string {
    return BigQueryServiceAccountProfile.BQ_SERVICE_ACCOUNT_FILE_DOCS;
  }

  validateProfile(targetConfig: TargetConfig): Result<void, string> {
    const { project } = targetConfig;
    if (!project) {
      return err('project');
    }

    const keyFilePath = targetConfig.keyfile;
    if (!keyFilePath) {
      return err('keyfile');
    }

    return ok(undefined);
  }

  async createClient(profile: unknown): Promise<Result<DbtDestinationClient, string>> {
    return this.createClientInternal(profile as Required<TargetConfig>);
  }

  private async createClientInternal(profile: Required<TargetConfig>): Promise<Result<DbtDestinationClient, string>> {
    const { project } = profile;
    const keyFilePath = YamlUtils.replaceTilde(profile.keyfile);

    const options: BigQueryOptions = {
      projectId: project,
      keyFilename: keyFilePath,
    };
    const bigQuery = new BigQuery(options);
    const client = new BigQueryClient(project, () => bigQuery);

    const testResult = await client.test();
    if (testResult.isErr()) {
      return err(testResult.error);
    }

    return ok(client);
  }
}
