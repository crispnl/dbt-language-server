import { BigQueryServiceAccountProfile } from '../../bigquery/BigQueryServiceAccountProfile';
import { YamlUtils } from '../../YamlUtils';
import {
  BIG_QUERY_CONFIG,
  BQ_SERVICE_ACCOUNT,
  BQ_SERVICE_ACCOUNT_MISSING_KEYFILE,
  getConfigPath,
  shouldPassValidProfile,
  shouldRequireProfileField,
} from '../helper';

describe('Service account profile', () => {
  it('Should pass valid profile', () => {
    shouldPassValidProfile(BIG_QUERY_CONFIG, BQ_SERVICE_ACCOUNT);
  });

  it('Should require service account fields', () => {
    const profiles = YamlUtils.parseYamlFile(getConfigPath(BIG_QUERY_CONFIG));
    const serviceAccountProfile = new BigQueryServiceAccountProfile();
    shouldRequireProfileField(profiles, serviceAccountProfile, BQ_SERVICE_ACCOUNT_MISSING_KEYFILE, 'keyfile');
  });
});
