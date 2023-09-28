import { SnowflakeUserPassProfile } from '../../snowflake/SnowflakeUserPassProfile';
import { YamlUtils } from '../../YamlUtils';
import { getConfigPath, shouldPassValidProfile, shouldRequireProfileField, SNOWFLAKE_CONFIG } from '../helper';

describe('SnowflakeUserPassProfile', () => {
  it('Should pass valid profile', () => {
    shouldPassValidProfile(SNOWFLAKE_CONFIG, 'correct_user_password');
  });

  it('Should require user', () => {
    shouldRequireField('user');
  });

  it('Should require password', () => {
    shouldRequireField('password');
  });

  it('Should require database', () => {
    shouldRequireField('database');
  });

  it('Should require warehouse', () => {
    shouldRequireField('warehouse');
  });

  it('Should require schema', () => {
    shouldRequireField('schema');
  });
});

function shouldRequireField(field: string): void {
  const profiles = YamlUtils.parseYamlFile(getConfigPath(SNOWFLAKE_CONFIG));
  const oauthTokenBasedProfile = new SnowflakeUserPassProfile();
  shouldRequireProfileField(profiles, oauthTokenBasedProfile, `user_password_missing_${field}`, field);
}
