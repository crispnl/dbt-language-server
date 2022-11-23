import { AdapterInfo, DbtVersionInfo, getStringVersion, PythonInfo, Version } from 'dbt-language-server-common';
import { Command } from '../dbt_execution/commands/Command';
import { DbtCommandExecutor } from '../dbt_execution/commands/DbtCommandExecutor';
import { DbtCommandFactory } from '../dbt_execution/DbtCommandFactory';

export class FeatureFinderBase {
  private static readonly DBT_INSTALLED_VERSION_PATTERN = /installed.*:\s+(\d+)\.(\d+)\.(\d+)/;
  private static readonly DBT_LATEST_VERSION_PATTERN = /latest.*:\s+(\d+)\.(\d+)\.(\d+)/;
  private static readonly DBT_ADAPTER_PATTERN = /- (\w+):.*/g;
  private static readonly DBT_ADAPTER_VERSION_PATTERN = /:\s+(\d+)\.(\d+)\.(\d+)/;

  dbtCommandFactory: DbtCommandFactory;
  versionInfo?: DbtVersionInfo;

  constructor(public pythonInfo: PythonInfo | undefined, private dbtCommandExecutor: DbtCommandExecutor) {
    this.dbtCommandFactory = new DbtCommandFactory(pythonInfo?.path);
  }

  getPythonPath(): string | undefined {
    return this.pythonInfo?.path;
  }

  protected async findDbtRpcPythonInfo(): Promise<DbtVersionInfo | undefined> {
    return this.findCommandPythonInfo(this.dbtCommandFactory.getDbtRpcWithPythonVersion());
  }

  protected async findDbtPythonInfo(): Promise<DbtVersionInfo | undefined> {
    return this.findCommandPythonInfo(this.dbtCommandFactory.getDbtWithPythonVersion());
  }

  protected async findCommandInfo(command: Command): Promise<DbtVersionInfo> {
    const { stderr } = await this.dbtCommandExecutor.execute(command);

    const installedVersion = FeatureFinderBase.readVersionByPattern(stderr, FeatureFinderBase.DBT_INSTALLED_VERSION_PATTERN);
    const latestVersion = FeatureFinderBase.readVersionByPattern(stderr, FeatureFinderBase.DBT_LATEST_VERSION_PATTERN);
    const installedAdapters = FeatureFinderBase.getInstalledAdapters(stderr.slice(stderr.indexOf('Plugins:')));

    return {
      installedVersion,
      latestVersion,
      installedAdapters,
    };
  }

  private async findCommandPythonInfo(command: Command): Promise<DbtVersionInfo | undefined> {
    return command.python ? this.findCommandInfo(command) : undefined;
  }

  protected getLogString(name: string, dbtVersionInfo?: DbtVersionInfo): string {
    return dbtVersionInfo ? `${name} = ${getStringVersion(dbtVersionInfo.installedVersion)} ` : '';
  }

  private static getInstalledAdapters(data: string): AdapterInfo[] {
    const adaptersInfo: AdapterInfo[] = [];
    let m: RegExpExecArray | null;

    while ((m = FeatureFinderBase.DBT_ADAPTER_PATTERN.exec(data))) {
      if (m.length >= 2) {
        adaptersInfo.push({
          name: m[1],
          version: FeatureFinderBase.readVersionByPattern(m[0], FeatureFinderBase.DBT_ADAPTER_VERSION_PATTERN),
        });
      }
    }

    return adaptersInfo;
  }

  private static readVersionByPattern(data: string, pattern: RegExp): Version | undefined {
    const matchResults = data.match(pattern);
    return matchResults?.length === 4
      ? {
          major: Number(matchResults[1]),
          minor: Number(matchResults[2]),
          patch: Number(matchResults[3]),
        }
      : undefined;
  }
}