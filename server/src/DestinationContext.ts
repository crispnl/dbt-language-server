import { Err, err, ok, Result } from 'neverthrow';
import { Emitter, Event } from 'vscode-languageserver';
import { BigQueryZetaSqlWrapper } from './BigQueryZetaSqlWrapper';
import { DagNode } from './dag/DagNode';
import { DbtProfileSuccess } from './DbtProfileCreator';
import { DbtRepository } from './DbtRepository';
import { DestinationDefinition } from './DestinationDefinition';
import { AnalyzeResult, ModelsAnalyzeResult, ProjectAnalyzer } from './ProjectAnalyzer';
import { AnalyzeTrackerFunc, ProjectAnalyzeTask } from './ProjectAnalyzeTask';
import { SnowflakeZetaSqlWrapper } from './SnowflakeZetaSqlWrapper';
import { SqlHeaderAnalyzer } from './SqlHeaderAnalyzer';
import { SupportedDestinations, ZetaSqlApi } from './ZetaSqlApi';
import { ZetaSqlParser } from './ZetaSqlParser';
import { KnownColumn } from './ZetaSqlWrapper';

export class DestinationContext {
  private static readonly ZETASQL_SUPPORTED_PLATFORMS = ['darwin', 'linux', 'win32'];
  private static readonly NOT_INITIALIZED_ERROR = 'projectAnalyzer is not initialized';

  destinationDefinition?: DestinationDefinition;
  public projectAnalyzer?: ProjectAnalyzer;
  private projectAnalyzeTask?: ProjectAnalyzeTask;
  private projectName?: string;
  private destination?: SupportedDestinations;

  contextInitialized = false;
  onContextInitializedEmitter = new Emitter<void>();

  constructor(private enableSnowflakeSyntaxCheck: boolean) {}

  isEmpty(): boolean {
    return this.projectAnalyzer === undefined;
  }

  onDestinationPrepared(): void {
    this.contextInitialized = true;
    this.onContextInitializedEmitter.fire();
  }

  get onContextInitialized(): Event<void> {
    return this.onContextInitializedEmitter.event;
  }

  getDestination(): SupportedDestinations | undefined {
    return this.destination;
  }

  async initialize(
    profileResult: DbtProfileSuccess,
    dbtRepository: DbtRepository,
    ubuntuInWslWorks: boolean,
    projectName: string,
  ): Promise<Result<void, string>> {
    this.projectName = projectName;
    if (profileResult.dbtProfile && this.canUseDestination(profileResult, ubuntuInWslWorks)) {
      try {
        const clientResult = await profileResult.dbtProfile.createClient(profileResult.targetConfig);
        if (clientResult.isErr()) {
          return this.onError(clientResult.error);
        }

        const destinationClient = clientResult.value;
        this.destinationDefinition = new DestinationDefinition(destinationClient);

        this.destination = profileResult.type?.toLowerCase().trim() === 'snowflake' ? 'snowflake' : 'bigquery';
        const zetaSqlApi = new ZetaSqlApi(this.destination);
        const zetaSqlParser = new ZetaSqlParser(zetaSqlApi);
        const sqlHeaderAnalyzer = new SqlHeaderAnalyzer(zetaSqlApi);
        const zetaSqlWrapper =
          this.destination === 'bigquery'
            ? new BigQueryZetaSqlWrapper(destinationClient, zetaSqlApi, zetaSqlParser, sqlHeaderAnalyzer)
            : new SnowflakeZetaSqlWrapper(destinationClient, zetaSqlApi, zetaSqlParser, sqlHeaderAnalyzer);

        this.projectAnalyzer = new ProjectAnalyzer(dbtRepository, destinationClient, zetaSqlWrapper, zetaSqlApi);
        await this.projectAnalyzer.initialize();
      } catch (e) {
        const message = e instanceof Error ? e.message : JSON.stringify(e);
        return this.onError(message);
      }
    }
    this.onDestinationPrepared();
    return ok(undefined);
  }

  onError(message: string): Err<void, string> {
    console.log(message);
    this.onDestinationPrepared();
    return err(`Destination initialization failed. ${message}`);
  }

  canUseDestination(profileResult: DbtProfileSuccess, ubuntuInWslWorks: boolean): boolean {
    return (
      DestinationContext.ZETASQL_SUPPORTED_PLATFORMS.includes(process.platform) &&
      (profileResult.type?.toLowerCase().trim() === 'bigquery' ||
        (profileResult.type?.toLowerCase().trim() === 'snowflake' && this.enableSnowflakeSyntaxCheck)) &&
      ubuntuInWslWorks
    );
  }

  async analyzeModel(node: DagNode, signal: AbortSignal): Promise<ModelsAnalyzeResult[]> {
    this.ensureProjectAnalyzer(this.projectAnalyzer);
    return this.projectAnalyzer.analyzeModel(node, signal);
  }

  async analyzeModelTree(node: DagNode, sql: string | undefined, signal: AbortSignal): Promise<ModelsAnalyzeResult[]> {
    this.ensureProjectAnalyzer(this.projectAnalyzer);
    return this.projectAnalyzer.analyzeModelTree(node, sql, signal);
  }

  async analyzeSql(sql: string, signal: AbortSignal): Promise<AnalyzeResult> {
    this.ensureProjectAnalyzer(this.projectAnalyzer);
    return this.projectAnalyzer.analyzeSql(sql, signal);
  }

  // TODO: delete
  cancelAnalyze(): void {
    this.projectAnalyzeTask?.stop();
  }

  async analyzeProject(analyzeTracker: AnalyzeTrackerFunc): Promise<ModelsAnalyzeResult[]> {
    this.ensureProjectAnalyzer(this.projectAnalyzer);
    if (!this.projectName) {
      throw new Error('projectName is not initialized');
    }
    this.projectAnalyzeTask?.stop();
    this.projectAnalyzeTask = new ProjectAnalyzeTask(this.projectAnalyzer, this.projectName, analyzeTracker);
    return this.projectAnalyzeTask.start();
  }

  async analyzeSources(): Promise<void> {
    this.ensureProjectAnalyzer(this.projectAnalyzer);

    await this.projectAnalyzer.analyzeSources(new AbortController().signal);
  }

  async analyzeSeeds(): Promise<void> {
    this.ensureProjectAnalyzer(this.projectAnalyzer);

    await this.projectAnalyzer.analyzeSeeds(new AbortController().signal);
  }

  resetCache(): void {
    this.projectAnalyzer?.resetCache();
  }

  getColumnsInRelation(db: string | undefined, schema: string | undefined, tableName: string): KnownColumn[] | undefined {
    this.ensureProjectAnalyzer(this.projectAnalyzer);
    return this.projectAnalyzer.getColumnsInRelation(db, schema, tableName);
  }

  private ensureProjectAnalyzer(projectAnalyzer?: ProjectAnalyzer): asserts projectAnalyzer is ProjectAnalyzer {
    if (!projectAnalyzer) {
      throw new Error(DestinationContext.NOT_INITIALIZED_ERROR);
    }
  }

  dispose(): void {
    this.projectAnalyzer?.dispose();
  }
}
