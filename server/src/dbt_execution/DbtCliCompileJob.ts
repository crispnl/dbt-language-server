import { err, ok, Result } from 'neverthrow';
import { ChildProcess, ExecException } from 'node:child_process';
import * as fs from 'node:fs/promises';
import path from 'node:path';
import { DbtRepository } from '../DbtRepository';
import { ManifestParser } from '../manifest/ManifestParser';
import { runWithTimeout } from '../utils/Utils';
import { DbtCli } from './DbtCli';
import { DbtCompileJob } from './DbtCompileJob';

export class DbtCliCompileJob extends DbtCompileJob {
  static COMPILE_MODEL_TIMEOUT_MS = 110_000;
  static COMPILE_PROJECT_TIMEOUT_MS = 300_000;
  static TIMEOUT_EXCEEDED_ERROR = 'dbt compile timeout exceeded';

  private process?: ChildProcess;
  private canceleld = false;

  result?: Result<string, string>;

  /** If modelPath === undefined then we compile project */
  constructor(
    private modelPath: string | undefined,
    private dbtRepository: DbtRepository,
    private allowFallback: boolean,
    private dbtCli: DbtCli,
    private useTrackManifest: boolean,
  ) {
    super();
  }

  async start(): Promise<Result<undefined, string>> {
    if (!this.allowFallback) {
      this.result = ok(DbtCompileJob.NO_RESULT_FROM_COMPILER);
      return ok(undefined);
    }

    const promise = this.dbtCli.compile(this.modelPath, this.useTrackManifest);
    this.process = promise.child;

    try {
      await runWithTimeout(
        promise,
        this.modelPath ? DbtCliCompileJob.COMPILE_MODEL_TIMEOUT_MS : DbtCliCompileJob.COMPILE_PROJECT_TIMEOUT_MS,
        DbtCliCompileJob.TIMEOUT_EXCEEDED_ERROR,
      );
    } catch (e: unknown) {
      if (this.canceleld) {
        this.result = err('Canceled');
      } else if (e instanceof Object && 'stdout' in e) {
        const error = e as ExecException & { stdout?: string; stderr?: string };
        this.result = err(error.stdout ? DbtCompileJob.extractDbtError(error.stdout) : error.message);
      } else {
        this.result = err(e instanceof Error ? e.message : String(e));
      }
      return err(this.result.error);
    }

    if (this.modelPath) {
      await this.findResultFromFile(this.modelPath, this.dbtRepository);
    }
    await this.copyManifest();

    return ok(undefined);
  }

  private async copyManifest(): Promise<void> {
    const manifestPath = ManifestParser.getManifestPath(this.dbtRepository.dbtTargetPath);
    const trackManifestPath = ManifestParser.getManifestPath('.dbt-track-manifest');

    await fs.mkdir(path.dirname(trackManifestPath), { recursive: true });

    await fs.copyFile(manifestPath, trackManifestPath);
  }

  private async findResultFromFile(modelPath: string, dbtRepository: DbtRepository): Promise<void> {
    try {
      const compiledPath = await DbtCompileJob.findCompiledFilePath(modelPath, dbtRepository);
      const sql = await this.getCompiledSql(compiledPath);

      this.result = sql ? ok(sql) : err('Compiled file not found');
    } catch (e) {
      this.result = err(e instanceof Error ? e.message : String(e));
    }
  }

  forceStop(): void {
    this.canceleld = true;
    this.process?.kill('SIGKILL');
  }

  getResult(): Result<string, string> | undefined {
    return this.process?.exitCode === null ? undefined : this.result;
  }

  private async getCompiledSql(filePath: string): Promise<string | undefined> {
    try {
      return await fs.readFile(filePath, 'utf8');
    } catch {
      console.log(`Cannot get compiled sql for ${filePath}`);
      return undefined;
    }
  }
}
