import { Result, err, ok } from 'neverthrow';
import { PromiseWithChild } from 'node:child_process';
import path from 'node:path';
import { Emitter, Event, _Connection } from 'vscode-languageserver';
import { DbtRepository } from '../DbtRepository';
import { InstallUtils } from '../InstallUtils';
import { MacroCompilationServer } from '../MacroCompilationServer';
import { ModelProgressReporter } from '../ModelProgressReporter';
import { NotificationSender } from '../NotificationSender';
import { FeatureFinder } from '../feature_finder/FeatureFinder';
import { DbtCliCompileJob } from './DbtCliCompileJob';
import { DbtCommandExecutor } from './DbtCommandExecutor';
import { DbtCompileJob } from './DbtCompileJob';
import slash = require('slash');

export class DbtCli {
  dbtReady = false;
  onDbtReadyEmitter = new Emitter<void>();
  projectCompileJob?: DbtCompileJob;

  constructor(
    private featureFinder: FeatureFinder,
    private connection: _Connection,
    private modelProgressReporter: ModelProgressReporter,
    private notificationSender: NotificationSender,
    private macroCompilationServer: MacroCompilationServer,
    private dbtCommandExecutor: DbtCommandExecutor,
    private dbtCompileEnvVars?: Record<string, string>,
  ) {
    this.dbtReady = false;
    this.onDbtReadyEmitter = new Emitter<void>();
    this.macroCompilationServer
      .start()
      .catch(e => console.log(`Failed to start macroCompilationServer: ${e instanceof Error ? e.message : String(e)}`));
  }

  compile(
    modelName?: string,
    modifiedOnly?: boolean,
    target?: string,
  ): PromiseWithChild<{
    stdout: string;
    stderr: string;
  }> {
    const params = ['-f']; // Always compile full-refreshed to avoid issues with partial incremental updates

    if (target) {
      params.push('-t', target);
    }

    if (modelName) {
      params.push('-m', `+${slash(modelName)}`);
    } else {
      params.push('--exclude', 'resource_type:test'); // We ignore tests altogether

      if (modifiedOnly) {
        params.push('-m', 'state:modified', '--state', path.resolve('.dbt-track-manifest'));
      }
    }

    const log = (data: string): void => console.log(data);

    if (!this.macroCompilationServer.port) {
      throw new Error('Incorrect state: macroCompilationServer port is required');
    }
    return this.dbtCommandExecutor.compile(this.macroCompilationServer.port, this.featureFinder.profilesYmlDir, log, params, this.dbtCompileEnvVars);
  }

  async prepare(dbtProfileType?: string): Promise<void> {
    await this.featureFinder.availableDbtPromise;
    if (!this.featureFinder.versionInfo?.installedVersion || !this.featureFinder.versionInfo.installedAdapters.some(a => a.name === dbtProfileType)) {
      try {
        if (dbtProfileType) {
          await this.suggestToInstallDbt(this.featureFinder.getPythonPath(), dbtProfileType);
        } else {
          this.onDbtFindFailed(dbtProfileType);
        }
      } catch {
        this.onDbtFindFailed(dbtProfileType);
      }
    }
    this.dbtReady = true;
    this.onDbtReadyEmitter.fire();
  }

  createCompileJob(
    modelPath: string | undefined,
    dbtRepository: DbtRepository,
    allowFallback: boolean,
    useTrackManifest?: boolean,
    target?: string,
  ): DbtCompileJob {
    return new DbtCliCompileJob(modelPath, dbtRepository, allowFallback, this, useTrackManifest || false, target);
  }

  cancelCompileProject(): void {
    this.projectCompileJob?.forceStop();
  }

  async compileProject(dbtRepository: DbtRepository): Promise<Result<void, string>> {
    this.projectCompileJob = this.createCompileJob(undefined, dbtRepository, true);
    console.log('Starting project compilation');
    const result = await this.projectCompileJob.start();

    if (result.isOk()) {
      console.log('Project compiled successfully');
      return ok(undefined);
    }

    console.log(`There was an error while project compilation ${result.error}`);
    return err(result.error);
  }

  async deps(onStdoutData: (data: string) => void, onStderrData: (data: string) => void): Promise<void> {
    if (!this.macroCompilationServer.port) {
      throw new Error('Incorrect state: macroCompilationServer port is required');
    }
    await this.dbtCommandExecutor.deps(this.macroCompilationServer.port, this.featureFinder.profilesYmlDir, onStdoutData, onStderrData);
  }

  getError(): string {
    return this.getInstallError('dbt', 'python3 -m pip install dbt-bigquery');
  }

  async suggestToInstallDbt(python: string, dbtProfileType: string): Promise<void> {
    const actions = { title: 'Install', id: 'install' };
    const errorMessageResult = await this.connection.window.showErrorMessage(
      `dbt/adapters are not installed. You can specify [python environment](command:python.setInterpreter) that contains dbt with needed adapter. Otherwise you can install dbt and ${dbtProfileType} adapter by pressing Install button.`,
      actions,
    );

    if (errorMessageResult?.id === 'install') {
      console.log(`Trying to install dbt, and ${dbtProfileType} adapter`);
      const sendLog = (data: string): void => this.notificationSender.sendInstallDbtCoreLog(data);
      const installResult = await InstallUtils.installDbt(python, undefined, dbtProfileType, sendLog, sendLog);
      if (installResult.isOk()) {
        this.notificationSender.sendRestart();
      } else {
        this.finishWithError(installResult.error, dbtProfileType);
      }
    } else {
      this.onDbtFindFailed(dbtProfileType);
    }
  }

  finishWithError(message: string, dbtProfileType: string | undefined): void {
    this.modelProgressReporter.sendFinish();
    this.notificationSender.sendTelemetry('error', {
      name: 'vscodeErrorMessage',
      message: `${message}.
        Profile: ${dbtProfileType ?? 'undefined'}.
        Python: ${this.featureFinder.pythonInfo.version?.join('.') ?? '_'}.
        dbt: ${JSON.stringify(this.featureFinder.versionInfo?.installedVersion)}.
        Adapters: ${JSON.stringify(this.featureFinder.versionInfo?.installedAdapters)}`,
      stack: new Error('vscodeErrorMessage').stack ?? '',
    });
    this.connection.window.showErrorMessage(message);
  }

  getInstallError(command: string, pythonInstallCommand: string): string {
    return `Failed to find ${command}. You can use '${pythonInstallCommand}' command to install it. Check in Terminal that ${command} works running '${command} --version' command or [specify the Python environment](https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter) for VS Code that was used to install dbt (e.g. ~/dbt-env/bin/python3).`;
  }

  get onDbtReady(): Event<void> {
    return this.onDbtReadyEmitter.event;
  }

  private onDbtFindFailed(dbtProfileType: string | undefined): void {
    this.finishWithError(this.getError(), dbtProfileType);
  }
}
