import { Result } from 'neverthrow';
import { Emitter, Event } from 'vscode-languageserver';
import { DbtRepository } from './DbtRepository';
import { LogLevel } from './Logger';
import { DbtCli } from './dbt_execution/DbtCli';
import { DbtCompileJob } from './dbt_execution/DbtCompileJob';
import { wait } from './utils/Utils';

export class ModelCompiler {
  private dbtCompileJobQueue: DbtCompileJob[] = [];
  private pollIsRunning = false;

  private onCompilationErrorEmitter = new Emitter<string>();
  private onCompilationFinishedEmitter = new Emitter<string>();
  private onFinishAllCompilationJobsEmitter = new Emitter<void>();

  compilationInProgress = false;

  get onCompilationError(): Event<string> {
    return this.onCompilationErrorEmitter.event;
  }

  get onCompilationFinished(): Event<string> {
    return this.onCompilationFinishedEmitter.event;
  }

  get onFinishAllCompilationJobs(): Event<void> {
    return this.onFinishAllCompilationJobsEmitter.event;
  }

  constructor(
    private dbtCli: DbtCli,
    private dbtRepository: DbtRepository,
  ) {}

  async compile(modelPath: string, allowFallback: boolean): Promise<void> {
    console.log(`Start compiling ${modelPath}`, LogLevel.Debug);
    this.compilationInProgress = true;

    if (this.dbtCompileJobQueue.length > 3) {
      const jobToStop = this.dbtCompileJobQueue.shift();
      jobToStop?.forceStop();
    }
    // eslint-disable-next-line @typescript-eslint/no-floating-promises
    this.startNewJob(modelPath, allowFallback);

    await this.pollResults();
  }

  async startNewJob(modelPath: string, allowFallback: boolean, target?: string): Promise<Result<undefined, string> | void> {
    const job = this.dbtCli.createCompileJob(modelPath, this.dbtRepository, allowFallback, false, target);
    this.dbtCompileJobQueue.push(job);
    try {
      return await job.start();
    } catch (e) {
      return console.log(`Failed to start job: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  async pollResults(): Promise<void> {
    if (this.pollIsRunning) {
      return;
    }
    this.pollIsRunning = true;

    while (this.dbtCompileJobQueue.length > 0) {
      const { length } = this.dbtCompileJobQueue;

      for (let i = length - 1; i >= 0; i--) {
        const result = this.dbtCompileJobQueue[i].getResult();

        if (result) {
          const jobsToStop = this.dbtCompileJobQueue.splice(0, i + 1);
          for (let j = 0; j < i; j++) {
            jobsToStop[j].forceStop();
          }

          if (result.isErr()) {
            this.onCompilationErrorEmitter.fire(result.error);
          } else {
            this.onCompilationFinishedEmitter.fire(result.value);
          }
          break;
        }
      }

      if (this.dbtCompileJobQueue.length === 0) {
        break;
      }
      await wait(500);
    }
    this.pollIsRunning = false;
    this.compilationInProgress = false;
    this.onFinishAllCompilationJobsEmitter.fire();
  }
}
