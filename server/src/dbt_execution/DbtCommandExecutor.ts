import { PromiseWithChild } from 'node:child_process';
import { ProcessExecutor } from '../ProcessExecutor';

export class DbtCommandExecutor {
  static readonly PROCESS_EXECUTOR = new ProcessExecutor();

  constructor(
    private python: string,
    private scriptPath: string,
  ) {}

  compile(
    macroCompilerPort: number,
    profilesDir: string,
    onStderrData: (data: string) => void,
    params: string[],
    envVars?: Record<string, string>,
  ): PromiseWithChild<{
    stdout: string;
    stderr: string;
  }> {
    return DbtCommandExecutor.PROCESS_EXECUTOR.execProcess(
      `${this.python} ${this.scriptPath} ${macroCompilerPort} ${profilesDir} --cache-selected-only compile ${params.join(' ')}`,
      onStderrData,
      undefined,
      envVars,
    );
  }

  deps(
    macroCompilerPort: number,
    profilesDir: string,
    onStdoutData: (data: string) => void,
    onStderrData: (data: string) => void,
  ): PromiseWithChild<{
    stdout: string;
    stderr: string;
  }> {
    return DbtCommandExecutor.PROCESS_EXECUTOR.execProcess(
      `${this.python} ${this.scriptPath} ${macroCompilerPort} ${profilesDir} deps`,
      onStdoutData,
      onStderrData,
    );
  }

  version(): PromiseWithChild<{
    stdout: string;
    stderr: string;
  }> {
    return DbtCommandExecutor.PROCESS_EXECUTOR.execProcess(`${this.python} ${this.scriptPath} --version`);
  }
}
