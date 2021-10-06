import { exec } from 'child_process';
import { promisify } from 'util';

export class ProcessExecutor {
  async execProcess(
    command: string,
    onData?: (data: any) => void,
  ): Promise<{
    stdout: string;
    stderr: string;
  }> {
    const promisifiedExec = promisify(exec);

    const promiseWithChild = promisifiedExec(command);
    const childProcess = promiseWithChild.child;

    childProcess.stdout?.on('data', chunk => {
      if (onData) {
        onData(chunk);
      }
    });
    childProcess.on('exit', code => {
      console.log(`Child process '${command}' exited with code ${code}`);
    });

    const kill = () => {
      childProcess.kill();
    };
    childProcess.on('exit', kill);
    // Catches Ctrl+C event
    childProcess.on('SIGINT', kill);
    // Catches "kill pid" (for example: nodemon restart)
    childProcess.on('SIGUSR1', kill);
    childProcess.on('SIGUSR2', kill);
    childProcess.on('uncaughtException', kill);
    return promiseWithChild;
  }
}
