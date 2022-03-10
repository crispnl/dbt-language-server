import { DbtRpcClient } from './DbtRpcClient';
import { Command } from './dbt_commands/Command';
import { DbtCommandExecutor } from './dbt_commands/DbtCommandExecutor';
import { deferred } from './utils/Utils';

export class DbtRpcServer {
  static readonly DBT_COMMAND_EXECUTOR = new DbtCommandExecutor();

  startDeferred = deferred<void>();

  private dbtRpcClient?: DbtRpcClient;
  private rpcPid?: number;

  async startDbtRpc(command: Command, dbtRpcClient: DbtRpcClient): Promise<void> {
    this.dbtRpcClient = dbtRpcClient;

    try {
      let started = false;
      console.log(`Starting dbt-rpc: ${command.toString()}`);
      const promiseWithChild = DbtRpcServer.DBT_COMMAND_EXECUTOR.execute(command, async (data: string) => {
        if (!started) {
          if (!this.rpcPid) {
            const matchResults = data.match(/"process": (\d*)/);
            if (matchResults?.length === 2) {
              this.rpcPid = Number(matchResults[1]);
            }
          }

          if (data.includes('Serving RPC server')) {
            try {
              await this.ensureCompilationFinished();
            } catch (e) {
              // The server is started here but there is some problem with project compilation
              console.log(e);
            }
            console.log('dbt rpc started');
            if (!this.rpcPid) {
              this.startDeferred.reject("Couldn't find dbt-rpc process id");
            }
            started = true;
            this.startDeferred.resolve();
          }
        }
      });

      promiseWithChild.catch(e => {
        console.log(`dbt rpc command failed: ${JSON.stringify(e)}`);
        this.startDeferred.reject(e.stdout);
      });

      await this.startDeferred.promise;
    } catch (e) {
      this.startDeferred.reject(e);
    }
  }

  async ensureCompilationFinished(): Promise<void> {
    return new Promise((resolve, reject) => {
      const intervalId = setInterval(async () => {
        const status = await this.dbtRpcClient?.getStatus();
        if (status) {
          switch (status.result.state) {
            case 'compiling':
              break;
            case 'ready':
              clearInterval(intervalId);
              resolve();
              break;
            case 'error':
              clearInterval(intervalId);
              reject(status.result.error);
              break;
            default:
              console.log('State is not supported');
              break;
          }
        }
      }, 300);
    });
  }

  refreshServer(): void {
    if (this.rpcPid) {
      process.kill(this.rpcPid, 'SIGHUP');
    }
  }

  dispose(): void {
    if (this.rpcPid) {
      process.kill(this.rpcPid, 'SIGTERM');
    }
  }
}