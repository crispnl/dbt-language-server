import { window } from 'vscode';
import { DbtLanguageClientManager } from '../DbtLanguageClientManager';
import { log } from '../Logger';
import { Command } from './CommandManager';

export class GenerateDocumentation implements Command {
  readonly id = 'WizardForDbtCore(TM).generateDocumentation';

  constructor(private dbtLanguageClientManager: DbtLanguageClientManager) {}

  async execute(): Promise<void> {
    // TODO: Support retrieving the current model from the cursor position in a dbt schema YAML file
    const client = await this.dbtLanguageClientManager.getClientForActiveDocument();
    if (client) {
      client.sendNotification('custom/generateDocumentation', window.activeTextEditor?.document.uri.toString());
    } else {
      window.showWarningMessage('First, open the model from the dbt project.').then(undefined, e => {
        log(`Error while sending notification: ${e instanceof Error ? e.message : String(e)}`);
      });
    }
  }
}
