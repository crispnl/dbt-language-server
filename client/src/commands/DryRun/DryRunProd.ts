import { DryRun } from './DryRun';

export class DryRunProd extends DryRun {
  readonly id = 'WizardForDbtCore(TM).dryRunProd';

  override execute(): Promise<void> {
    return this.dryRun('prod');
  }
}
