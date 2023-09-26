import { DryRun } from './DryRun';

export class DryRunDev extends DryRun {
  readonly id = 'WizardForDbtCore(TM).dryRunDev';

  override execute(): Promise<void> {
    return this.dryRun('dev');
  }
}
