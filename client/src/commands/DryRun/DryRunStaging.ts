import { DryRun } from './DryRun';

export class DryRunStaging extends DryRun {
  readonly id = 'WizardForDbtCore(TM).dryRunStaging';

  override execute(): Promise<void> {
    return this.dryRun('staging');
  }
}
