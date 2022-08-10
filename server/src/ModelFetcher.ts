import { pathEqual } from 'path-equal';
import { DbtRepository } from './DbtRepository';
import { ManifestModel } from './manifest/ManifestJson';
import retry = require('async-retry');

export class ModelFetcher {
  model: ManifestModel | undefined;
  fetchCompleted = false;

  constructor(private dbtRepository: DbtRepository, private modelPath: string) {}

  /** We retry here because in some situations manifest.json can appear a bit later after compilation is finished */
  async getModel(): Promise<ManifestModel | undefined> {
    if (!this.fetchCompleted) {
      try {
        this.model = await retry(
          () => {
            const model = this.dbtRepository.models.find(m => pathEqual(m.originalFilePath, this.modelPath));
            if (model === undefined) {
              console.log('Model not found in manifest.json, retrying...');
              throw new Error('Model not found in manifest.json');
            }

            return model;
          },
          { factor: 1, retries: 3, minTimeout: 100 },
        );
      } catch (e) {
        // Do nothing
      }
      this.fetchCompleted = true;
    }

    return this.model;
  }
}