import { AnalyzeResponse__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/AnalyzeResponse';
import { err, ok, Result } from 'neverthrow';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { DbtProfileSuccess } from '../DbtProfileCreator';
import { DestinationDefinition } from '../DestinationDefinition';
import { SchemaTracker } from '../SchemaTracker';
import { ZetaSqlWrapper } from '../ZetaSqlWrapper';
import { BigQueryClient } from './BigQueryClient';

export class BigQueryContext {
  private constructor(
    public schemaTracker: SchemaTracker,
    public destinationDefinition: DestinationDefinition,
    public zetaSqlWrapper: ZetaSqlWrapper,
  ) {}

  public static async createContext(profileResult: DbtProfileSuccess): Promise<Result<BigQueryContext, string>> {
    try {
      const clientResult = await profileResult.dbtProfile.createClient(profileResult.targetConfig);
      if (clientResult.isErr()) {
        return err(clientResult.error);
      }

      const bigQueryClient = clientResult.value as BigQueryClient;
      const destinationDefinition = new DestinationDefinition(bigQueryClient);

      const zetaSqlWrapper = new ZetaSqlWrapper();
      await zetaSqlWrapper.initializeZetaSql();

      const schemaTracker = new SchemaTracker(bigQueryClient, zetaSqlWrapper);
      return ok(new BigQueryContext(schemaTracker, destinationDefinition, zetaSqlWrapper));
    } catch (e) {
      return err('Data Warehouse initialization failed.');
    }
  }

  async getAstOrError(compiledDocument: TextDocument): Promise<Result<AnalyzeResponse__Output, string>> {
    try {
      const ast = await this.zetaSqlWrapper.analyze(compiledDocument.getText());
      console.log('AST was successfully received');
      return ok(ast);
    } catch (e: any) {
      console.log('There was an error wile parsing SQL query');
      return err(e.details ?? 'Unknown parser error [at 0:0]');
    }
  }

  async ensureCatalogInitialized(compiledDocument: TextDocument): Promise<void> {
    await this.schemaTracker.refreshTableNames(compiledDocument.getText());
    if (this.schemaTracker.hasNewTables || !this.zetaSqlWrapper.isCatalogRegistered()) {
      await this.registerCatalog();
    }
  }

  async registerCatalog(): Promise<void> {
    await this.zetaSqlWrapper.registerCatalog(this.schemaTracker.tableDefinitions);
    this.schemaTracker.resetHasNewTables();
  }

  public dispose(): void {
    void this.zetaSqlWrapper.terminateServer();
  }
}