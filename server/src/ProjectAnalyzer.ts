import { AnalyzeResponse__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/AnalyzeResponse';
import { err, Result } from 'neverthrow';
import { DagNode } from './dag/DagNode';
import { DbtDestinationClient } from './DbtDestinationClient';
import { DbtRepository } from './DbtRepository';
import { ManifestModel, ManifestSource } from './manifest/ManifestJson';
import { TableDefinition } from './TableDefinition';
import { TableFetcher } from './TableFetcher';
import { getTableRefUniqueId } from './utils/ManifestUtils';
import { ZetaSqlApi } from './ZetaSqlApi';
import { ParseResult } from './ZetaSqlParser';
import { KnownColumn, ZetaSqlWrapper } from './ZetaSqlWrapper';

export type ModelsAnalyzeResult = {
  modelUniqueId: string;
  analyzeResult: AnalyzeResult;
};

export interface AnalyzeResult {
  ast: Result<AnalyzeResponse__Output, string>;
  parseResult: ParseResult;
}

export class ProjectAnalyzer {
  private tableFetcher: TableFetcher;

  constructor(
    public dbtRepository: DbtRepository,
    private destinationClient: DbtDestinationClient,
    public zetaSqlWrapper: ZetaSqlWrapper,
    private zetaSqlApi: ZetaSqlApi,
  ) {
    this.tableFetcher = this.createTableFetcher();
  }

  async initialize(): Promise<void> {
    await this.zetaSqlWrapper.initializeZetaSql();
  }

  createTableFetcher(): TableFetcher {
    return new TableFetcher(this.destinationClient, this.zetaSqlApi);
  }

  resetCache(): void {
    this.zetaSqlWrapper.resetCatalog();
    this.tableFetcher = this.createTableFetcher();
  }

  getColumnsInRelation(db: string | undefined, schema: string | undefined, tableName: string): KnownColumn[] | undefined {
    return this.zetaSqlWrapper.getColumnsInTable(db, schema, tableName);
  }

  /** Analyzes a single model */
  async analyzeModel(node: DagNode, signal: AbortSignal): Promise<ModelsAnalyzeResult[]> {
    return this.analyzeModelTreeInternal(node, undefined, new Map(), signal);
  }

  /** Analyzes a single model and all models that depend on it */
  async analyzeModelTree(node: DagNode, sql: string | undefined, signal: AbortSignal): Promise<ModelsAnalyzeResult[]> {
    return this.analyzeModelTreeInternal(node, sql, new Map(), signal);
  }

  async analyzeSql(sql: string, signal: AbortSignal): Promise<AnalyzeResult> {
    return this.analyzeModelCached(undefined, sql, new Map(), signal);
  }

  async analyzeSources(signal: AbortSignal): Promise<void> {
    const settledResult = await Promise.allSettled(
      this.dbtRepository.sources.map(async s => {
        const tableDefinition = this.createTableDefinitionForSource(s);
        // TODO: Probably need to implement abort here too
        return this.tableFetcher.fetchTable(tableDefinition).then(ts => {
          if (ts) {
            tableDefinition.columns = ts.columns;
            tableDefinition.timePartitioning = ts.timePartitioning;
            tableDefinition.external = ts.external;
          }
          return tableDefinition;
        });
      }),
    );
    settledResult
      .filter((v): v is PromiseFulfilledResult<TableDefinition> => v.status === 'fulfilled')
      .forEach(v => {
        if (v.value.schemaIsFilled() && !signal.aborted) {
          this.zetaSqlWrapper.registerTable(v.value);
        }
      });
  }

  async analyzeSeeds(signal: AbortSignal): Promise<void> {
    const settledResult = await Promise.allSettled(
      this.dbtRepository.seeds.map(async s => {
        const tableDefinition = this.createTableDefinitionForSource(s);
        // TODO: Probably need to implement abort here too
        return this.tableFetcher.fetchTable(tableDefinition).then(ts => {
          if (ts) {
            tableDefinition.columns = ts.columns;
            tableDefinition.timePartitioning = ts.timePartitioning;
            tableDefinition.external = ts.external;
          }
          return tableDefinition;
        });
      }),
    );
    settledResult
      .filter((v): v is PromiseFulfilledResult<TableDefinition> => v.status === 'fulfilled')
      .forEach(v => {
        if (v.value.schemaIsFilled() && !signal.aborted) {
          this.zetaSqlWrapper.registerTable(v.value);
        }
      });
  }

  dispose(): void {
    this.zetaSqlWrapper.terminateServer();
  }

  async analyzeModelTreeInternal(
    node: DagNode,
    sql: string | undefined,
    visitedModels: Map<string, Promise<AnalyzeResult>>,
    signal: AbortSignal,
  ): Promise<ModelsAnalyzeResult[]> {
    const model = node.getValue();
    const analyzeResult = await this.analyzeModelCached(model, sql, visitedModels, signal);
    return [{ modelUniqueId: model.uniqueId, analyzeResult }];

    /* TODO: Uncomment when we will be able to analyze all models or part of models in the tree
    let results: ModelsAnalyzeResult[] = [{ modelUniqueId: model.uniqueId, analyzeResult }];

    if (analyzeResult.isErr()) {
      // We don't analyze models that depend on this model because they all will have errors
      return results;
    }

    // If main model is OK we analyze it's first level children
    const children = node.getChildren();
    for (const child of children) {
      console.log('ANALYZE CHILD ' + child.getValue().uniqueId);
      const childModel = child.getValue();
      const childResult = await this.analyzeModelCached(childModel, tableFetcher, undefined, visitedModels);
      results = [...results, { modelUniqueId: childModel.uniqueId, analyzeResult: childResult }];
    }
    return results;
    */
  }

  private async analyzeModelCached(
    model: ManifestModel | undefined,
    sql: string | undefined,
    visitedModels: Map<string, Promise<AnalyzeResult>>,
    signal: AbortSignal,
  ): Promise<AnalyzeResult> {
    await this.zetaSqlWrapper.registerAllLanguageFeatures();
    const cacheKey = model?.uniqueId;
    let promise = cacheKey ? visitedModels.get(cacheKey) : undefined;
    if (!promise) {
      promise = this.analyzeModelInternal(model, sql, visitedModels, signal);
      if (cacheKey) {
        visitedModels.set(cacheKey, promise);
      }
    }
    return promise;
  }

  private async analyzeModelInternal(
    model: ManifestModel | undefined,
    sql: string | undefined,
    visitedModels: Map<string, Promise<AnalyzeResult>>,
    signal: AbortSignal,
  ): Promise<AnalyzeResult> {
    const compiledSql = sql ?? this.getCompiledCode(model);
    if (compiledSql === undefined) {
      return {
        ast: err(`Compiled SQL not found for model ${model?.uniqueId ?? 'undefined'}`),
        parseResult: {
          functions: [],
          selects: [],
          definitions: [],
        },
      };
    }

    const tables = await this.zetaSqlWrapper.findTableNames(compiledSql);
    if (tables.length > 0) {
      await this.analyzeAllEphemeralModels(model, visitedModels, signal);
    }

    const sources: TableDefinition[] = [];

    for (const table of tables) {
      if (signal.aborted) {
        return this.abortedResult();
      }
      if (!this.zetaSqlWrapper.isTableRegistered(table)) {
        const refId = getTableRefUniqueId(model, table.getTableName(), this.dbtRepository, table.getDataSetName());
        if (refId) {
          // TODO: Fix finding seeds
          const refModel = this.dbtRepository.dag.nodes.find(n => n.getValue().uniqueId === refId)?.getValue();
          if (refModel) {
            await this.analyzeModelCached(refModel, undefined, visitedModels, signal);
          } else {
            console.log("Can't find ref model by id");
          }
        } else {
          // We are dealing with a source here, probably.  Read definition remotely.
          sources.push(table);
        }
      }
    }

    const settledResult = await Promise.allSettled(
      sources.map(t =>
        this.tableFetcher.fetchTable(t).then(ts => {
          if (ts) {
            t.columns = ts.columns;
            t.timePartitioning = ts.timePartitioning;
            t.external = ts.external;
          }
          return t;
        }),
      ),
    );
    settledResult
      .filter((v): v is PromiseFulfilledResult<TableDefinition> => v.status === 'fulfilled')
      .forEach(v => {
        if (v.value.schemaIsFilled() && !signal.aborted) {
          this.zetaSqlWrapper.registerTable(v.value);
        }
      });
    if (signal.aborted) {
      return this.abortedResult();
    }

    const parseResult = await this.zetaSqlWrapper.getParseResult(compiledSql);
    await this.zetaSqlWrapper.registerPersistentUdfs(parseResult);
    const tempUdfs = await this.zetaSqlWrapper.getTempUdfs(model?.config?.sqlHeader);
    const catalogWithTempUdfs = this.zetaSqlWrapper.createCatalogWithTempUdfs(tempUdfs);

    const ast = await this.zetaSqlWrapper.getAstOrError(compiledSql, catalogWithTempUdfs);
    if (ast.isOk() && model) {
      const table = this.createTableDefinition(model);
      this.fillTableWithAnalyzeResponse(table, ast.value);
      this.zetaSqlWrapper.registerTable(table);
    } else {
      // TODO: Send error to VSC and highlight error line
      console.log(ast, model);
    }

    return {
      ast,
      parseResult,
    };
  }

  private abortedResult(): AnalyzeResult {
    return {
      ast: err('Aborted'),
      parseResult: {
        functions: [],
        selects: [],
        definitions: [],
      },
    };
  }

  private createTableDefinition(model: ManifestModel): TableDefinition {
    return this.zetaSqlWrapper.createTableDefinition([model.database, model.schema, model.alias ?? model.name]);
  }

  private createTableDefinitionForSource(model: ManifestSource): TableDefinition {
    return this.zetaSqlWrapper.createTableDefinition([model.database, model.schema, model.name]);
  }

  private fillTableWithAnalyzeResponse(table: TableDefinition, analyzeOutput: AnalyzeResponse__Output): void {
    table.columns = analyzeOutput.resolvedStatement?.resolvedQueryStmtNode?.outputColumnList
      .filter(c => c.column !== null)
      .map(c => ZetaSqlWrapper.createSimpleColumn(c.name, c.column?.type ?? null));
  }

  private async analyzeAllEphemeralModels(
    model: ManifestModel | undefined,
    visitedModels: Map<string, Promise<AnalyzeResult>>,
    signal: AbortSignal,
  ): Promise<void> {
    for (const node of model?.dependsOn.nodes ?? []) {
      const dependsOnEphemeralModel = this.dbtRepository.dag.nodes
        .find(n => n.getValue().uniqueId === node && n.getValue().config?.materialized === 'ephemeral')
        ?.getValue();
      if (dependsOnEphemeralModel) {
        const table = this.createTableDefinition(dependsOnEphemeralModel);
        if (!this.zetaSqlWrapper.isTableRegistered(table)) {
          await this.analyzeModelCached(dependsOnEphemeralModel, undefined, visitedModels, signal);
        }
      }
    }
  }

  private getCompiledCode(model?: ManifestModel): string | undefined {
    return model ? this.dbtRepository.getModelCompiledCode(model) : undefined;
  }
}
