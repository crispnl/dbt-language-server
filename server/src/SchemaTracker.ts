import { ZetaSQLClient } from '@fivetrandevelopers/zetasql';
import { ExtractTableNamesFromStatementRequest } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/ExtractTableNamesFromStatementRequest';
import { BigQueryClient } from './BigQueryClient';
import { TableDefinition } from './TableDefinition';
import { ServiceAccountCreds } from './YamlParser';

export class SchemaTracker {
  tableDefinitions: TableDefinition[] = [];
  serviceAccountCreds: ServiceAccountCreds | undefined;
  hasNewTables = false;

  constructor(serviceAccountCreds?: ServiceAccountCreds) {
    this.serviceAccountCreds = serviceAccountCreds;
  }

  resetHasNewTables() {
    this.hasNewTables = false;
  }

  async findTableNames(sql: string): Promise<TableDefinition[] | undefined> {
    const request: ExtractTableNamesFromStatementRequest = {
      sqlStatement: sql,
    };
    try {
      const extractResult = await ZetaSQLClient.INSTANCE.extractTableNamesFromStatement(request);
      return extractResult.tableName.map(t => new TableDefinition(t.tableNameSegment));
    } catch (e) {
      console.log(e);
    }
  }

  async refreshTableNames(sql: string) {
    const tableDefinitions = await this.findTableNames(sql);
    if (!tableDefinitions) {
      return;
    }

    const newTables = tableDefinitions.filter(
      newTable => !this.tableDefinitions.find(oldTable => this.arraysAreEqual(oldTable.name, newTable.name) && oldTable.rawName === newTable.rawName),
    );

    if (newTables.length > 0 && this.serviceAccountCreds) {
      const bigQueryClient = new BigQueryClient(this.serviceAccountCreds.keyFile, this.serviceAccountCreds.project);
      for (const table of newTables) {
        if (table.getDatasetName() && table.getTableName()) {
          // TODO: handle different project names?
          const schema = await bigQueryClient?.getTableSchema(table.getDatasetName(), table.getTableName());
          if (schema) {
            this.tableDefinitions.push(table);
            table.schema = schema;
          }
        }
      }
      this.hasNewTables = true;
    }
  }

  arraysAreEqual(a1: string[], a2: string[]) {
    return a1.length === a2.length && a1.every((value, index) => value === a2[index]);
  }
}
