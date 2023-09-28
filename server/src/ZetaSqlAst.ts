/* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call */

import { AnyResolvedScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/AnyResolvedScanProto';
import { ParseLocationRangeProto, ParseLocationRangeProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ParseLocationRangeProto';
import { ResolvedColumnRefProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedColumnRefProto';
import { ResolvedComputedColumnProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedComputedColumnProto';
import { ResolvedFunctionCallProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedFunctionCallProto';
import { ResolvedLiteralProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedLiteralProto';
import { ResolvedOutputColumnProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedOutputColumnProto';
import { ResolvedProjectScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedProjectScanProto';
import { ResolvedQueryStmtProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedQueryStmtProto';
import { ResolvedScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedScanProto';
import { ResolvedSetOperationScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedSetOperationScanProto';
import { ResolvedTableScanProto, ResolvedTableScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedTableScanProto';
import { ResolvedWithEntryProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedWithEntryProto';
import { ResolvedWithRefScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedWithRefScanProto';
import { ResolvedWithScanProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedWithScanProto';
import { AnalyzeResponse__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/AnalyzeResponse';
import { extractDatasetFromFullName } from './utils/Utils';
import { TYPE_KIND_NAMES, positionInRange, rangeContainsRange, rangesEqual, traverse } from './utils/ZetaSqlUtils';

export interface SourceColumn {
  table?: string;
  literal?: any;
  columnId: Long;
  name: string;
}

interface ColumnEdge {
  sourceColumnId: Long;
  targetColumnId: Long;
  edgeType: 'union' | 'function' | 'cte';
}

interface OutputColumn {
  columnId: Long;
  name: string;
}

export interface LineageInfo {
  sourceColumns: SourceColumn[];
  edges: ColumnEdge[];
  outputColumns: OutputColumn[];
}

export class ZetaSqlAst {
  propertyNames = [
    'aggregateExpressionList',
    'aggregateList',
    'annotations',
    'anonymizationOptionList',
    'argumentList',
    'arguments',
    'arrayExpr',
    'arrayOffsetColumn',
    'assertRowsModified',
    'body',
    'childList',
    'cloneFrom',
    'clusterByList',
    'collationName',
    'columnDefinitionList',
    'columnRef',
    'computedColumnsList',
    'connection',
    'defaultExpression',
    'descriptorArg',
    'elementList',
    'expr',
    'expression',
    'exprList',
    'extendedCast',
    'fieldList',
    'filterExpr',
    'filterFieldArgList',
    'format',
    'forSystemTimeExpr',
    'fromScan',
    'functionExpression',
    'functionGroupList',
    'generatedColumnInfo',
    'genericArgumentList',
    'getFieldList',
    'granteeExprList',
    'groupByColumnList',
    'groupByList',
    'groupingSetList',
    'havingModifier',
    'hintList',
    'indexItemList',
    'inExpr',
    'inlineLambda',
    'inputColumnList',
    'inputItemList',
    'inputScan',
    'joinExpr',
    'kThresholdExpr',
    'leftScan',
    'likeExpr',
    'limit',
    'mergeExpr',
    'model',
    'offset',
    'optionList',
    'orderByItemList',
    'outputColumnList',
    'parameterList',
    'partitionByList',
    'predicate',
    'query',
    'queryParameterList',
    'repeatableArgument',
    'replaceFieldItemList',
    'returning',
    'rightScan',
    'rollupColumnList',
    'rowList',
    'scan',
    'signature',
    'size',
    'sql',
    'statement',
    'storingExpressionList',
    'subquery',
    'tableAndColumnIndexList',
    'tableScan',
    'target',
    'targetTable',
    'timeZone',
    'transformAnalyticFunctionGroupList',
    'transformInputColumnList',
    'transformList',
    'transformOutputColumnList',
    'unnestExpressionsList',
    'usingArgumentList',
    'weightColumn',
    'whenClauseList',
    'whereExpr',
    'windowFrame',
    'withEntryList',
    'withGroupRowsParameterList',
    'withGroupRowsSubquery',
    'withPartitionColumns',
    'withSubquery',
  ];

  getLineage(ast: AnalyzeResponse__Output): LineageInfo {
    const result: LineageInfo = {
      sourceColumns: [],
      edges: [],
      outputColumns: [],
    };

    const resolvedStatementNode = ast.resolvedStatement?.resolvedQueryStmtNode;
    if (!resolvedStatementNode) {
      return result;
    }

    result.outputColumns = resolvedStatementNode.outputColumnList.flatMap(c =>
      c.column
        ? [
            {
              columnId: c.column.columnId,
              name: c.column.name,
            },
          ]
        : [],
    );

    const cteMaps: Record<string, { targetColumnId: Long }[]> = {};

    const targetColumnStack: {
      name: string;
      columnId: Long;
    }[] = [];

    // https://github.com/google/zetasql/blob/master/docs/resolved_ast.md
    traverse(
      resolvedStatementNode,
      new Map([
        [
          'resolvedTableScanNode',
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedTableScanProto__Output;

              const newSourceColumns = typedNode.parent?.columnList.map(c => ({
                table: typedNode.table?.fullName,
                columnId: c.columnId,
                name: c.name,
              }));

              if (newSourceColumns) result.sourceColumns.push(...newSourceColumns);
            },
          },
        ],
        [
          'resolvedLiteralNode',
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedLiteralProto__Output;
              const targetCol = targetColumnStack.at(-1);

              if (targetCol) {
                result.sourceColumns.push({
                  columnId: targetCol.columnId,
                  literal: typedNode.value?.value,
                  name: targetCol.name,
                });
              }
            },
          },
        ],
        [
          'resolvedComputedColumnNode',
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedComputedColumnProto__Output;

              if (typedNode.column) {
                targetColumnStack.push({
                  columnId: typedNode.column.columnId,
                  name: typedNode.column.name,
                });
              }
            },
            actionAfter: (): void => {
              targetColumnStack.pop();
            },
          },
        ],
        [
          'resolvedColumnRefNode',
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedColumnRefProto__Output;
              const targetCol = targetColumnStack.at(-1);
              if (typedNode.column && targetCol) {
                result.edges.push({
                  targetColumnId: targetCol.columnId,
                  sourceColumnId: typedNode.column.columnId,
                  edgeType: 'function',
                });
              }
            },
          },
        ],
        [
          'resolvedWithEntryNode',
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedWithEntryProto__Output;

              if (typedNode.withSubquery) {
                const innerNodeType = typedNode.withSubquery.node;

                // TODO: Could be a few other types
                const innerNode = typedNode.withSubquery[innerNodeType] as ResolvedProjectScanProto__Output;

                cteMaps[typedNode.withQueryName] = innerNode.parent?.columnList.map(c => ({ targetColumnId: c.columnId })) || [];
              }
            },
          },
        ],
        [
          'resolvedWithRefScanNode',
          // "The column_list produced here will match 1:1 with the column_list produced
          // by the referenced subquery and will given a new unique id to each column
          // produced for this scan."
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedWithRefScanProto__Output;

              const extraEdges = typedNode.parent?.columnList.map<ColumnEdge>((c, i) => ({
                sourceColumnId: cteMaps[c.tableName][i].targetColumnId,
                targetColumnId: c.columnId,
                edgeType: 'cte',
              }));

              if (extraEdges) result.edges.push(...extraEdges);
            },
          },
        ],
        [
          'resolvedSetOperationScanNode',
          {
            actionBefore: (node: unknown): void => {
              const typedNode = node as ResolvedSetOperationScanProto__Output;

              const outputColumns = typedNode.parent?.columnList;
              if (!outputColumns) return;

              for (const inputItem of typedNode.inputItemList) {
                const newEdges = inputItem.outputColumnList.map<ColumnEdge>((c, i) => ({
                  sourceColumnId: c.columnId,
                  targetColumnId: outputColumns[i].columnId,
                  edgeType: 'union',
                }));

                result.edges.push(...newEdges);
              }
            },
          },
        ],
      ]),
    );

    return result;
  }

  getHoverInfo(ast: AnalyzeResponse__Output, text: string): HoverInfo {
    const result: HoverInfo = {};
    const resolvedStatementNode = ast.resolvedStatement ? ast.resolvedStatement[ast.resolvedStatement.node] : undefined;
    if (resolvedStatementNode) {
      this.traversal(
        resolvedStatementNode,
        (node: unknown, nodeName?: string) => {
          if (nodeName === NODE.resolvedQueryStmtNode) {
            const queryStmtNode = node as ResolvedQueryStmtProto__Output;
            result.outputColumn = queryStmtNode.outputColumnList.find(c => c.name === text);
            if (!result.outputColumn && queryStmtNode.outputColumnList.some(c => c.column?.tableName === text)) {
              result.tableName = text;
            }
          }
          if (nodeName === NODE.resolvedTableScanNode) {
            const tableScanNode = node as ResolvedTableScanProto__Output;
            if (tableScanNode.table?.fullName === text || tableScanNode.table?.name === text) {
              result.tableName = tableScanNode.table.fullName || tableScanNode.table.name;
            }
          }
          if (nodeName === NODE.resolvedFunctionCallNode) {
            const functionCallNode = node as ResolvedFunctionCallProto__Output;
            if (functionCallNode.parent?.function?.name === `ZetaSQL:${text}`) {
              result.function = true;
            }
          }
          if (!nodeName && (node as { withQueryName?: string }).withQueryName === text) {
            result.withQueryName = text;
          }
        },
        ast.resolvedStatement?.node,
      );
    }
    return result;
  }

  getCompletionInfo(ast: AnalyzeResponse__Output, offset: number): CompletionInfo {
    const completionInfo: CompletionInfo = {
      resolvedTables: new Map<string, string[]>(),
      withNames: new Set<string>(),
      withSubqueries: new Map<string, WithSubqueryInfo>(),
      activeTables: [],
    };

    const parentNodes: {
      name?: string;
      parseLocationRange: ParseLocationRangeProto__Output;
      value: unknown;
      activeTableLocationRanges?: ParseLocationRangeProto__Output[];
      activeTables: ActiveTableInfo[];
    }[] = [];

    const { resolvedStatement } = ast;
    const resolvedStatementNode = resolvedStatement?.node ? resolvedStatement[resolvedStatement.node] : undefined;
    if (resolvedStatementNode) {
      let activeTablesFound = false;
      this.traversal(
        resolvedStatementNode,
        (node: unknown, nodeName?: string) => {
          if (nodeName !== NODE.resolvedTableScanNode && nodeName !== NODE.resolvedJoinScanNode) {
            const parseLocationRange = this.getParseLocationRange(node);
            if (parseLocationRange) {
              parentNodes.push({ name: nodeName, parseLocationRange, value: node, activeTables: [] });
            }
          }

          if (nodeName === NODE.resolvedTableScanNode) {
            const typedNode = node as ResolvedTableScanProto;
            const { resolvedTables } = completionInfo;
            if (typedNode.table?.fullName) {
              const { fullName } = typedNode.table;
              if (!resolvedTables.get(fullName)) {
                const columns: string[] = [];
                resolvedTables.set(fullName, columns);
                typedNode.parent?.columnList?.forEach(column => {
                  if (column.name) {
                    columns.push(column.name);
                  }
                });
              }
            }
          }

          if (nodeName === NODE.resolvedWithScanNode) {
            const resolvedWithScan = node as ResolvedWithScanProto__Output;
            const { withEntryList } = resolvedWithScan;
            withEntryList.map(w => w.withQueryName).forEach(n => completionInfo.withNames.add(n)); // TODO: delete completionInfo.withNames

            for (const withEntry of withEntryList) {
              this.getWithSubqueryInfo(withEntry.withSubquery, completionInfo, withEntry.withQueryName);
            }

            const { query } = resolvedWithScan;
            if (query) {
              const queryNode = query[query.node];
              if (queryNode && 'parent' in queryNode) {
                const withSubquery: WithSubqueryInfo = {
                  columns: [],
                };
                completionInfo.withSubqueries.set('___mainQuery', withSubquery);

                queryNode.parent?.columnList.forEach(c => {
                  if (c.name) {
                    withSubquery.columns.push({
                      name: c.name,
                      type: c.type?.typeKind ? TYPE_KIND_NAMES.get(c.type.typeKind) : undefined,
                      fromTable: c.tableName,
                    });
                  }
                });
              }
            }
          }
        },
        resolvedStatement?.node,
        (node: any, nodeName?: string) => {
          if (parentNodes.length > 0 && !activeTablesFound) {
            const parseLocationRange = this.getParseLocationRange(node);
            if (!parseLocationRange) {
              return;
            }
            const parentNode = parentNodes.at(-1);
            if (!parentNode) {
              return;
            }

            if (nodeName === NODE.resolvedTableScanNode) {
              if (rangeContainsRange(parentNode.parseLocationRange, parseLocationRange) && positionInRange(offset, parentNode.parseLocationRange)) {
                parentNode.activeTableLocationRanges = parentNode.activeTableLocationRanges ?? [];
                parentNode.activeTableLocationRanges.push(parseLocationRange);

                if (!positionInRange(offset, parseLocationRange)) {
                  const tableScanNode: ResolvedTableScanProto__Output = node;
                  const tables = parentNode.activeTables;
                  const name = tableScanNode.table?.name;
                  const tableNameRange = tableScanNode.parent?.parent?.parseLocationRange ?? undefined;
                  const alias = tableScanNode.alias || undefined; // for some tables alias is '' in ast
                  if (name && !tables.some(t => t.name === name && t.alias === alias) && tableScanNode.parent?.columnList) {
                    tables.push({
                      name,
                      alias,
                      columns: tableScanNode.parent.columnList.map<ResolvedColumn>(c => ({
                        name: c.name,
                        type: c.type?.typeKind ? TYPE_KIND_NAMES.get(c.type.typeKind) : undefined,
                        fromTable: c.tableName,
                      })),
                      tableNameRange,
                    });
                  }
                }
              }
            } else if (parentNode.name === nodeName && rangesEqual(parseLocationRange, parentNode.parseLocationRange)) {
              const n = parentNodes.pop();
              if (n?.activeTableLocationRanges && n.activeTableLocationRanges.length > 0) {
                completionInfo.activeTableLocationRanges = n.activeTableLocationRanges;
                completionInfo.activeTables = n.activeTables;
                activeTablesFound = true;
              } else if (positionInRange(offset, parentNode.parseLocationRange) && nodeName === NODE.resolvedProjectScanNode) {
                completionInfo.activeTableLocationRanges = [];
                activeTablesFound = true;
              }
            }
          }
        },
      );
    }
    return completionInfo;
  }

  getWithSubqueryInfo(node: AnyResolvedScanProto__Output | null, completionInfo: CompletionInfo, withQueryName: string): void {
    if (!node) {
      return;
    }
    const subqueryNode = node[node.node];
    if (subqueryNode && 'parent' in subqueryNode) {
      this.setWithSubquery(subqueryNode.parent, completionInfo, withQueryName);
      if ('inputScan' in subqueryNode) {
        const { inputScan } = subqueryNode;
        if (inputScan) {
          const inputScanNode = inputScan[inputScan.node];
          if (inputScanNode && 'withQueryName' in inputScanNode) {
            const existingWith = completionInfo.withSubqueries.get(inputScanNode.withQueryName);
            if (existingWith) {
              inputScanNode.parent?.columnList.forEach((c, i) => {
                if (c.name !== existingWith.columns[i].name) {
                  existingWith.columns[i].name = c.name;
                }
              });
            }
          } else {
            this.getWithSubqueryInfo(inputScan, completionInfo, withQueryName);
          }
        }
      }
    } else if (node.node === 'resolvedAggregateScanBaseNode') {
      const aggregateScanBase = node[node.node];
      // TODO: check 'resolvedAnonymizedAggregateScanNode'
      if (aggregateScanBase && aggregateScanBase.node === 'resolvedAggregateScanNode') {
        const aggregateScan = aggregateScanBase[aggregateScanBase.node];
        this.setWithSubquery(aggregateScan?.parent?.parent, completionInfo, withQueryName);

        aggregateScan?.parent?.groupByList.forEach(g => {
          if (g.expr?.node === 'resolvedColumnRefNode') {
            const column = g.expr[g.expr.node]?.column;
            const groupedColumn = g.column;
            const foundWith = completionInfo.withSubqueries.get(withQueryName);
            if (column && groupedColumn && foundWith) {
              const foundColumn = foundWith.columns.find(c => c.name === groupedColumn.name);
              if (foundColumn) {
                foundColumn.fromTable = column.tableName;
                foundColumn.name = column.name;
              }
            }
          }
        });
      }
    }
  }

  setWithSubquery(scanProto: ResolvedScanProto__Output | undefined | null, completionInfo: CompletionInfo, withQueryName: string): void {
    if (!completionInfo.withSubqueries.has(withQueryName)) {
      completionInfo.withSubqueries.set(withQueryName, {
        columns:
          scanProto?.columnList.map<ResolvedColumn>(c => ({
            name: c.name,
            type: c.type?.typeKind ? TYPE_KIND_NAMES.get(c.type.typeKind) : undefined,
            fromTable: c.tableName,
          })) ?? [],
        parseLocationRange: scanProto?.parent?.parseLocationRange ?? undefined,
      });
    }
  }

  getResolvedTables(ast: AnalyzeResponse__Output, text: string): ResolvedTable[] {
    const result: ResolvedTable[] = [];
    const resolvedStatementNode = ast.resolvedStatement ? ast.resolvedStatement[ast.resolvedStatement.node] : undefined;
    if (resolvedStatementNode) {
      this.traversal(
        resolvedStatementNode,
        (node: any, nodeName?: string) => {
          if (nodeName === NODE.resolvedTableScanNode) {
            const typedNode: ResolvedTableScanProto = node;
            const name = typedNode.table?.fullName;
            const parseLocationRange = this.requireParseLocationRange(typedNode.parent?.parent?.parseLocationRange);
            if (name && parseLocationRange) {
              const dataset = extractDatasetFromFullName(text.slice(parseLocationRange.start, parseLocationRange.end), name);
              if (dataset) {
                result.push({
                  schema: dataset,
                  name,
                  location: { start: parseLocationRange.start, end: parseLocationRange.end },
                });
              }
            }
          }
        },
        ast.resolvedStatement?.node,
      );
    }
    return result;
  }

  requireParseLocationRange(parseLocationRange?: ParseLocationRangeProto | null): ParseLocationRangeProto__Output | undefined {
    if (parseLocationRange?.start !== undefined && parseLocationRange.end !== undefined) {
      return parseLocationRange as ParseLocationRangeProto__Output;
    }
    return undefined;
  }

  getParseLocationRange(node: any): ParseLocationRangeProto__Output | undefined {
    let { parent } = node;
    while (parent) {
      const parseLocationRange = this.requireParseLocationRange(parent.parseLocationRange);
      if (parseLocationRange) {
        return parseLocationRange;
      }
      ({ parent } = parent);
    }
    return undefined;
  }

  traversal(node: any, beforeChildrenTraversal: ActionFunction, nodeName?: string, afterChildrenTraversal?: ActionFunction): void {
    beforeChildrenTraversal(node, nodeName);
    this.traversalChildren(this.propertyNames, node, beforeChildrenTraversal, afterChildrenTraversal);
    if (afterChildrenTraversal) {
      afterChildrenTraversal(node, nodeName);
    }
  }

  traversalChildren(propertyNames: string[], node: any, beforeChildrenTraversal: ActionFunction, afterChildrenTraversal?: ActionFunction): void {
    for (const name of propertyNames) {
      this.traversalChildIfExist(name, node, beforeChildrenTraversal, afterChildrenTraversal);
    }
    if (node.parent) {
      this.traversalChildren(this.propertyNames, node.parent, beforeChildrenTraversal, afterChildrenTraversal);
    }
    if (node.node) {
      this.traversal(node[node.node], beforeChildrenTraversal, node.node, afterChildrenTraversal);
    }
  }

  traversalChildIfExist(propertyName: string, node: any, beforeChildrenTraversal: ActionFunction, afterChildrenTraversal?: ActionFunction): void {
    if (propertyName in node) {
      const next = node[propertyName];
      if (next === null) {
        return;
      }
      if (Array.isArray(next)) {
        for (const nextItem of next) {
          this.traversal(nextItem, beforeChildrenTraversal, undefined, afterChildrenTraversal);
        }
      } else {
        const nextNodeName = next.node;
        const nextNode = nextNodeName ? node[propertyName][nextNodeName] : next;
        this.traversal(nextNode, beforeChildrenTraversal, nextNodeName, afterChildrenTraversal);
      }
    }
  }
}

type ActionFunction = (node: any, nodeName?: string) => void;

const NODE = {
  resolvedQueryStmtNode: 'resolvedQueryStmtNode',
  resolvedTableScanNode: 'resolvedTableScanNode',
  resolvedJoinScanNode: 'resolvedJoinScanNode',
  resolvedFunctionCallNode: 'resolvedFunctionCallNode',
  resolvedWithScanNode: 'resolvedWithScanNode',
  resolvedProjectScanNode: 'resolvedProjectScanNode',
};

interface HoverInfo {
  outputColumn?: ResolvedOutputColumnProto__Output;
  withQueryName?: string;
  tableName?: string;
  function?: boolean;
}

export interface CompletionInfo {
  resolvedTables: Map<string, string[]>;
  activeTableLocationRanges?: Location[];
  activeTables: ActiveTableInfo[];
  withNames: Set<string>;
  withSubqueries: Map<string, WithSubqueryInfo>;
}

interface WithSubqueryInfo {
  columns: ResolvedColumn[];
  parseLocationRange?: Location;
}

export interface ActiveTableInfo {
  name: string;
  alias?: string;
  columns: ResolvedColumn[];
  tableNameRange?: Location;
}

interface ResolvedColumn {
  name: string;
  type?: string;
  fromTable: string;
}

export interface ResolvedTable {
  schema: string;
  name: string;
  location: Location;
}

export interface Location {
  start: number;
  end: number;
}
