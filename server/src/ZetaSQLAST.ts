import { SimpleType, TypeKind } from '@fivetrandevelopers/zetasql';
import { AnalyzeResponse } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/AnalyzeResponse';
import { ParseLocationRangeProto, ParseLocationRangeProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ParseLocationRangeProto';
import { ResolvedFunctionCallProto } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedFunctionCallProto';
import { ResolvedOutputColumnProto } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedOutputColumnProto';
import { ResolvedQueryStmtProto } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedQueryStmtProto';
import { ResolvedTableScanProto } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ResolvedTableScanProto';

export class ZetaSQLAST {
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

  getHoverInfo(ast: AnalyzeResponse, text: string): HoverInfo {
    const result: HoverInfo = {};
    const resolvedStatementNode = ast.resolvedStatement && ast.resolvedStatement.node ? ast.resolvedStatement[ast.resolvedStatement.node] : undefined;
    if (resolvedStatementNode) {
      this.traversal(
        resolvedStatementNode,
        (node: any, nodeName?: string) => {
          if (nodeName === Node.resolvedQueryStmtNode) {
            const typedNode = <ResolvedQueryStmtProto>node;
            result.outputColumn = typedNode.outputColumnList?.find(c => c.name === text);
            if (!result.outputColumn && typedNode.outputColumnList?.find(c => c.column?.tableName === text)) {
              result.tableName = text;
            }
          }
          if (nodeName === Node.resolvedTableScanNode) {
            const typedNode = <ResolvedTableScanProto>node;
            if (typedNode.table?.fullName === text || typedNode.table?.name === text) {
              result.tableName = typedNode.table?.fullName ?? typedNode.table?.name;
            }
          }
          if (nodeName === Node.resolvedFunctionCallNode) {
            const typedNode = <ResolvedFunctionCallProto>node;
            if (typedNode.parent?.function?.name === 'ZetaSQL:' + text) {
              result.function = true;
            }
          }
          if (!nodeName) {
            if ('withQueryName' in node && node.withQueryName === text) {
              result.withQueryName = text;
            }
          }
        },
        ast.resolvedStatement?.node,
      );
    }
    return result;
  }

  getCompletionInfo(ast: AnalyzeResponse, offset: number): CompletionInfo {
    const completionInfo: CompletionInfo = {
      resolvedTables: new Map<string, string[]>(),
    };
    let parentNodes: any[] = [];
    const resolvedStatementNode = ast.resolvedStatement && ast.resolvedStatement.node ? ast.resolvedStatement[ast.resolvedStatement.node] : undefined;
    if (resolvedStatementNode) {
      this.traversal(
        resolvedStatementNode,
        (node: any, nodeName?: string) => {
          if (nodeName !== Node.resolvedTableScanNode) {
            const parseLocationRange = this.getParseLocationRange(node);
            if (parseLocationRange) {
              parentNodes.push({ name: nodeName, parseLocationRange: parseLocationRange, value: node });
            }
          }

          if (nodeName === Node.resolvedTableScanNode) {
            const typedNode = <ResolvedTableScanProto>node;
            const resolvedTables = completionInfo.resolvedTables;
            if (typedNode.table && typedNode.table.fullName) {
              const fullName = typedNode.table.fullName;
              if (!resolvedTables.get(fullName)) {
                resolvedTables.set(fullName, []);
                typedNode.parent?.columnList?.forEach(column => {
                  if (column.name) {
                    resolvedTables.get(fullName)?.push(column.name);
                  }
                });
              }
            }
          }
        },
        ast.resolvedStatement?.node,
        (node: any, nodeName?: string) => {
          if (parentNodes.length > 0 && completionInfo.activeTableLocationRanges === undefined) {
            const parseLocationRange = this.getParseLocationRange(node);
            const parentNode = parentNodes[parentNodes.length - 1];

            if (nodeName === Node.resolvedTableScanNode) {
              if (
                parseLocationRange &&
                parentNode.parseLocationRange.start <= parseLocationRange.start &&
                parseLocationRange.end <= parentNode.parseLocationRange.end &&
                parentNode.parseLocationRange.start <= offset &&
                offset <= parentNode.parseLocationRange.end
              ) {
                if (!parentNode.activeTableLocationRanges) {
                  parentNode.activeTableLocationRanges = [];
                  parentNode.activeTableColumns = new Map<string, ResolvedColumn[]>();
                }
                parentNode.activeTableLocationRanges.push(parseLocationRange);
                if (offset < parseLocationRange?.start || parseLocationRange.end < offset) {
                  const typedNode = <ResolvedTableScanProto>node;
                  const columns = <Map<string, ResolvedColumn[]>>parentNode.activeTableColumns;
                  const tableName = typedNode.table?.name;
                  if (tableName && !columns.has(tableName) && typedNode.parent?.columnList) {
                    columns.set(
                      tableName,
                      typedNode.parent.columnList.map(
                        c =>
                          <ResolvedColumn>{
                            name: c.name,
                            type: c.type?.typeKind ? new SimpleType(<TypeKind>c.type.typeKind).getTypeName() : undefined,
                          },
                      ),
                    );
                  }
                }
              }
            } else if (
              parentNode.name === nodeName &&
              parseLocationRange?.start === parentNode.parseLocationRange.start &&
              parseLocationRange?.end === parentNode.parseLocationRange.end
            ) {
              const node = parentNodes.pop();
              if (node.activeTableLocationRanges?.length > 0) {
                completionInfo.activeTableLocationRanges = node.activeTableLocationRanges;
                completionInfo.activeTableColumns = node.activeTableColumns;
              }
            }
          }
        },
      );
    }
    return completionInfo;
  }

  isParseLocationRangeExist(parseLocationRange?: ParseLocationRangeProto) {
    return parseLocationRange?.start !== undefined && parseLocationRange.end !== undefined;
  }

  getParseLocationRange(node: any): ParseLocationRangeProto__Output | undefined {
    let parent = node.parent;
    while (parent) {
      if (this.isParseLocationRangeExist(parent.parseLocationRange)) {
        return <ParseLocationRangeProto__Output>parent.parseLocationRange;
      }
      parent = parent.parent;
    }
  }

  traversal(node: any, beforeChildrenTraversal: actionFunction, nodeName?: string, afterChildrenTraversal?: actionFunction) {
    beforeChildrenTraversal(node, nodeName);
    this.traversalChildren(this.propertyNames, node, beforeChildrenTraversal, afterChildrenTraversal);
    if (afterChildrenTraversal) {
      afterChildrenTraversal(node, nodeName);
    }
  }

  traversalChildren(propertyNames: string[], node: any, beforeChildrenTraversal: actionFunction, afterChildrenTraversal?: actionFunction) {
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

  traversalChildIfExist(propertyName: string, node: any, beforeChildrenTraversal: actionFunction, afterChildrenTraversal?: actionFunction) {
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

type actionFunction = (node: any, nodeName?: string) => void;

const Node = {
  resolvedQueryStmtNode: 'resolvedQueryStmtNode',
  resolvedTableScanNode: 'resolvedTableScanNode',
  resolvedFunctionCallNode: 'resolvedFunctionCallNode',
};

export interface HoverInfo {
  outputColumn?: ResolvedOutputColumnProto;
  withQueryName?: string;
  tableName?: string;
  function?: boolean;
}

export interface CompletionInfo {
  resolvedTables: Map<string, string[]>;
  activeTableLocationRanges?: ParseLocationRangeProto[];
  activeTableColumns?: Map<string, ResolvedColumn[]>;
}

export interface ResolvedColumn {
  name: string;
  type?: string;
}