import { LineageInfo, SourceColumn } from '../ZetaSqlAst';

export function traceColumnSource(columnId: Long, lineage: LineageInfo): SourceColumn | undefined {
  // For now we only support non-branched paths (for unions, we take the first branch)
  const immediateSources = lineage.edges.filter(e => e.targetColumnId.eq(columnId));

  if (immediateSources.length === 0) {
    const sourceColumn = lineage.sourceColumns.find(c => c.columnId.eq(columnId));
    if (sourceColumn) {
      return sourceColumn;
    }
  } else if (immediateSources.length === 1 || immediateSources[0].edgeType === 'union') {
    return traceColumnSource(immediateSources[0].sourceColumnId, lineage);
  }

  return undefined;
}
