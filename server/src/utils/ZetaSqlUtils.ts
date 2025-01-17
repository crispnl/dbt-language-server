import { TypeKind } from '@fivetrandevelopers/zetasql';
import { ParseLocationRangeProto__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/ParseLocationRangeProto';
import { SimpleColumnProto } from '@fivetrandevelopers/zetasql/lib/types/zetasql/SimpleColumnProto';
import { TypeProto } from '@fivetrandevelopers/zetasql/lib/types/zetasql/TypeProto';

interface Node {
  node: string;
  [key: string]: unknown;
}

export function positionInRange(position: number, range: ParseLocationRangeProto__Output): boolean {
  return range.start <= position && position <= range.end;
}

export function rangeContainsRange(outerRange: ParseLocationRangeProto__Output, innerRange: ParseLocationRangeProto__Output): boolean {
  return outerRange.start <= innerRange.start && innerRange.end <= outerRange.end;
}

export function rangesEqual(range1: { start: number; end: number }, range2: { start: number; end: number }): boolean {
  return range1.start === range2.start && range1.end === range2.end;
}

export function traverse(
  unknownNode: unknown,
  actions: Map<
    string,
    {
      actionBefore: (node: unknown) => void;
      actionAfter?: (node: unknown) => void;
    }
  >,
): void {
  const node = unknownNode as Node;
  let nodeType = node.node;
  let innerNode = node[node.node];

  // Some nodes are expanded by the parser, removing the node type
  // We try to infer these nodetypes by looking for certain fields
  if ('column' in node && 'expr' in node) {
    nodeType = 'resolvedComputedColumnNode';
    innerNode = node;
  } else if ('withQueryName' in node && 'withSubquery' in node) {
    nodeType = 'resolvedWithEntryNode';
    innerNode = node;
  }

  const nodeActions = actions.get(nodeType);

  if (nodeActions) {
    nodeActions.actionBefore(innerNode);
  }

  for (const key of Object.keys(node)) {
    const child = node[key];
    if (typeof child === 'object' && child !== null) {
      traverse(child, actions);
    }
  }

  if (nodeActions?.actionAfter) {
    nodeActions.actionAfter(innerNode);
  }
}

export function createSimpleColumn(name: string, type: TypeProto | null): SimpleColumnProto {
  return { name, type };
}

// TODO: For Snowflake it should be [TypeKind.TYPE_BYTES.valueOf(), 'BINARY']
export const TYPE_KIND_NAMES = new Map<number, string>([
  [TypeKind.TYPE_UNKNOWN.valueOf(), 'UNKNOWN'],
  [TypeKind.TYPE_INT32.valueOf(), 'INT32'],
  [TypeKind.TYPE_INT64.valueOf(), 'INT64'],
  [TypeKind.TYPE_UINT32.valueOf(), 'UINT32'],
  [TypeKind.TYPE_UINT64.valueOf(), 'UINT64'],
  [TypeKind.TYPE_BOOL.valueOf(), 'BOOL'],
  [TypeKind.TYPE_FLOAT.valueOf(), 'FLOAT'],
  [TypeKind.TYPE_DOUBLE.valueOf(), 'DOUBLE'],
  [TypeKind.TYPE_STRING.valueOf(), 'STRING'],
  [TypeKind.TYPE_BYTES.valueOf(), 'BYTES'],
  [TypeKind.TYPE_DATE.valueOf(), 'DATE'],
  [11, 'TIMESTAMP_SECONDS'],
  [12, 'TIMESTAMP_MILLIS'],
  [13, 'TIMESTAMP_MICROS'],
  [14, ''],
  [TypeKind.TYPE_ENUM.valueOf(), 'ENUM'],
  [TypeKind.TYPE_ARRAY.valueOf(), 'ARRAY'],
  [TypeKind.TYPE_STRUCT.valueOf(), 'STRUCT'],
  [TypeKind.TYPE_PROTO.valueOf(), 'PROTO'],
  [TypeKind.TYPE_TIMESTAMP.valueOf(), 'TIMESTAMP'],
  [TypeKind.TYPE_TIME.valueOf(), 'TIME'],
  [TypeKind.TYPE_DATETIME.valueOf(), 'DATETIME'],
  [TypeKind.TYPE_GEOGRAPHY.valueOf(), 'GEOGRAPHY'],
  [TypeKind.TYPE_NUMERIC.valueOf(), 'NUMERIC'],
  [TypeKind.TYPE_BIGNUMERIC.valueOf(), 'BIGNUMERIC'],
  [TypeKind.TYPE_EXTENDED.valueOf(), 'EXTENDED'],
  [TypeKind.TYPE_JSON.valueOf(), 'JSON'],
  [TypeKind.TYPE_INTERVAL.valueOf(), 'INTERVAL'],
  // Snowflake:
  [1000, 'VARIANT'],
  [1001, 'OBJECT'],
]);
