import { CompletionItem, CompletionParams, Position, Range } from 'vscode-languageserver';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { DbtRepository } from '../DbtRepository';
import { DestinationContext } from '../DestinationContext';
import { JinjaParser, JinjaPartType } from '../JinjaParser';
import { DbtTextDocument, QueryParseInformation } from '../document/DbtTextDocument';
import { DiffUtils } from '../utils/DiffUtils';
import { comparePositions } from '../utils/Utils';
import { DbtCompletionProvider } from './DbtCompletionProvider';
import { SnippetsCompletionProvider } from './SnippetsCompletionProvider';
import { SqlCompletionProvider } from './SqlCompletionProvider';
import { getWordRangeAtPosition } from '../utils/TextUtils';
import { AnalyzeResponse__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/AnalyzeResponse';

// string[] is a better signature but this way TS doesn't complain
export type CompletionTextInput = [string, string | undefined];

export class CompletionProvider {
  sqlCompletionProvider = new SqlCompletionProvider();
  snippetsCompletionProvider = new SnippetsCompletionProvider();
  dbtCompletionProvider: DbtCompletionProvider;

  constructor(
    private rawDocument: TextDocument,
    private compiledDocument: TextDocument,
    dbtRepository: DbtRepository,
    private jinjaParser: JinjaParser,
    private destinationContext: DestinationContext,
  ) {
    this.dbtCompletionProvider = new DbtCompletionProvider(dbtRepository);
  }

  async provideCompletionItems(
    completionParams: CompletionParams,
    ast?: AnalyzeResponse__Output,
    queryInformation?: QueryParseInformation,
  ): Promise<CompletionItem[]> {
    const dbtCompletionItems = this.provideDbtCompletions(completionParams);
    if (dbtCompletionItems) {
      return dbtCompletionItems;
    }
    const completionText = this.getCompletionText(completionParams);
    const [tableOrColumn, column] = completionText;
    const snippetItems = this.snippetsCompletionProvider.provideSnippets(column ?? tableOrColumn);
    const sqlItems = await this.provideSqlCompletions(completionParams, completionText, ast, queryInformation);
    return [...snippetItems, ...sqlItems];
  }

  private provideDbtCompletions(completionParams: CompletionParams): CompletionItem[] | undefined {
    const jinjaParts = this.jinjaParser.findAllJinjaParts(this.rawDocument);
    const jinjasBeforePosition = jinjaParts.filter(p => comparePositions(p.range.start, completionParams.position) < 0);
    const closestJinjaPart =
      jinjasBeforePosition.length > 0
        ? jinjasBeforePosition.reduce((p1, p2) => (comparePositions(p1.range.start, p2.range.start) > 0 ? p1 : p2))
        : undefined;

    if (closestJinjaPart) {
      const jinjaPartType = this.jinjaParser.getJinjaPartType(closestJinjaPart.value);
      if ([JinjaPartType.EXPRESSION_START, JinjaPartType.BLOCK_START].includes(jinjaPartType)) {
        const jinjaBeforePositionText = this.rawDocument.getText(Range.create(closestJinjaPart.range.start, completionParams.position));
        return this.dbtCompletionProvider.provideCompletions(jinjaPartType, jinjaBeforePositionText);
      }
    }

    return undefined;
  }

  private async provideSqlCompletions(
    completionParams: CompletionParams,
    text: CompletionTextInput,
    ast?: AnalyzeResponse__Output,
    queryInformation?: QueryParseInformation,
  ): Promise<CompletionItem[]> {
    if (this.destinationContext.isEmpty()) {
      return [];
    }

    let aliases: Map<string, string> | undefined;

    let completionInfo = undefined;
    if (ast) {
      const line = DiffUtils.getOldLineNumber(this.compiledDocument.getText(), this.rawDocument.getText(), completionParams.position.line);
      const offset = this.compiledDocument.offsetAt(Position.create(line, completionParams.position.character));
      completionInfo = DbtTextDocument.ZETA_SQL_AST.getCompletionInfo(ast, offset);

      aliases = queryInformation?.selects.find(s => offset >= s.parseLocationRange.start && offset <= s.parseLocationRange.end)?.tableAliases;
    }

    return this.sqlCompletionProvider.onSqlCompletion(
      text,
      completionParams,
      this.destinationContext.destinationDefinition,
      completionInfo,
      this.destinationContext.getDestination(),
      aliases,
    );
  }

  private getCompletionText(completionParams: CompletionParams): CompletionTextInput {
    const previousPosition = Position.create(
      completionParams.position.line,
      completionParams.position.character > 0 ? completionParams.position.character - 1 : 0,
    );

    return this.rawDocument
      .getText(
        getWordRangeAtPosition(previousPosition, /[.|\w|`]+/, this.rawDocument.getText().split('\n')) ??
          Range.create(previousPosition, previousPosition),
      )
      .split('.') as CompletionTextInput;
  }
}
