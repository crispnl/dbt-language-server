import { DefinitionLink, DefinitionParams } from 'vscode-languageserver';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { DbtRepository } from '../DbtRepository';
import { JinjaParser } from '../JinjaParser';
import { AnalyzeResult } from '../ProjectAnalyzer';
import { QueryParseInformation } from '../document/DbtTextDocument';
import { positionInRange } from '../utils/Utils';
import { DbtDefinitionProvider } from './DbtDefinitionProvider';
import { SqlDefinitionProvider } from './SqlDefinitionProvider';
import { LspServer } from '../lsp_server/LspServer';

export class DefinitionProvider {
  dbtDefinitionProvider: DbtDefinitionProvider;
  sqlDefinitionProvider: SqlDefinitionProvider;

  constructor(
    dbtRepository: DbtRepository,
    private jinjaParser: JinjaParser,
    getServer: () => LspServer | undefined,
  ) {
    this.dbtDefinitionProvider = new DbtDefinitionProvider(dbtRepository);
    this.sqlDefinitionProvider = new SqlDefinitionProvider(dbtRepository, getServer);
  }

  async onDefinition(
    definitionParams: DefinitionParams,
    rawDocument: TextDocument,
    compiledDocument: TextDocument,
    queryInformation?: QueryParseInformation,
    analyzeResult?: AnalyzeResult,
  ): Promise<DefinitionLink[] | undefined> {
    const jinjas = this.jinjaParser.findAllEffectiveJinjas(rawDocument);
    for (const jinja of jinjas) {
      if (positionInRange(definitionParams.position, jinja.range)) {
        const jinjaType = this.jinjaParser.getJinjaType(jinja.value);
        return this.dbtDefinitionProvider.provideDefinitions(rawDocument, jinja, definitionParams.position, jinjaType);
      }
    }

    return this.sqlDefinitionProvider.provideDefinitions(definitionParams, queryInformation, analyzeResult, rawDocument, compiledDocument);
  }
}
