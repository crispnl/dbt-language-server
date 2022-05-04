import { SourceCompletionProvider } from '../../completion/SourceCompletionProvider';
import { DbtRepository } from '../../DbtRepository';
import { JinjaPartType } from '../../JinjaParser';
import { shouldNotProvideCompletions, shouldProvideCompletions } from '../helper';

describe('SourceCompletionProvider', () => {
  const PROJECT_PACKAGE = 'project_package';
  const INSTALLED_PACKAGE = 'installed_package';

  let dbtRepository: DbtRepository;
  let sourceCompletionProvider: SourceCompletionProvider;

  beforeEach(() => {
    dbtRepository = new DbtRepository();

    dbtRepository.projectName = PROJECT_PACKAGE;
    const sources = [
      {
        uniqueId: 'source_1_1_id',
        rootPath: '/sources/source_1.sql',
        originalFilePath: '/Users/user_name/project/sources/source_1.sql',
        name: 'table_1',
        packageName: PROJECT_PACKAGE,
        sourceName: 'source_1',
        columns: [],
      },
      {
        uniqueId: 'source_1_2_id',
        rootPath: '/sources/source_1.sql',
        originalFilePath: '/Users/user_name/project/sources/source_1.sql',
        name: 'table_2',
        packageName: PROJECT_PACKAGE,
        sourceName: 'source_1',
        columns: [],
      },
      {
        uniqueId: 'installed_package_source_1_1_id',
        rootPath: '/dbt_packages/installed_package/sources/installed_package_source_1.sql',
        originalFilePath: '/Users/user_name/project/dbt_packages/installed_package/sources/installed_package_source_1.sql',
        name: 'installed_package_source_table_1',
        packageName: INSTALLED_PACKAGE,
        sourceName: 'package_source_1',
        columns: [],
      },
      {
        uniqueId: 'installed_package_source_1_2_id',
        rootPath: '/dbt_packages/installed_package/sources/installed_package_source_1.sql',
        originalFilePath: '/Users/user_name/project/dbt_packages/installed_package/sources/installed_package_source_1.sql',
        name: 'installed_package_source_table_2',
        packageName: INSTALLED_PACKAGE,
        sourceName: 'package_source_1',
        columns: [],
      },
    ];
    dbtRepository.updateDbtNodes([], [], sources);

    sourceCompletionProvider = new SourceCompletionProvider(dbtRepository);
  });

  it('Should provide sources by pressing (', () => {
    shouldProvideCompletions(sourceCompletionProvider, JinjaPartType.EXPRESSION_START, `select * from {{ source(`, [
      { label: '(project_package) source_1', insertText: `'source_1'` },
      { label: '(installed_package) package_source_1', insertText: `'package_source_1'` },
    ]);
  });

  it(`Should provide sources by pressing '`, () => {
    shouldProvideCompletions(sourceCompletionProvider, JinjaPartType.EXPRESSION_START, `select * from {{ source('`, [
      { label: '(project_package) source_1', insertText: `source_1` },
      { label: '(installed_package) package_source_1', insertText: `package_source_1` },
    ]);
  });

  it('Should provide source tables', () => {
    shouldProvideCompletions(sourceCompletionProvider, JinjaPartType.EXPRESSION_START, `select * from {{ source('source_1', '`, [
      { label: 'table_1', insertText: `table_1` },
      { label: 'table_2', insertText: `table_2` },
    ]);
  });

  it(`Shouldn't provide completions for unknown source`, () => {
    shouldProvideCompletions(sourceCompletionProvider, JinjaPartType.EXPRESSION_START, `select * from {{ source('unknown_source', '`, []);
  });

  it(`Shouldn't provide completions for empty strings`, () => {
    shouldNotProvideCompletions(sourceCompletionProvider, JinjaPartType.EXPRESSION_START, 'select {{ ');
  });

  it(`Shouldn't provide completions after source`, () => {
    shouldNotProvideCompletions(sourceCompletionProvider, JinjaPartType.EXPRESSION_START, 'select {{ source');
  });
});