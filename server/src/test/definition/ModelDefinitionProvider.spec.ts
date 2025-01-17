import { assertThat } from 'hamjest';
import { Position, Range } from 'vscode-languageserver';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { DbtRepository } from '../../DbtRepository';
import { Dag } from '../../dag/Dag';
import { DagNode } from '../../dag/DagNode';
import { DbtDefinitionProvider } from '../../definition/DbtDefinitionProvider';
import { ModelDefinitionProvider } from '../../definition/ModelDefinitionProvider';
import { ManifestModel } from '../../manifest/ManifestJson';

describe('ModelDefinitionProvider', () => {
  const PATH_TO_PROJECT = '/Users/user_name/project';
  const FILE_NAME = 'model_with_refs.sql';

  const PACKAGE_NAME = 'package';
  const PACKAGE_MODEL = 'package_model';
  const PROJECT_NAME = 'project';
  const PROJECT_MODEL = 'project_model';

  const PACKAGE_MODEL_ORIGINAL_FILE_PATH = `models/${PACKAGE_MODEL}.sql`;
  const PROJECT_MODEL_ORIGINAL_FILE_PATH = `models/${PROJECT_MODEL}.sql`;
  const PACKAGE_MODEL_ROOT_PATH = `dbt_packages/${PACKAGE_NAME}/${PACKAGE_MODEL_ORIGINAL_FILE_PATH}`;

  const DBT_REPOSITORY = new DbtRepository(PATH_TO_PROJECT, Promise.resolve(undefined));
  DBT_REPOSITORY.projectName = PROJECT_NAME;
  const PROVIDER = new ModelDefinitionProvider(DBT_REPOSITORY);

  const MODEL_FILE_CONTENT =
    'select *\n' +
    "from {{ ref('package', 'package_model') }} pm1\n" +
    "  inner join {{ ref('package_model') }} pm2 on pm1.parent_id = pm2.id\n" +
    "  inner join {{ ref('project_model') }} prm on pm1.id = prm.id";

  let modelDocument: TextDocument;

  before(() => {
    DBT_REPOSITORY.dag = new Dag([
      new DagNode(createModel(`model.${PACKAGE_NAME}.${PACKAGE_MODEL}`, PACKAGE_MODEL_ORIGINAL_FILE_PATH, PACKAGE_MODEL, PACKAGE_NAME)),
      new DagNode(createModel(`model.${PROJECT_NAME}.${PROJECT_MODEL}`, PROJECT_MODEL_ORIGINAL_FILE_PATH, PROJECT_MODEL, PROJECT_NAME)),
    ]);

    modelDocument = TextDocument.create(`${PATH_TO_PROJECT}/models/${FILE_NAME}`, 'sql', 0, MODEL_FILE_CONTENT);
  });

  it('provideDefinitions should return definition for model from package (if package is specified)', () => {
    assertThat(
      PROVIDER.provideDefinitions(modelDocument, Position.create(1, 31), {
        value: "{{ ref('package', 'package_model') }}",
        range: Range.create(1, 5, 1, 42),
      }),
      [
        {
          originSelectionRange: Range.create(1, 24, 1, 37),
          targetUri: `file://${PATH_TO_PROJECT}/${PACKAGE_MODEL_ROOT_PATH}`,
          targetRange: DbtDefinitionProvider.MAX_RANGE,
          targetSelectionRange: DbtDefinitionProvider.MAX_RANGE,
        },
      ],
    );
  });

  it('provideDefinitions should return definition for model from package (if package is not specified)', () => {
    assertThat(
      PROVIDER.provideDefinitions(modelDocument, Position.create(2, 28), {
        value: "{{ ref('package_model') }}",
        range: Range.create(2, 13, 2, 39),
      }),
      [
        {
          originSelectionRange: Range.create(2, 21, 2, 34),
          targetUri: `file://${PATH_TO_PROJECT}/${PACKAGE_MODEL_ROOT_PATH}`,
          targetRange: DbtDefinitionProvider.MAX_RANGE,
          targetSelectionRange: DbtDefinitionProvider.MAX_RANGE,
        },
      ],
    );
  });

  it('provideDefinitions should return package definitions', () => {
    assertThat(
      PROVIDER.provideDefinitions(modelDocument, Position.create(1, 16), {
        value: "{{ ref('package', 'package_model') }}",
        range: Range.create(1, 5, 1, 42),
      }),
      [
        {
          originSelectionRange: Range.create(1, 13, 1, 20),
          targetUri: `file://${PATH_TO_PROJECT}/${PACKAGE_MODEL_ROOT_PATH}`,
          targetRange: DbtDefinitionProvider.MAX_RANGE,
          targetSelectionRange: DbtDefinitionProvider.MAX_RANGE,
        },
      ],
    );
  });

  it('provideDefinitions should return project model definition', () => {
    assertThat(
      PROVIDER.provideDefinitions(modelDocument, Position.create(3, 28), {
        value: "{{ ref('project_model') }}",
        range: Range.create(3, 13, 3, 39),
      }),
      [
        {
          originSelectionRange: Range.create(3, 21, 3, 34),
          targetUri: `file://${PATH_TO_PROJECT}/${PROJECT_MODEL_ORIGINAL_FILE_PATH}`,
          targetRange: DbtDefinitionProvider.MAX_RANGE,
          targetSelectionRange: DbtDefinitionProvider.MAX_RANGE,
        },
      ],
    );
  });
});

function createModel(uniqueId: string, originalFilePath: string, name: string, packageName: string): ManifestModel {
  return {
    uniqueId,
    originalFilePath,
    name,
    packageName,
    database: '',
    schema: '',
    rawCode: '',
    compiledCode: '',
    dependsOn: { nodes: [] },
    refs: [],
    description: '',
    columns: {},
    patchPath: '',
  };
}
