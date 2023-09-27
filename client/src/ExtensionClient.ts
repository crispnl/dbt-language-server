import * as fs from 'node:fs';
import * as os from 'node:os';
import * as yaml from 'yaml';
import { commands, ExtensionContext, languages, TextDocument, TextEditor, Uri, ViewColumn, window, workspace, WorkspaceFolder } from 'vscode';
import { ActiveTextEditorHandler } from './ActiveTextEditorHandler';
import { CommandManager } from './commands/CommandManager';
import { Compile } from './commands/Compile';
import { InstallDbtAdapters } from './commands/InstallDbtAdapters';
import { InstallDbtCore } from './commands/InstallDbtCore';
import { InstallDbtPackages } from './commands/InstallDbtPackages';
import { OpenOrCreatePackagesYml } from './commands/OpenOrCreatePackagesYml';
import { Restart } from './commands/Restart';
import { DbtLanguageClientManager } from './DbtLanguageClientManager';
import { log } from './Logger';
import { OutputChannelProvider } from './OutputChannelProvider';
import SqlPreviewContentProvider from './SqlPreviewContentProvider';
import { StatusHandler } from './status/StatusHandler';
import { TelemetryClient } from './TelemetryClient';
import { DBT_PROJECT_YML, isDocumentSupported, SQL_LANG_ID } from './Utils';

import { EventEmitter } from 'node:events';
import * as path from 'node:path';
import { AnalyzeEntireProject } from './commands/AnalyzeEntireProject';
import { CreateDbtProject } from './commands/CreateDbtProject/CreateDbtProject';
import { UseConfigForRefsPreview } from './commands/UseConfigForRefsPreview';
import { NotUseConfigForRefsPreview } from './commands/NotUseConfigForRefsPreview';
import { DryRunDev } from './commands/DryRun/DryRunDev';
import { DryRunStaging } from './commands/DryRun/DryRunStaging';
import { DryRunProd } from './commands/DryRun/DryRunProd';
import { tryReadFile } from './commands/DryRun/DryRun';

export interface PackageJson {
  name: string;
  version: string;
  aiKey: string;
}

export interface DBTProjectConfiguration {
  dbt_crisp_dwh?: {
    target: string;
    outputs: {
      dev?: ProjectTarget;
      staging?: ProjectTarget;
      prod?: ProjectTarget;
    };
  };
}

interface ProjectTarget {
  type: 'bigquery';
  method: string;
  project: string;
  dataset: string;
  threads: number;
  timeout_seconds: number;
  location: string;
  priority: string;
  retries: number;
}

export class ExtensionClient {
  previewContentProvider = new SqlPreviewContentProvider();
  statusHandler = new StatusHandler();
  dbtLanguageClientManager: DbtLanguageClientManager;
  commandManager = new CommandManager();
  packageJson?: PackageJson;
  activeTextEditorHandler: ActiveTextEditorHandler;

  constructor(
    private context: ExtensionContext,
    private outputChannelProvider: OutputChannelProvider,
    manifestParsedEventEmitter: EventEmitter,
  ) {
    this.dbtLanguageClientManager = new DbtLanguageClientManager(
      this.previewContentProvider,
      this.outputChannelProvider,
      this.context.asAbsolutePath(path.join('server', 'out', 'server.js')),
      manifestParsedEventEmitter,
      this.statusHandler,
    );
    this.activeTextEditorHandler = new ActiveTextEditorHandler(this.previewContentProvider, this.dbtLanguageClientManager, this.statusHandler);

    this.context.subscriptions.push(this.dbtLanguageClientManager, this.commandManager, this.activeTextEditorHandler);
  }

  public async onActivate(): Promise<void> {
    this.context.subscriptions.push(
      workspace.onDidOpenTextDocument(this.onDidOpenTextDocument.bind(this)),
      workspace.onDidChangeWorkspaceFolders(event => {
        for (const folder of event.removed) {
          log(`Workspace folder removed, stopping client ${folder.uri.fsPath}`);
          this.dbtLanguageClientManager.stopClient(folder.uri.fsPath);
        }
      }),

      workspace.onDidChangeTextDocument(e => {
        if (SqlPreviewContentProvider.isPreviewDocument(e.document.uri)) {
          this.dbtLanguageClientManager.applyPreviewDiagnostics();
        }
      }),

      workspace.onDidChangeConfiguration(e => this.dbtLanguageClientManager.onDidChangeConfiguration(e)),
    );
    workspace.textDocuments.forEach(t =>
      this.onDidOpenTextDocument(t).catch(e => log(`Error while opening text document ${e instanceof Error ? e.message : String(e)}`)),
    );
    this.registerSqlPreviewContentProvider(this.context);

    const dbtProjectContents = tryReadFile(path.join(os.homedir(), '.dbt', 'profiles.yml'));
    let dbtProjectConfiguration: DBTProjectConfiguration = {};
    if (dbtProjectContents) {
      dbtProjectConfiguration = ((): DBTProjectConfiguration => {
        try {
          return yaml.parse(dbtProjectContents) as DBTProjectConfiguration;
        } catch {
          return dbtProjectConfiguration;
        }
      })();
    }

    this.registerCommands(dbtProjectConfiguration);

    this.parseVersion();

    await this.activateDefaultProject(dbtProjectConfiguration);

    TelemetryClient.activate(this.context, this.packageJson);
    TelemetryClient.sendEvent('activate');
  }

  async activateDefaultProject(dbtProjectConfiguration: DBTProjectConfiguration): Promise<void> {
    let currentWorkspace: WorkspaceFolder | undefined = undefined;
    if (workspace.workspaceFolders && workspace.workspaceFolders.length > 0) {
      currentWorkspace = workspace.workspaceFolders.find(f => f.name === workspace.name);
    }

    if (!currentWorkspace && workspace.workspaceFolders?.length === 1) {
      currentWorkspace = workspace.workspaceFolders[0];
    }

    if (currentWorkspace) {
      const dbtProjectYmlPath = path.join(currentWorkspace.uri.fsPath, DBT_PROJECT_YML);
      const possibleProjectYmlUri = currentWorkspace.uri.with({ path: dbtProjectYmlPath });
      await this.dbtLanguageClientManager.ensureClient(possibleProjectYmlUri);
      if (this.context.globalState.get<boolean>(dbtProjectYmlPath)) {
        await this.context.globalState.update(dbtProjectYmlPath, false);
        await commands.executeCommand('vscode.open', Uri.file(dbtProjectYmlPath));
        await commands.executeCommand('workbench.action.keepEditor');
      }
    }

    await commands.executeCommand('setContext', 'WizardForDbtCore:hasProdEnvironment', Boolean(dbtProjectConfiguration.dbt_crisp_dwh?.outputs.prod));
    await commands.executeCommand(
      'setContext',
      'WizardForDbtCore:hasStagingEnvironment',
      Boolean(dbtProjectConfiguration.dbt_crisp_dwh?.outputs.staging),
    );
    await commands.executeCommand('setContext', 'WizardForDbtCore:hasDevEnvironment', Boolean(dbtProjectConfiguration.dbt_crisp_dwh?.outputs.dev));

    await this.dbtLanguageClientManager.ensureNoProjectClient();
  }

  parseVersion(): void {
    const extensionPath = path.join(this.context.extensionPath, 'package.json');
    this.packageJson = JSON.parse(fs.readFileSync(extensionPath, 'utf8')) as PackageJson;
    log(`Wizard for dbt Core (TM) version: ${this.packageJson.version}`);
  }

  registerCommands(dbtProjectConfiguration: DBTProjectConfiguration): void {
    this.commandManager.register(new Compile(this.dbtLanguageClientManager));
    this.commandManager.register(new AnalyzeEntireProject(this.dbtLanguageClientManager));
    this.commandManager.register(new CreateDbtProject(this.context.globalState));
    this.commandManager.register(new InstallDbtCore(this.dbtLanguageClientManager, this.outputChannelProvider));
    this.commandManager.register(new InstallDbtAdapters(this.dbtLanguageClientManager, this.outputChannelProvider));
    this.commandManager.register(new OpenOrCreatePackagesYml());
    this.commandManager.register(new Restart(this.dbtLanguageClientManager));
    this.commandManager.register(new InstallDbtPackages(this.dbtLanguageClientManager, this.outputChannelProvider));
    this.commandManager.register(new UseConfigForRefsPreview(this.previewContentProvider));
    this.commandManager.register(new NotUseConfigForRefsPreview(this.previewContentProvider));
    this.commandManager.register(new DryRunDev(dbtProjectConfiguration, this.dbtLanguageClientManager));
    this.commandManager.register(new DryRunStaging(dbtProjectConfiguration, this.dbtLanguageClientManager));
    this.commandManager.register(new DryRunProd(dbtProjectConfiguration, this.dbtLanguageClientManager));
  }

  registerSqlPreviewContentProvider(context: ExtensionContext): void {
    const providerRegistrations = workspace.registerTextDocumentContentProvider(SqlPreviewContentProvider.SCHEME, this.previewContentProvider);
    const commandRegistration = commands.registerTextEditorCommand('WizardForDbtCore(TM).showQueryPreview', async (editor: TextEditor) => {
      const projectUri = await this.dbtLanguageClientManager.getOuterMostDbtProjectUri(editor.document.uri);
      if (!projectUri) {
        return;
      }
      this.previewContentProvider.changeActiveDocument(editor.document.uri);

      if (window.visibleTextEditors.some(e => SqlPreviewContentProvider.isPreviewDocument(e.document.uri))) {
        return;
      }
      log('Opening Query Preview');

      const doc = await workspace.openTextDocument(SqlPreviewContentProvider.URI);
      await window.showTextDocument(doc, ViewColumn.Beside, false);
      await commands.executeCommand('workbench.action.lockEditorGroup');
      await commands.executeCommand('workbench.action.focusPreviousGroup');
      await languages.setTextDocumentLanguage(doc, SQL_LANG_ID);
      this.dbtLanguageClientManager.applyPreviewDiagnostics();
    });

    context.subscriptions.push(this.previewContentProvider, commandRegistration, providerRegistrations);
  }

  async onDidOpenTextDocument(document: TextDocument): Promise<void> {
    if (isDocumentSupported(document)) {
      await this.dbtLanguageClientManager.ensureClient(document.uri);
    }
  }

  onDeactivate(): Thenable<void> {
    return this.dbtLanguageClientManager.onDeactivate();
  }
}
