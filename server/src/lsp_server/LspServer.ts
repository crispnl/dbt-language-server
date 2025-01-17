import { getStringVersion, SelectedDbtPackage } from 'dbt-language-server-common';
import { Result } from 'neverthrow';
import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { performance } from 'node:perf_hooks';

import { ParseResponse__Output } from '@fivetrandevelopers/zetasql/lib/types/zetasql/local_service/ParseResponse';
import {
  _Connection,
  CodeAction,
  CodeActionKind,
  CodeActionParams,
  Command,
  CompletionItem,
  CompletionParams,
  DefinitionLink,
  DefinitionParams,
  DeleteFilesParams,
  DidChangeConfigurationNotification,
  DidChangeTextDocumentParams,
  DidChangeWatchedFilesNotification,
  DidChangeWatchedFilesParams,
  DidCreateFilesNotification,
  DidDeleteFilesNotification,
  DidOpenTextDocumentParams,
  DidRenameFilesNotification,
  DidSaveTextDocumentParams,
  ExecuteCommandParams,
  Hover,
  HoverParams,
  InitializeError,
  InitializeParams,
  InitializeResult,
  Range,
  RenameFilesParams,
  ResponseError,
  SignatureHelp,
  SignatureHelpParams,
  TextDocumentSyncKind,
  TextEdit,
  WillSaveTextDocumentParams,
} from 'vscode-languageserver';
import { FileOperationFilter } from 'vscode-languageserver-protocol/lib/common/protocol.fileOperations';
import { URI } from 'vscode-uri';
import { DbtCli } from '../dbt_execution/DbtCli';
import { DbtProfileCreator, DbtProfileInfo } from '../DbtProfileCreator';
import { DbtProject } from '../DbtProject';
import { DbtRepository } from '../DbtRepository';
import { DefinitionProvider } from '../definition/DefinitionProvider';
import { DestinationContext } from '../DestinationContext';
import { DiagnosticGenerator } from '../DiagnosticGenerator';
import { DbtDocumentKind } from '../document/DbtDocumentKind';
import { DbtDocumentKindResolver } from '../document/DbtDocumentKindResolver';
import { DbtTextDocument } from '../document/DbtTextDocument';
import { FeatureFinder } from '../feature_finder/FeatureFinder';
import { FileChangeListener } from '../FileChangeListener';
import { HoverProvider } from '../HoverProvider';
import { JinjaParser } from '../JinjaParser';
import { ModelCompiler } from '../ModelCompiler';
import { ModelProgressReporter } from '../ModelProgressReporter';
import { NotificationSender } from '../NotificationSender';
import { ProcessExecutor } from '../ProcessExecutor';
import { ProjectChangeListener } from '../ProjectChangeListener';
import { SignatureHelpProvider } from '../SignatureHelpProvider';
import { DbtProjectStatusSender } from '../status_bar/DbtProjectStatusSender';
import { LspServerBase } from './LspServerBase';

export class LspServer extends LspServerBase<FeatureFinder> {
  sqlToRefCommandName = randomUUID();
  filesFilter: FileOperationFilter[];
  hasConfigurationCapability = false;
  hasDidChangeWatchedFilesCapability = false;
  initStart = performance.now();
  zetaParserResultcache = new TimedModelAnalyzeResultCache();

  constructor(
    connection: _Connection,
    notificationSender: NotificationSender,
    featureFinder: FeatureFinder,
    private dbtCli: DbtCli,
    private modelProgressReporter: ModelProgressReporter,
    private dbtProject: DbtProject,
    private dbtRepository: DbtRepository,
    private fileChangeListener: FileChangeListener,
    private dbtProfileCreator: DbtProfileCreator,
    private statusSender: DbtProjectStatusSender,
    private dbtDocumentKindResolver: DbtDocumentKindResolver,
    private diagnosticGenerator: DiagnosticGenerator,
    private jinjaParser: JinjaParser,
    private definitionProvider: DefinitionProvider,
    private signatureHelpProvider: SignatureHelpProvider,
    private hoverProvider: HoverProvider,
    public destinationContext: DestinationContext,
    private openedDocumentsLowerCase: Map<string, DbtTextDocument>,
    private projectChangeListener: ProjectChangeListener,
    private enableSnowflakeSyntaxCheck: boolean,
  ) {
    super(connection, notificationSender, featureFinder);
    this.filesFilter = [{ scheme: 'file', pattern: { glob: `${dbtRepository.projectPath}/**/*`, matches: 'file' } }];
  }

  onInitialize(params: InitializeParams): InitializeResult<unknown> | ResponseError<InitializeError> {
    console.log(`Starting server for folder ${this.dbtRepository.projectPath}.`);

    this.initializeEvents();

    process.on('uncaughtException', this.onUncaughtException.bind(this));
    process.on('SIGTERM', () => this.onShutdown('SIGTERM'));
    process.on('SIGINT', () => this.onShutdown('SIGINT'));

    this.fileChangeListener.onInit();

    this.initializeNotifications();

    const { capabilities } = params;

    this.hasConfigurationCapability = Boolean(capabilities.workspace?.configuration);
    this.hasDidChangeWatchedFilesCapability = Boolean(capabilities.workspace?.didChangeWatchedFiles?.dynamicRegistration);
    return {
      capabilities: {
        textDocumentSync: {
          openClose: true,
          change: TextDocumentSyncKind.Incremental,
          willSave: true,
          save: true,
        },
        hoverProvider: true,
        completionProvider: {
          triggerCharacters: ['.', '(', '"', "'"],
        },
        signatureHelpProvider: {
          triggerCharacters: ['(', ',', ')'],
        },
        definitionProvider: true,
        codeActionProvider: true,
        executeCommandProvider: {
          commands: [this.sqlToRefCommandName],
        },
        workspace: {
          fileOperations: {
            didRename: {
              filters: this.filesFilter,
            },
          },
        },
      },
    };
  }

  initializeEvents(): void {
    this.connection.onInitialized(this.onInitialized.bind(this));
    this.connection.onHover(this.onHover.bind(this));
    this.connection.onCompletion(this.onCompletion.bind(this));
    this.connection.onSignatureHelp(this.onSignatureHelp.bind(this));
    this.connection.onDefinition(this.onDefinition.bind(this));

    this.connection.onWillSaveTextDocument(this.onWillSaveTextDocument.bind(this));
    this.connection.onDidSaveTextDocument(this.onDidSaveTextDocument.bind(this));
    this.connection.onDidOpenTextDocument(this.onDidOpenTextDocument.bind(this));
    this.connection.onDidChangeTextDocument(this.onDidChangeTextDocument.bind(this));

    this.connection.onCodeAction(this.onCodeAction.bind(this));
    this.connection.onExecuteCommand(this.onExecuteCommand.bind(this));
    this.connection.onDidChangeWatchedFiles(this.onDidChangeWatchedFiles.bind(this));

    this.connection.onShutdown(() => this.onShutdown('Client'));

    this.connection.workspace.onDidRenameFiles(this.onDidRenameFiles.bind(this));
    this.connection.workspace.onDidDeleteFiles(this.onDidDeleteFiles.bind(this));
  }

  override initializeNotifications(): void {
    super.initializeNotifications();

    this.connection.onNotification('custom/dbtCompile', (uri: string) => this.onDbtCompile(uri));
    this.connection.onNotification('WizardForDbtCore(TM)/resendDiagnostics', (uri: string) => this.onResendDiagnostics(uri));
    this.connection.onNotification('custom/analyzeEntireProject', () => this.onAnalyzeEntireProject());
    this.connection.onNotification('custom/generateDocumentation', (uri: string) => this.onGenerateDocumentation(uri));

    this.connection.onRequest('WizardForDbtCore(TM)/getProjectName', () => this.getProjectName());
    this.connection.onRequest('WizardForDbtCore(TM)/getListOfPackages', () => this.featureFinder.packageInfosPromise.get());
    this.connection.onRequest('WizardForDbtCore(TM)/getCompiledSql', async (params: { uri: string; target: string }) => {
      const textDocument = this.openedDocumentsLowerCase.get(params.uri.toLowerCase());
      if (!textDocument) {
        return null;
      }
      const result = await textDocument.modelCompiler.startNewJob(
        textDocument.getModelPathOrFullyQualifiedName(),
        textDocument.modelIsNotBlank(),
        params.target,
      );
      if (!result || !result.isOk()) {
        // Compilation failed
        return null;
      }
      return textDocument.compiledDocument.getText();
    });
    this.connection.onRequest('WizardForDbtCore(TM)/getPackageVersions', (dbtPackage: string) =>
      this.featureFinder.getDbtPackageVersions(dbtPackage),
    );
    this.connection.onRequest('WizardForDbtCore(TM)/addNewDbtPackage', (dbtPackage: SelectedDbtPackage) => this.onAddNewDbtPackage(dbtPackage));
  }

  getProjectName(): string {
    return this.dbtProject.findProjectName();
  }

  async onInitialized(): Promise<void> {
    this.registerClientNotification();

    const profileResult = this.dbtProfileCreator.createDbtProfile();
    const contextInfo = profileResult.match<DbtProfileInfo>(
      s => s,
      e => e,
    );
    const dbtProfileType = profileResult.isOk() ? profileResult.value.type : profileResult.error.type;

    if (profileResult.isErr()) {
      this.showProfileCreationWarning(profileResult.error.message);
    }

    const ubuntuInWslWorks = await this.featureFinder.ubuntuInWslWorks;
    if (!ubuntuInWslWorks) {
      await this.showWslWarning();
    }

    const destinationInitResult = profileResult.isOk()
      ? this.destinationContext
          .initialize(profileResult.value, this.dbtRepository, ubuntuInWslWorks, this.dbtProject.findProjectName())
          .then((initResult: Result<void, string>) => (initResult.isErr() ? this.showCreateContextWarning(initResult.error) : undefined))
      : Promise.resolve();

    const prepareDbt = this.dbtCli.prepare(dbtProfileType).then(_ => this.statusSender.sendStatus());

    await Promise.allSettled([prepareDbt, destinationInitResult]);

    this.projectChangeListener
      .compileAndAnalyzeProject()
      .catch(e => console.log(`Error while compiling/analyzing project: ${e instanceof Error ? e.message : String(e)}`));
    const initTime = performance.now() - this.initStart;
    this.logStartupInfo(contextInfo, initTime, ubuntuInWslWorks);

    this.featureFinder
      .runPostInitTasks()
      .catch(e => console.log(`Error while running post init tasks: ${e instanceof Error ? e.message : String(e)}`));
  }

  registerClientNotification(): void {
    this.registerManifestWatcher();
    this.registerModelsWatcher();

    if (this.hasConfigurationCapability) {
      this.connection.client
        .register(DidChangeConfigurationNotification.type, undefined)
        .catch(e => console.log(`Error while registering DidChangeConfiguration notification: ${e instanceof Error ? e.message : String(e)}`));
    }

    const filters = this.filesFilter;
    this.connection.client
      .register(DidCreateFilesNotification.type, { filters })
      .catch(e => console.log(`Error while registering DidCreateFiles notification: ${e instanceof Error ? e.message : String(e)}`));

    this.connection.client
      .register(DidRenameFilesNotification.type, { filters })
      .catch(e => console.log(`Error while registering DidRenameFiles notification: ${e instanceof Error ? e.message : String(e)}`));

    this.connection.client
      .register(DidDeleteFilesNotification.type, { filters })
      .catch(e => console.log(`Error while registering DidDeleteFiles notification: ${e instanceof Error ? e.message : String(e)}`));
  }

  registerManifestWatcher(): void {
    if (this.hasDidChangeWatchedFilesCapability) {
      const targetPath = this.dbtProject.findTargetPath();
      if (!fs.existsSync(targetPath)) {
        fs.mkdirSync(targetPath, { recursive: true });
      }
      this.registerDidChangeWatchedFilesNotification(URI.file(targetPath).toString(), 'manifest.json');
    }
  }

  registerModelsWatcher(): void {
    if (this.hasDidChangeWatchedFilesCapability) {
      const modelPaths = this.dbtProject.findModelPaths();

      for (const modelPath of modelPaths) {
        const baseUri = URI.file(path.resolve(modelPath)).toString();
        this.registerDidChangeWatchedFilesNotification(baseUri, '**/*.sql');
      }
    }
  }

  registerDidChangeWatchedFilesNotification(baseUri: string, pattern: string): void {
    this.connection.client
      .register(DidChangeWatchedFilesNotification.type, {
        watchers: [{ globPattern: { baseUri, pattern } }],
      })
      .catch(e =>
        console.log(`Error while registering for DidChangeWatchedFilesNotification notification: ${e instanceof Error ? e.message : String(e)}`),
      );
  }

  showWarning(message: string): void {
    console.log(message);
    this.connection.window.showWarningMessage(message);
  }

  showProfileCreationWarning(error: string): void {
    this.showWarning(`Dbt profile was not properly configured. ${error}`);
  }

  async showWslWarning(): Promise<void> {
    const command = `wsl --install -d ${FeatureFinder.getWslUbuntuName()}`;
    const result = await this.connection.window.showWarningMessage(
      `Extension requires WSL and ${FeatureFinder.getWslUbuntuName()} to be installed. Please run the following command as Administrator and then restart your computer ([see docs](https://learn.microsoft.com/en-us/windows/wsl/install)): ${command}`,
      { title: 'Run command', id: 'run' },
    );
    if (result?.id === 'run') {
      new ProcessExecutor()
        .execProcess(`powershell -Command "Start-Process cmd -Verb RunAs -ArgumentList '/k ${command}'"`)
        .catch(e => console.log(`Error while installing WSL and Ubuntu: ${e instanceof Error ? e.message : String(e)}`));
    }
  }

  showCreateContextWarning(error: string): void {
    this.showWarning(`Unable to initialize destination. ${error}`);
  }

  logStartupInfo(contextInfo: DbtProfileInfo, initTime: number, ubuntuInWslWorks: boolean): void {
    this.notificationSender.sendTelemetry('log', {
      dbtVersion: getStringVersion(this.featureFinder.versionInfo?.installedVersion),
      pythonVersion: this.featureFinder.pythonInfo.version?.join('.') ?? 'undefined',
      initTime: initTime.toString(),
      type: contextInfo.type ?? 'unknown type',
      method: contextInfo.method ?? 'unknown method',
      winWsl: String(process.platform === 'win32' && ubuntuInWslWorks),
      enableSnowflakeSyntaxCheck: this.enableSnowflakeSyntaxCheck.toString(),
      ftUser: (contextInfo.project === 'digital-arbor-400').toString(),
      ds: contextInfo.dataset ?? 'unknown',
    });
  }

  onDbtCompile(uri: string): void {
    this.getOpenedDocumentByUri(uri)?.forceRecompile();
  }

  onGenerateDocumentation(uri: string): void {
    const doc = this.getOpenedDocumentByUri(uri);
    if (doc) {
      doc.generateDocumentation();
    } else {
      this.notificationSender.sendWarning('Open a DBT model file first.');
    }
  }

  async onResendDiagnostics(uri: string): Promise<void> {
    await this.getOpenedDocumentByUri(uri)?.resendDiagnostics();
  }

  onAnalyzeEntireProject(): void {
    this.projectChangeListener.forceCompileAndAnalyzeProject();
  }

  onWillSaveTextDocument(params: WillSaveTextDocumentParams): void {
    this.getOpenedDocumentByUri(params.textDocument.uri)?.willSaveTextDocument(params.reason);
  }

  async onDidSaveTextDocument(params: DidSaveTextDocumentParams): Promise<void> {
    const document = this.getOpenedDocumentByUri(params.textDocument.uri);
    await document?.didSaveTextDocument();
  }

  async onDidOpenTextDocument(params: DidOpenTextDocumentParams, isVirtual: boolean = false): Promise<DbtTextDocument | undefined> {
    const { uri } = params.textDocument;
    console.log(params);
    let document = this.getOpenedDocumentByUri(uri);

    if (!document) {
      const dbtDocumentKind = this.dbtDocumentKindResolver.getDbtDocumentKind(uri);
      if (![DbtDocumentKind.MACRO, DbtDocumentKind.MODEL].includes(dbtDocumentKind)) {
        console.log('Not supported dbt document kind');
        return;
      }

      document = new DbtTextDocument(
        params.textDocument,
        dbtDocumentKind,
        this.notificationSender,
        this.modelProgressReporter,
        new ModelCompiler(this.dbtCli, this.dbtRepository),
        this.jinjaParser,
        this.dbtRepository,
        this.dbtCli,
        this.destinationContext,
        this.diagnosticGenerator,
        this.signatureHelpProvider,
        this.hoverProvider,
        this.definitionProvider,
        this.projectChangeListener,
      );
      if (!isVirtual) {
        this.openedDocumentsLowerCase.set(uri.toLowerCase(), document);
      }

      await document.didOpenTextDocument();
      // eslint-disable-next-line consistent-return
      return document;
    }
    // eslint-disable-next-line consistent-return
    return document;
  }

  onDidChangeTextDocument(params: DidChangeTextDocumentParams): void {
    this.getOpenedDocumentByUri(params.textDocument.uri)?.didChangeTextDocument(params);
  }

  onHover(hoverParams: HoverParams): Hover | null | undefined {
    const document = this.getOpenedDocumentByUri(hoverParams.textDocument.uri);
    return document?.onHover(hoverParams);
  }

  async onCompletion(completionParams: CompletionParams): Promise<CompletionItem[] | undefined> {
    const document = this.getOpenedDocumentByUri(completionParams.textDocument.uri);
    return document?.onCompletion(completionParams);
  }

  onSignatureHelp(params: SignatureHelpParams): SignatureHelp | undefined {
    const document = this.getOpenedDocumentByUri(params.textDocument.uri);
    return document?.onSignatureHelp(params);
  }

  async onDefinition(definitionParams: DefinitionParams): Promise<DefinitionLink[] | undefined> {
    const document = this.getOpenedDocumentByUri(definitionParams.textDocument.uri);
    return document?.onDefinition(definitionParams);
  }

  getOpenedDocumentByUri(uri: string): DbtTextDocument | undefined {
    return this.openedDocumentsLowerCase.get(uri.toLowerCase());
  }

  onDidChangeWatchedFiles(params: DidChangeWatchedFilesParams): void {
    this.fileChangeListener.onDidChangeWatchedFiles(params);
  }

  onCodeAction(params: CodeActionParams): CodeAction[] {
    const title = 'Change to ref';
    return params.context.diagnostics
      .filter(d => d.source === 'Wizard for dbt Core (TM)' && (d.data as { replaceText: string } | undefined)?.replaceText)
      .map<CodeAction>(d => ({
        title,
        diagnostics: [d],
        edit: {
          changes: {
            [params.textDocument.uri]: [TextEdit.replace(d.range, (d.data as { replaceText: string }).replaceText)],
          },
        },
        command: Command.create(title, this.sqlToRefCommandName, params.textDocument.uri, d.range),
        kind: CodeActionKind.QuickFix,
      }));
  }

  onExecuteCommand(params: ExecuteCommandParams): void {
    if (params.command === this.sqlToRefCommandName && params.arguments) {
      const textDocument = this.getOpenedDocumentByUri(params.arguments[0] as string);
      const range = params.arguments[1] as Range | undefined;
      if (textDocument && range) {
        textDocument.fixInformationDiagnostic(range);
      }
    }
  }

  onAddNewDbtPackage(dbtPackage: SelectedDbtPackage): void {
    this.dbtProject.addNewDbtPackage(dbtPackage.packageName, dbtPackage.version);
    const sendLog = (data: string): void => this.notificationSender.sendDbtDepsLog(data);
    this.dbtCli.deps(sendLog, sendLog).catch(e => console.log(`Error while running dbt deps: ${e instanceof Error ? e.message : String(e)}`));
  }

  onDidRenameFiles(params: RenameFilesParams): void {
    this.disposeOutdatedDocuments(params.files.filter(f => f.oldUri.toLowerCase() !== f.newUri.toLowerCase()).map(f => f.oldUri));

    for (const document of this.openedDocumentsLowerCase.values()) {
      if (this.projectChangeListener.currentDbtError) {
        document.forceRecompile();
        return;
      }
    }
  }

  onDidDeleteFiles(params: DeleteFilesParams): void {
    this.disposeOutdatedDocuments(params.files.map(f => f.uri));
  }

  disposeOutdatedDocuments(uris: string[]): void {
    uris.forEach(uri => {
      this.getOpenedDocumentByUri(uri)?.dispose();
      this.openedDocumentsLowerCase.delete(uri.toLowerCase());
    });
  }

  onShutdown(reason: string): void {
    console.log(`Shutdown: ${reason}`);
    this.dispose();
  }

  dispose(): void {
    console.log('Dispose start...');
    this.destinationContext.dispose();
    this.zetaParserResultcache[Symbol.dispose]();
    console.log('Dispose end.');
  }
}

class TimedModelAnalyzeResultCache implements Disposable {
  private timer: NodeJS.Timeout;
  private cache: Map<
    string,
    {
      result: ParseResponse__Output;
      expiresAt: number;
    }
  > = new Map();

  constructor() {
    this.timer = setInterval(this.removeExpired.bind(this), 60 * 1000);
  }

  private removeExpired(): void {
    const now = Date.now();
    for (const [key, { expiresAt }] of this.cache.entries()) {
      if (expiresAt < now) {
        this.cache.delete(key);
      }
    }
  }

  get(text: string): ParseResponse__Output | undefined {
    return this.cache.get(text)?.result;
  }

  set(text: string, result: ParseResponse__Output): void {
    this.cache.set(text, {
      result,
      expiresAt: Date.now() + 1000 * 60 * 15,
    });
  }

  [Symbol.dispose](): void {
    clearInterval(this.timer);
  }
}
