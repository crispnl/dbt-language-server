import * as vscode from 'vscode';
import * as path from 'path';
import * as assert from 'assert';
import SqlPreviewContentProvider from '../SqlPreviewContentProvider';

export let doc: vscode.TextDocument;
export let editor: vscode.TextEditor;

export async function activateAndWait(docUri: vscode.Uri) {
  // The extensionId is `publisher.name` from package.json
  const ext = vscode.extensions.getExtension('Fivetran.dbt-language-server')!;
  await ext.activate();
  try {
    doc = await vscode.workspace.openTextDocument(docUri);
    editor = await vscode.window.showTextDocument(doc);
    await showPreview();
    await waitDbtCommand();
  } catch (e) {
    console.error(e);
  }
}

export async function waitDbtCommand() {
  await sleep(500);
  const promise = <Promise<unknown>>await vscode.commands.executeCommand('dbt.getProgressPromise');
  await promise;
}

export async function showPreview() {
  await vscode.commands.executeCommand('editor.showQueryPreview');
}

export function getPreviewText() {
  return SqlPreviewContentProvider.texts.get(SqlPreviewContentProvider.activeDocUri);
}

export async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export const getDocPath = (p: string) => {
  return path.resolve(__dirname, '../../test-fixture/models', p);
};

export const getDocUri = (p: string) => {
  return vscode.Uri.file(getDocPath(p));
};

export async function setTestContent(content: string): Promise<void> {
  const all = new vscode.Range(doc.positionAt(0), doc.positionAt(doc.getText().length));
  await editor.edit(eb => eb.replace(all, content));
  editor.selection = new vscode.Selection(editor.selection.end, editor.selection.end);
}

export async function insertText(position: vscode.Position, value: string): Promise<void> {
  await editor.edit(eb => eb.insert(position, value));
}

export async function replaceText(oldText: string, newText: string): Promise<void> {
  const offsetStart = editor.document.getText().indexOf(oldText);
  if (offsetStart === -1) {
    throw new Error(`text "${oldText}"" not found in "${editor.document.getText()}"`);
  }

  const positionStart = editor.document.positionAt(offsetStart);
  const positionEnd = editor.document.positionAt(offsetStart + oldText.length);
  await editor.edit(eb => eb.replace(new vscode.Range(positionStart, positionEnd), newText));
}

export function getCursorPosition(): vscode.Position {
  return editor.selection.end;
}

export async function testCompletion(
  docUri: vscode.Uri,
  position: vscode.Position,
  expectedCompletionList: vscode.CompletionList,
  triggerChar?: string,
) {
  // Executing the command `vscode.executeCompletionItemProvider` to simulate triggering completion
  const actualCompletionList = (await vscode.commands.executeCommand(
    'vscode.executeCompletionItemProvider',
    docUri,
    position,
    triggerChar,
  )) as vscode.CompletionList;

  assert.ok(actualCompletionList.items.length >= 4);
  expectedCompletionList.items.forEach((expectedItem, i) => {
    const actualItem = actualCompletionList.items[i];
    assert.strictEqual(actualItem.label, expectedItem.label);
    assert.strictEqual(actualItem.kind, expectedItem.kind);
  });
}
