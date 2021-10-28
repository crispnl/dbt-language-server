import assert = require('assert');
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import { CompletionItem } from 'vscode';
import { activateAndWait, getDocUri, setTestContent, testCompletion, triggerCompletion, waitDbtCommand } from './helper';

suite('Should do completion inside jinjas expression', () => {
  suiteSetup(function () {
    fs.copyFile(
      path.resolve(__dirname, '../../test-fixture/target/test_manifest.json'),
      path.resolve(__dirname, '../../test-fixture/target/manifest.json'),
      e => {
        if (e) {
          throw e;
        }
        console.log('File was copied to destination');
      },
    );
  });

  suiteTeardown(function () {
    fs.unlink(path.resolve(__dirname, '../../test-fixture/target/manifest.json'), e => {
      if (e) {
        throw e;
      }
      console.log('File manifest.json was deleted');
    });
  });

  test('Should suggest models for ref function by pressing "("', async () => {
    const docUri = getDocUri('completion_jinja.sql');
    await activateAndWait(docUri);
    await setTestContent('select * from {{ref(');
    await waitDbtCommand();

    await testCompletion(docUri, new vscode.Position(0, 20), getCompletionList(true), '(');
  });

  test('Should suggest models for ref function', async () => {
    const docUri = getDocUri('completion_jinja.sql');
    await activateAndWait(docUri);
    await setTestContent('select * from {{ref(');
    await waitDbtCommand();

    await testCompletion(docUri, new vscode.Position(0, 20), getCompletionList(true));
  });

  test('Should suggest models for ref function by pressing "\'"', async () => {
    const docUri = getDocUri('completion_jinja.sql');
    await activateAndWait(docUri);
    await setTestContent(`select * from {{ref('`);
    await waitDbtCommand();

    await testCompletion(docUri, new vscode.Position(0, 21), getCompletionList(false), "'");
  });

  test('Should not suggest models outside jinja', async () => {
    // arrange
    const docUri = getDocUri('completion_jinja.sql');
    await activateAndWait(docUri);
    await setTestContent(`select * from {{}}ref('`);
    await waitDbtCommand();

    // act
    const actualCompletionList = await triggerCompletion(docUri, new vscode.Position(0, 22));

    // assert
    const actualLabels = actualCompletionList.items.map(i => <string>i.label);

    getCompletionList(false).items.forEach(i => assert.ok(!actualLabels.includes(<string>i.label)));
    getCompletionList(true).items.forEach(i => assert.ok(!actualLabels.includes(<string>i.label)));
  });

  function getCompletionList(withQuotes: boolean) {
    return { items: getLabels().map(l => <CompletionItem>{ label: withQuotes ? `'${l}'` : l, kind: vscode.CompletionItemKind.Value }) };
  }

  function getLabels() {
    return ['dbt_compile', 'errors', 'functions', 'jinja_sql', 'join_tables'];
  }
});