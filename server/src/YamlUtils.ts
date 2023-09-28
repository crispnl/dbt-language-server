import * as fs from 'node:fs';
import { homedir } from 'node:os';
import * as yaml from 'yaml';
import path = require('node:path');

export const YamlUtils = {
  replaceTilde(fsPath: string): string {
    if (fsPath[0] === '~') {
      return path.join(homedir(), fsPath.slice(1));
    }
    return fsPath;
  },

  parseYamlFile(filePath: string): unknown {
    const content = fs.readFileSync(filePath, 'utf8');
    return yaml.parse(content, { uniqueKeys: false });
  },

  writeYamlFile(contents: unknown, filePath: string): void {
    const yamlDoc = new yaml.Document(contents);
    yaml.visit(yamlDoc.contents, {
      Scalar(_key: number | string | null, node: yaml.Scalar): undefined {
        if (node.value === 'on') {
          node.type = 'QUOTE_SINGLE';
        }
      },
    });

    // There are some values like 'on', dbt treats as a boolean, so we enforce quoted values.
    const strContents = yamlDoc.toString();
    fs.writeFileSync(filePath, strContents);
  },
};
