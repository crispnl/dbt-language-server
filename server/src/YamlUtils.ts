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
    const strContents = yaml.stringify(contents);
    fs.writeFileSync(filePath, strContents);
  },
};
