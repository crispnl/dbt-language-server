import { ProgressLocation, window } from 'vscode';
import { WorkDoneProgressBegin, WorkDoneProgressEnd, WorkDoneProgressReport } from 'vscode-languageserver-protocol';

interface ProgressPromise {
  promise: Promise<unknown>;
  resolve: (value: unknown) => void;
}

export class ProgressHandler {
  progressPromise: ProgressPromise;

  createProgressPromise(): ProgressPromise {
    let promiseResolve;
    let promiseReject;

    const promise = new Promise(function (resolve, reject) {
      promiseResolve = resolve;
      promiseReject = reject;
    });

    return { promise: promise, resolve: promiseResolve };
  }

  onProgress(value: WorkDoneProgressBegin | WorkDoneProgressReport | WorkDoneProgressEnd) {
    switch (value.kind) {
      case 'begin':
        if (!this.progressPromise) {
          this.progressPromise = this.createProgressPromise();
          window.withProgress(
            {
              location: ProgressLocation.Window,
              title: 'Compiling dbt...',
              cancellable: false,
            },
            (progress, token) => {
              return this.progressPromise.promise;
            },
          );
        }
        break;
      case 'end':
        this.progressPromise.resolve(0);
        this.progressPromise = null;
        break;
    }
  }
}