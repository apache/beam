// Licensed under the Apache License, Version 2.0 (the 'License'); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

import {
  SessionContext,
  ISessionContext,
  sessionContextDialogs
} from '@jupyterlab/apputils';
import { IRenderMimeRegistry } from '@jupyterlab/rendermime';
import { ServiceManager } from '@jupyterlab/services';
import { Message } from '@lumino/messaging';
import { BoxPanel } from '@lumino/widgets';

// prettier-ignore
import {
  InteractiveInspectorWidget
} from './inspector/InteractiveInspectorWidget';

/**
 * The side panel: main user interface of the extension.
 *
 * Multiple instances of the side panel can be opened at the same time. They
 * can be operated independently but sharing the same kernel state if connected
 * to the same notebook session model or running kernel instance.
 */
export class SidePanel extends BoxPanel {
  constructor(
    manager: ServiceManager.IManager,
    rendermime: IRenderMimeRegistry
  ) {
    super({
      direction: 'top-to-bottom',
      alignment: 'end'
    });
    this.id = 'apache-beam-jupyterlab-sidepanel';
    this.title.label = 'Interactive Beam Inspector';
    this.title.closable = true;

    this._sessionContext = new SessionContext({
      sessionManager: manager.sessions,
      specsManager: manager.kernelspecs,
      name: 'Interactive Beam Inspector Session'
    });

    this._inspector = new InteractiveInspectorWidget(this._sessionContext);
    this.addWidget(this._inspector);

    void this._sessionContext
      .initialize()
      .then(async value => {
        if (value) {
          const sessionModelItr = manager.sessions.running();
          const firstModel = sessionModelItr.next();
          let onlyOneUniqueKernelExists = true;
          if (firstModel === undefined) {
            // There is zero unique running kernel.
            onlyOneUniqueKernelExists = false;
          } else {
            let sessionModel = sessionModelItr.next();
            while (sessionModel !== undefined) {
              if (sessionModel.kernel.id !== firstModel.kernel.id) {
                // There is more than one unique running kernel.
                onlyOneUniqueKernelExists = false;
                break;
              }
              sessionModel = sessionModelItr.next();
            }
          }
          // Create a new notebook session with the same model of the first
          // session (any session would suffice) when there is only one running
          // kernel.
          if (onlyOneUniqueKernelExists) {
            this._sessionContext.sessionManager.connectTo({
              model: firstModel,
              kernelConnectionOptions: {
                handleComms: true
              }
            });
            // Connect to the unique kernel.
            this._sessionContext.changeKernel(firstModel.kernel);
          } else {
            // Let the user choose among sessions and kernels when there is no
            // or more than 1 running kernels.
            await sessionContextDialogs.selectKernel(this._sessionContext);
          }
        }
      })
      .catch(reason => {
        console.error(
          `Failed to initialize the session in SidePanel.\n${reason}`
        );
      });
  }

  get session(): ISessionContext {
    return this._sessionContext;
  }

  dispose(): void {
    this._sessionContext.dispose();
    super.dispose();
  }

  protected onCloseRequest(msg: Message): void {
    super.onCloseRequest(msg);
    this.dispose();
  }

  private _inspector: InteractiveInspectorWidget;
  private _sessionContext: SessionContext;
}
