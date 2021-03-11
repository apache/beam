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
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { ICommandPalette } from '@jupyterlab/apputils';
import { ILauncher } from '@jupyterlab/launcher';
import { IMainMenu } from '@jupyterlab/mainmenu';
import { IRenderMimeRegistry } from '@jupyterlab/rendermime';
import { Menu } from '@lumino/widgets';

import { SidePanel } from './SidePanel';

namespace CommandIDs {
  export const open = 'apache-beam-jupyterlab-sidepanel:open';
}

/**
 * Initialization data for the apache-beam-jupyterlab-sidepanel extension.
 *
 * To open the main user interface of the side panel, a user can:
 * 1. Select the `Open Inspector` item from `Interactive Beam` category on the
 *    launcher page of JupyterLab.
 * 2. Same selection from the `Commands` palette on the left side of the
 *    workspace.
 * 3. Same selection from the top menu bar of the workspace.
 */
const extension: JupyterFrontEndPlugin<void> = {
  id: 'apache-beam-jupyterlab-sidepanel',
  autoStart: true,
  optional: [ILauncher],
  requires: [ICommandPalette, IMainMenu, IRenderMimeRegistry],
  activate: activate
};

function activate(
  app: JupyterFrontEnd,
  palette: ICommandPalette,
  mainMenu: IMainMenu,
  rendermime: IRenderMimeRegistry,
  launcher: ILauncher | null
): void {
  const category = 'Interactive Beam';
  const commandLabel = 'Open Inspector';
  const { commands, shell, serviceManager } = app;

  async function createPanel(): Promise<SidePanel> {
    const panel = new SidePanel(serviceManager, rendermime);
    shell.add(panel, 'main');
    shell.activateById(panel.id);
    return panel;
  }
  // The command is used by all 3 below entry points.
  commands.addCommand(CommandIDs.open, {
    label: commandLabel,
    execute: createPanel
  });

  // Entry point in launcher.
  if (launcher) {
    launcher.add({
      command: CommandIDs.open,
      category: category
    });
  }

  // Entry point in top menu.
  const menu = new Menu({ commands });
  menu.title.label = 'Interactive Beam';
  mainMenu.addMenu(menu);
  menu.addItem({ command: CommandIDs.open });

  // Entry point in commands palette.
  palette.addItem({ command: CommandIDs.open, category });
}

export default extension;
