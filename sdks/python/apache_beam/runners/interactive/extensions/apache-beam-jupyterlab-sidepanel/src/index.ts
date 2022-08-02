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

import { ClustersWidget } from './clusters/ClustersWidget';
import { SessionContext } from '@jupyterlab/apputils';
import { SidePanel } from './SidePanel';

// prettier-ignore
import {
  InteractiveInspectorWidget
} from './inspector/InteractiveInspectorWidget';

namespace CommandIDs {
  export const open_inspector =
    'apache-beam-jupyterlab-sidepanel:open_inspector';
  export const open_clusters_panel =
    'apache-beam-jupyterlab-sidepanel:open_clusters_panel';
}

/**
 * Initialization data for the apache-beam-jupyterlab-sidepanel extension.
 *
 * There are two user interfaces that use the JupyterLab sidepanel. There is
 * the Interactive Inspector, and the Cluster Management side panel.
 *
 * To open the main user interface of the side panel, a user can:
 * 1. Select either the `Open Inspector` or `Manage Clusters` item from
 * the `Interactive Beam` category on the launcher page of JupyterLab.
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
  const inspectorCommandLabel = 'Open Inspector';
  const clustersCommandLabel = 'Manage Clusters';
  const { commands, shell, serviceManager } = app;

  async function createInspectorPanel(): Promise<SidePanel> {
    const sessionContext = new SessionContext({
      sessionManager: serviceManager.sessions,
      specsManager: serviceManager.kernelspecs,
      name: 'Interactive Beam Inspector Session'
    });
    const inspector = new InteractiveInspectorWidget(sessionContext);
    const panel = new SidePanel(
      serviceManager,
      rendermime,
      sessionContext,
      'Interactive Beam Inspector',
      inspector
    );
    activatePanel(panel);
    return panel;
  }

  async function createClustersPanel(): Promise<SidePanel> {
    const sessionContext = new SessionContext({
      sessionManager: serviceManager.sessions,
      specsManager: serviceManager.kernelspecs,
      name: 'Interactive Beam Clusters Session'
    });
    const clusters = new ClustersWidget(sessionContext);
    const panel = new SidePanel(
      serviceManager,
      rendermime,
      sessionContext,
      'Interactive Beam Cluster Manager',
      clusters
    );
    activatePanel(panel);
    return panel;
  }

  function activatePanel(panel: SidePanel): void {
    shell.add(panel, 'main');
    shell.activateById(panel.id);
  }

  // The open_inspector command is used by all 3 below entry points.
  commands.addCommand(CommandIDs.open_inspector, {
    label: inspectorCommandLabel,
    execute: createInspectorPanel
  });

  // The open_clusters_panel command is also used by the below entry points.
  commands.addCommand(CommandIDs.open_clusters_panel, {
    label: clustersCommandLabel,
    execute: createClustersPanel
  });

  // Entry point in launcher.
  if (launcher) {
    launcher.add({
      command: CommandIDs.open_inspector,
      category: category
    });
    launcher.add({
      command: CommandIDs.open_clusters_panel,
      category: category
    });
  }

  // Entry point in top menu.
  const menu = new Menu({ commands });
  menu.title.label = 'Interactive Beam';
  mainMenu.addMenu(menu);
  menu.addItem({ command: CommandIDs.open_inspector });
  menu.addItem({ command: CommandIDs.open_clusters_panel });

  // Entry point in commands palette.
  palette.addItem({ command: CommandIDs.open_inspector, category });
  palette.addItem({ command: CommandIDs.open_clusters_panel, category });
}

export default extension;
