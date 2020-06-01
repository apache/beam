import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

import { requestAPI } from './interactivebeamsidepanel';

/**
 * Initialization data for the interactive_beam_side_panel extension.
 */
const extension: JupyterFrontEndPlugin<void> = {
  id: 'interactive-beam-side-panel',
  autoStart: true,
  activate: (app: JupyterFrontEnd) => {
    console.log('JupyterLab extension interactive-beam-side-panel is activated!');

    requestAPI<any>('get_example')
      .then(data => {
        console.log(data);
      })
      .catch(reason => {
        console.error(
          `The interactive_beam_side_panel server extension appears to be missing.\n${reason}`
        );
      });
  }
};

export default extension;
