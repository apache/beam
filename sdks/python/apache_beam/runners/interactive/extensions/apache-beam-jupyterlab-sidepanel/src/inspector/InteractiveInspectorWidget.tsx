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

import * as React from 'react';

import { ISessionContext, ReactWidget } from '@jupyterlab/apputils';

import { InspectableViewModel } from './InspectableViewModel';
import { InteractiveInspector } from './InteractiveInspector';

/**
 * Converts the React component InteractiveInspector into a lumino widget used
 * in Jupyter labextensions.
 */
export class InteractiveInspectorWidget extends ReactWidget {
  constructor(sessionContext: ISessionContext) {
    super();
    this._sessionContext = sessionContext;
    // The model is shared by Inspectables and InspectableView so that
    // sub-components of Inspectables can trigger events that alters the
    // InspectableView.
    this._inspectableViewModel = new InspectableViewModel(sessionContext);
  }

  protected render(): React.ReactElement<any> {
    return (
      <InteractiveInspector
        sessionContext={this._sessionContext}
        inspectableViewModel={this._inspectableViewModel}
      />
    );
  }

  private _sessionContext: ISessionContext;
  private _inspectableViewModel: InspectableViewModel;
}
