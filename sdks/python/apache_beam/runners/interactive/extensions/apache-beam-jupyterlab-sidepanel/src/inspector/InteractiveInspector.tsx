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

import { ISessionContext } from '@jupyterlab/apputils';

import { Drawer, DrawerAppContent, DrawerContent } from '@rmwc/drawer';
import {
  TopAppBar,
  TopAppBarFixedAdjust,
  TopAppBarNavigationIcon,
  TopAppBarRow,
  TopAppBarSection,
  TopAppBarTitle
} from '@rmwc/top-app-bar';

import { Inspectables } from './Inspectables';
import { InspectableView } from './InspectableView';
import { InspectableViewModel } from './InspectableViewModel';

import '@rmwc/drawer/styles';
import '@rmwc/top-app-bar/styles';

interface IInteractiveInspectorProps {
  sessionContext: ISessionContext;
  inspectableViewModel: InspectableViewModel;
}

interface IInteractiveInspectorState {
  drawerOpen: boolean;
  kernelDisplayName: string;
}

/**
 * An interactive inspector that inspects user defined Apache Beam pipelines
 * and PCollections in the given notebook session.
 *
 * The react component is composed with a fixed top app bar, a left side list
 * of all pipelines and PCollections and a right display area.
 *
 * The top app bar contains a 'menu' icon that on click toggles the side list.
 *
 * The side list on the left organizes PCollections by pipelines they belong to
 * in separate sub lists. Each sub list is collapsible on clicking the pipeline
 * header item.
 *
 * On clicking any item in the side list, the display area on the right displays
 * a pipeline graph for a pipeline or some data visualization for a PCollection.
 */
export class InteractiveInspector extends React.Component<
  IInteractiveInspectorProps,
  IInteractiveInspectorState
> {
  constructor(props: IInteractiveInspectorProps) {
    super(props);
    this.state = {
      drawerOpen: true,
      kernelDisplayName: 'no kernel'
    };
    this.flipDrawer = this.flipDrawer.bind(this);
  }

  componentDidMount(): void {
    this._updateSessionInfoTimerId = setInterval(
      () => this.updateSessionInfo(),
      2000
    );
  }

  componentWillUnmount(): void {
    clearInterval(this._updateSessionInfoTimerId);
  }

  updateSessionInfo(): void {
    if (this.props.sessionContext) {
      const newKernelDisplayName = this.props.sessionContext.kernelDisplayName;
      if (newKernelDisplayName !== this.state.kernelDisplayName) {
        this.setState({
          kernelDisplayName: newKernelDisplayName
        });
      }
    }
  }

  flipDrawer(): void {
    this.setState({
      drawerOpen: !this.state.drawerOpen
    });
  }

  render(): React.ReactNode {
    return (
      <React.Fragment>
        <TopAppBar fixed dense>
          <TopAppBarRow>
            <TopAppBarSection>
              <TopAppBarNavigationIcon icon="menu" onClick={this.flipDrawer} />
              <TopAppBarTitle>
                Inspector [kernel:{this.state.kernelDisplayName}]
              </TopAppBarTitle>
            </TopAppBarSection>
          </TopAppBarRow>
        </TopAppBar>
        <TopAppBarFixedAdjust />
        <div className="InteractiveInspector">
          <Drawer dismissible open={this.state.drawerOpen}>
            <DrawerContent>
              <Inspectables
                sessionContext={this.props.sessionContext}
                inspectableViewModel={this.props.inspectableViewModel}
              />
            </DrawerContent>
          </Drawer>
          <DrawerAppContent>
            <InspectableView model={this.props.inspectableViewModel} />
          </DrawerAppContent>
        </div>
      </React.Fragment>
    );
  }

  private _updateSessionInfoTimerId: number;
}
