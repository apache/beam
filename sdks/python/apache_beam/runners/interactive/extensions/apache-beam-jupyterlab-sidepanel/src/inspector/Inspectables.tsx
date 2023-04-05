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

import { List } from '@rmwc/list';

import { InspectableList } from './InspectableList';
import { InspectableViewModel } from './InspectableViewModel';
import { KernelModel } from '../kernel/KernelModel';

import '@rmwc/list/styles';

interface IInspectablesProps {
  sessionContext?: ISessionContext;
  inspectableViewModel?: InspectableViewModel;
}

interface IInspectablesState {
  inspectables: object;
}

/**
 * The side list of the InteractiveInspector parent component.
 *
 * The react component regularly checks the kernel in the current notebook
 * session for user defined pipelines and PCollections. If the listing has
 * changed, the component re-renders PCollections in each pipeline into a
 * separate list and put all the lists as list items in a parent list.
 */
export class Inspectables extends React.Component<
  IInspectablesProps,
  IInspectablesState
> {
  constructor(props: IInspectablesProps) {
    super(props);
    this._inspectKernelCode =
      'from apache_beam.runners.interactive ' +
      'import interactive_environment as ie\n' +
      'ie.current_env().inspector.list_inspectables()';
    this._model = new KernelModel(this.props.sessionContext);
    this.state = { inspectables: this._model.executeResult };
  }

  componentDidMount(): void {
    this._queryKernelTimerId = setInterval(() => this.queryKernel(), 2000);
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 1000);
  }

  componentWillUnmount(): void {
    clearInterval(this._queryKernelTimerId);
    clearInterval(this._updateRenderTimerId);
  }

  queryKernel(): void {
    this._model.execute(this._inspectKernelCode);
  }

  updateRender(): void {
    const inspectablesToUpdate = this._model.executeResult;
    if (
      Object.keys(inspectablesToUpdate).length &&
      JSON.stringify(this.state.inspectables) !==
        JSON.stringify(inspectablesToUpdate)
    ) {
      this.setState({ inspectables: inspectablesToUpdate });
    }
  }

  render(): React.ReactNode {
    if (Object.keys(this.state.inspectables).length) {
      const inspectableLists = Object.entries(this.state.inspectables).map(
        ([key, value]) => {
          const propsWithKey = {
            inspectableViewModel: this.props.inspectableViewModel,
            id: key,
            ...value
          };
          return <InspectableList key={key} {...propsWithKey} />;
        }
      );

      return <List className="Inspectables">{inspectableLists}</List>;
    }

    return <div>No inspectable pipeline nor pcollection has been defined.</div>;
  }

  private _inspectKernelCode: string;
  private _model: KernelModel;
  private _queryKernelTimerId: number;
  private _updateRenderTimerId: number;
}
