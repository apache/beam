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

import { CollapsibleList, SimpleListItem, ListDivider } from '@rmwc/list';

import { InspectableListItem } from './InspectableListItem';
import { InspectableViewModel } from './InspectableViewModel';

import '@rmwc/list/styles';

export interface IInspectableMeta {
  name: string;
  // The id() value of the inspectable item in the kernel.
  inMemoryId: number;
  type: string;
}

interface IKeyedInspectableMeta {
  [key: string]: IInspectableMeta;
}

interface IInspectableListProps {
  inspectableViewModel?: InspectableViewModel;
  id: string;
  metadata: IInspectableMeta;
  pcolls: IKeyedInspectableMeta;
}

interface IInspectableListState {
  activated: boolean;
}

/**
 * The PCollection sub list of the side list of the InteractiveInspector parent
 * component.
 *
 * Each sub list only contains listing of PCollections for one pipeline.
 *
 * The pipeline item functions as a header of the collapsible sub list. It
 * alters the shared inspectableViewModel of the display area on click to query
 * and display a graph that is the DOT representation of the pipeline.
 */
export class InspectableList extends React.Component<
  IInspectableListProps,
  IInspectableListState
> {
  constructor(props: IInspectableListProps) {
    super(props);
    this._onClickHeader = this._onClickHeader.bind(this);
    this.state = { activated: false };
  }

  componentDidMount(): void {
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 500);
  }

  componentWillUnmount(): void {
    clearInterval(this._updateRenderTimerId);
  }

  render(): React.ReactNode {
    return (
      <React.Fragment>
        <CollapsibleList
          defaultOpen
          handle={
            <SimpleListItem
              style={{
                height: '55px',
                paddingLeft: '0px'
              }}
              text={this.props.metadata.name}
              secondaryText="pipeline"
              metaIcon="chevron_right"
              activated={this.state.activated}
            />
          }
          onOpen={this._onClickHeader}
          onClose={this._onClickHeader}
        >
          {this._buildListItems()}
        </CollapsibleList>
        <ListDivider />
      </React.Fragment>
    );
  }

  updateRender(): void {
    this.setState({
      activated: this.props?.inspectableViewModel?.identifier === this.props.id
    });
  }

  private _onClickHeader(): void {
    if (this.props?.inspectableViewModel?.identifier === this.props.id) {
      return;
    }
    this.props.inspectableViewModel.queryKernel(
      this.props.metadata.type,
      this.props.id
    );
  }

  private _buildListItems(): React.ReactNode {
    return Object.entries(this.props.pcolls).map(([key, value]) => {
      const propsWithKey = {
        inspectableViewModel: this.props.inspectableViewModel,
        id: key,
        metadata: value
      };
      return <InspectableListItem key={key} {...propsWithKey} />;
    });
  }

  private _updateRenderTimerId: number;
}
