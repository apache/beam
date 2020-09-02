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

import {
  ListItem,
  ListItemText,
  ListItemPrimaryText,
  ListItemSecondaryText
} from '@rmwc/list';

import { InspectableViewModel } from './InspectableViewModel';
import { IInspectableMeta } from './InspectableList';

import '@rmwc/list/styles';

/**
 * The item of each PCollection sub list under the side list of the
 * InteractiveInspector parent component.
 *
 * Each list item contains 2 rows. The primary row contains the variable name
 * of a PCollection. The secondary row contains the 'pcollection' text to
 * indicate the type of the item.
 */
interface IInspectableListItemProps {
  inspectableViewModel?: InspectableViewModel;
  id: string;
  metadata: IInspectableMeta;
}

export class InspectableListItem extends React.Component<
  IInspectableListItemProps,
  {}
> {
  constructor(props: IInspectableListItemProps) {
    super(props);
    this.show = this.show.bind(this);
  }

  /**
   * Alters the shared inspectableViewModel of the display area to query and
   * display inspection of the selected PCollection item.
   */
  show(): void {
    this.props.inspectableViewModel.queryKernel(
      this.props.metadata.type,
      this.props.id,
      this.props.inspectableViewModel.options
    );
  }

  render(): React.ReactNode {
    return (
      <ListItem
        style={{
          height: '55px',
          paddingLeft: '5px'
        }}
        onClick={this.show}
      >
        <ListItemText>
          <ListItemPrimaryText>{this.props.metadata.name}</ListItemPrimaryText>
          <ListItemSecondaryText>pcollection</ListItemSecondaryText>
        </ListItemText>
      </ListItem>
    );
  }
}
