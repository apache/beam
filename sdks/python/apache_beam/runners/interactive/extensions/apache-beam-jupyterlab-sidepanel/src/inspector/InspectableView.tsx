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

import { Checkbox } from '@rmwc/checkbox';

import {
  InspectableViewModel,
  IOptions,
  IShowOptions
} from './InspectableViewModel';
import { HtmlView } from '../common/HtmlView';
import { IHtmlProvider } from '../common/HtmlView';

import '@rmwc/checkbox/styles';

interface IInspectableViewProps {
  model: InspectableViewModel;
}

interface IInspectableViewState {
  inspectableType: string;
  // options used in kernel messaging.
  options: IOptions;
}

/**
 * The display area of the InteractiveInspector parent component.
 *
 * The react component is composed with a top checkbox section of display
 * options and a main HtmlView area that displays HTML from IOPub messaging of
 * its kernel model.
 */
export class InspectableView extends React.Component<
  IInspectableViewProps,
  IInspectableViewState
> {
  constructor(props: IInspectableViewProps) {
    super(props);
    this.state = {
      inspectableType: 'pipeline',
      options: props.model.options
    };
  }

  componentDidMount(): void {
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 1500);
  }

  componentWillUnmount(): void {
    clearInterval(this._updateRenderTimerId);
  }

  updateRender(): void {
    if (this.props.model.inspectableType === 'pcollection') {
      this.setState({
        inspectableType: 'pcollection',
        options: this._buildShowOptions(this.props.model.options)
      });
    } else {
      this.setState({
        inspectableType: 'pipeline',
        options: {}
      });
    }
  }

  renderOptions(): React.ReactNode {
    if (this.props.model.inspectableType !== 'pcollection') {
      return <span />;
    }
    const showOptions = this._buildShowOptions(this.state.options);
    return (
      <React.Fragment>
        <Checkbox
          label="window info"
          checked={showOptions.includeWindowInfo}
          onChange={(e): void => {
            showOptions.includeWindowInfo = !!e.currentTarget.checked;
            this.setState({ options: showOptions });
            // Back store the new state to the model.
            this.props.model.options = showOptions;
          }}
        />
        <Checkbox
          label="facets"
          checked={showOptions.visualizeInFacets}
          onChange={(e): void => {
            showOptions.visualizeInFacets = !!e.currentTarget.checked;
            this.setState({ options: showOptions });
            this.props.model.options = showOptions;
          }}
        />
      </React.Fragment>
    );
  }

  render(): React.ReactNode {
    const options = this.renderOptions();
    const htmlProvider = this.props.model as IHtmlProvider;
    return (
      <div className="InspectableView">
        <div>{options}</div>
        <HtmlView htmlProvider={htmlProvider} />
      </div>
    );
  }

  private _buildShowOptions(options: IOptions): IShowOptions {
    const optionsInput = options as IShowOptions;
    return {
      includeWindowInfo: !!optionsInput?.includeWindowInfo,
      visualizeInFacets: !!optionsInput?.visualizeInFacets
    } as IShowOptions;
  }

  private _updateRenderTimerId: number;
}
