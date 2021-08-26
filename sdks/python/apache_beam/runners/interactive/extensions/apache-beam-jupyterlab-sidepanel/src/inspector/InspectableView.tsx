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

import { Button } from '@rmwc/button';
import { Checkbox } from '@rmwc/checkbox';
import { TextField } from '@rmwc/textfield';
import { Tooltip } from '@rmwc/tooltip';

import {
  InspectableViewModel,
  IOptions,
  IShowOptions
} from './InspectableViewModel';
import { HtmlView } from '../common/HtmlView';
import { IHtmlProvider } from '../common/HtmlView';
import { InterruptKernelButton } from '../kernel/InterruptKernelButton';

import '@rmwc/button/styles';
import '@rmwc/checkbox/styles';
import '@rmwc/textfield/styles';
import '@rmwc/tooltip/styles';

interface IInspectableViewProps {
  model: InspectableViewModel;
}

interface IInspectableViewState {
  inspectableType: string;
  identifier: string;
  // options used in kernel messaging.
  options: IOptions;
}

/**
 * The display area of the InteractiveInspector parent component.
 *
 * The react component is composed with a top section of display options and a
 * main HtmlView area that displays HTML from IOPub messaging of its kernel
 * model.
 */
export class InspectableView extends React.Component<
  IInspectableViewProps,
  IInspectableViewState
> {
  constructor(props: IInspectableViewProps) {
    super(props);
    this.state = {
      inspectableType: props.model.inspectableType,
      identifier: props.model.identifier,
      options: props.model.options
    };
    this.applyShowOptions = this.applyShowOptions.bind(this);
  }

  componentDidMount(): void {
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 1000);
  }

  componentWillUnmount(): void {
    clearInterval(this._updateRenderTimerId);
  }

  updateRender(): void {
    // Only self update when there is change in the selected item.
    if (this.props.model.identifier === this.state.identifier) {
      return;
    }
    if (this.props.model.inspectableType === 'pcollection') {
      this.setState({
        inspectableType: 'pcollection',
        identifier: this.props.model.identifier,
        options: this._buildShowOptions(this.props.model.options)
      });
    } else {
      this.setState({
        inspectableType: 'pipeline',
        identifier: this.props.model.identifier,
        options: {}
      });
    }
  }

  renderOptions(): React.ReactNode {
    if (this.props.model.inspectableType !== 'pcollection') {
      return <span />;
    }
    const showOptions = this._buildShowOptions(this.state.options);
    const includeWindowInfo = this._renderIncludeWindowInfo(showOptions);
    const visualizeInFacets = this._renderVisualizeInFacets(showOptions);
    const duration = this._renderDuration(showOptions);
    const n = this._renderN(showOptions);
    return (
      <div
        style={{
          padding: '5px'
        }}
      >
        {includeWindowInfo}
        {visualizeInFacets}
        {duration}
        {n}
        <Button
          label="apply"
          onClick={(): void => this.applyShowOptions(showOptions)}
          raised
        />
      </div>
    );
  }

  applyShowOptions(showOptions: IShowOptions): void {
    this.setState({ options: showOptions });
    // Back store the new state to the model.
    this.props.model.options = showOptions;
  }

  private _renderIncludeWindowInfo(showOptions: IShowOptions): React.ReactNode {
    return (
      <Tooltip content="Whether to include window info.">
        <Checkbox
          label="Include Window Info"
          checked={showOptions.includeWindowInfo}
          onChange={(e): void => {
            showOptions.includeWindowInfo = !!e.currentTarget.checked;
            this.setState({ options: showOptions });
          }}
        />
      </Tooltip>
    );
  }

  private _renderVisualizeInFacets(showOptions: IShowOptions): React.ReactNode {
    return (
      <Tooltip content="Whether to visualize the data in Facets.">
        <Checkbox
          label="Visualize in Facets"
          checked={showOptions.visualizeInFacets}
          onChange={(e): void => {
            showOptions.visualizeInFacets = !!e.currentTarget.checked;
            this.setState({ options: showOptions });
          }}
        />
      </Tooltip>
    );
  }

  private _renderDuration(showOptions: IShowOptions): React.ReactNode {
    const tooltip =
      'Max duration of elements to read in integer seconds or ' +
      'a string duration that is parsable by pandas.to_timedelta, such as 1m ' +
      'or 60s. Otherwise, default value `inf` is used: the visualization ' +
      'will never end until manually stopped by interrupting the kernel or ' +
      'reaches a hard cap set by ib.options.capture_size_limit.';
    return (
      <Tooltip content={tooltip}>
        <TextField
          outlined
          label="Duration"
          floatLabel
          placeholder={showOptions.duration}
          onChange={(e): void => {
            showOptions.duration = e.currentTarget.value;
          }}
        />
      </Tooltip>
    );
  }

  private _renderN(showOptions: IShowOptions): React.ReactNode {
    const tooltip =
      'Max number of elements to visualize. Please fill in a ' +
      'positive integer. Otherwise, default value `inf` is used: the ' +
      'visualization will never end until manually stopped by interrupting ' +
      'the kernel or reaches a hard cap set by ib.options.capture_duration.';
    return (
      <Tooltip content={tooltip}>
        <TextField
          outlined
          label="Element Number"
          floatLabel
          placeholder={showOptions.n}
          onChange={(e): void => {
            showOptions.n = e.currentTarget.value;
          }}
        />
      </Tooltip>
    );
  }

  render(): React.ReactNode {
    const options = this.renderOptions();
    const htmlProvider = this.props.model as IHtmlProvider;
    return (
      <div className="InspectableView">
        <div>{options}</div>
        <InterruptKernelButton model={this.props.model.kernelModel} />
        <HtmlView htmlProvider={htmlProvider} />
      </div>
    );
  }

  private _buildShowOptions(options: IOptions): IShowOptions {
    const optionsInput = options as IShowOptions;
    return {
      includeWindowInfo: !!optionsInput?.includeWindowInfo,
      visualizeInFacets: !!optionsInput?.visualizeInFacets,
      duration: optionsInput?.duration,
      n: optionsInput?.n
    } as IShowOptions;
  }

  private _updateRenderTimerId: number;
}
