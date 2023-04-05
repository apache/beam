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

import { KernelModel } from './KernelModel';

import '@rmwc/button/styles';

interface IInterruptKernelButtonProps {
  model: KernelModel;
}

interface IInterruptKernelButtonState {
  hidden: boolean;
}

/**
 * A button, on click, interrupts the kernel.
 *
 * Based on the given kernel model's execution state: if the execution is done,
 * the button is not rendered; otherwise, the button is visible.
 */
export class InterruptKernelButton extends React.Component<
  IInterruptKernelButtonProps,
  IInterruptKernelButtonState
> {
  constructor(props: IInterruptKernelButtonProps) {
    super(props);
    this.state = {
      hidden: true
    };
    this.onClick = this.onClick.bind(this);
  }

  componentDidMount(): void {
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 1000);
  }

  componentWillUnmount(): void {
    clearInterval(this._updateRenderTimerId);
  }

  onClick(): void {
    this.props.model.interruptKernel();
    this.setState({
      hidden: true
    });
  }

  updateRender(): void {
    if (this.props.model.isDone) {
      this.setState({
        hidden: true
      });
    } else {
      this.setState({
        hidden: false
      });
    }
  }

  render(): React.ReactNode {
    if (this.state.hidden) {
      return null;
    }
    return <Button label="stop" onClick={this.onClick} danger raised />;
  }

  private _updateRenderTimerId: number;
}
