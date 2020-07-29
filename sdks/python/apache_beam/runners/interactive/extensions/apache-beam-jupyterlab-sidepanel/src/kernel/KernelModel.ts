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

/**
 * The module holds the model that handles messaging between the frontend and
 * the connected kernel.
 */

import { ISessionContext } from '@jupyterlab/apputils';

import {
  IDisplayData,
  IDisplayUpdate,
  IExecuteResult
} from '@jupyterlab/nbformat';

import { Kernel, KernelMessage } from '@jupyterlab/services';

import { ISignal, Signal } from '@lumino/signaling';

import { KernelCode } from '../kernel/KernelCode';

export class KernelModel {
  constructor(sessionContext: ISessionContext, enableConsoleLog = false) {
    this._sessionContext = sessionContext;
    this._enableConsoleLog = enableConsoleLog;
  }

  get future(): Kernel.IFuture<
    KernelMessage.IExecuteRequestMsg,
    KernelMessage.IExecuteReplyMsg
  > | null {
    return this._future;
  }

  set future(
    value: Kernel.IFuture<
      KernelMessage.IExecuteRequestMsg,
      KernelMessage.IExecuteReplyMsg
    > | null
  ) {
    if (this._future === value) {
      return;
    }

    if (this._future) {
      this._future.dispose();
    }

    this._future = value;

    if (!value) {
      return;
    }

    value.onIOPub = this._onIOPub.bind(this);
  }

  get executeResult(): object {
    if (this._executeResult) {
      const dataInPlainText = this._executeResult.data['text/plain'] as string;
      if (dataInPlainText) {
        try {
          // The slice removes trailing single quotes from the nbformat output.
          // The replace removes literal backslashes from the nbformat output.
          const dataInJsonString = dataInPlainText
            .slice(1, -1)
            .replace(/\\'/g, "'");
          return JSON.parse(dataInJsonString);
        } catch (e) {
          console.error(e);
          return {};
        }
      }
    }
    return {};
  }

  get displayData(): Array<IDisplayData> {
    return this._displayData;
  }

  get displayUpdate(): Array<IDisplayUpdate> {
    return this._displayUpdate;
  }

  get stateChanged(): ISignal<KernelModel, void> {
    return this._stateChanged;
  }

  execute(code: string, expectReply = true): void {
    // Dispose the kernel future so that no more IOPub will be handled.
    if (this.future) {
      this.future.dispose();
      this.future = null;
    }
    // Clear the outputs from previous kernel executions.
    this._executeResult = null;
    this._displayData.length = 0;
    this._displayUpdate.length = 0;
    if (!this._sessionContext || !this._sessionContext.session?.kernel) {
      return;
    }
    this.future = this._sessionContext.session?.kernel?.requestExecute({
      code: KernelCode.COMMON_KERNEL_IMPORTS + code,
      silent: !expectReply,
      store_history: false // eslint-disable-line @typescript-eslint/camelcase
    });
  }

  private _onIOPub(msg: KernelMessage.IIOPubMessage): void {
    if (this._enableConsoleLog) {
      console.log(msg);
    }
    const msgType = msg.header.msg_type;
    switch (msgType) {
      case 'execute_result': {
        const executeResult = msg.content as IExecuteResult;
        this._executeResult = executeResult;
        this._stateChanged.emit();
        break;
      }
      case 'display_data': {
        const displayData = msg.content as IDisplayData;
        this._displayData.push(displayData);
        this._stateChanged.emit();
        break;
      }
      case 'update_display_data': {
        const displayUpdate = msg.content as IDisplayUpdate;
        this._displayUpdate.push(displayUpdate);
        this._stateChanged.emit();
        break;
      }
      default: {
        break;
      }
    }
    return;
  }

  private _future: Kernel.IFuture<
    KernelMessage.IExecuteRequestMsg,
    KernelMessage.IExecuteReplyMsg
  > | null = null;
  private _displayData: Array<IDisplayData> = new Array<IDisplayData>();
  private _displayUpdate: Array<IDisplayUpdate> = new Array<IDisplayUpdate>();
  private _executeResult: IExecuteResult | null = null;
  private _sessionContext: ISessionContext;
  private _stateChanged = new Signal<KernelModel, void>(this);
  private _enableConsoleLog = false;
}
