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

import { ISessionContext } from '@jupyterlab/apputils';
import { IDisplayData, IDisplayUpdate } from '@jupyterlab/nbformat';

import { IHtmlProvider } from '../common/HtmlView';
import { KernelModel } from '../kernel/KernelModel';

export interface IShowOptions {
  includeWindowInfo?: boolean;
  visualizeInFacets?: boolean;
  duration?: string;
  n?: string;
}

// Options depend on the inspectableType. Currently only one variation of
// IShowOptions for pcollection type.
export type IOptions = IShowOptions;

/**
 * The data model for the display area of the InteractiveInspector parent
 * component.
 *
 * It's a singleton and the display area will use the data model for rendering.
 * In the mean time, any sub component of the InteractiveInspector can trigger
 * events to alter the state of this data model to indirectly change the state
 * of the display area.
 *
 * The data model also functions as an IHtmlProvider to work with the common
 * module HtmlView.
 */
export class InspectableViewModel implements IHtmlProvider {
  constructor(sessionContext: ISessionContext) {
    this._model = new KernelModel(sessionContext);
  }

  buildShowGraphQuery(): string {
    return (
      'ib.show_graph(ie.current_env().inspector.' +
      `get_val('${this._identifier}')` +
      ')'
    );
  }

  buildShowQuery(options: IShowOptions = {}): string {
    let optionsAsString = '';
    if (options.includeWindowInfo) {
      optionsAsString += 'include_window_info=True';
    } else {
      optionsAsString += 'include_window_info=False';
    }
    optionsAsString += ', ';
    if (options.visualizeInFacets) {
      optionsAsString += 'visualize_data=True';
    } else {
      optionsAsString += 'visualize_data=False';
    }
    optionsAsString += ', ';
    if (!options.duration) {
      options.duration = 'inf';
    }
    const durationNum = Number(options.duration);
    if (isNaN(durationNum)) {
      optionsAsString += `duration='${options.duration}'`;
    } else if (durationNum <= 0) {
      options.duration = 'inf';
      optionsAsString += "duration='inf'";
    } else {
      options.duration = Math.floor(durationNum).toString(10);
      optionsAsString += 'duration=' + Math.floor(durationNum);
    }
    optionsAsString += ', ';
    const nNum = Number(options.n);
    const nInt = Math.floor(nNum);
    if (isNaN(nNum) || nInt <= 0) {
      options.n = 'inf';
      optionsAsString += "n='inf'";
    } else {
      options.n = nInt.toString(10);
      optionsAsString += 'n=' + nInt;
    }
    return (
      'ib.show(' +
      `ie.current_env().inspector.get_val('${this._identifier}'),` +
      optionsAsString +
      ')'
    );
  }

  get kernelModel(): KernelModel {
    return this._model;
  }

  interruptKernelIfNotDone(): void {
    if (!this._model.isDone) {
      this._model.interruptKernel();
    }
  }

  queryKernel(
    inspectableType: string,
    identifier: string,
    options: IOptions = {}
  ): void {
    this.interruptKernelIfNotDone();
    this._inspectableType = inspectableType.toLowerCase();
    this._identifier = identifier;
    this._options = options;
    if (this._inspectableType === 'pipeline') {
      this._model.execute(this.buildShowGraphQuery());
    } else {
      this._model.execute(this.buildShowQuery(options as IShowOptions));
    }
  }

  get inspectableType(): string {
    return this._inspectableType;
  }

  get identifier(): string {
    return this._identifier;
  }

  get options(): IOptions {
    return this._options;
  }

  set options(options: IOptions) {
    this._options = options;
    this.queryKernel(this.inspectableType, this.identifier, this.options);
  }

  get displayData(): Array<IDisplayData> {
    return this._model.displayData;
  }

  get displayUpdate(): Array<IDisplayUpdate> {
    return this._model.displayUpdate;
  }

  get html(): string {
    return this.displayData
      .map(displayData => this.extractHtmlFromDisplayData(displayData))
      .reduce((accumulatedHtml, html) => accumulatedHtml + html, '');
  }

  get script(): string[] {
    return this.displayData.map(displayData =>
      this.extractScriptFromDisplayData(displayData)
    );
  }

  extractHtmlFromDisplayData(displayData: IDisplayData): string {
    let htmlString = '';
    if ('data' in displayData) {
      const data = displayData.data;
      if ('text/html' in data) {
        // Script tags will not be executed, thus no need to remove.
        htmlString += data['text/html'] + '\n';
      }
    }
    return htmlString;
  }

  extractScriptFromDisplayData(displayData: IDisplayData): string {
    let script = '';
    if ('data' in displayData) {
      const data = displayData.data;
      if ('application/javascript' in data) {
        script += data['application/javascript'] + '\n';
      }
      if ('text/html' in data) {
        // Extract script tags from html.
        const div = document.createElement('div');
        div.innerHTML = data['text/html'] as string;
        const scriptTags = div.getElementsByTagName('script');
        for (const el of scriptTags) {
          script += el.text + '\n';
        }
      }
    }
    return script;
  }

  private _model: KernelModel;
  private _inspectableType: string;
  private _identifier: string;
  private _options: IOptions = {};
}
