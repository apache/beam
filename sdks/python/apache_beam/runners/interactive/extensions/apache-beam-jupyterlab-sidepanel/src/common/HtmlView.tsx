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

export interface IHtmlProvider {
  readonly html: string;
  readonly script: string[];
}

interface IHtmlViewProps {
  htmlProvider: IHtmlProvider;
}

interface IHtmlViewState {
  innerHtml: string;
  script: string[];
}

/**
 * A common HTML viewing component that renders given HTML and executes scripts
 * from the given provider.
 */
export class HtmlView extends React.Component<IHtmlViewProps, IHtmlViewState> {
  constructor(props: IHtmlViewProps) {
    super(props);
    this.state = {
      innerHtml: props.htmlProvider.html,
      script: []
    };
  }

  componentDidMount(): void {
    this._updateRenderTimerId = setInterval(() => this.updateRender(), 1000);
  }

  componentWillUnmount(): void {
    clearInterval(this._updateRenderTimerId);
  }

  updateRender(): void {
    const currentHtml = this.state.innerHtml;
    const htmlToUpdate = this.props.htmlProvider.html;
    const currentScript = this.state.script;
    const scriptToUpdate = [...this.props.htmlProvider.script];
    if (htmlToUpdate !== currentHtml) {
      this.setState({
        innerHtml: htmlToUpdate,
        // As long as the html is updated, clear the script state.
        script: []
      });
    }
    /* Depending on whether this iteration updates the html, the scripts
     * are executed differently.
     * Html updated: all scripts are new, start execution from index 0;
     * Html not updated: only newly added scripts need to be executed.
     */
    const currentScriptLength =
      htmlToUpdate === currentHtml ? currentScript.length : 0;
    if (scriptToUpdate.length > currentScriptLength) {
      this.setState(
        {
          script: scriptToUpdate
        },
        // Executes scripts once the state is updated.
        () => {
          for (let i = currentScriptLength; i < scriptToUpdate.length; ++i) {
            new Function(scriptToUpdate[i])();
          }
        }
      );
    }
  }

  render(): React.ReactNode {
    return (
      // This injects raw HTML fetched from kernel into JSX.
      <div dangerouslySetInnerHTML={{ __html: this.state.innerHtml }} />
    );
  }

  private _updateRenderTimerId: number;
}

/**
 * Makes the browser support HTML import and import HTML from given hrefs if
 * any is given.
 *
 * Native HTML import has been deprecated by modern browsers. To support
 * importing reusable HTML templates, webcomponentsjs library is needed.
 * The given hrefs will be imported once the library is loaded.
 *
 * Note everything is appended to head and if there are duplicated HTML
 * imports, only the first one will take effect.
 */
export function importHtml(hrefs: string[]): void {
  const webcomponentScript = document.createElement('script');
  webcomponentScript.src =
    'https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js';
  webcomponentScript.type = 'text/javascript';
  webcomponentScript.onload = (): void => {
    hrefs.forEach(href => {
      const link = document.createElement('link');
      link.rel = 'import';
      link.href = href;
      document.head.appendChild(link);
    });
  };
  document.head.appendChild(webcomponentScript);
}
