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
 * Tests for HtmlView module.
 */

import * as React from 'react';

import { render, unmountComponentAtNode } from 'react-dom';

import { act } from 'react-dom/test-utils';

import { HtmlView, IHtmlProvider, importHtml } from '../../common/HtmlView';

let container: null | Element = null;
beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
});

afterEach(() => {
  unmountComponentAtNode(container);
  container.remove();
  container = null;
  jest.clearAllMocks();
});

describe('HtmlView', () => {
  it('renders provided html', () => {
    const htmlViewRef: React.RefObject<HtmlView> = React.createRef<HtmlView>();
    const spiedConsole = jest.spyOn(console, 'log');
    const fakeHtmlProvider = {
      html: '<div>Test</div>',
      script: ['console.log(1);', 'console.log(2);']
    } as IHtmlProvider;
    act(() => {
      render(
        <HtmlView ref={htmlViewRef} htmlProvider={fakeHtmlProvider} />,
        container
      );
      const htmlView = htmlViewRef.current;
      if (htmlView) {
        htmlView.updateRender();
      }
    });
    const htmlViewElement: Element = container.firstElementChild;
    expect(htmlViewElement.tagName).toBe('DIV');
    expect(htmlViewElement.innerHTML).toBe('<div>Test</div>');
    expect(spiedConsole).toHaveBeenCalledWith(1);
    expect(spiedConsole).toHaveBeenCalledWith(2);
    expect(spiedConsole).toHaveBeenCalledTimes(2);
  });

  it(
    'only executes incrementally updated Javascript ' +
      'as html provider updated',
    () => {
      const htmlViewRef: React.RefObject<HtmlView> = React.createRef<
        HtmlView
      >();
      const spiedConsole = jest.spyOn(console, 'log');
      const fakeHtmlProvider = {
        html: '<div></div>',
        script: ['console.log(1);']
      } as IHtmlProvider;
      act(() => {
        render(
          <HtmlView ref={htmlViewRef} htmlProvider={fakeHtmlProvider} />,
          container
        );
        const htmlView = htmlViewRef.current;
        if (htmlView) {
          htmlView.updateRender();
        }
      });
      expect(spiedConsole).toHaveBeenCalledWith(1);
      expect(spiedConsole).toHaveBeenCalledTimes(1);
      fakeHtmlProvider.script.push('console.log(2);');
      act(() => {
        const htmlView = htmlViewRef.current;
        if (htmlView) {
          htmlView.updateRender();
        }
      });
      expect(spiedConsole).toHaveBeenCalledWith(2);
      expect(spiedConsole).toHaveBeenCalledTimes(2);
    }
  );
});
describe('Function importHtml', () => {
  it('imports webcomponents script', () => {
    act(() => {
      importHtml([]);
    });
    const scriptElement: Element = document.head.firstElementChild;
    expect(scriptElement.tagName).toBe('SCRIPT');
    expect(scriptElement.getAttribute('src')).toBe(
      'https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js'
    );
  });
});
