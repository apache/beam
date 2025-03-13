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

import { render, unmountComponentAtNode } from 'react-dom';

import { act } from 'react-dom/test-utils';

import { InspectableListItem } from '../../inspector/InspectableListItem';

let container: null | Element = null;
beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
});

afterEach(() => {
  unmountComponentAtNode(container);
  container.remove();
  container = null;
});

it('renders an item', () => {
  act(() => {
    render(
      <InspectableListItem
        id="id"
        metadata={{
          name: 'name',
          inMemoryId: 123456,
          type: 'pcollection'
        }}
      />,
      container
    );
  });
  const liElement: Element = container.firstElementChild;
  expect(liElement.tagName).toBe('LI');
  expect(liElement.getAttribute('class')).toBe('mdc-list-item');
  const textElement: Element = liElement.firstElementChild;
  expect(textElement.getAttribute('class')).toBe('mdc-list-item__text');
  const primaryTextElement: Element = textElement.firstElementChild;
  expect(primaryTextElement.getAttribute('class')).toBe(
    'mdc-list-item__primary-text'
  );
  expect(primaryTextElement.textContent).toBe('name');
  const secondaryTextElement: Element = textElement.children[1];
  expect(secondaryTextElement.getAttribute('class')).toBe(
    'mdc-list-item__secondary-text'
  );
  expect(secondaryTextElement.textContent).toBe('pcollection');
});
