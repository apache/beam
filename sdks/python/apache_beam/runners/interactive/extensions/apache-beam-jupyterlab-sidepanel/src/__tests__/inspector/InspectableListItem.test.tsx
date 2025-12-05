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

import { createRoot, Root } from 'react-dom/client';

import { act } from 'react';

import { InspectableListItem } from '../../inspector/InspectableListItem';

let container: null | Element = null;
let root: Root | null = null;
beforeEach(() => {
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(async () => {
  try {
    if (root) {
      await act(async () => {
        root.unmount();
        await new Promise(resolve => setTimeout(resolve, 0));
      });
    }
  } catch (error) {
    console.warn('During unmount:', error);
  } finally {
    if (container?.parentNode) {
      container.remove();
    }
    container = null;
    root = null;
  }
});

it('renders an item', async () => {
  await act(async () => {
    root.render(
      <InspectableListItem
        id="id"
        metadata={{
          name: 'name',
          inMemoryId: 123456,
          type: 'pcollection'
        }}
      />
    );
  });
  const liElement = container.firstElementChild as Element;
  expect(liElement.tagName).toBe('LI');
  expect(liElement.getAttribute('class')).toBe('mdc-list-item');
  const textElement = liElement.children[1] as Element;
  expect(textElement.getAttribute('class')).toBe('mdc-list-item__text');
  const primaryTextElement = textElement.firstElementChild as Element;
  expect(primaryTextElement.getAttribute('class')).toBe(
    'mdc-list-item__primary-text'
  );
  expect(primaryTextElement.textContent).toBe('name');
  const secondaryTextElement = textElement.children[1] as Element;
  expect(secondaryTextElement.getAttribute('class')).toBe(
    'mdc-list-item__secondary-text'
  );
  expect(secondaryTextElement.textContent).toBe('pcollection');
});
