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

import { InspectableView } from '../../inspector/InspectableView';

import {
  IOptions,
  InspectableViewModel
} from '../../inspector/InspectableViewModel';

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

it('does not render options if inspecting a pipeline', async () => {
  const fakeModel = {
    html: '',
    script: [] as string[],
    inspectableType: 'pipeline',
    identifier: 'id',
    options: {} as IOptions
  } as InspectableViewModel;
  await act(async () => {
    root.render(<InspectableView model={fakeModel} />);
  });
  const inspectableViewElement: Element = container.firstElementChild;
  const optionsElement: Element = inspectableViewElement.firstElementChild;
  expect(optionsElement.tagName).toBe('DIV');
  expect(optionsElement.innerHTML).toBe('<span></span>');
});

it('renders options if inspecting a pcollection', async () => {
  const inspectableViewRef: React.RefObject<InspectableView> =
    React.createRef<InspectableView>();
  const fakeModel = {
    html: '',
    script: [] as string[],
    inspectableType: 'pcollection',
    identifier: 'id',
    options: {
      // includeWindowInfo is undefined, thus should be treated as false.
      visualizeInFacets: true
    } as IOptions
  } as InspectableViewModel;
  await act(async () => {
    root.render(<InspectableView ref={inspectableViewRef} model={fakeModel} />);
    const inspectableView = inspectableViewRef.current;
    if (inspectableView) {
      inspectableView.setState({
        options: fakeModel.options
      });
    }
  });
  const inspectableViewElement: Element = container.firstElementChild;
  const optionsElement: Element = inspectableViewElement.firstElementChild;
  expect(optionsElement.tagName).toBe('DIV');
  const includeWindowInfoCheckbox: Element =
    optionsElement.firstElementChild.firstElementChild;
  expect(
    includeWindowInfoCheckbox.firstElementChild.getAttribute('class')
  ).toContain('mdc-checkbox');
  expect(
    includeWindowInfoCheckbox.firstElementChild.getAttribute('class')
  ).not.toContain('mdc-checkbox--selected');
  const visualizeInFacetsCheckbox: Element =
    optionsElement.firstElementChild.children[1];
  expect(
    visualizeInFacetsCheckbox.firstElementChild.getAttribute('class')
  ).toContain('mdc-checkbox');
  expect(
    visualizeInFacetsCheckbox.firstElementChild.getAttribute('class')
  ).toContain('mdc-checkbox--selected');
  const durationTextField: Element =
    optionsElement.firstElementChild.children[2];
  expect(durationTextField.getAttribute('class')).toContain(
    'mdc-text-field--outlined'
  );
  const nTextField: Element = optionsElement.firstElementChild.children[3];
  expect(nTextField.getAttribute('class')).toContain(
    'mdc-text-field--outlined'
  );
});

it('renders an html view', async () => {
  const fakeModel = {
    html: '<div>fake html</div>',
    script: ['console.log(1)'],
    inspectableType: 'pcollection',
    identifier: 'id',
    options: {} as IOptions
  } as InspectableViewModel;
  await act(async () => {
    root.render(<InspectableView model={fakeModel} />);
  });
  const inspectableViewElement: Element = container.firstElementChild;
  expect(inspectableViewElement.innerHTML).toContain('<div>fake html</div>');
});
