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

import { InspectableView } from '../../inspector/InspectableView';

import {
  IOptions,
  InspectableViewModel
} from '../../inspector/InspectableViewModel';

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

it('does not render options if inspecting a pipeline', () => {
  const fakeModel = {
    html: '',
    script: [] as string[],
    inspectableType: 'pipeline',
    identifier: 'id',
    options: {} as IOptions
  } as InspectableViewModel;
  act(() => {
    render(<InspectableView model={fakeModel} />, container);
  });
  const inspectableViewElement: Element = container.firstElementChild;
  const optionsElement: Element = inspectableViewElement.firstElementChild;
  expect(optionsElement.tagName).toBe('DIV');
  expect(optionsElement.innerHTML).toBe('<span></span>');
});

it('renders options if inspecting a pcollection', () => {
  const inspectableViewRef: React.RefObject<InspectableView> = React.createRef<
    InspectableView
  >();
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
  act(() => {
    render(
      <InspectableView ref={inspectableViewRef} model={fakeModel} />,
      container
    );
    const inspectableView = inspectableViewRef.current;
    if (inspectableView) {
      inspectableView.updateRender();
    }
  });
  const inspectableViewElement: Element = container.firstElementChild;
  const optionsElement: Element = inspectableViewElement.firstElementChild;
  expect(optionsElement.tagName).toBe('DIV');
  const includeWindowInfoCheckbox: Element = optionsElement.firstElementChild;
  expect(
    includeWindowInfoCheckbox.firstElementChild.getAttribute('class')
  ).toContain('mdc-checkbox');
  expect(
    includeWindowInfoCheckbox.firstElementChild.getAttribute('class')
  ).not.toContain('mdc-checkbox--selected');
  const visualizeInFacetsCheckbox: Element = optionsElement.children[1];
  expect(
    visualizeInFacetsCheckbox.firstElementChild.getAttribute('class')
  ).toContain('mdc-checkbox');
  expect(
    visualizeInFacetsCheckbox.firstElementChild.getAttribute('class')
  ).toContain('mdc-checkbox--selected');
});

it('renders an html view', () => {
  const fakeModel = {
    html: '<div>fake html</div>',
    script: ['console.log(1)'],
    inspectableType: 'pcollection',
    identifier: 'id',
    options: {} as IOptions
  } as InspectableViewModel;
  act(() => {
    render(<InspectableView model={fakeModel} />, container);
  });
  const inspectableViewElement: Element = container.firstElementChild;
  expect(inspectableViewElement.innerHTML).toContain('<div>fake html</div>');
});
