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

import { InterruptKernelButton } from '../../kernel/InterruptKernelButton';

const fakeKernelModel = {
  isDone: true,
  interruptKernel: function(): void {
    // do nothing.
  }
};
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
  fakeKernelModel.isDone = true;
});

it('displays a button when the kernel model is not done with execution', () => {
  let button: InterruptKernelButton;
  act(() => {
    render(
      <InterruptKernelButton
        ref={(node): void => {
          button = node;
        }}
        model={fakeKernelModel as any}
      />,
      container
    );
    fakeKernelModel.isDone = false;
    if (button) {
      button.updateRender();
    }
  });
  const buttonElement: null | Element = container.firstElementChild;
  expect(buttonElement.tagName).toBe('BUTTON');
  expect(buttonElement.getAttribute('class')).toContain('mdc-button');
  expect(buttonElement.getAttribute('class')).toContain('mdc-button--raised');
  const labelElement: Element = buttonElement.children[1];
  expect(labelElement.tagName).toBe('SPAN');
  expect(labelElement.getAttribute('class')).toContain('mdc-button__label');
  expect(labelElement.innerHTML).toBe('stop');
});

it('renders nothing when the kernel model is done with execution', () => {
  act(() => {
    render(<InterruptKernelButton model={fakeKernelModel as any} />, container);
  });
  const buttonElement: null | Element = container.firstElementChild;
  expect(buttonElement).toBe(null);
});

it('interrupts the kernel when clicked', () => {
  let button: InterruptKernelButton;
  const spiedInterrruptCall = jest.spyOn(fakeKernelModel, 'interruptKernel');
  act(() => {
    render(
      <InterruptKernelButton
        ref={(node): void => {
          button = node;
        }}
        model={fakeKernelModel as any}
      />,
      container
    );
    if (button) {
      button.onClick();
    }
  });
  expect(spiedInterrruptCall).toHaveBeenCalledTimes(1);
});
