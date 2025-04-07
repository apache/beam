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

import { InterruptKernelButton } from '../../kernel/InterruptKernelButton';

import { waitFor } from '@testing-library/react';

const fakeKernelModel = {
  isDone: true,
  interruptKernel: function (): void {
    // do nothing.
  }
};

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
    jest.clearAllMocks();
    container = null;
    root = null;
  }
});

it(`displays a button when the kernel model
   is not done with execution`, async () => {
  let button: InterruptKernelButton;

  await act(async () => {
    root.render(
      <InterruptKernelButton
        ref={(node): void => {
          button = node;
        }}
        model={fakeKernelModel as any}
      />
    );
  });

  await act(async () => {
    fakeKernelModel.isDone = false;
    button?.updateRender();
  });

  await waitFor(() => {
    const button = container.firstElementChild;
    expect(button).not.toBeNull();
    expect(button?.tagName).toBe('BUTTON');
  });
  const buttonElement = container.firstElementChild as Element;
  expect(buttonElement.getAttribute('class')).toContain('mdc-button');
  expect(buttonElement.getAttribute('class')).toContain('mdc-button--raised');
  const labelElement = buttonElement.children[1] as Element;
  expect(labelElement.tagName).toBe('SPAN');
  expect(labelElement.getAttribute('class')).toContain('mdc-button__label');
  expect(labelElement.innerHTML).toBe('stop');
});

it(`renders nothing when the kernel
   model is done with execution`, async () => {
  await act(async () => {
    root.render(<InterruptKernelButton model={fakeKernelModel as any} />);
  });
  const buttonElement: null | Element = container.firstElementChild;
  expect(buttonElement).toBe(null);
});

it('interrupts the kernel when clicked', async () => {
  let button: InterruptKernelButton;
  const spiedInterruptCall = jest.spyOn(fakeKernelModel, 'interruptKernel');
  await act(async () => {
    root.render(
      <InterruptKernelButton
        ref={(node): void => {
          button = node;
        }}
        model={fakeKernelModel as any}
      />
    );
  });
  await act(async () => {
    button?.onClick();
    await waitFor(() => {
      expect(spiedInterruptCall).toHaveBeenCalledTimes(1);
    });
  });
});
