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

import { Inspectables } from '../../inspector/Inspectables';
import { waitFor } from '@testing-library/dom';

jest.mock('../../inspector/InspectableList', () => {
  const FakeInspectableList = function (): React.ReactNode {
    return <div></div>;
  };
  FakeInspectableList.displayName = 'FakeInspectableList';

  return {
    InspectableList: FakeInspectableList
  };
});

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

it(`renders info message about no inspectable
   when none is available`, async () => {
  const inspectablesRef: React.RefObject<Inspectables> =
    React.createRef<Inspectables>();
  await act(async () => {
    root.render(<Inspectables ref={inspectablesRef} />);
    const inspectables = inspectablesRef.current;
    if (inspectables) {
      inspectables.setState({ inspectables: {} });
    }
  });
  const infoElement = container.firstElementChild as Element;
  expect(infoElement.tagName).toBe('DIV');
  expect(infoElement.textContent).toBe(
    'No inspectable pipeline nor pcollection has been defined.'
  );
});

it('renders inspectables as a list of collapsible lists', async () => {
  const inspectablesRef: React.RefObject<Inspectables> =
    React.createRef<Inspectables>();
  const testData = {
    pipelineId1: {
      metadata: {
        name: 'pipeline_1',
        inMemoryId: 1,
        type: 'pipeline'
      },
      pcolls: {
        pcollId1: {
          name: 'pcoll_1',
          inMemoryId: 2,
          type: 'pcollection'
        }
      }
    },
    pipelineId2: {
      metadata: {
        name: 'pipeline_2',
        inMemoryId: 3,
        type: 'pipeline'
      },
      pcolls: {
        pcollId2: {
          name: 'pcoll_2',
          inMemoryId: 4,
          type: 'pcollection'
        }
      }
    }
  };

  await act(async () => {
    root.render(<Inspectables ref={inspectablesRef} />);
  });

  await act(async () => {
    inspectablesRef.current?.setState({ inspectables: testData });
  });

  await waitFor(() => {
    const listElement = container.firstElementChild as Element;
    expect(listElement.tagName).toBe('UL');
    expect(listElement.getAttribute('class')).toContain('mdc-list');
    // Only checks the length of dummy InspectableList items.
    // Each InspectableList has its own unit tests.
    expect(listElement.children).toHaveLength(2);
  });
});
