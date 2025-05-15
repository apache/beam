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

import { Clusters } from '../../clusters/Clusters';

import { waitFor } from '@testing-library/dom';

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
it('renders info message about no clusters being available', async () => {
  const clustersRef: React.RefObject<Clusters> = React.createRef<Clusters>();
  await act(async () => {
    root.render(<Clusters sessionContext={{} as any} ref={clustersRef} />);
    const clusters = clustersRef.current;
    if (clusters) {
      clusters.setState({ clusters: {} });
    }
  });
  const infoElement = container.firstElementChild as Element;
  expect(infoElement.tagName).toBe('DIV');
  expect(infoElement.textContent).toBe('No clusters detected.');
});

it('renders a data-table', async () => {
  const clustersRef: React.RefObject<Clusters> = React.createRef<Clusters>();
  const testData = {
    key: {
      cluster_name: 'test-cluster',
      project: 'test-project',
      region: 'test-region',
      pipelines: ['p'],
      master_url: 'test-master_url',
      dashboard: 'test-dashboard'
    }
  };
  await act(async () => {
    root.render(<Clusters sessionContext={{} as any} ref={clustersRef} />);
  });

  await act(async () => {
    clustersRef.current?.setState({ clusters: testData });
  });

  await waitFor(() =>
    expect(container.querySelector('.mdc-data-table__table')).toBeTruthy()
  );

  const topAppBarHeader = container.firstElementChild as Element;
  expect(topAppBarHeader.tagName).toBe('HEADER');
  expect(topAppBarHeader.getAttribute('class')).toContain('mdc-top-app-bar');
  expect(topAppBarHeader.getAttribute('class')).toContain(
    'mdc-top-app-bar--fixed'
  );
  expect(topAppBarHeader.getAttribute('class')).toContain(
    'mdc-top-app-bar--dense'
  );
  expect(topAppBarHeader.innerHTML).toContain('Clusters [kernel:no kernel]');
  const topAppBarFixedAdjust = container.children[1] as Element;
  expect(topAppBarFixedAdjust.tagName).toBe('DIV');
  expect(topAppBarFixedAdjust.getAttribute('class')).toContain(
    'mdc-top-app-bar--fixed-adjust'
  );
  const selectBar = container.children[2] as Element;
  expect(selectBar.tagName).toBe('DIV');
  expect(selectBar.getAttribute('class')).toContain('mdc-select');
  const dialogBox = container.children[3] as Element;
  expect(dialogBox.tagName).toBe('DIV');
  expect(dialogBox.getAttribute('class')).toContain('mdc-dialog');
  const clustersComponent = container.children[4] as Element;
  expect(clustersComponent.tagName).toBe('DIV');
  expect(clustersComponent.getAttribute('class')).toContain('Clusters');
  const dataTableDiv = clustersComponent.children[0] as Element;
  expect(dataTableDiv.tagName).toBe('DIV');
  expect(dataTableDiv.getAttribute('class')).toContain('mdc-data-table');
  const dataTable = dataTableDiv.children[0].firstElementChild as Element;
  expect(dataTable.tagName).toBe('TABLE');
  expect(dataTable.getAttribute('class')).toContain('mdc-data-table__table');
  const dataTableHead = dataTable.children[0] as Element;
  expect(dataTableHead.tagName).toBe('THEAD');
  expect(dataTableHead.getAttribute('class')).toContain(
    'rmwc-data-table__head'
  );
  const dataTableHeaderRow = dataTableHead.children[0] as Element;
  expect(dataTableHeaderRow.tagName).toBe('TR');
  expect(dataTableHeaderRow.getAttribute('class')).toContain(
    'mdc-data-table__header-row'
  );
  const dataTableBody = dataTable.children[1] as Element;
  expect(dataTableBody.tagName).toBe('TBODY');
  expect(dataTableBody.getAttribute('class')).toContain(
    'mdc-data-table__content'
  );
  const dataTableBodyRow = dataTableBody.children[0] as Element;
  expect(dataTableBodyRow.tagName).toBe('TR');
  expect(dataTableBodyRow.getAttribute('class')).toContain(
    'mdc-data-table__row'
  );
});
