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

import { Clusters } from '../../clusters/Clusters';

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

it('renders info message about no clusters being available', () => {
  const clustersRef: React.RefObject<Clusters> = React.createRef<Clusters>();
  act(() => {
    render(
      <Clusters sessionContext={{} as any} ref={clustersRef} />,
      container
    );
    const clusters = clustersRef.current;
    if (clusters) {
      clusters.setState({ clusters: {} });
    }
  });
  const infoElement: Element = container.firstElementChild;
  expect(infoElement.tagName).toBe('DIV');
  expect(infoElement.textContent).toBe('No clusters detected.');
});

it('renders a data-table', () => {
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
  act(() => {
    render(
      <Clusters sessionContext={{} as any} ref={clustersRef} />,
      container
    );
    const clusters = clustersRef.current;
    if (clusters) {
      clusters.setState({ clusters: testData });
    }
  });
  const topAppBarHeader: Element = container.firstElementChild;
  expect(topAppBarHeader.tagName).toBe('HEADER');
  expect(topAppBarHeader.getAttribute('class')).toContain('mdc-top-app-bar');
  expect(topAppBarHeader.getAttribute('class')).toContain(
    'mdc-top-app-bar--fixed'
  );
  expect(topAppBarHeader.getAttribute('class')).toContain(
    'mdc-top-app-bar--dense'
  );
  expect(topAppBarHeader.innerHTML).toContain('Clusters [kernel:no kernel]');
  const topAppBarFixedAdjust: Element = container.children[1];
  expect(topAppBarFixedAdjust.tagName).toBe('DIV');
  expect(topAppBarFixedAdjust.getAttribute('class')).toContain(
    'mdc-top-app-bar--fixed-adjust'
  );
  const selectBar: Element = container.children[2];
  expect(selectBar.tagName).toBe('DIV');
  expect(selectBar.getAttribute('class')).toContain('mdc-select');
  const dialogBox: Element = container.children[3];
  expect(dialogBox.tagName).toBe('DIV');
  expect(dialogBox.getAttribute('class')).toContain('mdc-dialog');
  const clustersComponent: Element = container.children[4];
  expect(clustersComponent.tagName).toBe('DIV');
  expect(clustersComponent.getAttribute('class')).toContain('Clusters');
  const dataTableDiv: Element = clustersComponent.children[0];
  expect(dataTableDiv.tagName).toBe('DIV');
  expect(dataTableDiv.getAttribute('class')).toContain('mdc-data-table');
  const dataTable: Element = dataTableDiv.children[0];
  expect(dataTable.tagName).toBe('TABLE');
  expect(dataTable.getAttribute('class')).toContain('mdc-data-table__table');
  const dataTableHead: Element = dataTable.children[0];
  expect(dataTableHead.tagName).toBe('THEAD');
  expect(dataTableHead.getAttribute('class')).toContain(
    'rmwc-data-table__head'
  );
  const dataTableHeaderRow: Element = dataTableHead.children[0];
  expect(dataTableHeaderRow.tagName).toBe('TR');
  expect(dataTableHeaderRow.getAttribute('class')).toContain(
    'mdc-data-table__header-row'
  );
  const dataTableBody: Element = dataTable.children[1];
  expect(dataTableBody.tagName).toBe('TBODY');
  expect(dataTableBody.getAttribute('class')).toContain(
    'mdc-data-table__content'
  );
  const dataTableBodyRow: Element = dataTableBody.children[0];
  expect(dataTableBodyRow.tagName).toBe('TR');
  expect(dataTableBodyRow.getAttribute('class')).toContain(
    'mdc-data-table__row'
  );
});
