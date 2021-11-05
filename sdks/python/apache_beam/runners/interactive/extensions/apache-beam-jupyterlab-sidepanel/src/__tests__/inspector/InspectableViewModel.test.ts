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

import {
  IShowOptions,
  InspectableViewModel
} from '../../inspector/InspectableViewModel';

const fakeSessionContext = {
  session: {
    kernel: {
      requestExecute: function(): object {
        return {
          onIOPub: function(): void {
            // do nothing
          }
        };
      },
      interrupt: function(): void {
        // do nothing
      }
    }
  }
};

it('builds show_graph query correctly', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(inspectableViewModel.buildShowGraphQuery()).toBe(
    "ib.show_graph(ie.current_env().inspector.get_val('id'))"
  );
});

it('builds show query correctly with default values', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(inspectableViewModel.buildShowQuery({} as IShowOptions)).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration='inf', n='inf')"
  );
});

it('builds show query with given checkbox values correctly', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      includeWindowInfo: true,
      visualizeInFacets: true
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=True, visualize_data=True, ' +
      "duration='inf', n='inf')"
  );
});

it('builds show query with string duration', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      duration: '60s'
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration='60s', n='inf')"
  );
});

it('builds show query with negative duration as inf', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      duration: '-1'
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration='inf', n='inf')"
  );
});

it('builds show query with positive duration as int', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      duration: '5.5'
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration=5, n='inf')"
  );
});

it('builds show query with string n as inf', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      n: 'abcd'
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration='inf', n='inf')"
  );
});

it('builds show query with negative n as inf', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      n: '-1'
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration='inf', n='inf')"
  );
});

it('builds show query with positive n as int', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  inspectableViewModel.queryKernel('pcollection', 'id');
  expect(
    inspectableViewModel.buildShowQuery({
      n: '5.5'
    } as IShowOptions)
  ).toBe(
    "ib.show(ie.current_env().inspector.get_val('id')," +
      'include_window_info=False, visualize_data=False, ' +
      "duration='inf', n=5)"
  );
});

it('extracts html from display data', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  const fakeDisplayData = {
    data: {
      'text/html': '<div>fake html</div>'
    }
  } as any;
  expect(inspectableViewModel.extractHtmlFromDisplayData(fakeDisplayData)).toBe(
    '<div>fake html</div>\n'
  );
});

it('extracts script from display data', () => {
  const inspectableViewModel = new InspectableViewModel(
    fakeSessionContext as any
  );
  const fakeDisplayData = {
    data: {
      'text/html': '<script>console.log(1)</script>',
      'application/javascript': 'console.log(2)'
    }
  } as any;
  expect(
    inspectableViewModel.extractScriptFromDisplayData(fakeDisplayData)
  ).toBe('console.log(2)\nconsole.log(1)\n');
});
