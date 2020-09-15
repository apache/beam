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

/**
 * Tests for KernelModel module.
 *
 * Non camelcase fields are nbformat fields used in notebooks. Lint is ignored
 * for them.
 */

import { KernelModel } from '../../kernel/KernelModel';

const fakeSessionContext = {
  session: {
    kernel: {
      requestExecute: function(): object {
        return {
          onIOPub: function(): void {
            // do nothing
          }
        };
      }
    }
  }
};

it(
  'creates new future with IOPub callbacks ' +
    'when executing new code in kernel',
  () => {
    const kernelModel = new KernelModel(fakeSessionContext as any);
    kernelModel.execute('new code');
    expect(kernelModel.future).not.toBe(null);
    expect(kernelModel.future.onIOPub).not.toBe(null);
  }
);

it('handles execute result from IOPub channel', () => {
  const kernelModel = new KernelModel(fakeSessionContext as any);
  kernelModel.execute('any code');
  kernelModel.future.onIOPub({
    header: {
      // eslint-disable-next-line @typescript-eslint/camelcase
      msg_type: 'execute_result'
    },
    content: {
      data: {
        'text/plain':
          '\'{"pipelineId": {"metadata": {"name": "pipeline", ' +
          '"inMemoryId": 1, "type": "pipeline"}, "pcolls": ' +
          '{"pcollId": {"name": "pcoll", "inMemoryId": 2, ' +
          '"type": "pcollection"}}}}\''
      },
      channel: 'iopub'
    }
  } as any);
  expect(kernelModel.executeResult).toEqual({
    pipelineId: {
      metadata: {
        name: 'pipeline',
        inMemoryId: 1,
        type: 'pipeline'
      },
      pcolls: {
        pcollId: {
          name: 'pcoll',
          inMemoryId: 2,
          type: 'pcollection'
        }
      }
    }
  });
});

it('handles display data from IOPub channel', () => {
  const kernelModel = new KernelModel(fakeSessionContext as any);
  kernelModel.execute('any code');
  const displayData = {
    // eslint-disable-next-line @typescript-eslint/camelcase
    output_type: 'display_data',
    data: {
      'text/html': '<div></div>',
      'application/javascript': 'console.log(1);'
    },
    metadata: {
      some: 'data'
    }
  };

  kernelModel.future.onIOPub({
    header: {
      // eslint-disable-next-line @typescript-eslint/camelcase
      msg_type: 'display_data'
    },
    content: displayData
  } as any);
  expect(kernelModel.displayData).toEqual([displayData]);
});

it('handles display update from IOPub channel', () => {
  const kernelModel = new KernelModel(fakeSessionContext as any);
  kernelModel.execute('any code');
  const updateDisplayData = {
    // eslint-disable-next-line @typescript-eslint/camelcase
    output_type: 'update_display_data',
    data: {
      'text/html': '<div id="abc"></div>',
      'application/javascript': 'console.log(2)'
    },
    metadata: {
      some: 'data'
    }
  };
  kernelModel.future.onIOPub({
    header: {
      // eslint-disable-next-line @typescript-eslint/camelcase
      msg_type: 'update_display_data'
    },
    content: updateDisplayData
  } as any);
  expect(kernelModel.displayUpdate).toEqual([updateDisplayData]);
});
