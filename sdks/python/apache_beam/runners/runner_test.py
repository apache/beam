#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit tests for the PipelineRunner and DirectPipelineRunner classes.

Note that PipelineRunner and DirectPipelineRunner functionality is tested in all
the other unit tests. In this file we choose to test only aspects related to
caching and clearing values that are not tested elsewhere.
"""

from datetime import datetime
import json
import unittest

import apache_beam as beam

from apache_beam.internal import apiclient
from apache_beam.pipeline import Pipeline
from apache_beam.runners import create_runner
from apache_beam.runners import DataflowPipelineRunner
from apache_beam.runners import DirectPipelineRunner
import apache_beam.transforms as ptransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.utils.options import PipelineOptions


class RunnerTest(unittest.TestCase):
  default_properties = [
      '--dataflow_endpoint=ignored',
      '--job_name=test-job',
      '--project=test-project',
      '--staging_location=ignored',
      '--temp_location=/dev/null',
      '--no_auth=True']

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DirectPipelineRunner'), DirectPipelineRunner))
    self.assertTrue(
        isinstance(create_runner('DataflowPipelineRunner'),
                   DataflowPipelineRunner))
    self.assertTrue(
        isinstance(create_runner('BlockingDataflowPipelineRunner'),
                   DataflowPipelineRunner))
    self.assertRaises(ValueError, create_runner, 'xyz')

  def test_remote_runner_translation(self):
    remote_runner = DataflowPipelineRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    (p | 'create' >> ptransform.Create([1, 2, 3])  # pylint: disable=expression-not-assigned
     | 'do' >> ptransform.FlatMap(lambda x: [(x, x)])
     | 'gbk' >> ptransform.GroupByKey())
    remote_runner.job = apiclient.Job(p.options)
    super(DataflowPipelineRunner, remote_runner).run(p)

  def test_remote_runner_display_data(self):
    remote_runner = DataflowPipelineRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    # TODO: Should not subclass ParDo. Switch to PTransform as soon as
    # composite transforms support display data.
    class SpecialParDo(beam.ParDo):
      def __init__(self, fn, now):
        super(SpecialParDo, self).__init__(fn)
        self.fn = fn
        self.now = now

      # Make this a list to be accessible within closure
      def display_data(self):
        return {'asubcomponent': self.fn,
                'a_class': SpecialParDo,
                'a_time': self.now}

    class SpecialDoFn(beam.DoFn):
      def display_data(self):
        return {'dofn_value': 42}

      def process(self, context):
        pass

    now = datetime.now()
    # pylint: disable=expression-not-assigned
    (p | 'create' >> ptransform.Create([1, 2, 3, 4, 5])
     | 'do' >> SpecialParDo(SpecialDoFn(), now))

    remote_runner.job = apiclient.Job(p.options)
    super(DataflowPipelineRunner, remote_runner).run(p)
    job_dict = json.loads(str(remote_runner.job))
    steps = [step
             for step in job_dict['steps']
             if len(step['properties'].get('display_data', [])) > 0]
    step = steps[0]
    disp_data = step['properties']['display_data']
    disp_data = sorted(disp_data, key=lambda x: x['namespace']+x['key'])
    nspace = SpecialParDo.__module__+ '.'
    expected_data = [{'type': 'TIMESTAMP', 'namespace': nspace+'SpecialParDo',
                      'value': DisplayDataItem._format_value(now, 'TIMESTAMP'),
                      'key': 'a_time'},
                     {'type': 'STRING', 'namespace': nspace+'SpecialParDo',
                      'value': nspace+'SpecialParDo', 'key': 'a_class',
                      'shortValue': 'SpecialParDo'},
                     {'type': 'INTEGER', 'namespace': nspace+'SpecialDoFn',
                      'value': 42, 'key': 'dofn_value'}]
    expected_data = sorted(expected_data, key=lambda x: x['namespace']+x['key'])
    self.assertEqual(len(disp_data), 3)
    self.assertEqual(disp_data, expected_data)

  def test_no_group_by_key_directly_after_bigquery(self):
    remote_runner = DataflowPipelineRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions([
                     '--dataflow_endpoint=ignored',
                     '--job_name=test-job',
                     '--project=test-project',
                     '--staging_location=ignored',
                     '--temp_location=/dev/null',
                     '--no_auth=True'
                 ]))
    rows = p | beam.io.Read('read',
                            beam.io.BigQuerySource('dataset.faketable'))
    with self.assertRaises(ValueError,
                           msg=('Coder for the GroupByKey operation'
                                '"GroupByKey" is not a key-value coder: '
                                'RowAsDictJsonCoder')):
      unused_invalid = rows | beam.GroupByKey()


if __name__ == '__main__':
  unittest.main()
