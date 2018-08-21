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

"""Unit tests for the DataflowRunner class."""

from __future__ import absolute_import

import json
import unittest
from builtins import object
from builtins import range
from datetime import datetime

import mock

import apache_beam as beam
import apache_beam.transforms as ptransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import PCollection
from apache_beam.runners import DataflowRunner
from apache_beam.runners import TestDataflowRunner
from apache_beam.runners import create_runner
from apache_beam.runners.dataflow import dataflow_runner
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
from apache_beam.runners.dataflow.dataflow_runner import DataflowRuntimeException
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import window
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.core import _GroupByKeyOnly
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.typehints import typehints

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class DataflowRunnerTest(unittest.TestCase):
  default_properties = [
      '--dataflow_endpoint=ignored',
      '--job_name=test-job',
      '--project=test-project',
      '--staging_location=ignored',
      '--temp_location=/dev/null',
      '--no_auth=True',
      '--dry_run=True']

  @mock.patch('time.sleep', return_value=None)
  def test_wait_until_finish(self, patched_time_sleep):
    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum

    class MockDataflowRunner(object):

      def __init__(self, states):
        self.dataflow_client = mock.MagicMock()
        self.job = mock.MagicMock()
        self.job.currentState = values_enum.JOB_STATE_UNKNOWN
        self._states = states
        self._next_state_index = 0

        def get_job_side_effect(*args, **kwargs):
          self.job.currentState = self._states[self._next_state_index]
          if self._next_state_index < (len(self._states) - 1):
            self._next_state_index += 1
          return mock.DEFAULT

        self.dataflow_client.get_job = mock.MagicMock(
            return_value=self.job, side_effect=get_job_side_effect)
        self.dataflow_client.list_messages = mock.MagicMock(
            return_value=([], None))

    with self.assertRaisesRegexp(
        DataflowRuntimeException, 'Dataflow pipeline failed. State: FAILED'):
      failed_runner = MockDataflowRunner([values_enum.JOB_STATE_FAILED])
      failed_result = DataflowPipelineResult(failed_runner.job, failed_runner)
      failed_result.wait_until_finish()

    succeeded_runner = MockDataflowRunner([values_enum.JOB_STATE_DONE])
    succeeded_result = DataflowPipelineResult(
        succeeded_runner.job, succeeded_runner)
    result = succeeded_result.wait_until_finish()
    self.assertEqual(result, PipelineState.DONE)

    # Time array has duplicate items, because some logging implementations also
    # call time.
    with mock.patch('time.time', mock.MagicMock(side_effect=[1, 1, 2, 2, 3])):
      duration_succeeded_runner = MockDataflowRunner(
          [values_enum.JOB_STATE_RUNNING, values_enum.JOB_STATE_DONE])
      duration_succeeded_result = DataflowPipelineResult(
          duration_succeeded_runner.job, duration_succeeded_runner)
      result = duration_succeeded_result.wait_until_finish(5000)
      self.assertEqual(result, PipelineState.DONE)

    with mock.patch('time.time', mock.MagicMock(side_effect=[1, 9, 9, 20, 20])):
      duration_timedout_runner = MockDataflowRunner(
          [values_enum.JOB_STATE_RUNNING])
      duration_timedout_result = DataflowPipelineResult(
          duration_timedout_runner.job, duration_timedout_runner)
      result = duration_timedout_result.wait_until_finish(5000)
      self.assertEqual(result, PipelineState.RUNNING)

    with mock.patch('time.time', mock.MagicMock(side_effect=[1, 1, 2, 2, 3])):
      with self.assertRaisesRegexp(
          DataflowRuntimeException,
          'Dataflow pipeline failed. State: CANCELLED'):
        duration_failed_runner = MockDataflowRunner(
            [values_enum.JOB_STATE_CANCELLED])
        duration_failed_result = DataflowPipelineResult(
            duration_failed_runner.job, duration_failed_runner)
        duration_failed_result.wait_until_finish(5000)

  @mock.patch('time.sleep', return_value=None)
  def test_cancel(self, patched_time_sleep):
    values_enum = dataflow_api.Job.CurrentStateValueValuesEnum

    class MockDataflowRunner(object):

      def __init__(self, state, cancel_result):
        self.dataflow_client = mock.MagicMock()
        self.job = mock.MagicMock()
        self.job.currentState = state

        self.dataflow_client.get_job = mock.MagicMock(return_value=self.job)
        self.dataflow_client.modify_job_state = mock.MagicMock(
            return_value=cancel_result)
        self.dataflow_client.list_messages = mock.MagicMock(
            return_value=([], None))

    with self.assertRaisesRegexp(
        DataflowRuntimeException, 'Failed to cancel job'):
      failed_runner = MockDataflowRunner(values_enum.JOB_STATE_RUNNING, False)
      failed_result = DataflowPipelineResult(failed_runner.job, failed_runner)
      failed_result.cancel()

    succeeded_runner = MockDataflowRunner(values_enum.JOB_STATE_RUNNING, True)
    succeeded_result = DataflowPipelineResult(
        succeeded_runner.job, succeeded_runner)
    succeeded_result.cancel()

    terminal_runner = MockDataflowRunner(values_enum.JOB_STATE_DONE, False)
    terminal_result = DataflowPipelineResult(
        terminal_runner.job, terminal_runner)
    terminal_result.cancel()

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DataflowRunner'),
                   DataflowRunner))
    self.assertTrue(
        isinstance(create_runner('TestDataflowRunner'),
                   TestDataflowRunner))

  def test_remote_runner_translation(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    (p | ptransform.Create([1, 2, 3])  # pylint: disable=expression-not-assigned
     | 'Do' >> ptransform.FlatMap(lambda x: [(x, x)])
     | ptransform.GroupByKey())
    p.run()

  def test_streaming_create_translation(self):
    remote_runner = DataflowRunner()
    self.default_properties.append("--streaming")
    p = Pipeline(remote_runner, PipelineOptions(self.default_properties))
    p | ptransform.Create([1])  # pylint: disable=expression-not-assigned
    p.run()
    job_dict = json.loads(str(remote_runner.job))
    self.assertEqual(len(job_dict[u'steps']), 2)

    self.assertEqual(job_dict[u'steps'][0][u'kind'], u'ParallelRead')
    self.assertEqual(
        job_dict[u'steps'][0][u'properties'][u'pubsub_subscription'],
        '_starting_signal/')
    self.assertEqual(job_dict[u'steps'][1][u'kind'], u'ParallelDo')

  def test_biqquery_read_streaming_fail(self):
    remote_runner = DataflowRunner()
    self.default_properties.append("--streaming")
    p = Pipeline(remote_runner, PipelineOptions(self.default_properties))
    _ = p | beam.io.Read(beam.io.BigQuerySource('some.table'))
    with self.assertRaisesRegexp(ValueError,
                                 r'source is not currently available'):
      p.run()

  def test_remote_runner_display_data(self):
    remote_runner = DataflowRunner()
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

      def process(self):
        pass

    now = datetime.now()
    # pylint: disable=expression-not-assigned
    (p | ptransform.Create([1, 2, 3, 4, 5])
     | 'Do' >> SpecialParDo(SpecialDoFn(), now))

    p.run()
    job_dict = json.loads(str(remote_runner.job))
    steps = [step
             for step in job_dict['steps']
             if len(step['properties'].get('display_data', [])) > 0]
    step = steps[1]
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
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions([
                     '--dataflow_endpoint=ignored',
                     '--job_name=test-job',
                     '--project=test-project',
                     '--staging_location=ignored',
                     '--temp_location=/dev/null',
                     '--no_auth=True'
                 ]))
    rows = p | beam.io.Read(beam.io.BigQuerySource('dataset.faketable'))
    with self.assertRaises(ValueError,
                           msg=('Coder for the GroupByKey operation'
                                '"GroupByKey" is not a key-value coder: '
                                'RowAsDictJsonCoder')):
      unused_invalid = rows | beam.GroupByKey()

  def test_group_by_key_input_visitor_with_valid_inputs(self):
    p = TestPipeline()
    pcoll1 = PCollection(p)
    pcoll2 = PCollection(p)
    pcoll3 = PCollection(p)
    for transform in [_GroupByKeyOnly(), beam.GroupByKey()]:
      pcoll1.element_type = None
      pcoll2.element_type = typehints.Any
      pcoll3.element_type = typehints.KV[typehints.Any, typehints.Any]
      for pcoll in [pcoll1, pcoll2, pcoll3]:
        applied = AppliedPTransform(None, transform, "label", [pcoll])
        applied.outputs[None] = PCollection(None)
        DataflowRunner.group_by_key_input_visitor().visit_transform(
            applied)
        self.assertEqual(pcoll.element_type,
                         typehints.KV[typehints.Any, typehints.Any])

  def test_group_by_key_input_visitor_with_invalid_inputs(self):
    p = TestPipeline()
    pcoll1 = PCollection(p)
    pcoll2 = PCollection(p)
    for transform in [_GroupByKeyOnly(), beam.GroupByKey()]:
      pcoll1.element_type = str
      pcoll2.element_type = typehints.Set
      err_msg = (
          r"Input to 'label' must be compatible with KV\[Any, Any\]. "
          "Found .*")
      for pcoll in [pcoll1, pcoll2]:
        with self.assertRaisesRegexp(ValueError, err_msg):
          DataflowRunner.group_by_key_input_visitor().visit_transform(
              AppliedPTransform(None, transform, "label", [pcoll]))

  def test_group_by_key_input_visitor_for_non_gbk_transforms(self):
    p = TestPipeline()
    pcoll = PCollection(p)
    for transform in [beam.Flatten(), beam.Map(lambda x: x)]:
      pcoll.element_type = typehints.Any
      DataflowRunner.group_by_key_input_visitor().visit_transform(
          AppliedPTransform(None, transform, "label", [pcoll]))
      self.assertEqual(pcoll.element_type, typehints.Any)

  def test_flatten_input_with_visitor_with_single_input(self):
    self._test_flatten_input_visitor(typehints.KV[int, int], typehints.Any, 1)

  def test_flatten_input_with_visitor_with_multiple_inputs(self):
    self._test_flatten_input_visitor(
        typehints.KV[int, typehints.Any], typehints.Any, 5)

  def _test_flatten_input_visitor(self, input_type, output_type, num_inputs):
    p = TestPipeline()
    inputs = []
    for _ in range(num_inputs):
      input_pcoll = PCollection(p)
      input_pcoll.element_type = input_type
      inputs.append(input_pcoll)
    output_pcoll = PCollection(p)
    output_pcoll.element_type = output_type

    flatten = AppliedPTransform(None, beam.Flatten(), "label", inputs)
    flatten.add_output(output_pcoll, None)
    DataflowRunner.flatten_input_visitor().visit_transform(flatten)
    for _ in range(num_inputs):
      self.assertEqual(inputs[0].element_type, output_type)

  def test_gbk_then_flatten_input_visitor(self):
    p = TestPipeline(
        runner=DataflowRunner(),
        options=PipelineOptions(self.default_properties))
    none_str_pc = p | 'c1' >> beam.Create({None: 'a'})
    none_int_pc = p | 'c2' >> beam.Create({None: 3})
    flat = (none_str_pc, none_int_pc) | beam.Flatten()
    _ = flat | beam.GroupByKey()

    # This may change if type inference changes, but we assert it here
    # to make sure the check below is not vacuous.
    self.assertNotIsInstance(flat.element_type, typehints.TupleConstraint)

    p.visit(DataflowRunner.group_by_key_input_visitor())
    p.visit(DataflowRunner.flatten_input_visitor())

    # The dataflow runner requires gbk input to be tuples *and* flatten
    # inputs to be equal to their outputs. Assert both hold.
    self.assertIsInstance(flat.element_type, typehints.TupleConstraint)
    self.assertEqual(flat.element_type, none_str_pc.element_type)
    self.assertEqual(flat.element_type, none_int_pc.element_type)

  def test_serialize_windowing_strategy(self):
    # This just tests the basic path; more complete tests
    # are in window_test.py.
    strategy = Windowing(window.FixedWindows(10))
    self.assertEqual(
        strategy,
        DataflowRunner.deserialize_windowing_strategy(
            DataflowRunner.serialize_windowing_strategy(strategy)))

  def test_side_input_visitor(self):
    p = TestPipeline()
    pc = p | beam.Create([])

    transform = beam.Map(
        lambda x, y, z: (x, y, z),
        beam.pvalue.AsSingleton(pc),
        beam.pvalue.AsMultiMap(pc))
    applied_transform = AppliedPTransform(None, transform, "label", [pc])
    DataflowRunner.side_input_visitor().visit_transform(applied_transform)
    self.assertEqual(2, len(applied_transform.side_inputs))
    for side_input in applied_transform.side_inputs:
      self.assertEqual(
          dataflow_runner._DataflowSideInput.DATAFLOW_MULTIMAP_URN,
          side_input._side_input_data().access_pattern)


if __name__ == '__main__':
  unittest.main()
