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

# pytype: skip-file

from __future__ import absolute_import

import json
import sys
import unittest
from builtins import object
from builtins import range
from datetime import datetime

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import mock

import apache_beam as beam
import apache_beam.transforms as ptransform
from apache_beam.coders import BytesCoder
from apache_beam.coders import coders
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import Pipeline
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.pvalue import PCollection
from apache_beam.runners import DataflowRunner
from apache_beam.runners import TestDataflowRunner
from apache_beam.runners import create_runner
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
from apache_beam.runners.dataflow.dataflow_runner import DataflowRuntimeException
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import environments
from apache_beam.transforms import window
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.typehints import typehints

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position


# SpecialParDo and SpecialDoFn are used in test_remote_runner_display_data.
# Due to BEAM-8482, these need to be declared outside of the test method.
# TODO: Should not subclass ParDo. Switch to PTransform as soon as
# composite transforms support display data.
class SpecialParDo(beam.ParDo):
  def __init__(self, fn, now):
    super(SpecialParDo, self).__init__(fn)
    self.fn = fn
    self.now = now

  # Make this a list to be accessible within closure
  def display_data(self):
    return {
        'asubcomponent': self.fn, 'a_class': SpecialParDo, 'a_time': self.now
    }


class SpecialDoFn(beam.DoFn):
  def display_data(self):
    return {'dofn_value': 42}

  def process(self):
    pass


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class DataflowRunnerTest(unittest.TestCase, ExtraAssertionsMixin):
  def setUp(self):
    self.default_properties = [
        '--dataflow_endpoint=ignored',
        '--job_name=test-job',
        '--project=test-project',
        '--staging_location=ignored',
        '--temp_location=/dev/null',
        '--no_auth',
        '--dry_run=True',
        '--sdk_location=container'
    ]

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

    with self.assertRaisesRegex(DataflowRuntimeException,
                                'Dataflow pipeline failed. State: FAILED'):
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
      with self.assertRaisesRegex(DataflowRuntimeException,
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

    with self.assertRaisesRegex(DataflowRuntimeException,
                                'Failed to cancel job'):
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
    self.assertTrue(isinstance(create_runner('DataflowRunner'), DataflowRunner))
    self.assertTrue(
        isinstance(create_runner('TestDataflowRunner'), TestDataflowRunner))

  def test_environment_override_translation(self):
    self.default_properties.append('--experiments=beam_fn_api')
    self.default_properties.append('--worker_harness_container_image=FOO')
    remote_runner = DataflowRunner()
    with Pipeline(remote_runner,
                  options=PipelineOptions(self.default_properties)) as p:
      (  # pylint: disable=expression-not-assigned
          p | ptransform.Create([1, 2, 3])
          | 'Do' >> ptransform.FlatMap(lambda x: [(x, x)])
          | ptransform.GroupByKey())
    self.assertEqual(
        list(remote_runner.proto_pipeline.components.environments.values()),
        [
            beam_runner_api_pb2.Environment(
                urn=common_urns.environments.DOCKER.urn,
                payload=beam_runner_api_pb2.DockerPayload(
                    container_image='FOO').SerializeToString(),
                capabilities=environments.python_sdk_capabilities())
        ])

  def test_remote_runner_translation(self):
    remote_runner = DataflowRunner()
    with Pipeline(remote_runner,
                  options=PipelineOptions(self.default_properties)) as p:

      (  # pylint: disable=expression-not-assigned
          p | ptransform.Create([1, 2, 3])
          | 'Do' >> ptransform.FlatMap(lambda x: [(x, x)])
          | ptransform.GroupByKey())

  def test_streaming_create_translation(self):
    remote_runner = DataflowRunner()
    self.default_properties.append("--streaming")
    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned
    job_dict = json.loads(str(remote_runner.job))
    self.assertEqual(len(job_dict[u'steps']), 3)

    self.assertEqual(job_dict[u'steps'][0][u'kind'], u'ParallelRead')
    self.assertEqual(
        job_dict[u'steps'][0][u'properties'][u'pubsub_subscription'],
        '_starting_signal/')
    self.assertEqual(job_dict[u'steps'][1][u'kind'], u'ParallelDo')
    self.assertEqual(job_dict[u'steps'][2][u'kind'], u'ParallelDo')

  def test_bigquery_read_streaming_fail(self):
    remote_runner = DataflowRunner()
    self.default_properties.append("--streaming")
    with self.assertRaisesRegex(ValueError,
                                r'source is not currently available'):
      with Pipeline(remote_runner,
                    PipelineOptions(self.default_properties)) as p:
        _ = p | beam.io.Read(beam.io.BigQuerySource('some.table'))

  def test_biqquery_read_fn_api_fail(self):
    remote_runner = DataflowRunner()
    for flag in ['beam_fn_api', 'use_unified_worker', 'use_runner_v2']:
      self.default_properties.append("--experiments=%s" % flag)
      with self.assertRaisesRegex(
          ValueError,
          'The Read.BigQuerySource.*is not supported.*'
          'apache_beam.io.gcp.bigquery.ReadFromBigQuery.*'):
        with Pipeline(remote_runner,
                      PipelineOptions(self.default_properties)) as p:
          _ = p | beam.io.Read(beam.io.BigQuerySource('some.table'))

  def test_remote_runner_display_data(self):
    remote_runner = DataflowRunner()
    p = Pipeline(
        remote_runner, options=PipelineOptions(self.default_properties))

    now = datetime.now()
    # pylint: disable=expression-not-assigned
    (
        p | ptransform.Create([1, 2, 3, 4, 5])
        | 'Do' >> SpecialParDo(SpecialDoFn(), now))

    # TODO(BEAM-366) Enable runner API on this test.
    p.run(test_runner_api=False)
    job_dict = json.loads(str(remote_runner.job))
    steps = [
        step for step in job_dict['steps']
        if len(step['properties'].get('display_data', [])) > 0
    ]
    step = steps[1]
    disp_data = step['properties']['display_data']
    nspace = SpecialParDo.__module__ + '.'
    expected_data = [{
        'type': 'TIMESTAMP',
        'namespace': nspace + 'SpecialParDo',
        'value': DisplayDataItem._format_value(now, 'TIMESTAMP'),
        'key': 'a_time'
    },
                     {
                         'type': 'STRING',
                         'namespace': nspace + 'SpecialParDo',
                         'value': nspace + 'SpecialParDo',
                         'key': 'a_class',
                         'shortValue': 'SpecialParDo'
                     },
                     {
                         'type': 'INTEGER',
                         'namespace': nspace + 'SpecialDoFn',
                         'value': 42,
                         'key': 'dofn_value'
                     }]
    self.assertUnhashableCountEqual(disp_data, expected_data)

  def test_no_group_by_key_directly_after_bigquery(self):
    remote_runner = DataflowRunner()
    with self.assertRaises(ValueError,
                           msg=('Coder for the GroupByKey operation'
                                '"GroupByKey" is not a key-value coder: '
                                'RowAsDictJsonCoder')):
      with beam.Pipeline(runner=remote_runner,
                         options=PipelineOptions(self.default_properties)) as p:
        # pylint: disable=expression-not-assigned
        p | beam.io.Read(
            beam.io.BigQuerySource('dataset.faketable')) | beam.GroupByKey()

  def test_group_by_key_input_visitor_with_valid_inputs(self):
    p = TestPipeline()
    pcoll1 = PCollection(p)
    pcoll2 = PCollection(p)
    pcoll3 = PCollection(p)

    pcoll1.element_type = None
    pcoll2.element_type = typehints.Any
    pcoll3.element_type = typehints.KV[typehints.Any, typehints.Any]
    for pcoll in [pcoll1, pcoll2, pcoll3]:
      applied = AppliedPTransform(None, beam.GroupByKey(), "label", [pcoll])
      applied.outputs[None] = PCollection(None)
      DataflowRunner.group_by_key_input_visitor().visit_transform(applied)
      self.assertEqual(
          pcoll.element_type, typehints.KV[typehints.Any, typehints.Any])

  def test_group_by_key_input_visitor_with_invalid_inputs(self):
    p = TestPipeline()
    pcoll1 = PCollection(p)
    pcoll2 = PCollection(p)

    pcoll1.element_type = str
    pcoll2.element_type = typehints.Set
    err_msg = (
        r"Input to 'label' must be compatible with KV\[Any, Any\]. "
        "Found .*")
    for pcoll in [pcoll1, pcoll2]:
      with self.assertRaisesRegex(ValueError, err_msg):
        DataflowRunner.group_by_key_input_visitor().visit_transform(
            AppliedPTransform(None, beam.GroupByKey(), "label", [pcoll]))

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
            DataflowRunner.serialize_windowing_strategy(strategy, None)))

  def test_side_input_visitor(self):
    p = TestPipeline()
    pc = p | beam.Create([])

    transform = beam.Map(
        lambda x,
        y,
        z: (x, y, z),
        beam.pvalue.AsSingleton(pc),
        beam.pvalue.AsMultiMap(pc))
    applied_transform = AppliedPTransform(None, transform, "label", [pc])
    DataflowRunner.side_input_visitor(
        use_fn_api=True).visit_transform(applied_transform)
    self.assertEqual(2, len(applied_transform.side_inputs))
    for side_input in applied_transform.side_inputs:
      self.assertEqual(
          common_urns.side_inputs.MULTIMAP.urn,
          side_input._side_input_data().access_pattern)

  def test_min_cpu_platform_flag_is_propagated_to_experiments(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--min_cpu_platform=Intel Haswell')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned
    self.assertIn(
        'min_cpu_platform=Intel Haswell',
        remote_runner.job.options.view_as(DebugOptions).experiments)

  def test_streaming_engine_flag_adds_windmill_experiments(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--streaming')
    self.default_properties.append('--enable_streaming_engine')
    self.default_properties.append('--experiment=some_other_experiment')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    experiments_for_job = (
        remote_runner.job.options.view_as(DebugOptions).experiments)
    self.assertIn('enable_streaming_engine', experiments_for_job)
    self.assertIn('enable_windmill_service', experiments_for_job)
    self.assertIn('some_other_experiment', experiments_for_job)

  def test_upload_graph_experiment(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--experiment=upload_graph')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    experiments_for_job = (
        remote_runner.job.options.view_as(DebugOptions).experiments)
    self.assertIn('upload_graph', experiments_for_job)

  def test_dataflow_worker_jar_flag_non_fnapi_noop(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--experiment=some_other_experiment')
    self.default_properties.append('--dataflow_worker_jar=test.jar')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    experiments_for_job = (
        remote_runner.job.options.view_as(DebugOptions).experiments)
    self.assertIn('some_other_experiment', experiments_for_job)
    self.assertNotIn('use_staged_dataflow_worker_jar', experiments_for_job)

  def test_dataflow_worker_jar_flag_adds_use_staged_worker_jar_experiment(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--experiment=beam_fn_api')
    self.default_properties.append('--dataflow_worker_jar=test.jar')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    experiments_for_job = (
        remote_runner.job.options.view_as(DebugOptions).experiments)
    self.assertIn('beam_fn_api', experiments_for_job)
    self.assertIn('use_staged_dataflow_worker_jar', experiments_for_job)

  def test_use_fastavro_experiment_is_added_on_py3_and_onwards(self):
    remote_runner = DataflowRunner()

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    self.assertEqual(
        sys.version_info[0] > 2,
        remote_runner.job.options.view_as(DebugOptions).lookup_experiment(
            'use_fastavro', False))

  def test_use_fastavro_experiment_is_not_added_when_use_avro_is_present(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--experiment=use_avro')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    debug_options = remote_runner.job.options.view_as(DebugOptions)

    self.assertFalse(debug_options.lookup_experiment('use_fastavro', False))

  def test_unsupported_fnapi_features(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--experiment=beam_fn_api')
    self.default_properties.append('--experiment=use_runner_v2')

    with self.assertRaisesRegex(RuntimeError, 'Unsupported merging'):
      with Pipeline(remote_runner,
                    options=PipelineOptions(self.default_properties)) as p:
        # pylint: disable=expression-not-assigned
        p | beam.Create([]) | beam.WindowInto(CustomMergingWindowFn())

    with self.assertRaisesRegex(RuntimeError, 'Unsupported window coder'):
      with Pipeline(remote_runner,
                    options=PipelineOptions(self.default_properties)) as p:
        # pylint: disable=expression-not-assigned
        p | beam.Create([]) | beam.WindowInto(CustomWindowTypeWindowFn())

  @mock.patch('os.environ.get', return_value=None)
  @mock.patch('apache_beam.utils.processes.check_output', return_value=b'')
  def test_get_default_gcp_region_no_default_returns_none(
      self, patched_environ, patched_processes):
    runner = DataflowRunner()
    result = runner.get_default_gcp_region()
    self.assertIsNone(result)

  @mock.patch('os.environ.get', return_value='some-region1')
  @mock.patch('apache_beam.utils.processes.check_output', return_value=b'')
  def test_get_default_gcp_region_from_environ(
      self, patched_environ, patched_processes):
    runner = DataflowRunner()
    result = runner.get_default_gcp_region()
    self.assertEqual(result, 'some-region1')

  @mock.patch('os.environ.get', return_value=None)
  @mock.patch(
      'apache_beam.utils.processes.check_output',
      return_value=b'some-region2\n')
  def test_get_default_gcp_region_from_gcloud(
      self, patched_environ, patched_processes):
    runner = DataflowRunner()
    result = runner.get_default_gcp_region()
    self.assertEqual(result, 'some-region2')

  @mock.patch('os.environ.get', return_value=None)
  @mock.patch(
      'apache_beam.utils.processes.check_output',
      side_effect=RuntimeError('Executable gcloud not found'))
  def test_get_default_gcp_region_ignores_error(
      self, patched_environ, patched_processes):
    runner = DataflowRunner()
    result = runner.get_default_gcp_region()
    self.assertIsNone(result)

  def test_combine_values_translation(self):
    runner = DataflowRunner()

    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      (  # pylint: disable=expression-not-assigned
          p
          | beam.Create([('a', [1, 2]), ('b', [3, 4])])
          | beam.CombineValues(lambda v, _: sum(v)))

    job_dict = json.loads(str(runner.job))
    self.assertIn(
        u'CombineValues', set(step[u'kind'] for step in job_dict[u'steps']))

  def _find_step(self, job, step_name):
    job_dict = json.loads(str(job))
    maybe_step = [
        s for s in job_dict[u'steps']
        if s[u'properties'][u'user_name'] == step_name
    ]
    self.assertTrue(maybe_step, 'Could not find step {}'.format(step_name))
    return maybe_step[0]

  def expect_correct_override(self, job, step_name, step_kind):
    """Expects that a transform was correctly overriden."""

    # If the typing information isn't being forwarded correctly, the component
    # encodings here will be incorrect.
    expected_output_info = [{
        "encoding": {
            "@type": "kind:windowed_value",
            "component_encodings": [{
                "@type": "kind:bytes"
            }, {
                "@type": "kind:global_window"
            }],
            "is_wrapper": True
        },
        "output_name": "out",
        "user_name": step_name + ".out"
    }]

    step = self._find_step(job, step_name)
    self.assertEqual(step[u'kind'], step_kind)

    # The display data here is forwarded because the replace transform is
    # subclassed from iobase.Read.
    self.assertGreater(len(step[u'properties']['display_data']), 0)
    self.assertEqual(step[u'properties']['output_info'], expected_output_info)

  def test_read_create_translation(self):
    runner = DataflowRunner()

    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      # pylint: disable=expression-not-assigned
      p | beam.Create([b'a', b'b', b'c'])

    self.expect_correct_override(runner.job, u'Create/Read', u'ParallelRead')

  def test_read_bigquery_translation(self):
    runner = DataflowRunner()

    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      # pylint: disable=expression-not-assigned
      p | beam.io.Read(beam.io.BigQuerySource('some.table', coder=BytesCoder()))

    self.expect_correct_override(runner.job, u'Read', u'ParallelRead')

  def test_read_pubsub_translation(self):
    runner = DataflowRunner()

    self.default_properties.append("--streaming")

    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      # pylint: disable=expression-not-assigned
      p | beam.io.ReadFromPubSub(topic='projects/project/topics/topic')

    self.expect_correct_override(
        runner.job, u'ReadFromPubSub/Read', u'ParallelRead')

  def test_gbk_translation(self):
    runner = DataflowRunner()
    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      # pylint: disable=expression-not-assigned
      p | beam.Create([(1, 2)]) | beam.GroupByKey()

    expected_output_info = [{
        "encoding": {
            "@type": "kind:windowed_value",
            "component_encodings": [{
                "@type": "kind:pair",
                "component_encodings": [{
                    "@type": "kind:varint"
                },
                {
                    "@type": "kind:stream",
                    "component_encodings": [{
                        "@type": "kind:varint"
                    }],
                    "is_stream_like": True
                }],
                "is_pair_like": True
            }, {
                "@type": "kind:global_window"
            }],
            "is_wrapper": True
        },
        "output_name": "out",
        "user_name": "GroupByKey.out"
    }]  # yapf: disable

    gbk_step = self._find_step(runner.job, u'GroupByKey')
    self.assertEqual(gbk_step[u'kind'], u'GroupByKey')
    self.assertEqual(
        gbk_step[u'properties']['output_info'], expected_output_info)

  def test_bigquerysource_translation(self):
    runner = DataflowRunner()
    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      _ = p | 'TestedRead' >> beam.io.Read(
          bigquery.BigQuerySource(table='myproject:mydset.mytable'))

    job_dict = json.loads(str(runner.job))
    read_step = [
        s for s in job_dict[u'steps']
        if s[u'properties'][u'user_name'].startswith('TestedRead')
    ][0]
    print(read_step)
    self.assertEqual('ParallelRead', read_step['kind'])
    source_name = [
        dd['shortValue'] for dd in read_step['properties']['display_data']
        if dd['key'] == 'source'
    ][0]
    self.assertEqual('_CustomBigQuerySource', source_name)

  def test_write_bigquery_translation(self):
    runner = DataflowRunner()

    self.default_properties.append('--experiments=use_legacy_bq_sink')
    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      # pylint: disable=expression-not-assigned
      p | beam.Create([1]) | beam.io.WriteToBigQuery('some.table')

    job_dict = json.loads(str(runner.job))

    expected_step = {
        "kind": "ParallelWrite",
        "name": "s2",
        "properties": {
            "create_disposition": "CREATE_IF_NEEDED",
            "dataset": "some",
            "display_data": [],
            "encoding": {
                "@type": "kind:windowed_value",
                "component_encodings": [{
                    "component_encodings": [],
                    "pipeline_proto_coder_id": "ref_Coder_RowAsDictJsonCoder_4"
                }, {
                    "@type": "kind:global_window"
                }],
                "is_wrapper": True
            },
            "format": "bigquery",
            "parallel_input": {
                "@type": "OutputReference",
                "output_name": "out",
                "step_name": "s1"
            },
            "table": "table",
            "user_name": "WriteToBigQuery/Write/NativeWrite",
            "write_disposition": "WRITE_APPEND"
        }
    }
    job_dict = json.loads(str(runner.job))
    write_step = [
        s for s in job_dict[u'steps']
        if s[u'properties'][u'user_name'].startswith('WriteToBigQuery')
    ][0]

    # Delete the @type field because in this case it is a hash which may change
    # depending on the pickling version.
    step_encoding = write_step[u'properties'][u'encoding']
    del step_encoding[u'component_encodings'][0][u'@type']
    self.assertEqual(expected_step, write_step)

  def test_write_bigquery_failed_translation(self):
    """Tests that WriteToBigQuery cannot have any consumers if replaced."""
    runner = DataflowRunner()

    self.default_properties.append('--experiments=use_legacy_bq_sink')
    with self.assertRaises(Exception):
      with beam.Pipeline(runner=runner,
                         options=PipelineOptions(self.default_properties)) as p:
        # pylint: disable=expression-not-assigned
        out = p | beam.Create([1]) | beam.io.WriteToBigQuery('some.table')
        out['destination_file_pairs'] | 'MyTransform' >> beam.Map(lambda _: _)


class CustomMergingWindowFn(window.WindowFn):
  def assign(self, assign_context):
    return []

  def merge(self, merge_context):
    pass

  def get_window_coder(self):
    return coders.IntervalWindowCoder()


class CustomWindowTypeWindowFn(window.NonMergingWindowFn):
  def assign(self, assign_context):
    return []

  def get_window_coder(self):
    return coders.BytesCoder()


if __name__ == '__main__':
  unittest.main()
