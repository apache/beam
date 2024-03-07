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

import unittest

import mock

import apache_beam as beam
import apache_beam.transforms as ptransform
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import Pipeline
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.pvalue import PCollection
from apache_beam.runners import DataflowRunner
from apache_beam.runners import TestDataflowRunner
from apache_beam.runners import common
from apache_beam.runners import create_runner
from apache_beam.runners.dataflow.dataflow_runner import DataflowPipelineResult
from apache_beam.runners.dataflow.dataflow_runner import DataflowRuntimeException
from apache_beam.runners.dataflow.dataflow_runner import _check_and_add_missing_options
from apache_beam.runners.dataflow.dataflow_runner import _check_and_add_missing_streaming_options
from apache_beam.runners.dataflow.internal.clients import dataflow as dataflow_api
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import combiners
from apache_beam.transforms import environments
from apache_beam.typehints import typehints

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position


# SpecialParDo and SpecialDoFn are used in test_remote_runner_display_data.
# Due to https://github.com/apache/beam/issues/19848, these need to be declared
# outside of the test method.
# TODO: Should not subclass ParDo. Switch to PTransform as soon as
# composite transforms support display data.
class SpecialParDo(beam.ParDo):
  def __init__(self, fn, now):
    super().__init__(fn)
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
        '--job_name=test-job',
        '--project=test-project',
        '--region=us-central1'
        '--staging_location=gs://beam/test',
        '--temp_location=gs://beam/tmp',
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

    # check the second call can still triggers the exception
    with self.assertRaisesRegex(DataflowRuntimeException,
                                'Dataflow pipeline failed. State: FAILED'):
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

  def test_environment_override_translation_legacy_worker_harness_image(self):
    self.default_properties.append('--experiments=beam_fn_api')
    self.default_properties.append('--worker_harness_container_image=LEGACY')
    remote_runner = DataflowRunner()
    options = PipelineOptions(self.default_properties)
    options.view_as(DebugOptions).add_experiment(
        'disable_logging_submission_environment')
    with Pipeline(remote_runner, options=options) as p:
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
                    container_image='LEGACY').SerializeToString(),
                capabilities=environments.python_sdk_docker_capabilities(),
                dependencies=environments.python_sdk_dependencies(
                    options=options))
        ])

  def test_environment_override_translation_sdk_container_image(self):
    self.default_properties.append('--experiments=beam_fn_api')
    self.default_properties.append('--sdk_container_image=FOO')
    remote_runner = DataflowRunner()
    options = PipelineOptions(self.default_properties)
    options.view_as(DebugOptions).add_experiment(
        'disable_logging_submission_environment')
    with Pipeline(remote_runner, options=options) as p:
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
                capabilities=environments.python_sdk_docker_capabilities(),
                dependencies=environments.python_sdk_dependencies(
                    options=options))
        ])

  def test_remote_runner_translation(self):
    remote_runner = DataflowRunner()
    with Pipeline(remote_runner,
                  options=PipelineOptions(self.default_properties)) as p:

      (  # pylint: disable=expression-not-assigned
          p | ptransform.Create([1, 2, 3])
          | 'Do' >> ptransform.FlatMap(lambda x: [(x, x)])
          | ptransform.GroupByKey())

  def test_group_by_key_input_visitor_with_valid_inputs(self):
    p = TestPipeline()
    pcoll1 = PCollection(p)
    pcoll2 = PCollection(p)
    pcoll3 = PCollection(p)

    pcoll1.element_type = None
    pcoll2.element_type = typehints.Any
    pcoll3.element_type = typehints.KV[typehints.Any, typehints.Any]
    for pcoll in [pcoll1, pcoll2, pcoll3]:
      applied = AppliedPTransform(
          None, beam.GroupByKey(), "label", {'pcoll': pcoll})
      applied.outputs[None] = PCollection(None)
      common.group_by_key_input_visitor().visit_transform(applied)
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
        common.group_by_key_input_visitor().visit_transform(
            AppliedPTransform(None, beam.GroupByKey(), "label", {'in': pcoll}))

  def test_group_by_key_input_visitor_for_non_gbk_transforms(self):
    p = TestPipeline()
    pcoll = PCollection(p)
    for transform in [beam.Flatten(), beam.Map(lambda x: x)]:
      pcoll.element_type = typehints.Any
      common.group_by_key_input_visitor().visit_transform(
          AppliedPTransform(None, transform, "label", {'in': pcoll}))
      self.assertEqual(pcoll.element_type, typehints.Any)

  def test_flatten_input_with_visitor_with_single_input(self):
    self._test_flatten_input_visitor(typehints.KV[int, int], typehints.Any, 1)

  def test_flatten_input_with_visitor_with_multiple_inputs(self):
    self._test_flatten_input_visitor(
        typehints.KV[int, typehints.Any], typehints.Any, 5)

  def _test_flatten_input_visitor(self, input_type, output_type, num_inputs):
    p = TestPipeline()
    inputs = {}
    for ix in range(num_inputs):
      input_pcoll = PCollection(p)
      input_pcoll.element_type = input_type
      inputs[str(ix)] = input_pcoll
    output_pcoll = PCollection(p)
    output_pcoll.element_type = output_type

    flatten = AppliedPTransform(None, beam.Flatten(), "label", inputs)
    flatten.add_output(output_pcoll, None)
    DataflowRunner.flatten_input_visitor().visit_transform(flatten)
    for _ in range(num_inputs):
      self.assertEqual(inputs['0'].element_type, output_type)

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

    p.visit(common.group_by_key_input_visitor())
    p.visit(DataflowRunner.flatten_input_visitor())

    # The dataflow runner requires gbk input to be tuples *and* flatten
    # inputs to be equal to their outputs. Assert both hold.
    self.assertIsInstance(flat.element_type, typehints.TupleConstraint)
    self.assertEqual(flat.element_type, none_str_pc.element_type)
    self.assertEqual(flat.element_type, none_int_pc.element_type)

  def test_side_input_visitor(self):
    p = TestPipeline()
    pc = p | beam.Create([])

    transform = beam.Map(
        lambda x,
        y,
        z: (x, y, z),
        beam.pvalue.AsSingleton(pc),
        beam.pvalue.AsMultiMap(pc))
    applied_transform = AppliedPTransform(None, transform, "label", {'pc': pc})
    DataflowRunner.side_input_visitor().visit_transform(applied_transform)
    self.assertEqual(2, len(applied_transform.side_inputs))
    self.assertEqual(
        common_urns.side_inputs.ITERABLE.urn,
        applied_transform.side_inputs[0]._side_input_data().access_pattern)
    self.assertEqual(
        common_urns.side_inputs.MULTIMAP.urn,
        applied_transform.side_inputs[1]._side_input_data().access_pattern)

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

  def test_use_fastavro_experiment_is_not_added_when_use_avro_is_present(self):
    remote_runner = DataflowRunner()
    self.default_properties.append('--experiment=use_avro')

    with Pipeline(remote_runner, PipelineOptions(self.default_properties)) as p:
      p | ptransform.Create([1])  # pylint: disable=expression-not-assigned

    debug_options = remote_runner.job.options.view_as(DebugOptions)

    self.assertFalse(debug_options.lookup_experiment('use_fastavro', False))

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

  @unittest.skip(
      'https://github.com/apache/beam/issues/18716: enable once '
      'CombineFnVisitor is fixed')
  def test_unsupported_combinefn_detection(self):
    class CombinerWithNonDefaultSetupTeardown(combiners.CountCombineFn):
      def setup(self, *args, **kwargs):
        pass

      def teardown(self, *args, **kwargs):
        pass

    runner = DataflowRunner()
    with self.assertRaisesRegex(ValueError,
                                'CombineFn.setup and CombineFn.'
                                'teardown are not supported'):
      with beam.Pipeline(runner=runner,
                         options=PipelineOptions(self.default_properties)) as p:
        _ = (
            p | beam.Create([1])
            | beam.CombineGlobally(CombinerWithNonDefaultSetupTeardown()))

    try:
      with beam.Pipeline(runner=runner,
                         options=PipelineOptions(self.default_properties)) as p:
        _ = (
            p | beam.Create([1])
            | beam.CombineGlobally(
                combiners.SingleInputTupleCombineFn(
                    combiners.CountCombineFn(), combiners.CountCombineFn())))
    except ValueError:
      self.fail('ValueError raised unexpectedly')

  def test_pack_combiners(self):
    class PackableCombines(beam.PTransform):
      def annotations(self):
        return {python_urns.APPLY_COMBINER_PACKING: b''}

      def expand(self, pcoll):
        _ = pcoll | 'PackableMin' >> beam.CombineGlobally(min)
        _ = pcoll | 'PackableMax' >> beam.CombineGlobally(max)

    runner = DataflowRunner()
    with beam.Pipeline(runner=runner,
                       options=PipelineOptions(self.default_properties)) as p:
      _ = p | beam.Create([10, 20, 30]) | PackableCombines()

    unpacked_minimum_step_name = (
        'PackableCombines/PackableMin/CombinePerKey/Combine')
    unpacked_maximum_step_name = (
        'PackableCombines/PackableMax/CombinePerKey/Combine')
    packed_step_name = (
        'PackableCombines/Packed[PackableMin_CombinePerKey, '
        'PackableMax_CombinePerKey]/Pack')
    transform_names = set(
        transform.unique_name
        for transform in runner.proto_pipeline.components.transforms.values())
    self.assertNotIn(unpacked_minimum_step_name, transform_names)
    self.assertNotIn(unpacked_maximum_step_name, transform_names)
    self.assertIn(packed_step_name, transform_names)

  def test_batch_is_runner_v2(self):
    options = PipelineOptions(['--sdk_location=container'])
    _check_and_add_missing_options(options)
    for expected in ['beam_fn_api',
                     'use_unified_worker',
                     'use_runner_v2',
                     'use_portable_job_submission']:
      self.assertTrue(
          options.view_as(DebugOptions).lookup_experiment(expected, False),
          expected)

  def test_streaming_is_runner_v2(self):
    options = PipelineOptions(['--sdk_location=container', '--streaming'])
    _check_and_add_missing_options(options)
    _check_and_add_missing_streaming_options(options)
    for expected in ['beam_fn_api',
                     'use_unified_worker',
                     'use_runner_v2',
                     'use_portable_job_submission',
                     'enable_windmill_service',
                     'enable_streaming_engine']:
      self.assertTrue(
          options.view_as(DebugOptions).lookup_experiment(expected, False),
          expected)

  def test_dataflow_service_options_enable_prime_sets_runner_v2(self):
    options = PipelineOptions([
        '--sdk_location=container',
        '--streaming',
        '--dataflow_service_options=enable_prime'
    ])
    _check_and_add_missing_options(options)
    for expected in ['beam_fn_api',
                     'use_unified_worker',
                     'use_runner_v2',
                     'use_portable_job_submission']:
      self.assertTrue(
          options.view_as(DebugOptions).lookup_experiment(expected, False),
          expected)

    options = PipelineOptions([
        '--sdk_location=container',
        '--streaming',
        '--dataflow_service_options=enable_prime'
    ])
    _check_and_add_missing_options(options)
    _check_and_add_missing_streaming_options(options)
    for expected in ['beam_fn_api',
                     'use_unified_worker',
                     'use_runner_v2',
                     'use_portable_job_submission',
                     'enable_windmill_service',
                     'enable_streaming_engine']:
      self.assertTrue(
          options.view_as(DebugOptions).lookup_experiment(expected, False),
          expected)

  @unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
  @mock.patch(
      'apache_beam.options.pipeline_options.GoogleCloudOptions.validate',
      lambda *args: [])
  def test_auto_streaming_with_unbounded(self):
    options = PipelineOptions([
        '--sdk_location=container',
        '--runner=DataflowRunner',
        '--dry_run=True',
        '--temp_location=gs://bucket',
        '--project=project',
        '--region=region'
    ])
    with beam.Pipeline(options=options) as p:
      _ = p | beam.io.ReadFromPubSub('projects/some-project/topics/some-topic')
    self.assertEqual(
        p.result.job.proto.type,
        apiclient.dataflow.Job.TypeValueValuesEnum.JOB_TYPE_STREAMING)

  @unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
  @mock.patch(
      'apache_beam.options.pipeline_options.GoogleCloudOptions.validate',
      lambda *args: [])
  def test_auto_streaming_no_unbounded(self):
    options = PipelineOptions([
        '--sdk_location=container',
        '--runner=DataflowRunner',
        '--dry_run=True',
        '--temp_location=gs://bucket',
        '--project=project',
        '--region=region'
    ])
    with beam.Pipeline(options=options) as p:
      _ = p | beam.Create([1, 2, 3])
    self.assertEqual(
        p.result.job.proto.type,
        apiclient.dataflow.Job.TypeValueValuesEnum.JOB_TYPE_BATCH)

  @unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
  @mock.patch(
      'apache_beam.options.pipeline_options.GoogleCloudOptions.validate',
      lambda *args: [])
  def test_explicit_streaming_no_unbounded(self):
    options = PipelineOptions([
        '--streaming',
        '--sdk_location=container',
        '--runner=DataflowRunner',
        '--dry_run=True',
        '--temp_location=gs://bucket',
        '--project=project',
        '--region=region'
    ])
    with beam.Pipeline(options=options) as p:
      _ = p | beam.Create([1, 2, 3])
    self.assertEqual(
        p.result.job.proto.type,
        apiclient.dataflow.Job.TypeValueValuesEnum.JOB_TYPE_STREAMING)


if __name__ == '__main__':
  unittest.main()
