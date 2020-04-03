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

"""Unit tests for the apiclient module."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import sys
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import mock

from apache_beam.metrics.cells import DistributionData
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.dataflow.internal.clients import dataflow
from apache_beam.transforms import Create
from apache_beam.transforms import DataflowDistributionCounter
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms.environments import DockerEnvironment

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position

FAKE_PIPELINE_URL = "gs://invalid-bucket/anywhere"
_LOGGER = logging.getLogger(__name__)


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class UtilTest(unittest.TestCase):
  @unittest.skip("Enable once BEAM-1080 is fixed.")
  def test_create_application_client(self):
    pipeline_options = PipelineOptions()
    apiclient.DataflowApplicationClient(pipeline_options)

  def test_pipeline_url(self):
    pipeline_options = PipelineOptions([
        '--subnetwork',
        '/regions/MY/subnetworks/SUBNETWORK',
        '--temp_location',
        'gs://any-location/temp'
    ])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0', # any environment version
                                FAKE_PIPELINE_URL)

    recovered_options = None
    for additionalProperty in env.proto.sdkPipelineOptions.additionalProperties:
      if additionalProperty.key == 'options':
        recovered_options = additionalProperty.value
        break
    else:
      self.fail(
          'No pipeline options found in %s' % env.proto.sdkPipelineOptions)

    pipeline_url = None
    for property in recovered_options.object_value.properties:
      if property.key == 'pipelineUrl':
        pipeline_url = property.value
        break
    else:
      self.fail('No pipeline_url found in %s' % recovered_options)

    self.assertEqual(pipeline_url.string_value, FAKE_PIPELINE_URL)

  def test_set_network(self):
    pipeline_options = PipelineOptions([
        '--network',
        'anetworkname',
        '--temp_location',
        'gs://any-location/temp'
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.workerPools[0].network, 'anetworkname')

  def test_set_subnetwork(self):
    pipeline_options = PipelineOptions([
        '--subnetwork',
        '/regions/MY/subnetworks/SUBNETWORK',
        '--temp_location',
        'gs://any-location/temp'
    ])

    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].subnetwork,
        '/regions/MY/subnetworks/SUBNETWORK')

  def test_flexrs_blank(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])

    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.flexResourceSchedulingGoal, None)

  def test_flexrs_cost(self):
    pipeline_options = PipelineOptions([
        '--flexrs_goal',
        'COST_OPTIMIZED',
        '--temp_location',
        'gs://any-location/temp'
    ])

    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.flexResourceSchedulingGoal,
        (
            dataflow.Environment.FlexResourceSchedulingGoalValueValuesEnum.
            FLEXRS_COST_OPTIMIZED))

  def test_flexrs_speed(self):
    pipeline_options = PipelineOptions([
        '--flexrs_goal',
        'SPEED_OPTIMIZED',
        '--temp_location',
        'gs://any-location/temp'
    ])

    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.flexResourceSchedulingGoal,
        (
            dataflow.Environment.FlexResourceSchedulingGoalValueValuesEnum.
            FLEXRS_SPEED_OPTIMIZED))

  def test_sdk_harness_container_images_get_set(self):

    pipeline_options = PipelineOptions([
        '--experiments=beam_fn_api',
        '--experiments=use_unified_worker',
        '--temp_location',
        'gs://any-location/temp'
    ])

    pipeline = Pipeline(options=pipeline_options)
    pipeline | Create([1, 2, 3]) | ParDo(DoFn())  # pylint:disable=expression-not-assigned

    test_environment = DockerEnvironment(container_image='test_default_image')
    proto_pipeline, _ = pipeline.to_runner_api(
        return_context=True, default_environment=test_environment)

    # We have to manually add environments since Dataflow only sets
    # 'sdkHarnessContainerImages' when there are at least two environments.
    dummy_env = beam_runner_api_pb2.Environment(
        urn=common_urns.environments.DOCKER.urn,
        payload=(
            beam_runner_api_pb2.DockerPayload(
                container_image='dummy_image')).SerializeToString())
    proto_pipeline.components.environments['dummy_env_id'].CopyFrom(dummy_env)

    dummy_transform = beam_runner_api_pb2.PTransform(
        environment_id='dummy_env_id')
    proto_pipeline.components.transforms['dummy_transform_id'].CopyFrom(
        dummy_transform)

    env = apiclient.Environment([],  # packages
                                pipeline_options,
                                '2.0.0',  # any environment version
                                FAKE_PIPELINE_URL, proto_pipeline,
                                _sdk_image_overrides={
                                    '.*dummy.*':'dummy_image',
                                  '.*test.*': 'test_default_image'})
    worker_pool = env.proto.workerPools[0]

    # For the test, a third environment get added since actual default
    # container image for Dataflow is different from 'test_default_image'
    # we've provided above.
    self.assertEqual(3, len(worker_pool.sdkHarnessContainerImages))

    # Container image should be overridden by a Dataflow specific URL.
    self.assertTrue(
        str.startswith(
            (worker_pool.sdkHarnessContainerImages[0]).containerImage,
            'gcr.io/cloud-dataflow/v1beta3/python'))

  def test_sdk_harness_container_image_overrides(self):
    test_environment = DockerEnvironment(
        container_image='dummy_container_image')
    proto_pipeline, _ = Pipeline().to_runner_api(
      return_context=True, default_environment=test_environment)

    # Accessing non-public method for testing.
    apiclient.DataflowApplicationClient._apply_sdk_environment_overrides(
        proto_pipeline, {'.*dummy.*': 'new_dummy_container_image'})

    self.assertIsNotNone(1, len(proto_pipeline.components.environments))
    env = list(proto_pipeline.components.environments.values())[0]

    from apache_beam.utils import proto_utils
    docker_payload = proto_utils.parse_Bytes(
        env.payload, beam_runner_api_pb2.DockerPayload)

    # Container image should be overridden by a the given override.
    self.assertEqual(
        docker_payload.container_image, 'new_dummy_container_image')

  def test_invalid_default_job_name(self):
    # Regexp for job names in dataflow.
    regexp = '^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$'

    job_name = apiclient.Job._build_default_job_name('invalid.-_user_n*/ame')
    self.assertRegex(job_name, regexp)

    job_name = apiclient.Job._build_default_job_name(
        'invalid-extremely-long.username_that_shouldbeshortened_or_is_invalid')
    self.assertRegex(job_name, regexp)

  def test_default_job_name(self):
    job_name = apiclient.Job.default_job_name(None)
    regexp = 'beamapp-.*-[0-9]{10}-[0-9]{6}'
    self.assertRegex(job_name, regexp)

  def test_split_int(self):
    number = 12345
    split_number = apiclient.to_split_int(number)
    self.assertEqual((split_number.lowBits, split_number.highBits), (number, 0))
    shift_number = number << 32
    split_number = apiclient.to_split_int(shift_number)
    self.assertEqual((split_number.lowBits, split_number.highBits), (0, number))

  def test_translate_distribution_using_accumulator(self):
    metric_update = dataflow.CounterUpdate()
    accumulator = mock.Mock()
    accumulator.min = 1
    accumulator.max = 15
    accumulator.sum = 16
    accumulator.count = 2
    apiclient.translate_distribution(accumulator, metric_update)
    self.assertEqual(metric_update.distribution.min.lowBits, accumulator.min)
    self.assertEqual(metric_update.distribution.max.lowBits, accumulator.max)
    self.assertEqual(metric_update.distribution.sum.lowBits, accumulator.sum)
    self.assertEqual(
        metric_update.distribution.count.lowBits, accumulator.count)

  def test_translate_distribution_using_distribution_data(self):
    metric_update = dataflow.CounterUpdate()
    distribution_update = DistributionData(16, 2, 1, 15)
    apiclient.translate_distribution(distribution_update, metric_update)
    self.assertEqual(
        metric_update.distribution.min.lowBits, distribution_update.min)
    self.assertEqual(
        metric_update.distribution.max.lowBits, distribution_update.max)
    self.assertEqual(
        metric_update.distribution.sum.lowBits, distribution_update.sum)
    self.assertEqual(
        metric_update.distribution.count.lowBits, distribution_update.count)

  def test_translate_distribution_using_dataflow_distribution_counter(self):
    counter_update = DataflowDistributionCounter()
    counter_update.add_input(1)
    counter_update.add_input(3)
    metric_proto = dataflow.CounterUpdate()
    apiclient.translate_distribution(counter_update, metric_proto)
    histogram = mock.Mock(firstBucketOffset=None, bucketCounts=None)
    counter_update.translate_to_histogram(histogram)
    self.assertEqual(metric_proto.distribution.min.lowBits, counter_update.min)
    self.assertEqual(metric_proto.distribution.max.lowBits, counter_update.max)
    self.assertEqual(metric_proto.distribution.sum.lowBits, counter_update.sum)
    self.assertEqual(
        metric_proto.distribution.count.lowBits, counter_update.count)
    self.assertEqual(
        metric_proto.distribution.histogram.bucketCounts,
        histogram.bucketCounts)
    self.assertEqual(
        metric_proto.distribution.histogram.firstBucketOffset,
        histogram.firstBucketOffset)

  def test_translate_means(self):
    metric_update = dataflow.CounterUpdate()
    accumulator = mock.Mock()
    accumulator.sum = 16
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_int(
        accumulator, metric_update)
    self.assertEqual(metric_update.integerMean.sum.lowBits, accumulator.sum)
    self.assertEqual(metric_update.integerMean.count.lowBits, accumulator.count)

    accumulator.sum = 16.0
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_float(
        accumulator, metric_update)
    self.assertEqual(metric_update.floatingPointMean.sum, accumulator.sum)
    self.assertEqual(
        metric_update.floatingPointMean.count.lowBits, accumulator.count)

  def test_translate_means_using_distribution_accumulator(self):
    # This is the special case for MeanByteCount.
    # Which is reported over the FnAPI as a beam distribution,
    # and to the service as a MetricUpdate IntegerMean.
    metric_update = dataflow.CounterUpdate()
    accumulator = mock.Mock()
    accumulator.min = 7
    accumulator.max = 9
    accumulator.sum = 16
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_int(
        accumulator, metric_update)
    self.assertEqual(metric_update.integerMean.sum.lowBits, accumulator.sum)
    self.assertEqual(metric_update.integerMean.count.lowBits, accumulator.count)

    accumulator.sum = 16.0
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_float(
        accumulator, metric_update)
    self.assertEqual(metric_update.floatingPointMean.sum, accumulator.sum)
    self.assertEqual(
        metric_update.floatingPointMean.count.lowBits, accumulator.count)

  def test_default_ip_configuration(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0',
                                FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.workerPools[0].ipConfiguration, None)

  def test_public_ip_configuration(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--use_public_ips'])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0',
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].ipConfiguration,
        dataflow.WorkerPool.IpConfigurationValueValuesEnum.WORKER_IP_PUBLIC)

  def test_private_ip_configuration(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--no_use_public_ips'])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0',
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].ipConfiguration,
        dataflow.WorkerPool.IpConfigurationValueValuesEnum.WORKER_IP_PRIVATE)

  def test_number_of_worker_harness_threads(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--number_of_worker_harness_threads',
        '2'
    ])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0',
                                FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.workerPools[0].numThreadsPerWorker, 2)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_harness_override_default_in_released_sdks(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    override = ''.join([
        'runner_harness_container_image=',
        names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY,
        '/harness:2.2.0'
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertIn(override, env.proto.experiments)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_harness_override_absent_in_released_sdks_with_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--streaming',
        '--experiments=use_runner_v2'
    ])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    if env.proto.experiments:
      for experiment in env.proto.experiments:
        self.assertNotIn('runner_harness_container_image=', experiment)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_harness_override_custom_in_released_sdks(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--streaming',
        '--experiments=runner_harness_container_image=fake_image'
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        1,
        len([
            x for x in env.proto.experiments
            if x.startswith('runner_harness_container_image=')
        ]))
    self.assertIn(
        'runner_harness_container_image=fake_image', env.proto.experiments)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_harness_override_custom_in_released_sdks_with_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--streaming',
        '--experiments=runner_harness_container_image=fake_image',
        '--experiments=use_runner_v2',
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        1,
        len([
            x for x in env.proto.experiments
            if x.startswith('runner_harness_container_image=')
        ]))
    self.assertIn(
        'runner_harness_container_image=fake_image', env.proto.experiments)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0.rc1')
  def test_harness_override_uses_base_version_in_rc_releases(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    override = ''.join([
        'runner_harness_container_image=',
        names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY,
        '/harness:2.2.0'
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertIn(override, env.proto.experiments)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0.dev')
  def test_harness_override_absent_in_unreleased_sdk(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if env.proto.experiments:
      for experiment in env.proto.experiments:
        self.assertNotIn('runner_harness_container_image=', experiment)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0.dev')
  def test_pinned_worker_harness_image_tag_used_in_dev_sdk(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if sys.version_info[0] == 2:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python-fnapi:' +
              names.BEAM_FNAPI_CONTAINER_VERSION))
    elif sys.version_info[0:2] == (3, 5):
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python3-fnapi:' +
              names.BEAM_FNAPI_CONTAINER_VERSION))
    else:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
              '/python%d%d-fnapi:%s' % (
                  sys.version_info[0],
                  sys.version_info[1],
                  names.BEAM_FNAPI_CONTAINER_VERSION)))

    # batch, legacy pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if sys.version_info[0] == 2:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python:' +
              names.BEAM_CONTAINER_VERSION))
    elif sys.version_info[0:2] == (3, 5):
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python3:' +
              names.BEAM_CONTAINER_VERSION))
    else:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python%d%d:%s' % (
                  sys.version_info[0],
                  sys.version_info[1],
                  names.BEAM_CONTAINER_VERSION)))

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_worker_harness_image_tag_matches_released_sdk_version(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if sys.version_info[0] == 2:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python-fnapi:2.2.0'))
    elif sys.version_info[0:2] == (3, 5):
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python3-fnapi:2.2.0'))
    else:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
              '/python%d%d-fnapi:2.2.0' %
              (sys.version_info[0], sys.version_info[1])))

    # batch, legacy pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if sys.version_info[0] == 2:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python:2.2.0'))
    elif sys.version_info[0:2] == (3, 5):
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python3:2.2.0'))
    else:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python%d%d:2.2.0' %
              (sys.version_info[0], sys.version_info[1])))

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0.rc1')
  def test_worker_harness_image_tag_matches_base_sdk_version_of_an_rc(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if sys.version_info[0] == 2:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python-fnapi:2.2.0'))
    elif sys.version_info[0:2] == (3, 5):
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python3-fnapi:2.2.0'))
    else:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
              '/python%d%d-fnapi:2.2.0' %
              (sys.version_info[0], sys.version_info[1])))

    # batch, legacy pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    if sys.version_info[0] == 2:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python:2.2.0'))
    elif sys.version_info[0:2] == (3, 5):
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python3:2.2.0'))
    else:
      self.assertEqual(
          env.proto.workerPools[0].workerHarnessContainerImage,
          (
              names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY + '/python%d%d:2.2.0' %
              (sys.version_info[0], sys.version_info[1])))

  def test_worker_harness_override_takes_precedence_over_sdk_defaults(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--streaming',
        '--worker_harness_container_image=some:image'
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage, 'some:image')
    # batch, legacy pipeline.
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--worker_harness_container_image=some:image'
    ])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage, 'some:image')

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.Job.'
      'job_id_for_name',
      return_value='test_id')
  def test_transform_name_mapping(self, mock_job):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--update',
        '--transform_name_mapping',
        '{\"from\":\"to\"}'
    ])
    job = apiclient.Job(pipeline_options, FAKE_PIPELINE_URL)
    self.assertIsNotNone(job.proto.transformNameMapping)

  def test_labels(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp'
    ])
    job = apiclient.Job(pipeline_options, FAKE_PIPELINE_URL)
    self.assertIsNone(job.proto.labels)

    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--label',
        'key1=value1',
        '--label',
        'key2',
        '--label',
        'key3=value3',
        '--labels',
        'key4=value4',
        '--labels',
        'key5'
    ])
    job = apiclient.Job(pipeline_options, FAKE_PIPELINE_URL)
    self.assertEqual(5, len(job.proto.labels.additionalProperties))
    self.assertEqual('key1', job.proto.labels.additionalProperties[0].key)
    self.assertEqual('value1', job.proto.labels.additionalProperties[0].value)
    self.assertEqual('key2', job.proto.labels.additionalProperties[1].key)
    self.assertEqual('', job.proto.labels.additionalProperties[1].value)
    self.assertEqual('key3', job.proto.labels.additionalProperties[2].key)
    self.assertEqual('value3', job.proto.labels.additionalProperties[2].value)
    self.assertEqual('key4', job.proto.labels.additionalProperties[3].key)
    self.assertEqual('value4', job.proto.labels.additionalProperties[3].value)
    self.assertEqual('key5', job.proto.labels.additionalProperties[4].key)
    self.assertEqual('', job.proto.labels.additionalProperties[4].value)

  def test_experiment_use_multiple_sdk_containers(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--experiments',
        'beam_fn_api'
    ])
    environment = apiclient.Environment([],
                                        pipeline_options,
                                        1,
                                        FAKE_PIPELINE_URL)
    self.assertIn('use_multiple_sdk_containers', environment.proto.experiments)

    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--experiments',
        'beam_fn_api',
        '--experiments',
        'use_multiple_sdk_containers'
    ])
    environment = apiclient.Environment([],
                                        pipeline_options,
                                        1,
                                        FAKE_PIPELINE_URL)
    self.assertIn('use_multiple_sdk_containers', environment.proto.experiments)

    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--experiments',
        'beam_fn_api',
        '--experiments',
        'no_use_multiple_sdk_containers'
    ])
    environment = apiclient.Environment([],
                                        pipeline_options,
                                        1,
                                        FAKE_PIPELINE_URL)
    self.assertNotIn(
        'use_multiple_sdk_containers', environment.proto.experiments)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 5))
  def test_get_python_sdk_name(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--experiments',
        'beam_fn_api',
        '--experiments',
        'use_multiple_sdk_containers'
    ])
    environment = apiclient.Environment([],
                                        pipeline_options,
                                        1,
                                        FAKE_PIPELINE_URL)
    self.assertEqual(
        'Apache Beam Python 3.5 SDK', environment._get_python_sdk_name())

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (2, 7))
  def test_interpreter_version_check_passes_py27(self):
    pipeline_options = PipelineOptions([])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 5, 2))
  def test_interpreter_version_check_passes_py352(self):
    pipeline_options = PipelineOptions([])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 5, 6))
  def test_interpreter_version_check_passes_py356(self):
    pipeline_options = PipelineOptions([])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 9, 0))
  def test_interpreter_version_check_passes_with_experiment(self):
    pipeline_options = PipelineOptions(
        ["--experiment=ignore_py3_minor_version"])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 9, 0))
  def test_interpreter_version_check_fails_py39(self):
    pipeline_options = PipelineOptions([])
    self.assertRaises(
        Exception,
        apiclient._verify_interpreter_version_is_supported,
        pipeline_options)

  def test_use_unified_worker(self):
    pipeline_options = PipelineOptions([])
    self.assertFalse(apiclient._use_unified_worker(pipeline_options))

    pipeline_options = PipelineOptions(['--experiments=beam_fn_api'])
    self.assertFalse(apiclient._use_unified_worker(pipeline_options))

    pipeline_options = PipelineOptions(['--experiments=use_unified_worker'])
    self.assertFalse(apiclient._use_unified_worker(pipeline_options))

    pipeline_options = PipelineOptions(
        ['--experiments=use_unified_worker', '--experiments=beam_fn_api'])
    self.assertTrue(apiclient._use_unified_worker(pipeline_options))

    pipeline_options = PipelineOptions(
        ['--experiments=use_runner_v2', '--experiments=beam_fn_api'])
    self.assertTrue(apiclient._use_unified_worker(pipeline_options))

    pipeline_options = PipelineOptions([
        '--experiments=use_unified_worker',
        '--experiments=use_runner_v2',
        '--experiments=beam_fn_api'
    ])
    self.assertTrue(apiclient._use_unified_worker(pipeline_options))

  def test_get_response_encoding(self):
    encoding = apiclient.get_response_encoding()
    version_to_encoding = {3: 'utf8', 2: None}

    assert encoding == version_to_encoding[sys.version_info[0]]

  @unittest.skip("Enable once BEAM-1080 is fixed.")
  def test_graph_is_uploaded(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--experiments',
        'beam_fn_api',
        '--experiments',
        'upload_graph'
    ])
    job = apiclient.Job(pipeline_options, FAKE_PIPELINE_URL)
    client = apiclient.DataflowApplicationClient(pipeline_options)
    with mock.patch.object(client, 'stage_file', side_effect=None):
      with mock.patch.object(client, 'create_job_description',
                             side_effect=None):
        with mock.patch.object(client,
                               'submit_job_description',
                               side_effect=None):
          client.create_job(job)
          client.stage_file.assert_called_once_with(
              mock.ANY, "dataflow_graph.json", mock.ANY)
          client.create_job_description.assert_called_once()


if __name__ == '__main__':
  unittest.main()
