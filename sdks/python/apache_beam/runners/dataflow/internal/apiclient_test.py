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

import itertools
import json
import logging
import os
import sys
import unittest

import mock

from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics.cells import DistributionData
from apache_beam.options.pipeline_options import GoogleCloudOptions
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
    env = apiclient.Environment(
        [],
        pipeline_options,
        '2.0.0',  # any environment version
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
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.workerPools[0].network, 'anetworkname')

  def test_set_subnetwork(self):
    pipeline_options = PipelineOptions([
        '--subnetwork',
        '/regions/MY/subnetworks/SUBNETWORK',
        '--temp_location',
        'gs://any-location/temp'
    ])

    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].subnetwork,
        '/regions/MY/subnetworks/SUBNETWORK')

  def test_flexrs_blank(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])

    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.flexResourceSchedulingGoal, None)

  def test_flexrs_cost(self):
    pipeline_options = PipelineOptions([
        '--flexrs_goal',
        'COST_OPTIMIZED',
        '--temp_location',
        'gs://any-location/temp'
    ])

    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
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

    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.flexResourceSchedulingGoal,
        (
            dataflow.Environment.FlexResourceSchedulingGoalValueValuesEnum.
            FLEXRS_SPEED_OPTIMIZED))

  def _verify_sdk_harness_container_images_get_set(self, pipeline_options):
    pipeline = Pipeline(options=pipeline_options)
    pipeline | Create([1, 2, 3]) | ParDo(DoFn())  # pylint:disable=expression-not-assigned

    test_environment = DockerEnvironment(container_image='test_default_image')
    proto_pipeline, _ = pipeline.to_runner_api(
        return_context=True, default_environment=test_environment)

    dummy_env = beam_runner_api_pb2.Environment(
        urn=common_urns.environments.DOCKER.urn,
        payload=(
            beam_runner_api_pb2.DockerPayload(
                container_image='dummy_image')).SerializeToString())
    dummy_env.capabilities.append(
        common_urns.protocols.MULTI_CORE_BUNDLE_PROCESSING.urn)
    proto_pipeline.components.environments['dummy_env_id'].CopyFrom(dummy_env)

    dummy_transform = beam_runner_api_pb2.PTransform(
        environment_id='dummy_env_id')
    proto_pipeline.components.transforms['dummy_transform_id'].CopyFrom(
        dummy_transform)

    env = apiclient.Environment(
        [],  # packages
        pipeline_options,
        '2.0.0',  # any environment version
        FAKE_PIPELINE_URL,
        proto_pipeline)
    worker_pool = env.proto.workerPools[0]

    self.assertEqual(2, len(worker_pool.sdkHarnessContainerImages))
    # Only one of the environments is missing MULTI_CORE_BUNDLE_PROCESSING.
    self.assertEqual(
        1,
        sum(
            c.useSingleCorePerContainer
            for c in worker_pool.sdkHarnessContainerImages))

    env_and_image = [(item.environmentId, item.containerImage)
                     for item in worker_pool.sdkHarnessContainerImages]
    self.assertIn(('dummy_env_id', 'dummy_image'), env_and_image)
    self.assertIn((mock.ANY, 'test_default_image'), env_and_image)

  def test_sdk_harness_container_images_get_set_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--experiments=use_runner_v2',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_sdk_harness_container_images_get_set(pipeline_options)

  def test_sdk_harness_container_images_get_set_prime(self):
    pipeline_options = PipelineOptions([
        '--dataflow_service_options=enable_prime',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_sdk_harness_container_images_get_set(pipeline_options)

  def _verify_sdk_harness_container_image_overrides(self, pipeline_options):
    test_environment = DockerEnvironment(
        container_image='dummy_container_image')
    proto_pipeline, _ = Pipeline().to_runner_api(
      return_context=True, default_environment=test_environment)

    # Accessing non-public method for testing.
    apiclient.DataflowApplicationClient._apply_sdk_environment_overrides(
        proto_pipeline,
        {
            '.*dummy.*': 'new_dummy_container_image',
            '.*notfound.*': 'new_dummy_container_image_2'
        },
        pipeline_options)

    self.assertIsNotNone(1, len(proto_pipeline.components.environments))
    env = list(proto_pipeline.components.environments.values())[0]

    from apache_beam.utils import proto_utils
    docker_payload = proto_utils.parse_Bytes(
        env.payload, beam_runner_api_pb2.DockerPayload)

    # Container image should be overridden by the given override.
    self.assertEqual(
        docker_payload.container_image, 'new_dummy_container_image')

  def test_sdk_harness_container_image_overrides_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--experiments=use_runner_v2',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_sdk_harness_container_image_overrides(pipeline_options)

  def test_sdk_harness_container_image_overrides_prime(self):
    pipeline_options = PipelineOptions([
        '--dataflow_service_options=enable_prime',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_sdk_harness_container_image_overrides(pipeline_options)

  def _verify_dataflow_container_image_override(self, pipeline_options):
    pipeline = Pipeline(options=pipeline_options)
    pipeline | Create([1, 2, 3]) | ParDo(DoFn())  # pylint:disable=expression-not-assigned

    dummy_env = DockerEnvironment(
        container_image='apache/beam_dummy_name:dummy_tag')
    proto_pipeline, _ = pipeline.to_runner_api(
        return_context=True, default_environment=dummy_env)

    # Accessing non-public method for testing.
    apiclient.DataflowApplicationClient._apply_sdk_environment_overrides(
        proto_pipeline, {}, pipeline_options)

    from apache_beam.utils import proto_utils
    found_override = False
    for env in proto_pipeline.components.environments.values():
      docker_payload = proto_utils.parse_Bytes(
          env.payload, beam_runner_api_pb2.DockerPayload)
      if docker_payload.container_image.startswith(
          names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY):
        found_override = True

    self.assertTrue(found_override)

  def test_dataflow_container_image_override_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--experiments=use_runner_v2',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_dataflow_container_image_override(pipeline_options)

  def test_dataflow_container_image_override_prime(self):
    pipeline_options = PipelineOptions([
        '--dataflow_service_options=enable_prime',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_dataflow_container_image_override(pipeline_options)

  def _verify_dataflow_container_image_override_rc(self, pipeline_options):
    pipeline = Pipeline(options=pipeline_options)
    pipeline | Create([1, 2, 3]) | ParDo(DoFn())  # pylint:disable=expression-not-assigned

    dummy_env = DockerEnvironment(
        container_image='apache/beam_dummy_name:2.00.0RC10')
    proto_pipeline, _ = pipeline.to_runner_api(
        return_context=True, default_environment=dummy_env)

    # Accessing non-public method for testing.
    apiclient.DataflowApplicationClient._apply_sdk_environment_overrides(
        proto_pipeline, {}, pipeline_options)

    from apache_beam.utils import proto_utils
    found_override = False
    trimed_rc = True
    for env in proto_pipeline.components.environments.values():
      docker_payload = proto_utils.parse_Bytes(
          env.payload, beam_runner_api_pb2.DockerPayload)
      if docker_payload.container_image.startswith(
          names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY):
        found_override = True
        if docker_payload.container_image.split(':')[-1] != '2.00.0':
          trimed_rc = False

    self.assertTrue(found_override)
    self.assertTrue(trimed_rc)

  def test_dataflow_container_image_override_rc(self):
    pipeline_options = PipelineOptions([
        '--experiments=use_runner_v2',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_dataflow_container_image_override_rc(pipeline_options)

  def _verify_non_apache_container_not_overridden(self, pipeline_options):
    pipeline = Pipeline(options=pipeline_options)
    pipeline | Create([1, 2, 3]) | ParDo(DoFn())  # pylint:disable=expression-not-assigned

    dummy_env = DockerEnvironment(
        container_image='other_org/dummy_name:dummy_tag')
    proto_pipeline, _ = pipeline.to_runner_api(
        return_context=True, default_environment=dummy_env)

    # Accessing non-public method for testing.
    apiclient.DataflowApplicationClient._apply_sdk_environment_overrides(
        proto_pipeline, {}, pipeline_options)

    self.assertIsNotNone(2, len(proto_pipeline.components.environments))

    from apache_beam.utils import proto_utils
    found_override = False
    for env in proto_pipeline.components.environments.values():
      docker_payload = proto_utils.parse_Bytes(
          env.payload, beam_runner_api_pb2.DockerPayload)
      if docker_payload.container_image.startswith(
          names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY):
        found_override = True

    self.assertFalse(found_override)

  def test_non_apache_container_not_overridden_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--experiments=use_runner_v2',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_non_apache_container_not_overridden(pipeline_options)

  def test_non_apache_container_not_overridden_prime(self):
    pipeline_options = PipelineOptions([
        '--dataflow_service_options=enable_prime',
        '--temp_location',
        'gs://any-location/temp'
    ])

    self._verify_non_apache_container_not_overridden(pipeline_options)

  def _verify_pipeline_sdk_not_overridden(self, pipeline_options):
    pipeline = Pipeline(options=pipeline_options)
    pipeline | Create([1, 2, 3]) | ParDo(DoFn())  # pylint:disable=expression-not-assigned

    proto_pipeline, _ = pipeline.to_runner_api(return_context=True)

    dummy_env = DockerEnvironment(
        container_image='dummy_prefix/dummy_name:dummy_tag')
    proto_pipeline, _ = pipeline.to_runner_api(
        return_context=True, default_environment=dummy_env)

    # Accessing non-public method for testing.
    apiclient.DataflowApplicationClient._apply_sdk_environment_overrides(
        proto_pipeline, {}, pipeline_options)

    self.assertIsNotNone(2, len(proto_pipeline.components.environments))

    from apache_beam.utils import proto_utils
    found_override = False
    for env in proto_pipeline.components.environments.values():
      docker_payload = proto_utils.parse_Bytes(
          env.payload, beam_runner_api_pb2.DockerPayload)
      if docker_payload.container_image.startswith(
          names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY):
        found_override = True

    self.assertFalse(found_override)

  def test_pipeline_sdk_not_overridden_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--experiments=use_runner_v2',
        '--temp_location',
        'gs://any-location/temp',
        '--sdk_container_image=dummy_prefix/dummy_name:dummy_tag'
    ])

    self._verify_pipeline_sdk_not_overridden(pipeline_options)

  def test_pipeline_sdk_not_overridden_prime(self):
    pipeline_options = PipelineOptions([
        '--dataflow_service_options=enable_prime',
        '--temp_location',
        'gs://any-location/temp',
        '--sdk_container_image=dummy_prefix/dummy_name:dummy_tag'
    ])

    self._verify_pipeline_sdk_not_overridden(pipeline_options)

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
    regexp = 'beamapp-.*-[0-9]{10}-[0-9]{6}-[a-z0-9]{8}$'
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
  def test_harness_override_absent_with_runner_v2(self):
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
  def test_custom_harness_override_present_with_runner_v2(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--streaming',
        '--experiments=runner_harness_container_image=fake_image',
        '--experiments=use_runner_v2',
    ])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
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
      '2.2.0.dev')
  def test_pinned_worker_harness_image_tag_used_in_dev_sdk(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (
            names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
            '/beam_python%d.%d_sdk:%s' % (
                sys.version_info[0],
                sys.version_info[1],
                names.BEAM_DEV_SDK_CONTAINER_TAG)))

    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (
            names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
            '/beam_python%d.%d_sdk:%s' % (
                sys.version_info[0],
                sys.version_info[1],
                names.BEAM_DEV_SDK_CONTAINER_TAG)))

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_worker_harness_image_tag_matches_released_sdk_version(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (
            names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
            '/beam_python%d.%d_sdk:2.2.0' %
            (sys.version_info[0], sys.version_info[1])))

    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (
            names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
            '/beam_python%d.%d_sdk:2.2.0' %
            (sys.version_info[0], sys.version_info[1])))

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0.rc1')
  def test_worker_harness_image_tag_matches_base_sdk_version_of_an_rc(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (
            names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
            '/beam_python%d.%d_sdk:2.2.0' %
            (sys.version_info[0], sys.version_info[1])))

    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (
            names.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
            '/beam_python%d.%d_sdk:2.2.0' %
            (sys.version_info[0], sys.version_info[1])))

  def test_worker_harness_override_takes_precedence_over_sdk_defaults(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--streaming',
        '--sdk_container_image=some:image'
    ])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage, 'some:image')
    # batch, legacy pipeline.
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://any-location/temp',
        '--sdk_container_image=some:image'
    ])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
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
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    self.assertIsNotNone(job.proto.transformNameMapping)

  def test_created_from_snapshot_id(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--create_from_snapshot',
        'test_snapshot_id'
    ])
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    self.assertEqual('test_snapshot_id', job.proto.createdFromSnapshotId)

  def test_labels(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp'
    ])
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
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
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
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

    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--labels',
        '{ "name": "wrench", "mass": "1_3kg", "count": "3" }'
    ])
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    self.assertEqual(3, len(job.proto.labels.additionalProperties))
    self.assertEqual('name', job.proto.labels.additionalProperties[0].key)
    self.assertEqual('wrench', job.proto.labels.additionalProperties[0].value)
    self.assertEqual('mass', job.proto.labels.additionalProperties[1].key)
    self.assertEqual('1_3kg', job.proto.labels.additionalProperties[1].value)
    self.assertEqual('count', job.proto.labels.additionalProperties[2].key)
    self.assertEqual('3', job.proto.labels.additionalProperties[2].value)

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
      (3, 9))
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
        'Apache Beam Python 3.9 SDK', environment._get_python_sdk_name())

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (2, 7))
  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_interpreter_version_check_fails_py27(self):
    pipeline_options = PipelineOptions([])
    self.assertRaises(
        Exception,
        apiclient._verify_interpreter_version_is_supported,
        pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 0, 0))
  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0.dev')
  def test_interpreter_version_check_passes_on_dev_sdks(self):
    pipeline_options = PipelineOptions([])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 0, 0))
  def test_interpreter_version_check_passes_with_experiment(self):
    pipeline_options = PipelineOptions(
        ["--experiment=use_unsupported_python_version"])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 8, 2))
  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_interpreter_version_check_passes_py38(self):
    pipeline_options = PipelineOptions([])
    apiclient._verify_interpreter_version_is_supported(pipeline_options)

  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.sys.version_info',
      (3, 13, 0))
  @mock.patch(
      'apache_beam.runners.dataflow.internal.apiclient.'
      'beam_version.__version__',
      '2.2.0')
  def test_interpreter_version_check_fails_on_not_yet_supported_version(self):
    pipeline_options = PipelineOptions([])
    self.assertRaises(
        Exception,
        apiclient._verify_interpreter_version_is_supported,
        pipeline_options)

  def test_get_response_encoding(self):
    encoding = apiclient.get_response_encoding()

    assert encoding == 'utf8'

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
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    pipeline_options.view_as(GoogleCloudOptions).no_auth = True
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

  def test_create_job_returns_existing_job(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
    ])
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    self.assertTrue(job.proto.clientRequestId)  # asserts non-empty string
    pipeline_options.view_as(GoogleCloudOptions).no_auth = True
    client = apiclient.DataflowApplicationClient(pipeline_options)

    response = dataflow.Job()
    # different clientRequestId from `job`
    response.clientRequestId = "20210821081910123456-1234"
    response.name = 'test_job_name'
    response.id = '2021-08-19_21_18_43-9756917246311111021'

    with mock.patch.object(client._client.projects_locations_jobs,
                           'Create',
                           side_effect=[response]):
      with mock.patch.object(client, 'create_job_description',
                             side_effect=None):
        with self.assertRaises(
            apiclient.DataflowJobAlreadyExistsError) as context:
          client.create_job(job)

        self.assertEqual(
            str(context.exception),
            'There is already active job named %s with id: %s. If you want to '
            'submit a second job, try again by setting a different name using '
            '--job_name.' % ('test_job_name', response.id))

  def test_update_job_returns_existing_job(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--region',
        'us-central1',
        '--update',
    ])
    replace_job_id = '2021-08-21_00_00_01-6081497447916622336'
    with mock.patch('apache_beam.runners.dataflow.internal.apiclient.Job.'
                    'job_id_for_name',
                    return_value=replace_job_id) as job_id_for_name_mock:
      job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    job_id_for_name_mock.assert_called_once()

    self.assertTrue(job.proto.clientRequestId)  # asserts non-empty string

    pipeline_options.view_as(GoogleCloudOptions).no_auth = True
    client = apiclient.DataflowApplicationClient(pipeline_options)

    response = dataflow.Job()
    # different clientRequestId from `job`
    response.clientRequestId = "20210821083254123456-1234"
    response.name = 'test_job_name'
    response.id = '2021-08-19_21_29_07-5725551945600207770'

    with mock.patch.object(client, 'create_job_description', side_effect=None):
      with mock.patch.object(client._client.projects_locations_jobs,
                             'Create',
                             side_effect=[response]):

        with self.assertRaises(
            apiclient.DataflowJobAlreadyExistsError) as context:
          client.create_job(job)

      self.assertEqual(
          str(context.exception),
          'The job named %s with id: %s has already been updated into job '
          'id: %s and cannot be updated again.' %
          ('test_job_name', replace_job_id, response.id))

  def test_template_file_generation_with_upload_graph(self):
    pipeline_options = PipelineOptions([
        '--project',
        'test_project',
        '--job_name',
        'test_job_name',
        '--temp_location',
        'gs://test-location/temp',
        '--experiments',
        'upload_graph',
        '--template_location',
        'gs://test-location/template'
    ])
    job = apiclient.Job(pipeline_options, beam_runner_api_pb2.Pipeline())
    job.proto.steps.append(dataflow.Step(name='test_step_name'))

    pipeline_options.view_as(GoogleCloudOptions).no_auth = True
    client = apiclient.DataflowApplicationClient(pipeline_options)
    with mock.patch.object(client, 'stage_file', side_effect=None):
      with mock.patch.object(client, 'create_job_description',
                             side_effect=None):
        with mock.patch.object(client,
                               'submit_job_description',
                               side_effect=None):
          client.create_job(job)

          client.stage_file.assert_has_calls([
              mock.call(mock.ANY, 'dataflow_graph.json', mock.ANY),
              mock.call(mock.ANY, 'template', mock.ANY)
          ])
          client.create_job_description.assert_called_once()
          # template is generated, but job should not be submitted to the
          # service.
          client.submit_job_description.assert_not_called()

          template_filename = client.stage_file.call_args_list[-1][0][1]
          self.assertTrue('template' in template_filename)
          template_content = client.stage_file.call_args_list[-1][0][2].read(
          ).decode('utf-8')
          template_obj = json.loads(template_content)
          self.assertFalse(template_obj.get('steps'))
          self.assertTrue(template_obj['stepsLocation'])

  def test_stage_resources(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://test-location/temp',
        '--staging_location',
        'gs://test-location/staging',
        '--no_auth'
    ])
    pipeline = beam_runner_api_pb2.Pipeline(
        components=beam_runner_api_pb2.Components(
            environments={
                'env1': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/foo1').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/bar1').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/baz').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/renamed1',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed1').SerializeToString())
                    ]),
                'env2': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/foo2').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/bar2').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/baz').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/renamed2',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed2').SerializeToString())
                    ])
            }))
    client = apiclient.DataflowApplicationClient(pipeline_options)
    with mock.patch.object(apiclient._LegacyDataflowStager,
                           'stage_job_resources') as mock_stager:
      client._stage_resources(pipeline, pipeline_options)
    mock_stager.assert_called_once_with(
        [('/tmp/foo1', 'foo1', ''), ('/tmp/bar1', 'bar1', ''),
         ('/tmp/baz', 'baz1', ''), ('/tmp/renamed1', 'renamed1', 'abcdefg'),
         ('/tmp/foo2', 'foo2', ''), ('/tmp/bar2', 'bar2', '')],
        staging_location='gs://test-location/staging')

    pipeline_expected = beam_runner_api_pb2.Pipeline(
        components=beam_runner_api_pb2.Components(
            environments={
                'env1': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/foo1'
                            ).SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/bar1').
                            SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/baz1').
                            SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/renamed1',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed1').SerializeToString())
                    ]),
                'env2': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/foo2').
                            SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/bar2').
                            SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/baz1').
                            SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/renamed1',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed1').SerializeToString())
                    ])
            }))
    self.assertEqual(pipeline, pipeline_expected)

  def test_set_dataflow_service_option(self):
    pipeline_options = PipelineOptions([
        '--dataflow_service_option',
        'whizz=bang',
        '--temp_location',
        'gs://any-location/temp'
    ])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.serviceOptions, ['whizz=bang'])

  def test_enable_hot_key_logging(self):
    # Tests that the enable_hot_key_logging is not set by default.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertIsNone(env.proto.debugOptions)

    # Now test that it is set when given.
    pipeline_options = PipelineOptions([
        '--enable_hot_key_logging', '--temp_location', 'gs://any-location/temp'
    ])
    env = apiclient.Environment(
        [],  #packages
        pipeline_options,
        '2.0.0',  #any environment version
        FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.debugOptions, dataflow.DebugOptions(enableHotKeyLogging=True))

  def _mock_uncached_copy(self, staging_root, src, sha256, dst_name=None):
    sha_prefix = sha256[0:2]
    gcs_cache_path = FileSystems.join(
        staging_root,
        apiclient.DataflowApplicationClient._GCS_CACHE_PREFIX,
        sha_prefix,
        sha256)

    if not dst_name:
      _, dst_name = os.path.split(src)
    return [
        mock.call.gcs_exists(gcs_cache_path),
        mock.call.gcs_upload(src, gcs_cache_path),
        mock.call.gcs_gcs_copy(
            source_file_names=[gcs_cache_path],
            destination_file_names=[f'gs://test-location/staging/{dst_name}'])
    ]

  def _mock_cached_copy(self, staging_root, src, sha256, dst_name=None):
    uncached = self._mock_uncached_copy(staging_root, src, sha256, dst_name)
    uncached.pop(1)
    return uncached

  def test_stage_artifacts_with_caching(self):
    pipeline_options = PipelineOptions([
        '--temp_location',
        'gs://test-location/temp',
        '--staging_location',
        'gs://test-location/staging',
        '--no_auth',
        '--enable_artifact_caching'
    ])
    pipeline = beam_runner_api_pb2.Pipeline(
        components=beam_runner_api_pb2.Components(
            environments={
                'env1': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/foo1',
                                sha256='abcd').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/bar1',
                                sha256='defg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(path='/tmp/baz', sha256='hijk'
                                                ).SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/renamed1',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed1').SerializeToString())
                    ]),
                'env2': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/foo2',
                                sha256='lmno').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/bar2',
                                sha256='pqrs').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(path='/tmp/baz', sha256='tuv'
                                                ).SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.FILE.urn,
                            type_payload=beam_runner_api_pb2.
                            ArtifactFilePayload(
                                path='/tmp/renamed2',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed2').SerializeToString())
                    ])
            }))
    client = apiclient.DataflowApplicationClient(pipeline_options)
    staging_root = 'gs://test-location/staging'

    # every other artifact already exists
    n = [0]

    def exists_return_value(*args):
      n[0] += 1
      return n[0] % 2 == 0

    with mock.patch.object(FileSystems,
                           'exists',
                           side_effect=exists_return_value) as mock_gcs_exists:
      with mock.patch.object(apiclient.DataflowApplicationClient,
                             '_uncached_gcs_file_copy') as mock_gcs_copy:
        with mock.patch.object(FileSystems, 'copy') as mock_gcs_gcs_copy:

          manager = mock.Mock()
          manager.attach_mock(mock_gcs_exists, 'gcs_exists')
          manager.attach_mock(mock_gcs_copy, 'gcs_upload')
          manager.attach_mock(mock_gcs_gcs_copy, 'gcs_gcs_copy')

          client._stage_resources(pipeline, pipeline_options)
          expected_calls = list(
              itertools.chain.from_iterable([
                  self._mock_uncached_copy(staging_root, '/tmp/foo1', 'abcd'),
                  self._mock_cached_copy(staging_root, '/tmp/bar1', 'defg'),
                  self._mock_uncached_copy(
                      staging_root, '/tmp/baz', 'hijk', 'baz1'),
                  self._mock_cached_copy(
                      staging_root, '/tmp/renamed1', 'abcdefg'),
                  self._mock_uncached_copy(staging_root, '/tmp/foo2', 'lmno'),
                  self._mock_cached_copy(staging_root, '/tmp/bar2', 'pqrs'),
              ]))
          assert manager.mock_calls == expected_calls

    pipeline_expected = beam_runner_api_pb2.Pipeline(
        components=beam_runner_api_pb2.Components(
            environments={
                'env1': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/foo1',
                                sha256='abcd').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/bar1',
                                sha256='defg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/baz1',
                                sha256='hijk').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/renamed1',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed1').SerializeToString())
                    ]),
                'env2': beam_runner_api_pb2.Environment(
                    dependencies=[
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/foo2',
                                sha256='lmno').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='foo2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/bar2',
                                sha256='pqrs').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='bar2').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/baz1',
                                sha256='tuv').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='baz1').SerializeToString()),
                        beam_runner_api_pb2.ArtifactInformation(
                            type_urn=common_urns.artifact_types.URL.urn,
                            type_payload=beam_runner_api_pb2.ArtifactUrlPayload(
                                url='gs://test-location/staging/renamed1',
                                sha256='abcdefg').SerializeToString(),
                            role_urn=common_urns.artifact_roles.STAGING_TO.urn,
                            role_payload=beam_runner_api_pb2.
                            ArtifactStagingToRolePayload(
                                staged_name='renamed1').SerializeToString())
                    ])
            }))
    self.assertEqual(pipeline, pipeline_expected)


if __name__ == '__main__':
  unittest.main()
