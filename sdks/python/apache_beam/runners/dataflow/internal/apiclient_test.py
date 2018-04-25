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
import unittest

import mock

from apache_beam.metrics.cells import DistributionData
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dataflow.internal import dependency
from apache_beam.runners.dataflow.internal.clients import dataflow
from apache_beam.transforms import DataflowDistributionCounter

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None
# pylint: enable=wrong-import-order, wrong-import-position

FAKE_PIPELINE_URL = "gs://invalid-bucket/anywhere"


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class UtilTest(unittest.TestCase):

  @unittest.skip("Enable once BEAM-1080 is fixed.")
  def test_create_application_client(self):
    pipeline_options = PipelineOptions()
    apiclient.DataflowApplicationClient(pipeline_options)

  def test_pipeline_url(self):
    pipeline_options = PipelineOptions(
        ['--subnetwork', '/regions/MY/subnetworks/SUBNETWORK',
         '--temp_location', 'gs://any-location/temp'])
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
      self.fail('No pipeline options found in %s'
                % env.proto.sdkPipelineOptions)

    pipeline_url = None
    for property in recovered_options.object_value.properties:
      if property.key == 'pipelineUrl':
        pipeline_url = property.value
        break
    else:
      self.fail('No pipeline_url found in %s' % recovered_options)

    self.assertEqual(pipeline_url.string_value, FAKE_PIPELINE_URL)

  def test_set_network(self):
    pipeline_options = PipelineOptions(
        ['--network', 'anetworkname',
         '--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.workerPools[0].network,
                     'anetworkname')

  def test_set_subnetwork(self):
    pipeline_options = PipelineOptions(
        ['--subnetwork', '/regions/MY/subnetworks/SUBNETWORK',
         '--temp_location', 'gs://any-location/temp'])

    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(env.proto.workerPools[0].subnetwork,
                     '/regions/MY/subnetworks/SUBNETWORK')

  def test_invalid_default_job_name(self):
    # Regexp for job names in dataflow.
    regexp = '^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$'

    job_name = apiclient.Job._build_default_job_name('invalid.-_user_n*/ame')
    self.assertRegexpMatches(job_name, regexp)

    job_name = apiclient.Job._build_default_job_name(
        'invalid-extremely-long.username_that_shouldbeshortened_or_is_invalid')
    self.assertRegexpMatches(job_name, regexp)

  def test_default_job_name(self):
    job_name = apiclient.Job.default_job_name(None)
    regexp = 'beamapp-.*-[0-9]{10}-[0-9]{6}'
    self.assertRegexpMatches(job_name, regexp)

  def test_split_int(self):
    number = 12345
    split_number = apiclient.to_split_int(number)
    self.assertEqual((split_number.lowBits, split_number.highBits),
                     (number, 0))
    shift_number = number << 32
    split_number = apiclient.to_split_int(shift_number)
    self.assertEqual((split_number.lowBits, split_number.highBits),
                     (0, number))

  def test_translate_distribution(self):
    metric_update = dataflow.CounterUpdate()
    distribution_update = DistributionData(16, 2, 1, 15)
    apiclient.translate_distribution(distribution_update, metric_update)
    self.assertEqual(metric_update.distribution.min.lowBits,
                     distribution_update.min)
    self.assertEqual(metric_update.distribution.max.lowBits,
                     distribution_update.max)
    self.assertEqual(metric_update.distribution.sum.lowBits,
                     distribution_update.sum)
    self.assertEqual(metric_update.distribution.count.lowBits,
                     distribution_update.count)

  def test_translate_distribution_counter(self):
    counter_update = DataflowDistributionCounter()
    counter_update.add_input(1)
    counter_update.add_input(3)
    metric_proto = dataflow.CounterUpdate()
    apiclient.translate_distribution(counter_update, metric_proto)
    histogram = mock.Mock(firstBucketOffset=None, bucketCounts=None)
    counter_update.translate_to_histogram(histogram)
    self.assertEqual(metric_proto.distribution.min.lowBits,
                     counter_update.min)
    self.assertEqual(metric_proto.distribution.max.lowBits,
                     counter_update.max)
    self.assertEqual(metric_proto.distribution.sum.lowBits,
                     counter_update.sum)
    self.assertEqual(metric_proto.distribution.count.lowBits,
                     counter_update.count)
    self.assertEqual(metric_proto.distribution.histogram.bucketCounts,
                     histogram.bucketCounts)
    self.assertEqual(metric_proto.distribution.histogram.firstBucketOffset,
                     histogram.firstBucketOffset)

  def test_translate_means(self):
    metric_update = dataflow.CounterUpdate()
    accumulator = mock.Mock()
    accumulator.sum = 16
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_int(accumulator,
                                                                metric_update)
    self.assertEqual(metric_update.integerMean.sum.lowBits, accumulator.sum)
    self.assertEqual(metric_update.integerMean.count.lowBits, accumulator.count)

    accumulator.sum = 16.0
    accumulator.count = 2
    apiclient.MetricUpdateTranslators.translate_scalar_mean_float(accumulator,
                                                                  metric_update)
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
        ['--temp_location', 'gs://any-location/temp',
         '--use_public_ips'])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0',
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].ipConfiguration,
        dataflow.WorkerPool.IpConfigurationValueValuesEnum.WORKER_IP_PUBLIC)

  def test_private_ip_configuration(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp',
         '--no_use_public_ips'])
    env = apiclient.Environment([],
                                pipeline_options,
                                '2.0.0',
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].ipConfiguration,
        dataflow.WorkerPool.IpConfigurationValueValuesEnum.WORKER_IP_PRIVATE)

  @mock.patch('apache_beam.runners.dataflow.internal.dependency.'
              'beam_version.__version__', '2.2.0')
  def test_harness_override_present_in_released_sdks(self):
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    override = ''.join(
        ['runner_harness_container_image=',
         dependency.DATAFLOW_CONTAINER_IMAGE_REPOSITORY,
         '/harness:2.2.0'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertIn(override, env.proto.experiments)

  @mock.patch('apache_beam.runners.dataflow.internal.dependency.'
              'beam_version.__version__', '2.2.0.dev')
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

  @mock.patch('apache_beam.runners.dataflow.internal.dependency.'
              'beam_version.__version__', '2.2.0.dev')
  def test_pinned_worker_harness_image_tag_used_in_dev_sdk(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (dependency.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
         '/python-fnapi:' + dependency.BEAM_FNAPI_CONTAINER_VERSION))

    # batch, legacy pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (dependency.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
         '/python:' + dependency.BEAM_CONTAINER_VERSION))

  @mock.patch('apache_beam.runners.dataflow.internal.dependency.'
              'beam_version.__version__', '2.2.0')
  def test_worker_harness_image_tag_matches_released_sdk_version(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (dependency.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
         '/python-fnapi:2.2.0'))

    # batch, legacy pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        (dependency.DATAFLOW_CONTAINER_IMAGE_REPOSITORY +
         '/python:2.2.0'))

  def test_worker_harness_override_takes_precedence_over_sdk_defaults(self):
    # streaming, fnapi pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp', '--streaming',
         '--worker_harness_container_image=some:image'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        'some:image')
    # batch, legacy pipeline.
    pipeline_options = PipelineOptions(
        ['--temp_location', 'gs://any-location/temp',
         '--worker_harness_container_image=some:image'])
    env = apiclient.Environment([], #packages
                                pipeline_options,
                                '2.0.0', #any environment version
                                FAKE_PIPELINE_URL)
    self.assertEqual(
        env.proto.workerPools[0].workerHarnessContainerImage,
        'some:image')

  def test_labels(self):
    pipeline_options = PipelineOptions(
        ['--project', 'test_project', '--job_name', 'test_job_name',
         '--temp_location', 'gs://test-location/temp'])
    job = apiclient.Job(pipeline_options, FAKE_PIPELINE_URL)
    self.assertIsNone(job.proto.labels)

    pipeline_options = PipelineOptions(
        ['--project', 'test_project', '--job_name', 'test_job_name',
         '--temp_location', 'gs://test-location/temp',
         '--label', 'key1=value1',
         '--label', 'key2',
         '--label', 'key3=value3',
         '--labels', 'key4=value4',
         '--labels', 'key5'])
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


if __name__ == '__main__':
  unittest.main()
