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

"""Tests for the Java external transforms."""

import argparse
import logging
import subprocess
import sys

import grpc
from mock import patch

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient as _apiclient
except ImportError:
  apiclient = None
else:
  apiclient = _apiclient
# pylint: enable=wrong-import-order, wrong-import-position


class JavaExternalTransformTest(object):

  # This will be overwritten if set via a flag.
  expansion_service_jar = None  # type: str
  expansion_service_port = None  # type: int

  class _RunWithExpansion(object):
    def __init__(self):
      self._server = None

    def __enter__(self):
      if not (JavaExternalTransformTest.expansion_service_jar or
              JavaExternalTransformTest.expansion_service_port):
        raise RuntimeError('No expansion service jar or port provided.')

      JavaExternalTransformTest.expansion_service_port = (
          JavaExternalTransformTest.expansion_service_port or 8091)

      jar = JavaExternalTransformTest.expansion_service_jar
      port = JavaExternalTransformTest.expansion_service_port

      # Start the java server and wait for it to be ready.
      if jar:
        self._server = subprocess.Popen(['java', '-jar', jar, str(port)])

      address = 'localhost:%s' % str(port)

      with grpc.insecure_channel(address) as channel:
        grpc.channel_ready_future(channel).result()

    def __exit__(self, type, value, traceback):
      if self._server:
        self._server.kill()
        self._server = None

  @staticmethod
  def test_java_expansion_dataflow():
    if apiclient is None:
      return

    # This test does not actually running the pipeline in Dataflow. It just
    # tests the translation to a Dataflow job request.

    with patch.object(apiclient.DataflowApplicationClient,
                      'create_job') as mock_create_job:
      with JavaExternalTransformTest._RunWithExpansion():
        pipeline_options = PipelineOptions([
            '--runner=DataflowRunner',
            '--project=dummyproject',
            '--region=some-region1',
            '--experiments=beam_fn_api',
            '--temp_location=gs://dummybucket/'
        ])

        # Run a simple count-filtered-letters pipeline.
        JavaExternalTransformTest.run_pipeline(
            pipeline_options,
            JavaExternalTransformTest.expansion_service_port,
            False)

        mock_args = mock_create_job.call_args_list
        assert mock_args
        args, kwargs = mock_args[0]
        job = args[0]
        job_str = '%s' % job
        assert 'beam:transforms:xlang:filter_less_than_eq' in job_str

  @staticmethod
  def run_pipeline_with_expansion_service(pipeline_options):
    with JavaExternalTransformTest._RunWithExpansion():
      # Run a simple count-filtered-letters pipeline.
      JavaExternalTransformTest.run_pipeline(
          pipeline_options,
          JavaExternalTransformTest.expansion_service_port,
          True)

  @staticmethod
  def run_pipeline(pipeline_options, expansion_service, wait_until_finish=True):
    # The actual definitions of these transforms is in
    # org.apache.beam.runners.core.construction.TestExpansionService.
    TEST_COUNT_URN = "beam:transforms:xlang:count"
    TEST_FILTER_URN = "beam:transforms:xlang:filter_less_than_eq"

    # Run a simple count-filtered-letters pipeline.
    p = TestPipeline(options=pipeline_options)

    if isinstance(expansion_service, int):
      # Only the port was specified.
      expansion_service = 'localhost:%s' % str(expansion_service)

    res = (
        p
        | beam.Create(list('aaabccxyyzzz'))
        | beam.Map(str)
        | beam.ExternalTransform(
            TEST_FILTER_URN,
            ImplicitSchemaPayloadBuilder({'data': u'middle'}),
            expansion_service)
        | beam.ExternalTransform(TEST_COUNT_URN, None, expansion_service)
        | beam.Map(lambda kv: '%s: %s' % kv))

    assert_that(res, equal_to(['a: 3', 'b: 1', 'c: 2']))

    result = p.run()
    if wait_until_finish:
      result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser()
  parser.add_argument('--expansion_service_jar')
  parser.add_argument('--expansion_service_port')
  parser.add_argument('--expansion_service_target')
  parser.add_argument('--expansion_service_target_appendix')
  known_args, pipeline_args = parser.parse_known_args(sys.argv)

  if known_args.expansion_service_jar:
    JavaExternalTransformTest.expansion_service_jar = (
        known_args.expansion_service_jar)
    JavaExternalTransformTest.expansion_service_port = int(
        known_args.expansion_service_port)
    pipeline_options = PipelineOptions(pipeline_args)
    JavaExternalTransformTest.run_pipeline_with_expansion_service(
        pipeline_options)
  elif known_args.expansion_service_target:
    pipeline_options = PipelineOptions(pipeline_args)
    JavaExternalTransformTest.run_pipeline(
        pipeline_options,
        beam.transforms.external.BeamJarExpansionService(
            known_args.expansion_service_target,
            gradle_appendix=known_args.expansion_service_target_appendix))
  else:
    raise RuntimeError(
        "--expansion_service_jar or --expansion_service_target "
        "should be provided.")
