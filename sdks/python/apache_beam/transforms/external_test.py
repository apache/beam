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

"""Unit tests for the transform.util classes."""

from __future__ import absolute_import

import argparse
import subprocess
import sys
import unittest

import grpc
from mock import patch
from past.builtins import unicode

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.portability import python_urns
from apache_beam.runners.portability import expansion_service
from apache_beam.runners.portability.expansion_service_test import FibTransform
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None
# pylint: enable=wrong-import-order, wrong-import-position


class ExternalTransformTest(unittest.TestCase):

  # This will be overwritten if set via a flag.
  expansion_service_jar = None
  expansion_service_port = None

  class _RunWithExpansion(object):
    def __init__(self, port, expansion_service_jar):
      self._port = port
      self._expansion_service_jar = expansion_service_jar

    def __enter__(self):
      if not ExternalTransformTest.expansion_service_jar:
        raise unittest.SkipTest('No expansion service jar provided.')

      # Start the java server and wait for it to be ready.
      self._server = subprocess.Popen(
          ['java', '-jar', self._expansion_service_jar, str(self._port)])

      port = ExternalTransformTest.expansion_service_port or 8091
      address = 'localhost:%s' % str(port)

      with grpc.insecure_channel(address) as channel:
        grpc.channel_ready_future(channel).result()

    def __exit__(self, type, value, traceback):
      self._server.kill()

  def test_pipeline_generation(self):
    pipeline = beam.Pipeline()
    res = (
        pipeline
        | beam.Create(['a', 'b'])
        | beam.ExternalTransform(
            'simple', None, expansion_service.ExpansionServiceServicer())
)
    assert_that(res, equal_to(['Simple(a)', 'Simple(b)']))

    proto, _ = pipeline.to_runner_api(return_context=True)
    pipeline_from_proto = Pipeline.from_runner_api(
        proto, pipeline.runner, pipeline._options)

    # Original pipeline has the un-expanded external transform
    self.assertEqual([], pipeline.transforms_stack[0].parts[1].parts)

    # new pipeline has the expanded external transform
    self.assertNotEqual(
        [], pipeline_from_proto.transforms_stack[0].parts[1].parts)
    self.assertEqual(
        u'ExternalTransform(simple)/TestLabel',
        pipeline_from_proto.transforms_stack[0].parts[1].parts[0].full_label)

  def test_simple(self):
    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(['a', 'b'])
          | beam.ExternalTransform(
              'simple', None, expansion_service.ExpansionServiceServicer())
)
      assert_that(res, equal_to(['Simple(a)', 'Simple(b)']))

  def test_multi(self):
    with beam.Pipeline() as p:
      main1 = p | 'Main1' >> beam.Create(['a', 'bb'], reshuffle=False)
      main2 = p | 'Main2' >> beam.Create(['x', 'yy', 'zzz'], reshuffle=False)
      side = p | 'Side' >> beam.Create(['s'])
      res = dict(main1=main1, main2=main2, side=side) | beam.ExternalTransform(
          'multi', None, expansion_service.ExpansionServiceServicer())
      assert_that(res['main'], equal_to(['as', 'bbs', 'xs', 'yys', 'zzzs']))
      assert_that(res['side'], equal_to(['ss']), label='CheckSide')

  def test_payload(self):
    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(['a', 'bb'], reshuffle=False)
          | beam.ExternalTransform(
              'payload', b's', expansion_service.ExpansionServiceServicer())
)
      assert_that(res, equal_to(['as', 'bbs']))

  def test_nested(self):
    with beam.Pipeline() as p:
      assert_that(p | FibTransform(6), equal_to([8]))

  def test_java_expansion_portable_runner(self):
    pipeline_options = PipelineOptions(
        [
            '--runner=PortableRunner',
            '--experiments=beam_fn_api',
            '--environment_type=%s' % python_urns.EMBEDDED_PYTHON,
            '--job_endpoint=embed',
        ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options.view_as(SetupOptions).save_main_session = True

    ExternalTransformTest.run_pipeline_with_portable_runner(pipeline_options)

  @unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
  def test_java_expansion_dataflow(self):
    # This test does not actually running the pipeline in Dataflow. It just
    # tests the translation to a Dataflow job request.

    with patch.object(
        apiclient.DataflowApplicationClient, 'create_job'
    ) as mock_create_job:
      port = ExternalTransformTest.expansion_service_port or 8091
      with self._RunWithExpansion(port, self.expansion_service_jar):
        pipeline_options = PipelineOptions(
            [
                '--runner=DataflowRunner',
                '--project=dummyproject',
                '--experiments=beam_fn_api',
                '--temp_location=gs://dummybucket/',
            ])

        # We use the save_main_session option because one or more DoFn's in this
        # workflow rely on global context (e.g., a module imported at module
        # level).
        pipeline_options.view_as(SetupOptions).save_main_session = True

        # Run a simple count-filtered-letters pipeline.
        self.run_pipeline(pipeline_options, port, False)

        mock_args = mock_create_job.call_args_list
        assert mock_args
        args, kwargs = mock_args[0]
        job = args[0]
        job_str = '%s' % job
        self.assertIn('pytest:beam:transforms:filter_less_than', job_str)

  @staticmethod
  def run_pipeline_with_portable_runner(pipeline_options):

    port = ExternalTransformTest.expansion_service_port or 8091

    with ExternalTransformTest._RunWithExpansion(
        port, ExternalTransformTest.expansion_service_jar):
      # Run a simple count-filtered-letters pipeline.
      ExternalTransformTest.run_pipeline(pipeline_options, port, True)

  @staticmethod
  def run_pipeline(
      pipeline_options, expansion_service_port, wait_until_finish=True):
    # The actual definitions of these transforms is in
    # org.apache.beam.runners.core.construction.TestExpansionService.
    TEST_COUNT_URN = "pytest:beam:transforms:count"
    TEST_FILTER_URN = "pytest:beam:transforms:filter_less_than"

    # Run a simple count-filtered-letters pipeline.
    p = beam.Pipeline(options=pipeline_options)
    address = 'localhost:%s' % str(expansion_service_port)
    res = (
        p
        | beam.Create(list('aaabccxyyzzz'))
        | beam.Map(unicode)
        # TODO(BEAM-6587): Use strings directly rather than ints.
        | beam.Map(lambda x: int(ord(x)))
        | beam.ExternalTransform(TEST_FILTER_URN, b'middle', address)
        | beam.ExternalTransform(TEST_COUNT_URN, None, address)
        # # TODO(BEAM-6587): Remove when above is removed.
        | beam.Map(lambda kv: (chr(kv[0]), kv[1]))
        | beam.Map(lambda kv: '%s: %s' % kv))

    assert_that(res, equal_to(['a: 3', 'b: 1', 'c: 2']))

    result = p.run()
    if wait_until_finish:
      result.wait_until_finish()


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--expansion_service_jar')
  parser.add_argument('--expansion_service_port')
  known_args, pipeline_args = parser.parse_known_args(sys.argv)

  if known_args.expansion_service_jar:
    ExternalTransformTest.expansion_service_jar = (
        known_args.expansion_service_jar)
    ExternalTransformTest.expansion_service_port = int(
        known_args.expansion_service_port)
    pipeline_options = PipelineOptions(pipeline_args)
    ExternalTransformTest.run_pipeline_with_portable_runner(pipeline_options)
  else:
    sys.argv = pipeline_args
    unittest.main()
