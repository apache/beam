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

"""Tests for apache_beam.runners.interactive.background_caching_job."""
from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.runners import runner
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch


def _build_a_test_stream_pipeline():
  test_stream = (TestStream()
                 .advance_watermark_to(0)
                 .add_elements([TimestampedValue('a', 1)])
                 .advance_processing_time(5)
                 .advance_watermark_to_infinity())
  p = beam.Pipeline(runner=interactive_runner.InteractiveRunner())
  events = p | test_stream  # pylint: disable=possibly-unused-variable
  ib.watch(locals())
  return p


class BackgroundCachingJobTest(unittest.TestCase):

  def tearDown(self):
    ie.new_env()

  # TODO(BEAM-8335): remove the patches when there are appropriate test sources
  # that meet the boundedness checks.
  @patch('apache_beam.runners.interactive.pipeline_instrument'
         '.has_unbounded_sources', lambda x: True)
  def test_background_caching_job_starts_when_none_such_job_exists(self):
    p = _build_a_test_stream_pipeline()
    p.run()
    self.assertIsNotNone(
        ie.current_env().pipeline_result(p, is_main_job=False))

  @patch('apache_beam.runners.interactive.pipeline_instrument'
         '.has_unbounded_sources', lambda x: False)
  def test_background_caching_job_not_start_for_batch_pipeline(self):
    p = _build_a_test_stream_pipeline()
    p.run()
    self.assertIsNone(
        ie.current_env().pipeline_result(p, is_main_job=False))

  @patch('apache_beam.runners.interactive.pipeline_instrument'
         '.has_unbounded_sources', lambda x: True)
  def test_background_caching_job_not_start_when_such_job_exists(self):
    p = _build_a_test_stream_pipeline()
    a_running_result = runner.PipelineResult(runner.PipelineState.RUNNING)
    ie.current_env().set_pipeline_result(p, a_running_result, is_main_job=False)
    main_job_result = p.run()
    # No background caching job is started so result is still the running one.
    self.assertIs(a_running_result,
                  ie.current_env().pipeline_result(p, is_main_job=False))
    # A new main job is started so result of the main job is set.
    self.assertIs(main_job_result,
                  ie.current_env().pipeline_result(p))

  @patch('apache_beam.runners.interactive.pipeline_instrument'
         '.has_unbounded_sources', lambda x: True)
  def test_background_caching_job_not_start_when_such_job_is_done(self):
    p = _build_a_test_stream_pipeline()
    a_done_result = runner.PipelineResult(runner.PipelineState.DONE)
    ie.current_env().set_pipeline_result(p, a_done_result, is_main_job=False)
    main_job_result = p.run()
    # No background caching job is started so result is still the running one.
    self.assertIs(a_done_result,
                  ie.current_env().pipeline_result(p, is_main_job=False))
    # A new main job is started so result of the main job is set.
    self.assertIs(main_job_result,
                  ie.current_env().pipeline_result(p))


if __name__ == '__main__':
  unittest.main()
