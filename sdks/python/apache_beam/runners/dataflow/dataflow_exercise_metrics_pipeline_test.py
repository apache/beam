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

"""A word-counting workflow."""

from __future__ import absolute_import

import argparse
import unittest

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.dataflow import dataflow_exercise_metrics_pipeline
from apache_beam.testing import metric_result_matchers
from apache_beam.testing.test_pipeline import TestPipeline


class ExerciseMetricsPipelineTest(unittest.TestCase):

  def run_pipeline(self, **opts):
    test_pipeline = TestPipeline(is_integration_test=True)
    argv = test_pipeline.get_full_options_as_args(**opts)
    parser = argparse.ArgumentParser()
    unused_known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
    return dataflow_exercise_metrics_pipeline.apply_and_run(p)

  @attr('IT')
  def test_metrics_it(self):
    result = self.run_pipeline()
    errors = metric_result_matchers.verify_all(
        result.metrics().all_metrics(),
        dataflow_exercise_metrics_pipeline.legacy_metric_matchers())
    self.assertFalse(errors, str(errors))

  @attr('IT', 'ValidatesContainer')
  def test_metrics_fnapi_it(self):
    result = self.run_pipeline(experiment='beam_fn_api')
    errors = metric_result_matchers.verify_all(
        result.metrics().all_metrics(),
        dataflow_exercise_metrics_pipeline.fn_api_metric_matchers())
    self.assertFalse(errors, str(errors))


if __name__ == '__main__':
  unittest.main()
