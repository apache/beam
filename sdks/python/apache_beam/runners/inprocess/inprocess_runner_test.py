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

"""Tests for InProcessPipelineRunner."""

import logging
import unittest

from apache_beam import Pipeline
import apache_beam.examples.snippets.snippets_test as snippets_test
import apache_beam.io.fileio_test as fileio_test
import apache_beam.io.textio_test as textio_test
import apache_beam.io.sources_test as sources_test
import apache_beam.pipeline_test as pipeline_test
import apache_beam.pvalue_test as pvalue_test
from apache_beam.runners.inprocess.inprocess_runner import InProcessPipelineRunner
import apache_beam.transforms.aggregator_test as aggregator_test
import apache_beam.transforms.combiners_test as combiners_test
import apache_beam.transforms.ptransform_test as ptransform_test
import apache_beam.transforms.trigger_test as trigger_test
import apache_beam.transforms.window_test as window_test
import apache_beam.transforms.write_ptransform_test as write_ptransform_test
import apache_beam.typehints.typed_pipeline_test as typed_pipeline_test


class TestWithInProcessPipelineRunner(object):

  def setUp(self):
    original_init = Pipeline.__init__

    def override_pipeline_init(self, runner=None, options=None, argv=None):
      runner = InProcessPipelineRunner()
      return original_init(self, runner, options, argv)

    self.runner_name = None
    self.original_init = original_init
    Pipeline.__init__ = override_pipeline_init

  def tearDown(self):
    Pipeline.__init__ = self.original_init


class InProcessPipelineRunnerPipelineTest(
    TestWithInProcessPipelineRunner, pipeline_test.PipelineTest):

  def test_cached_pvalues_are_refcounted(self):
    # InProcessPipelineRunner does not have a refcounted cache.
    pass

  def test_eager_pipeline(self):
    # Tests eager runner only
    pass


class InProcessPipelineRunnerSnippetsTest(
    TestWithInProcessPipelineRunner, snippets_test.SnippetsTest,
    snippets_test.ParDoTest, snippets_test.TypeHintsTest,
    snippets_test.CombineTest):
  pass


class InProcessPipelineRunnerTransform(
    TestWithInProcessPipelineRunner, aggregator_test.AggregatorTest,
    combiners_test.CombineTest, ptransform_test.PTransformTest,
    pvalue_test.PValueTest, window_test.WindowTest,
    typed_pipeline_test.MainInputTest, typed_pipeline_test.SideInputTest,
    typed_pipeline_test.CustomTransformTest, trigger_test.TriggerPipelineTest,
    write_ptransform_test.WriteTest):
  pass


class TestTextFileSource(
    TestWithInProcessPipelineRunner, fileio_test.TestTextFileSource):
  pass


class TestNativeTextFileSink(
    TestWithInProcessPipelineRunner, fileio_test.TestNativeTextFileSink):

  def setUp(self):
    TestWithInProcessPipelineRunner.setUp(self)
    fileio_test.TestNativeTextFileSink.setUp(self)


class TestTextFileSink(
    TestWithInProcessPipelineRunner, textio_test.TextSinkTest):

  def setUp(self):
    TestWithInProcessPipelineRunner.setUp(self)
    textio_test.TextSinkTest.setUp(self)


class MyFileSink(TestWithInProcessPipelineRunner, fileio_test.MyFileSink):
  pass


class TestFileSink(TestWithInProcessPipelineRunner, fileio_test.TestFileSink):
  pass


class SourcesTest(TestWithInProcessPipelineRunner, sources_test.SourcesTest):
  pass


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
