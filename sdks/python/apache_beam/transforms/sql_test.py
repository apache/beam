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

"""Tests for transforms that use the SQL Expansion service."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import typing
import unittest

from nose.plugins.attrib import attr
from past.builtins import unicode

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.sql import SqlTransform
from apache_beam.utils import subprocess_server

SimpleRow = typing.NamedTuple(
    "SimpleRow", [("int", int), ("str", unicode), ("flt", float)])
coders.registry.register_coder(SimpleRow, coders.RowCoder)


@attr('UsesSqlExpansionService')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner is
    None,
    "Must be run with a runner that supports staging java artifacts.")
class SqlTransformTest(unittest.TestCase):
  """Tests that exercise the cross-language SqlTransform (implemented in java).

  Note this test must be executed with pipeline options that run jobs on a local
  job server. The easiest way to accomplish this is to run the
  `validatesCrossLanguageRunnerPythonUsingSql` gradle target for a particular
  job server, which will start the runner and job server for you. For example,
  `:runners:flink:1.10:job-server:validatesCrossLanguageRunnerPythonUsingSql` to
  test on Flink 1.10.

  Alternatively, you may be able to iterate faster if you run the tests directly
  using a runner like `FlinkRunner`, which can start a local Flink cluster and
  job server for you:
    $ pip install -e './sdks/python[gcp,test]'
    $ python ./sdks/python/setup.py nosetests \\
        --tests apache_beam.transforms.sql_test \\
        --test-pipeline-options="--runner=FlinkRunner"
  """
  @staticmethod
  def make_test_pipeline():
    path_to_jar = subprocess_server.JavaJarServer.path_to_beam_jar(
        ":sdks:java:extensions:sql:expansion-service:shadowJar")
    test_pipeline = TestPipeline()
    # TODO(BEAM-9238): Remove this when it's no longer needed for artifact
    # staging.
    test_pipeline.get_pipeline_options().view_as(DebugOptions).experiments = [
        'jar_packages=' + path_to_jar
    ]
    return test_pipeline

  def test_generate_data(self):
    with self.make_test_pipeline() as p:
      out = p | SqlTransform(
          """SELECT
            CAST(1 AS INT) AS `int`,
            CAST('foo' AS VARCHAR) AS `str`,
            CAST(3.14  AS DOUBLE) AS `flt`""")
      assert_that(out, equal_to([(1, "foo", 3.14)]))

  def test_project(self):
    with self.make_test_pipeline() as p:
      out = (
          p | beam.Create([SimpleRow(1, "foo", 3.14)])
          | SqlTransform("SELECT `int`, `flt` FROM PCOLLECTION"))
      assert_that(out, equal_to([(1, 3.14)]))

  def test_filter(self):
    with self.make_test_pipeline() as p:
      out = (
          p
          | beam.Create([SimpleRow(1, "foo", 3.14), SimpleRow(2, "bar", 1.414)])
          | SqlTransform("SELECT * FROM PCOLLECTION WHERE `str` = 'bar'"))
      assert_that(out, equal_to([(2, "bar", 1.414)]))

  def test_agg(self):
    with self.make_test_pipeline() as p:
      out = (
          p
          | beam.Create([
              SimpleRow(1, "foo", 1.),
              SimpleRow(1, "foo", 2.),
              SimpleRow(1, "foo", 3.),
              SimpleRow(2, "bar", 1.414),
              SimpleRow(2, "bar", 1.414),
              SimpleRow(2, "bar", 1.414),
              SimpleRow(2, "bar", 1.414),
          ])
          | SqlTransform(
              """
              SELECT
                `str`,
                COUNT(*) AS `count`,
                SUM(`int`) AS `sum`,
                AVG(`flt`) AS `avg`
              FROM PCOLLECTION GROUP BY `str`"""))
      assert_that(out, equal_to([("foo", 3, 3, 2), ("bar", 4, 8, 1.414)]))

  def test_datacatalog_tableprovider(self):
    with self.make_test_pipeline() as p:
      out = (
          p | SqlTransform(
              """
              SELECT
                id, name, type FROM datacatalog.entry.`apache-beam-testing`.`us-central1`.samples.`integ_test_small_csv_test_1`"""
          ))
      assert_that(
          out,
          equal_to([(1, "customer1", "test"), (2, "customer2", "test"),
                    (3, "customer1", "test"), (4, "customer2", "test")]))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
