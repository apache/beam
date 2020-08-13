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
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.sql import SqlTransform
from apache_beam.utils.timestamp import Timestamp

SimpleRow = typing.NamedTuple(
    "SimpleRow", [("id", int), ("str", unicode), ("flt", float)])
coders.registry.register_coder(SimpleRow, coders.RowCoder)

Enrich = typing.NamedTuple("Enrich", [("id", int), ("metadata", unicode)])
coders.registry.register_coder(Enrich, coders.RowCoder)

Shopper = typing.NamedTuple(
    "Shopper", [("shopper", unicode), ("cart", typing.Mapping[unicode, int])])
coders.registry.register_coder(Shopper, coders.RowCoder)

TimestampWrapper = typing.NamedTuple(
    "TimestampWrapper", [("timestamp", Timestamp)])
coders.registry.register_coder(TimestampWrapper, coders.RowCoder)


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
  def test_generate_data(self):
    with TestPipeline() as p:
      out = p | SqlTransform(
          """SELECT
            CAST(1 AS INT) AS `id`,
            CAST('foo' AS VARCHAR) AS `str`,
            CAST(3.14  AS DOUBLE) AS `flt`""")
      assert_that(out, equal_to([(1, "foo", 3.14)]))

  def test_project(self):
    with TestPipeline() as p:
      out = (
          p | beam.Create([SimpleRow(1, "foo", 3.14)])
          | SqlTransform("SELECT `id`, `flt` FROM PCOLLECTION"))
      assert_that(out, equal_to([(1, 3.14)]))

  def test_filter(self):
    with TestPipeline() as p:
      out = (
          p
          | beam.Create([SimpleRow(1, "foo", 3.14), SimpleRow(2, "bar", 1.414)])
          | SqlTransform("SELECT * FROM PCOLLECTION WHERE `str` = 'bar'"))
      assert_that(out, equal_to([(2, "bar", 1.414)]))

  def test_agg(self):
    with TestPipeline() as p:
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
                SUM(`id`) AS `sum`,
                AVG(`flt`) AS `avg`
              FROM PCOLLECTION GROUP BY `str`"""))
      assert_that(out, equal_to([("foo", 3, 3, 2), ("bar", 4, 8, 1.414)]))

  def test_tagged_join(self):
    with TestPipeline() as p:
      enrich = (
          p | "Create enrich" >> beam.Create(
              [Enrich(1, "a"), Enrich(2, "b"), Enrich(26, "z")]))
      simple = (
          p | "Create simple" >> beam.Create([
              SimpleRow(1, "foo", 3.14),
              SimpleRow(26, "bar", 1.11),
              SimpleRow(1, "baz", 2.34)
          ]))
      out = ({
          'simple': simple, 'enrich': enrich
      }
             | SqlTransform(
                 """
              SELECT
                simple.`id` AS `id`,
                enrich.metadata AS metadata
              FROM simple
              JOIN enrich
              ON simple.`id` = enrich.`id`"""))
      assert_that(out, equal_to([(1, "a"), (26, "z"), (1, "a")]))

  def test_row(self):
    with TestPipeline() as p:
      out = (
          p
          | beam.Create([1, 2, 10])
          | beam.Map(lambda x: beam.Row(a=x, b=unicode(x)))
          | SqlTransform("SELECT a*a as s, LENGTH(b) AS c FROM PCOLLECTION"))
      assert_that(out, equal_to([(1, 1), (4, 1), (100, 2)]))

  def test_zetasql_generate_data(self):
    with TestPipeline() as p:
      out = p | SqlTransform(
          """SELECT
            CAST(1 AS INT64) AS `int`,
            CAST('foo' AS STRING) AS `str`,
            CAST(3.14  AS FLOAT64) AS `flt`""",
          dialect="zetasql")
      assert_that(out, equal_to([(1, "foo", 3.14)]))

  def test_windowing_before_sql(self):
    with TestPipeline() as p:
      out = (
          p | beam.Create([
              SimpleRow(5, "foo", 1.),
              SimpleRow(15, "bar", 2.),
              SimpleRow(25, "baz", 3.)
          ])
          | beam.Map(lambda v: beam.window.TimestampedValue(v, v.id)).
          with_output_types(SimpleRow)
          | beam.WindowInto(
              beam.window.FixedWindows(10)).with_output_types(SimpleRow)
          | SqlTransform("SELECT COUNT(*) as `count` FROM PCOLLECTION"))
      assert_that(out, equal_to([(1, ), (1, ), (1, )]))

  def test_map(self):
    with TestPipeline() as p:
      out = (
          p
          | beam.Create([
              Shopper('bob', {
                  'bananas': 6, 'cherries': 3
              }),
              Shopper('alice', {
                  'apples': 2, 'bananas': 3
              })
          ]).with_output_types(Shopper)
          | SqlTransform("SELECT * FROM PCOLLECTION WHERE shopper = 'alice'"))
      assert_that(out, equal_to([('alice', {'apples': 2, 'bananas': 3})]))

  def test_generate_timestamp(self):
    with TestPipeline() as p:
      out = (
          p | SqlTransform(
              "SELECT TIMESTAMP '2020-08-12 15:51:00.032' as `timestamp`"))
      assert_that(
          out,
          equal_to([(Timestamp.from_rfc3339('2020-08-12T15:51:00.032Z'), )]))

  def test_process_timestamp(self):
    with TestPipeline() as p:
      out = (
          p | beam.Create([
              '2020-08-12T15:51:00.032Z',
              '1983-10-31T00:00:00.000Z',
              '1970-01-01T00:00:00.001Z',
          ])
          | beam.Map(Timestamp.from_rfc3339)
          # TODO: Why doesn't type inference work with Timestamp and beam.Row?
          | beam.Map(lambda x: TimestampWrapper(timestamp=x)).with_output_types(
              TimestampWrapper)
          | SqlTransform(
              """"
            SELECT
              YEAR(`timestamp`) AS `year`,
              MONTH(`timestamp`) AS `month`
            FROM PCOLLECTION"""))
      assert_that(out, equal_to([(2020, 8), (1983, 10), (1970, 1)]))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
