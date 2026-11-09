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

import logging
import typing
import unittest

import pytest

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.portability.api import schema_pb2
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.sql import SqlTransform

SimpleRow = typing.NamedTuple(
    "SimpleRow", [("id", int), ("str", str), ("flt", float)])
coders.registry.register_coder(SimpleRow, coders.RowCoder)

Enrich = typing.NamedTuple("Enrich", [("id", int), ("metadata", str)])
coders.registry.register_coder(Enrich, coders.RowCoder)

Shopper = typing.NamedTuple(
    "Shopper", [("shopper", str), ("cart", typing.Mapping[str, int])])
coders.registry.register_coder(Shopper, coders.RowCoder)


class Aribitrary:
  def __init__(self, obj):
    self.obj = obj

  def __eq__(self, other):
    return self.obj == other.obj


UserTypeRow = typing.NamedTuple(
    "UserTypeRow", [("id", int), ("arb", Aribitrary), ("complex", complex)])
coders.registry.register_coder(UserTypeRow, coders.RowCoder)

TupleRow = typing.NamedTuple(
    "TupleRow", [("id", int), ("coords", tuple[str, int])])
coders.registry.register_coder(TupleRow, coders.RowCoder)


@pytest.mark.xlang_sql_expansion_service
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is None,
    "Must be run with a runner that supports staging java artifacts.")
class SqlTransformTest(unittest.TestCase):
  """Tests that exercise the cross-language SqlTransform (implemented in java).

  To run these tests locally using PrismRunner, build the SQL expansion service
  and prism binary first:
    $ ./gradlew :sdks:java:extensions:sql:expansion-service:shadowJar
    $ ./gradlew :runners:prism:build
    $ pytest apache_beam/transforms/sql_test.py \\
        --test-pipeline-options="--runner=PrismRunner"
  """
  _multiprocess_can_split_ = True

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
          | beam.Map(lambda x: beam.Row(a=x, b=str(x)))
          | SqlTransform("SELECT a*a as s, LENGTH(b) AS c FROM PCOLLECTION"))
      assert_that(out, equal_to([(1, 1), (4, 1), (100, 2)]))

  def test_row_user_type(self):
    with TestPipeline() as p:
      out = (
          p | beam.Create([
              UserTypeRow(1, Aribitrary(1.0), 1 + 2.5j),
              UserTypeRow(1, Aribitrary("abc"), -1j),
          ])
          | SqlTransform("SELECT arb, complex FROM PCOLLECTION")
          | beam.Map(tuple))
      assert_that(
          out,
          equal_to([(Aribitrary(1.0), 1 + 2.5j), (Aribitrary("abc"), -1j)]))

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

  def test_sql_ddl_set_option(self):
    with TestPipeline() as p:
      input_data = [
          beam.Row(id=1, value=10),
          beam.Row(id=2, value=20),
          beam.Row(id=3, value=30)
      ]
      # DDL uses SET to modify a session option (tests DDL parsing)
      # Using a known Calcite option like sqlConformance
      ddl_statement = """
          SET sqlConformance = 'LENIENT'
      """
      # Query still operates on the implicit PCOLLECTION
      query_statement = "SELECT * FROM PCOLLECTION WHERE id > 2"

      # Input PCollection is piped directly
      out = (
          p | beam.Create(input_data)
          # Pass both the query and the DDL
          | SqlTransform(query=query_statement, ddl=ddl_statement))

      # Verify the output matches the query (unaffected by the SET DDL)
      assert_that(out, equal_to([(3, 30)]))

  def test_tuple_field(self):
    with TestPipeline() as p:
      out = (
          p
          | beam.Create([
              TupleRow(1, ("foo", 100)),
              TupleRow(2, ("bar", 200)),
          ])
          | SqlTransform(
              "SELECT t.id, t.coords.f0 AS `name`, t.coords.f1 AS `val`, t.coords "
              "FROM PCOLLECTION t WHERE t.coords.f1 > 150"))
      assert_that(out, equal_to([(2, "bar", 200, ("bar", 200))]))


class SqlTransformSchemaTransformConfigTest(unittest.TestCase):
  """Regression guard for the SQL SchemaTransform configuration schema.

  This is NOT an end-to-end test: it does not start an expansion service or
  talk to Java. It hand-builds the schema proto that
  SqlTransformSchemaTransformProvider.configurationSchema() emits in Java for
  ``schematransform:org.apache.beam:sql_transform:v1`` and checks that Python
  can decode it. The Java side is pinned separately by
  SqlTransformSchemaTransformProviderTest, so if the Java schema changes, this
  hand-built copy must be updated by hand.

  Previously ``dialect`` and ``tableproviders`` used the Java-only
  ``javasdk_enum`` logical type and ``parameters`` used ``javasdk_oneof``,
  which made schema discovery fail with "No logical type registered for URN".
  """
  @staticmethod
  def _field(name, atomic_type, nullable=True):
    return schema_pb2.Field(
        name=name,
        type=schema_pb2.FieldType(atomic_type=atomic_type, nullable=nullable))

  def _config_schema(self):
    string_array = schema_pb2.FieldType(
        nullable=True,
        array_type=schema_pb2.ArrayType(
            element_type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)))
    return schema_pb2.Schema(
        fields=[
            self._field('query', schema_pb2.STRING, nullable=False),
            self._field('ddl', schema_pb2.STRING),
            self._field('dialect', schema_pb2.STRING),
            self._field('autoload', schema_pb2.BOOLEAN),
            schema_pb2.Field(name='tableproviders', type=string_array),
        ])

  def test_config_schema_decodes(self):
    from apache_beam.typehints.schemas import named_tuple_from_schema
    config = named_tuple_from_schema(self._config_schema())
    self.assertEqual(
        list(config._fields),
        ['query', 'ddl', 'dialect', 'autoload', 'tableproviders'])
    hints = config.__annotations__
    self.assertEqual(hints['query'], str)
    self.assertEqual(hints['dialect'], typing.Optional[str])
    self.assertEqual(hints['autoload'], typing.Optional[bool])
    # Compare structurally; the repr of typing.Sequence varies by Python version.
    non_none = [
        arg for arg in typing.get_args(hints['tableproviders'])
        if arg is not type(None)
    ]
    self.assertEqual(len(non_none), 1)
    self.assertEqual(typing.get_args(non_none[0]), (str, ))
    self.assertIn(type(None), typing.get_args(hints['tableproviders']))

  def test_java_enum_logical_type_is_not_decodable(self):
    # Documents why the Java schema must not use EnumerationType.
    from apache_beam.typehints.schemas import named_tuple_from_schema
    enum_field = schema_pb2.Field(
        name='dialect',
        type=schema_pb2.FieldType(
            nullable=True,
            logical_type=schema_pb2.LogicalType(
                urn='beam:logical_type:javasdk_enum:v1',
                representation=schema_pb2.FieldType(
                    atomic_type=schema_pb2.INT32))))
    with self.assertRaisesRegex(ValueError, 'javasdk_enum'):
      named_tuple_from_schema(schema_pb2.Schema(fields=[enum_field]))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
