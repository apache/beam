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

"""Package for SqlTransform and related classes."""

# pytype: skip-file

import collections.abc
import inspect
import typing
import warnings

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = ['SqlTransform']

SqlTransformSchema = typing.NamedTuple(
    'SqlTransformSchema', [('query', str), ('dialect', typing.Optional[str])])


class SqlTransform(ExternalTransform):
  """A transform that can translate a SQL query into PTransforms.

  Input PCollections must have a schema. Currently, there are two ways to define
  a schema for a PCollection:

  1) Register a `typing.NamedTuple` type to use RowCoder, and specify it as the
     output type. For example::

      Purchase = typing.NamedTuple('Purchase',
                                   [('item_name', unicode), ('price', float)])
      coders.registry.register_coder(Purchase, coders.RowCoder)
      with Pipeline() as p:
        purchases = (p | beam.io...
                       | beam.Map(..).with_output_types(Purchase))

  2) Produce `beam.Row` instances. Note this option will fail if Beam is unable
     to infer data types for any of the fields. For example::

      with Pipeline() as p:
        purchases = (p | beam.io...
                       | beam.Map(lambda x: beam.Row(item_name=unicode(..),
                                                     price=float(..))))

  Similarly, the output of SqlTransform is a PCollection with a schema.
  The columns produced by the query can be accessed as attributes. For example::

    purchases | SqlTransform(\"\"\"
                  SELECT item_name, COUNT(*) AS `count`
                  FROM PCOLLECTION GROUP BY item_name\"\"\")
              | beam.Map(lambda row: "We've sold %d %ss!" % (row.count,
                                                             row.item_name))

  Additional examples can be found in
  `apache_beam.examples.wordcount_xlang_sql`, `apache_beam.examples.sql_taxi`,
  and `apache_beam.transforms.sql_test`.

  For more details about Beam SQL in general, see the `Java transform
  <https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/SqlTransform.html>`_,
  and the `documentation
  <https://beam.apache.org/documentation/dsls/sql/overview/>`_.
  """
  URN = 'beam:external:java:sql:v1'

  def __init__(
      self,
      query: typing.Optional[str] = None,
      dialect: typing.Optional[str] = None,
      expansion_service: typing.Optional[str] = None,
      sql_transform_schema: typing.Optional[typing.NamedTuple] = None,
  ):
    """Creates a SqlTransform which will be expanded to Java's SqlTransform.

    (See class docs).

    Args:
      query: The SQL query string. Required if sql_transform_schema is not
        provided. Ignored if sql_transform_schema is provided.
      dialect: (Optional) The dialect, e.g. use 'zetasql' for ZetaSQL.
        Ignored if sql_transform_schema is provided.
      expansion_service: (Optional) The URL of the expansion service to use.
      sql_transform_schema: (Optional) A NamedTuple instance containing the
        query and dialect information. If provided, it must have a 'query'
        field (str) and an optional 'dialect' field (str or None). Takes
        precedence over the 'query' and 'dialect' arguments.
    """
    expansion_service = expansion_service or BeamJarExpansionService(
        ':sdks:java:extensions:sql:expansion-service:shadowJar')

    schema_to_use = None
    if sql_transform_schema is not None:
      # Validate the provided schema
      if not (isinstance(sql_transform_schema, tuple) and
              hasattr(sql_transform_schema, '_fields') and
              hasattr(sql_transform_schema, '_asdict') and
              isinstance(getattr(sql_transform_schema, '_fields', None),
                         collections.abc.Sequence) and
              'query' in sql_transform_schema._fields):
        raise TypeError(
            "sql_transform_schema must be a NamedTuple-like object with at "
            f"least a 'query' field, but got {type(sql_transform_schema)}.")
      if not isinstance(sql_transform_schema.query, str):
        raise TypeError(
            "The 'query' field in the provided sql_transform_schema must be a "
            f"string, but got {type(sql_transform_schema.query)}.")

      schema_to_use = sql_transform_schema

      # Warn if query or dialect are also provided
      if query is not None or dialect is not None:
        caller_frame = inspect.currentframe().f_back
        caller_info = inspect.getframeinfo(caller_frame)
        # TODO(BEAM-12900): Convert this to a warning category BeamDeprecationWarning
        # once available.
        warnings.warn(
            f"'query' and 'dialect' parameters are ignored when "
            f"'sql_transform_schema' is provided. Called from "
            f"{caller_info.filename}:{caller_info.lineno}",
            UserWarning)

    elif query is not None:
      if not isinstance(query, str):
        raise TypeError(
            f"Parameter 'query' must be a string, but got {type(query)}.")
      schema_to_use = SqlTransformSchema(query=query, dialect=dialect)
    else:
      raise ValueError(
          "Either 'query' or 'sql_transform_schema' must be provided.")

    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(schema_to_use),
        expansion_service=expansion_service)
