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

import typing

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

  For more details about Beam SQL in general see the `Java transform
  <https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/SqlTransform.html>`_,
  and the `documentation
  <https://beam.apache.org/documentation/dsls/sql/overview/>`_.
  """
  URN = 'beam:external:java:sql:v1'

  def __init__(self, query, dialect=None, expansion_service=None):
    """
    Creates a SqlTransform which will be expanded to Java's SqlTransform.
    (See class docs).
    :param query: The SQL query.
    :param dialect: (optional) The dialect, e.g. use 'zetasql' for ZetaSQL.
    :param expansion_service: (optional) The URL of the expansion service to use
    """
    expansion_service = expansion_service or BeamJarExpansionService(
        ':sdks:java:extensions:sql:expansion-service:shadowJar')
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            SqlTransformSchema(query=query, dialect=dialect)),
        expansion_service=expansion_service)
