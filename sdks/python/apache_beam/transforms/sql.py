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

from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = ['SqlTransform']

SqlTransformSchema = typing.NamedTuple(
    'SqlTransformSchema', [('query', unicode)])


class SqlTransform(ExternalTransform):
  """A transform that can translate a SQL query into PTransforms.

  Input PCollections must have a schema. Currently, this means the PCollection
  *must* have a NamedTuple output type, and that type must be registered to use
  RowCoder. For example::

    Purchase = typing.NamedTuple('Purchase',
                                 [('item_name', unicode), ('price', float)])
    coders.registry.register_coder(Purchase, coders.RowCoder)

  Similarly, the output of SqlTransform is a PCollection with a generated
  NamedTuple type, and columns can be accessed as fields. For example::

    purchases | SqlTransform(\"\"\"
                  SELECT item_name, COUNT(*) AS `count`
                  FROM PCOLLECTION GROUP BY item_name\"\"\")
              | beam.Map(lambda row: "We've sold %d %ss!" % (row.count,
                                                             row.item_name))

  Additional examples can be found in
  `apache_beam.examples.wordcount_xlang_sql`, and
  `apache_beam.transforms.sql_test`.

  For more details about Beam SQL in general see the `Java transform
  <https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/SqlTransform.html>`_,
  and the `documentation
  <https://beam.apache.org/documentation/dsls/sql/overview/>`_.
  """
  URN = 'beam:external:java:sql:v1'

  def __init__(self, query, expansion_service=None):
    """
    Creates a SqlTransform which will be expanded to Java's SqlTransform.
    (See class docs).
    :param query: The SQL query.
    :param expansion_service: (optional) The URL of the expansion service to use
    """
    super(SqlTransform, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(SqlTransformSchema(query=query)),
        expansion_service=expansion_service)
