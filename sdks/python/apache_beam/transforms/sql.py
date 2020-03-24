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

"""Transforms that use the SQL Expansion service."""

# pytype: skip-file

from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

SqlTransformSchema = typing.NamedTuple(
    'SqlTransformSchema', [('query', unicode)])


class SqlTransform(ExternalTransform):
  URN = 'beam:external:java:sql:v1'

  def __init__(self, query):
    super(SqlTransform, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(SqlTransformSchema(query=query)),
        BeamJarExpansionService(
            ':sdks:java:extensions:sql:expansion-service:shadowJar'))
