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

# pytype: skip-file

from __future__ import absolute_import

from apache_beam.typehints import typehints


class RowTypeConstraint(typehints.TypeConstraint):
  def __init__(self, fields):
    self._fields = tuple(fields)

  def _consistent_with_check_(self, sub):
    return self == sub

  def type_check(self, instance):
    from apache_beam import Row
    return isinstance(instance, Row)

  def _inner_types(self):
    """Iterates over the inner types of the composite type."""
    return [field[1] for field in self._fields]

  def __eq__(self, other):
    return type(self) == type(other) and self._fields == other._fields

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(self._fields)

  def __repr__(self):
    return 'Row(%s)' % ', '.join(
        '%s=%s' % (name, typehints._unified_repr(t)) for name,
        t in self._fields)

  def get_type_for(self, name):
    return dict(self._fields)[name]
