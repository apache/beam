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

from typing import NamedTuple


class Row(object):
  """A dynamic schema'd row object.

  This objects attributes are initialized from the keywords passed into its
  constructor, e.g. Row(x=3, y=4) will create a Row with two attributes x and y.

  More importantly, when a Row object is returned from a `Map`, `FlatMap`, or
  `DoFn` type inference is able to deduce the schema of the resulting
  PCollection, e.g.

      pc | beam.Map(lambda x: Row(x=x, y=0.5 * x))

  when applied to a PCollection of ints will produce a PCollection with schema
  `(x=int, y=float)`.

  Note that in Beam 2.30.0 and later, Row objects are sensitive to field order.
  So `Row(x=3, y=4)` is not considered equal to `Row(y=4, x=3)`.
  """
  def __init__(self, **kwargs):
    self.__dict__.update(kwargs)

  def as_dict(self):
    return dict(self.__dict__)

  # For compatibility with named tuples.
  _asdict = as_dict

  def __iter__(self):
    for _, value in self.__dict__.items():
      yield value

  def __repr__(self):
    return 'Row(%s)' % ', '.join('%s=%r' % kv for kv in self.__dict__.items())

  def __hash__(self):
    return hash(self.__dict__.items())

  def __eq__(self, other):
    if type(self) == type(other):
      other_dict = other.__dict__
    elif type(other) == type(NamedTuple):
      other_dict = other._asdict()
    else:
      return False
    return (
        len(self.__dict__) == len(other_dict) and
        all(s == o for s, o in zip(self.__dict__.items(), other_dict.items())))

  def __reduce__(self):
    return _make_Row, tuple(self.__dict__.items())


def _make_Row(*items):
  return Row(**dict(items))
