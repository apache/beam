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

"""A transform that creates a PCollection from an iterable."""

from apache_beam import PTransform, Read
from apache_beam.io import iobase


class Create(PTransform):
  def __init__(self, value):
    """Initializes a Create transform.

    Args:
      value: An object of values for the PCollection
    """
    super(Create, self).__init__()
    if isinstance(value, basestring):
      raise TypeError('PTransform Create: Refusing to treat string as '
                      'an iterable. (string=%r)' % value)
    elif isinstance(value, dict):
      value = value.items()
    self.value = tuple(value)


  def expand(self, pvalue):
    return pvalue.pipeline | Read(self._source)


class _CreateSource(iobase.BoundedSource):
  def __init__(self, values):
    self._values = values

  def read(self, range_tracker):
    pass

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    pass

  def get_range_tracker(self, start_position, stop_position):
    pass

  def estimate_size(self):
    pass