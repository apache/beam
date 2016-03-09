# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Counters collect the progress of the Worker for reporting to the service."""

from __future__ import absolute_import

from google.cloud.dataflow.utils.counters import Counter


class OperationCounters(object):
  """The set of basic counters to attach to an Operation."""

  def __init__(self, step_name, output_index=0):
    self.element_counter = Counter(
        '%s-out%d-ElementCount' % (step_name, output_index), Counter.SUM)

  def update(self, windowed_value):  # pylint: disable=unused-argument
    """Add one value to this counter."""
    self.element_counter.update(1)

  def __iter__(self):
    """Iterator over all our counters."""
    yield self.element_counter

  def __str__(self):
    return '<%s [%s]>' % (self.__class__.__name__,
                          ', '.join([str(x) for x in self.__iter__()]))

  def __repr__(self):
    return '<%s %s at %s>' % (self.__class__.__name__,
                              [x for x in self.__iter__()], hex(id(self)))
