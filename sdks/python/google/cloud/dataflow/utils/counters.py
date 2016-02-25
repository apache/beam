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


class Counter(object):
  """A counter aggregates a series of values.

  The aggregation kind of the Counter is specified when the Counter
  is created.  The values aggregated must be of an appropriate for the
  aggregation used.  Aggregations supported are listed in the code.

  (The aggregated value will be reported to the Dataflow service.)

  Attributes:
    name: the name of the counter, a string
    aggregation_kind: one of the aggregation kinds defined by this class.
    total: the total size of all the items passed to update()
    elements: the number of times update() was called
  """

  # Aggregation kinds.  The protocol uses string names, so the values
  # assigned here are not externally visible.

  # Numeric:
  SUM = 1
  MAX = 2
  MIN = 3
  MEAN = 4  # arithmetic mean

  # Boolean
  AND = 5
  OR = 6

  _KIND_NAME_MAP = {SUM: 'SUM', MAX: 'MAX', MIN: 'MIN',
                    MEAN: 'MEAN', AND: 'AND', OR: 'OR'}

  def aggregation_kind_str(self):
    return self._KIND_NAME_MAP.get(self.aggregation_kind,
                                   'kind%d' % self.aggregation_kind)

  def __init__(self, name, aggregation_kind):
    """Creates a Counter object.

    Args:
      name: the name of this counter.  Typically has three parts:
        "step-output-counter".
      aggregation_kind: one of the kinds defined by this class.
    """
    self.name = name
    self.aggregation_kind = aggregation_kind
    self.total = 0
    self.elements = 0

  def update(self, count):
    self.total += count
    self.elements += 1

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s %s %s/%s' % (self.name, self.aggregation_kind_str(),
                            self.total, self.elements)


class AggregatorCounter(Counter):
  """A Counter that represents a step-specific instance of an Aggregator."""

  def __init__(self, step_name, aggregator):
    super(AggregatorCounter, self).__init__(
        'user-%s-%s' % (step_name, aggregator.name),
        aggregator.aggregation_kind)
