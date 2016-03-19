# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Support for user-defined Aggregators.

Aggregators allow a pipeline to have the workers do custom aggregation
of statistics about the data processed.  To update an aggregator's value,
call aggregate_to() on the context passed to a DoFn.

Example:
import google.cloud.dataflow as df

simple_counter = df.Aggregator('example-counter')

class ExampleDoFn(df.DoFn):
  def process(self, context):
    context.aggregate_to(simple_counter, 1)
    ...

The aggregators defined here show up in the UI as "Custom counters."

You can also query the combined value(s) of an aggregator by calling
aggregated_value() or aggregated_values() on the result of running a
pipeline.

"""

from __future__ import absolute_import

from google.cloud.dataflow.transforms import combiners
from google.cloud.dataflow.utils.counters import Counter


class Aggregator(object):
  """A user-specified aggregator of statistics about pipeline data.

  Args:
    combine_fn: how to combine values input to the aggregation.
      It must be one of these arithmetic functions:

       - Python's built-in sum
       - Python's built-in min
       - Python's built-in max
       - df.Mean()

      The default is sum.

    type: describes the numeric type that will be accepted as input
      for aggregation; by default types appropriate to the combine_fn
      are accepted.

  Example uses::

    import google.cloud.dataflow as df
    simple_counter = df.Aggregator('example-counter')
    complex_counter = df.Aggregator('other-counter', df.Mean(), float)
  """

  def __init__(self,
               name,
               combine_fn=sum,
               input_type=None):  # inferred from combine_fn
    self.name = name
    self.combine_fn = combine_fn
    self.aggregation_kind = self._aggregator_counter_kind(combine_fn)
    self.input_type = input_type

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    """Internal helper function for both __str__ and __repr__."""
    def get_name(thing):
      try:
        return thing.__name__
      except AttributeError:
        return thing.__class__.__name__

    combine_fn_str = get_name(self.combine_fn)
    input_arg = '(%s)' % get_name(self.input_type) if self.input_type else ''
    if combine_fn_str == 'sum' and not input_arg:
      combine_call = ''
    else:
      combine_call = ' %s%s' % (combine_fn_str, input_arg)
    return 'Aggregator %s%s' % (self.name, combine_call)

  @staticmethod
  def _aggregator_counter_kind(combine_fn):
    """Returns the counter aggregation kind for the combine_fn passed in.

    Args:
      combine_fn: The combining function used in an Aggregator.

    Returns:
      The aggregation_kind (to use in a Counter) that matches combine_fn.

    Raises:
      ValueError if the combine_fn doesn't map to any supported
      aggregation kind.
    """
    # We don't have combiner types that implement AND or OR.
    combine_kind_map = {sum: Counter.SUM, max: Counter.MAX, min: Counter.MIN,
                        combiners.Mean: Counter.MEAN}
    try:
      return combine_kind_map[combine_fn]
    except KeyError:
      try:
        return combine_kind_map[combine_fn.__class__]
      except KeyError:
        raise ValueError(
            'combine_fn %r (class %r) '
            'does not map to a supported aggregation kind'
            % (combine_fn, combine_fn.__class__))
