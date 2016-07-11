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

"""Support for user-defined Aggregators.

Aggregators allow steps in a pipeline to perform custom aggregation of
statistics about the data processed across all workers.  To update an
aggregator's value, call aggregate_to() on the context passed to a DoFn.

Example:
import apache_beam as beam

class ExampleDoFn(beam.DoFn):
  def __init__(self):
    super(ExampleDoFn, self).__init__()
    self.simple_counter = beam.Aggregator('example-counter')

  def process(self, context):
    context.aggregate_to(self.simple_counter, 1)
    ...

These aggregators may be used by runners to collect and present statistics of
a pipeline.  For example, in the Google Cloud Dataflow console, aggregators and
their values show up in the UI under "Custom counters."

You can also query the combined value(s) of an aggregator by calling
aggregated_value() or aggregated_values() on the result object returned after
running a pipeline.
"""

from __future__ import absolute_import

from apache_beam.transforms import core


class Aggregator(object):
  """A user-specified aggregator of statistics about a pipeline step.

  Args:
    combine_fn: how to combine values input to the aggregation.
      It must be one of these arithmetic functions:

       - Python's built-in sum, min, max, any, and all.
       - beam.combiners.MeanCombineFn()

      The default is sum of 64-bit ints.

    type: describes the type that will be accepted as input
      for aggregation; by default types appropriate to the combine_fn
      are accepted.

  Example uses::

    import apache_beam as beam

    class ExampleDoFn(beam.DoFn):
      def __init__(self):
        super(ExampleDoFn, self).__init__()
        self.simple_counter = beam.Aggregator('example-counter')
        self.complex_counter = beam.Aggregator('other-counter', beam.Mean(),
                                               float)

      def process(self, context):
        context.aggregate_to(self.simple_counter, 1)
        context.aggregate_to(self.complex_counter, float(len(context.element))
  """

  def __init__(self, name, combine_fn=sum, input_type=int):
    combine_fn = core.CombineFn.maybe_from_callable(combine_fn).for_input_type(
        input_type)
    if not _is_supported_kind(combine_fn):
      raise ValueError(
          'combine_fn %r (class %r) '
          'does not map to a supported aggregation kind'
          % (combine_fn, combine_fn.__class__))
    self.name = name
    self.combine_fn = combine_fn
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


def _is_supported_kind(combine_fn):
  # pylint: disable=wrong-import-order, wrong-import-position
  from apache_beam.internal.apiclient import metric_translations
  return combine_fn.__class__ in metric_translations
