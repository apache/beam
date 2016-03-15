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

# cython: profile=True

"""Worker operations executor."""

import sys

from google.cloud.dataflow.internal import util
from google.cloud.dataflow.pvalue import SideOutputValue
from google.cloud.dataflow.transforms import core
from google.cloud.dataflow.transforms.window import TimestampedValue
from google.cloud.dataflow.transforms.window import WindowedValue
from google.cloud.dataflow.transforms.window import WindowFn
from google.cloud.dataflow.utils import counters


class FakeLogger(object):
  def PerThreadLoggingContext(self, *unused_args, **unused_kwargs):
    return self
  def __enter__(self):
    pass
  def __exit__(self, *unused_args):
    pass


class DoFnRunner(object):
  """A helper class for executing ParDo operations.
  """

  def __init__(self,
               fn,
               args,
               kwargs,
               side_inputs,
               windowing,
               context,
               tagged_receivers,
               tagged_counters,
               logger=None,
               step_name=None):
    if not args and not kwargs:
      self.dofn = fn
    else:
      args, kwargs = util.insert_values_in_args(args, kwargs, side_inputs)

      class CurriedFn(core.DoFn):

        def start_bundle(self, context):
          return fn.start_bundle(context, *args, **kwargs)

        def process(self, context):
          return fn.process(context, *args, **kwargs)

        def finish_bundle(self, context):
          return fn.finish_bundle(context, *args, **kwargs)
      self.dofn = CurriedFn()
    self.window_fn = windowing.windowfn
    self.context = context
    self.tagged_receivers = tagged_receivers
    self.tagged_counters = tagged_counters
    self.logger = logger or FakeLogger()
    self.step_name = step_name

    # Optimize for the common case.
    self.main_receivers = tagged_receivers[None]
    self.main_counters = tagged_counters[None]

  def start(self):
    self.context.set_element(None)
    try:
      self._process_outputs(None, self.dofn.start_bundle(self.context))
    except BaseException as exn:
      raise self.augment_exception(exn)

  def finish(self):
    self.context.set_element(None)
    try:
      self._process_outputs(None, self.dofn.finish_bundle(self.context))
    except BaseException as exn:
      raise self.augment_exception(exn)

  def process(self, element):
    try:
      with self.logger.PerThreadLoggingContext(step_name=self.step_name):
        assert isinstance(element, WindowedValue)
        self.context.set_element(element)
        self._process_outputs(element, self.dofn.process(self.context))
    except BaseException as exn:
      self.reraise_augmented(exn)

  def reraise_augmented(self, exn):
    if getattr(exn, '_tagged_with_step', False) or not self.step_name:
      raise
    args = exn.args
    if args and isinstance(args[0], str):
      args = (args[0] + " [while running '%s']" % self.step_name,) + args[1:]
      # Poor man's exception chaining.
      raise type(exn), args, sys.exc_info()[2]
    else:
      raise

  def _process_outputs(self, element, results):
    """Dispatch the result of computation to the appropriate receivers.

    A value wrapped in a SideOutputValue object will be unwrapped and
    then dispatched to the appropriate indexed output.
    """
    if results is None:
      return
    for result in results:
      tag = None
      if isinstance(result, SideOutputValue):
        tag = result.tag
        if not isinstance(tag, basestring):
          raise TypeError('In %s, tag %s is not a string' % (self, tag))
        result = result.value
      if isinstance(result, WindowedValue):
        windowed_value = result
      elif element is None:
        # Start and finish have no element from which to grab context,
        # but may emit elements.
        if isinstance(result, TimestampedValue):
          value = result.value
          timestamp = result.timestamp
          assign_context = NoContext(value, timestamp)
        else:
          value = result
          timestamp = -1
          assign_context = NoContext(value)
        windowed_value = WindowedValue(
            value, timestamp, self.window_fn.assign(assign_context))
      elif isinstance(result, TimestampedValue):
        assign_context = WindowFn.AssignContext(
            result.timestamp, result.value, element.windows)
        windowed_value = WindowedValue(
            result.value, result.timestamp,
            self.window_fn.assign(assign_context))
      else:
        windowed_value = WindowedValue(
            result, element.timestamp, element.windows)
      # TODO(robertwb): Should the counters be on the context?
      if tag is None:
        self.main_counters.update(windowed_value)
        for receiver in self.main_receivers:
          receiver.process(windowed_value)
      else:
        self.tagged_counters[tag].update(windowed_value)
        for receiver in self.tagged_receivers[tag]:
          receiver.process(windowed_value)

class NoContext(WindowFn.AssignContext):
  """An uninspectable WindowFn.AssignContext."""
  NO_VALUE = object()
  def __init__(self, value, timestamp=NO_VALUE):
    self.value = value
    self._timestamp = timestamp
  @property
  def timestamp(self):
    if self._timestamp is self.NO_VALUE:
      raise ValueError('No timestamp in this context.')
    else:
      return self._timestamp
  @property
  def existing_windows(self):
    raise ValueError('No existing_windows in this context.')


class DoFnState(object):
  """Keeps track of state that DoFns want, currently, user counters.
  """

  def __init__(self):
    self.step_name = ''
    self._user_counters = {}

  def counter_for(self, aggregator):
    """Looks up the counter for this aggregator, creating one if necessary."""
    if aggregator not in self._user_counters:
      self._user_counters[aggregator] = counters.AggregatorCounter(
          self.step_name, aggregator)
    return self._user_counters[aggregator]

  def itercounters(self):
    """Returns an iterable of Counters (to be sent to the service)."""
    return self._user_counters.values()
