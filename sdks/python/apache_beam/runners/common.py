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

# cython: profile=True

"""Worker operations executor."""

import sys

from apache_beam.internal import util
from apache_beam.pvalue import SideOutputValue
from apache_beam.transforms import core
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.utils.windowed_value import WindowedValue


class LoggingContext(object):

  def enter(self):
    pass

  def exit(self):
    pass


class Receiver(object):
  """An object that consumes a WindowedValue.

  This class can be efficiently used to pass values between the
  sdk and worker harnesses.
  """

  def receive(self, windowed_value):
    raise NotImplementedError


class DoFnRunner(Receiver):
  """A helper class for executing ParDo operations.
  """

  def __init__(self,
               fn,
               args,
               kwargs,
               side_inputs,
               windowing,
               context=None,
               tagged_receivers=None,
               logger=None,
               step_name=None,
               # Preferred alternative to logger
               # TODO(robertwb): Remove once all runners are updated.
               logging_context=None,
               # Preferred alternative to context
               # TODO(robertwb): Remove once all runners are updated.
               state=None):
    if not args and not kwargs:
      self.dofn = fn
      self.dofn_process = fn.process
    else:
      args, kwargs = util.insert_values_in_args(args, kwargs, side_inputs)

      class CurriedFn(core.DoFn):

        def start_bundle(self, context):
          return fn.start_bundle(context)

        def process(self, context):
          return fn.process(context, *args, **kwargs)

        def finish_bundle(self, context):
          return fn.finish_bundle(context)
      self.dofn = CurriedFn()
      self.dofn_process = lambda context: fn.process(context, *args, **kwargs)

    self.window_fn = windowing.windowfn
    self.tagged_receivers = tagged_receivers
    self.step_name = step_name

    if state:
      assert context is None
      self.context = DoFnContext(self.step_name, state=state)
    else:
      assert context is not None
      self.context = context

    if logging_context:
      self.logging_context = logging_context
    else:
      self.logging_context = get_logging_context(logger, step_name=step_name)

    # Optimize for the common case.
    self.main_receivers = as_receiver(tagged_receivers[None])

  def receive(self, windowed_value):
    self.process(windowed_value)

  def start(self):
    self.context.set_element(None)
    try:
      self.logging_context.enter()
      self._process_outputs(None, self.dofn.start_bundle(self.context))
    except BaseException as exn:
      self.reraise_augmented(exn)
    finally:
      self.logging_context.exit()

  def finish(self):
    self.context.set_element(None)
    try:
      self.logging_context.enter()
      self._process_outputs(None, self.dofn.finish_bundle(self.context))
    except BaseException as exn:
      self.reraise_augmented(exn)
    finally:
      self.logging_context.exit()

  def process(self, element):
    try:
      self.logging_context.enter()
      self.context.set_element(element)
      self._process_outputs(element, self.dofn_process(self.context))
    except BaseException as exn:
      self.reraise_augmented(exn)
    finally:
      self.logging_context.exit()

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
        windowed_value = element.with_value(result)
      if tag is None:
        self.main_receivers.receive(windowed_value)
      else:
        self.tagged_receivers[tag].output(windowed_value)


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

  def __init__(self, counter_factory):
    self.step_name = ''
    self._counter_factory = counter_factory

  def counter_for(self, aggregator):
    """Looks up the counter for this aggregator, creating one if necessary."""
    return self._counter_factory.get_aggregator_counter(
        self.step_name, aggregator)


# TODO(robertwb): Replace core.DoFnContext with this.
class DoFnContext(object):

  def __init__(self, label, element=None, state=None):
    self.label = label
    self.state = state
    if element is not None:
      self.set_element(element)

  def set_element(self, windowed_value):
    self.windowed_value = windowed_value

  @property
  def element(self):
    if self.windowed_value is None:
      raise AttributeError('element not accessible in this context')
    else:
      return self.windowed_value.value

  @property
  def timestamp(self):
    if self.windowed_value is None:
      raise AttributeError('timestamp not accessible in this context')
    else:
      return self.windowed_value.timestamp

  @property
  def windows(self):
    if self.windowed_value is None:
      raise AttributeError('windows not accessible in this context')
    else:
      return self.windowed_value.windows

  def aggregate_to(self, aggregator, input_value):
    self.state.counter_for(aggregator).update(input_value)


# TODO(robertwb): Remove all these adapters once service is updated out.


class _LoggingContextAdapter(LoggingContext):

  def __init__(self, underlying):
    self.underlying = underlying

  def enter(self):
    self.underlying.enter()

  def exit(self):
    self.underlying.exit()


def get_logging_context(maybe_logger, **kwargs):
  if maybe_logger:
    maybe_context = maybe_logger.PerThreadLoggingContext(**kwargs)
    if isinstance(maybe_context, LoggingContext):
      return maybe_context
    else:
      return _LoggingContextAdapter(maybe_context)
  else:
    return LoggingContext()


class _ReceiverAdapter(Receiver):

  def __init__(self, underlying):
    self.underlying = underlying

  def receive(self, windowed_value):
    self.underlying.output(windowed_value)


def as_receiver(maybe_receiver):
  if isinstance(maybe_receiver, Receiver):
    return maybe_receiver
  else:
    return _ReceiverAdapter(maybe_receiver)
