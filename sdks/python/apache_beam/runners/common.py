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
from apache_beam.metrics.execution import ScopedMetricsContainer
from apache_beam.pvalue import SideOutputValue
from apache_beam.transforms import core
from apache_beam.transforms import window
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
               state=None,
               scoped_metrics_container=None):
    """Initializes a DoFnRunner.

    Args:
      fn: user DoFn to invoke
      args: positional side input arguments (static and placeholder), if any
      kwargs: keyword side input arguments (static and placeholder), if any
      side_inputs: list of sideinput.SideInputMaps for deferred side inputs
      windowing: windowing properties of the output PCollection(s)
      context: a DoFnContext to use (deprecated)
      tagged_receivers: a dict of tag name to Receiver objects
      logger: a logging module (deprecated)
      step_name: the name of this step
      logging_context: a LoggingContext object
      state: handle for accessing DoFn state
      scoped_metrics_container: Context switcher for metrics container
    """
    self.step_name = step_name
    self.window_fn = windowing.windowfn
    self.tagged_receivers = tagged_receivers
    self.scoped_metrics_container = (scoped_metrics_container
                                     or ScopedMetricsContainer())

    global_window = window.GlobalWindow()

    # Need to support multiple iterations.
    side_inputs = list(side_inputs)

    if logging_context:
      self.logging_context = logging_context
    else:
      self.logging_context = get_logging_context(logger, step_name=step_name)

    # Optimize for the common case.
    self.main_receivers = as_receiver(tagged_receivers[None])

    # TODO(sourabh): Deprecate the use of context
    if state:
      assert context is None
      self.context = DoFnContext(self.step_name, state=state)
    else:
      assert context is not None
      self.context = context

    # TODO(Sourabhbajaj): Remove the usage of OldDoFn
    if isinstance(fn, core.NewDoFn):
      self.is_new_dofn = True

      # Stash values for use in new_dofn_process.
      self.side_inputs = side_inputs
      self.has_windowed_side_inputs = not all(
          si.is_globally_windowed() for si in self.side_inputs)

      self.args = args if args else []
      self.kwargs = kwargs if kwargs else {}
      self.dofn = fn

    else:
      self.is_new_dofn = False
      self.has_windowed_side_inputs = False  # Set to True in one case below.
      if not args and not kwargs:
        self.dofn = fn
        self.dofn_process = fn.process
      else:
        if side_inputs and all(
            side_input.is_globally_windowed() for side_input in side_inputs):
          args, kwargs = util.insert_values_in_args(
              args, kwargs, [side_input[global_window]
                             for side_input in side_inputs])
          side_inputs = []
        if side_inputs:
          self.has_windowed_side_inputs = True

          def process(context):
            w = context.windows[0]
            cur_args, cur_kwargs = util.insert_values_in_args(
                args, kwargs, [side_input[w] for side_input in side_inputs])
            return fn.process(context, *cur_args, **cur_kwargs)
          self.dofn_process = process
        elif kwargs:
          self.dofn_process = lambda context: fn.process(
              context, *args, **kwargs)
        else:
          self.dofn_process = lambda context: fn.process(context, *args)

        class CurriedFn(core.DoFn):

          start_bundle = staticmethod(fn.start_bundle)
          process = staticmethod(self.dofn_process)
          finish_bundle = staticmethod(fn.finish_bundle)

        self.dofn = CurriedFn()

  def receive(self, windowed_value):
    self.process(windowed_value)

  def old_dofn_process(self, element):
    if self.has_windowed_side_inputs and len(element.windows) > 1:
      for w in element.windows:
        self.context.set_element(
            WindowedValue(element.value, element.timestamp, (w,)))
        self._process_outputs(element, self.dofn_process(self.context))
    else:
      self.context.set_element(element)
      self._process_outputs(element, self.dofn_process(self.context))

  def new_dofn_process(self, element):
    self.context.set_element(element)
    arguments, _, _, defaults = self.dofn.get_function_arguments('process')
    defaults = defaults if defaults else []

    self_in_args = int(self.dofn.is_process_bounded())

    # Call for the process function for each window if has windowed side inputs
    # or if the process accesses the window parameter. We can just call it once
    # otherwise as none of the arguments are changing
    if self.has_windowed_side_inputs or core.NewDoFn.WindowParam in defaults:
      windows = element.windows
    else:
      windows = [window.GlobalWindow()]

    for w in windows:
      args, kwargs = util.insert_values_in_args(
          self.args, self.kwargs,
          [s[w] for s in self.side_inputs])

      # If there are more arguments than the default then the first argument
      # should be the element and the rest should be picked from the side
      # inputs as window and timestamp should always be tagged
      if len(arguments) > len(defaults) + self_in_args:
        if core.NewDoFn.ElementParam not in defaults:
          args_to_pick = len(arguments) - len(defaults) - 1 - self_in_args
          final_args = [element.value] + args[:args_to_pick]
        else:
          args_to_pick = len(arguments) - len(defaults) - self_in_args
          final_args = args[:args_to_pick]
      else:
        args_to_pick = 0
        final_args = []
      args = iter(args[args_to_pick:])

      for a, d in zip(arguments[-len(defaults):], defaults):
        if d == core.NewDoFn.ElementParam:
          final_args.append(element.value)
        elif d == core.NewDoFn.ContextParam:
          final_args.append(self.context)
        elif d == core.NewDoFn.WindowParam:
          final_args.append(w)
        elif d == core.NewDoFn.TimestampParam:
          final_args.append(element.timestamp)
        elif d == core.NewDoFn.SideInputParam:
          # If no more args are present then the value must be passed via kwarg
          try:
            final_args.append(args.next())
          except StopIteration:
            if a not in kwargs:
              raise
        else:
          # If no more args are present then the value must be passed via kwarg
          try:
            final_args.append(args.next())
          except StopIteration:
            if a not in kwargs:
              kwargs[a] = d
      final_args.extend(list(args))
      self._process_outputs(element, self.dofn.process(*final_args, **kwargs))

  def _invoke_bundle_method(self, method):
    try:
      self.logging_context.enter()
      self.scoped_metrics_container.enter()
      self.context.set_element(None)
      f = getattr(self.dofn, method)

      # TODO(Sourabhbajaj): Remove this if-else
      if self.is_new_dofn:
        _, _, _, defaults = self.dofn.get_function_arguments(method)
        defaults = defaults if defaults else []
        args = [self.context if d == core.NewDoFn.ContextParam else d
                for d in defaults]
        self._process_outputs(None, f(*args))
      else:
        self._process_outputs(None, f(self.context))
    except BaseException as exn:
      self.reraise_augmented(exn)
    finally:
      self.scoped_metrics_container.exit()
      self.logging_context.exit()

  def start(self):
    self._invoke_bundle_method('start_bundle')

  def finish(self):
    self._invoke_bundle_method('finish_bundle')

  def process(self, element):
    try:
      self.logging_context.enter()
      self.scoped_metrics_container.enter()
      if self.is_new_dofn:
        self.new_dofn_process(element)
      else:
        self.old_dofn_process(element)
    except BaseException as exn:
      self.reraise_augmented(exn)
    finally:
      self.scoped_metrics_container.exit()
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
