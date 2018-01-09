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

"""Worker operations executor.

For internal use only; no backwards-compatibility guarantees.
"""

import sys
import traceback

import six

from apache_beam.internal import util
from apache_beam.metrics.execution import ScopedMetricsContainer
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import DoFn
from apache_beam.transforms import core
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.utils.windowed_value import WindowedValue


class LoggingContext(object):
  """For internal use only; no backwards-compatibility guarantees."""

  def enter(self):
    pass

  def exit(self):
    pass


class Receiver(object):
  """For internal use only; no backwards-compatibility guarantees.

  An object that consumes a WindowedValue.

  This class can be efficiently used to pass values between the
  sdk and worker harnesses.
  """

  def receive(self, windowed_value):
    raise NotImplementedError


class MethodWrapper(object):
  """For internal use only; no backwards-compatibility guarantees.

  Represents a method that can be invoked by `DoFnInvoker`."""

  def __init__(self, obj_to_invoke, method_name):
    """
    Initiates a ``MethodWrapper``.

    Args:
      obj_to_invoke: the object that contains the method. Has to either be a
                    `DoFn` object or a `RestrictionProvider` object.
      method_name: name of the method as a string.
    """

    if not isinstance(obj_to_invoke, (DoFn, RestrictionProvider)):
      raise ValueError('\'obj_to_invoke\' has to be either a \'DoFn\' or '
                       'a \'RestrictionProvider\'. Received %r instead.',
                       obj_to_invoke)

    args, _, _, defaults = core.get_function_arguments(
        obj_to_invoke, method_name)
    defaults = defaults if defaults else []
    method_value = getattr(obj_to_invoke, method_name)
    self.method_value = method_value
    self.args = args
    self.defaults = defaults


class DoFnSignature(object):
  """Represents the signature of a given ``DoFn`` object.

  Signature of a ``DoFn`` provides a view of the properties of a given ``DoFn``.
  Among other things, this will give an extensible way for for (1) accessing the
  structure of the ``DoFn`` including methods and method parameters
  (2) identifying features that a given ``DoFn`` support, for example, whether
  a given ``DoFn`` is a Splittable ``DoFn`` (
  https://s.apache.org/splittable-do-fn) (3) validating a ``DoFn`` based on the
  feature set offered by it.
  """

  def __init__(self, do_fn):
    # We add a property here for all methods defined by Beam DoFn features.

    assert isinstance(do_fn, core.DoFn)
    self.do_fn = do_fn

    self.process_method = MethodWrapper(do_fn, 'process')
    self.start_bundle_method = MethodWrapper(do_fn, 'start_bundle')
    self.finish_bundle_method = MethodWrapper(do_fn, 'finish_bundle')

    restriction_provider = self._get_restriction_provider(do_fn)
    self.initial_restriction_method = (
        MethodWrapper(restriction_provider, 'initial_restriction')
        if restriction_provider else None)
    self.restriction_coder_method = (
        MethodWrapper(restriction_provider, 'restriction_coder')
        if restriction_provider else None)
    self.create_tracker_method = (
        MethodWrapper(restriction_provider, 'create_tracker')
        if restriction_provider else None)
    self.split_method = (
        MethodWrapper(restriction_provider, 'split')
        if restriction_provider else None)

    self._validate()

  def _get_restriction_provider(self, do_fn):
    result = _find_param_with_default(self.process_method,
                                      default_as_type=RestrictionProvider)
    return result[1] if result else None

  def _validate(self):
    self._validate_process()
    self._validate_bundle_method(self.start_bundle_method)
    self._validate_bundle_method(self.finish_bundle_method)

  def _validate_process(self):
    """Validate that none of the DoFnParameters are repeated in the function
    """
    for param in core.DoFn.DoFnParams:
      assert self.process_method.defaults.count(param) <= 1

  def _validate_bundle_method(self, method_wrapper):
    """Validate that none of the DoFnParameters are used in the function
    """
    for param in core.DoFn.DoFnParams:
      assert param not in method_wrapper.defaults

  def is_splittable_dofn(self):
    return any([isinstance(default, RestrictionProvider) for default in
                self.process_method.defaults])


class DoFnInvoker(object):
  """An abstraction that can be used to execute DoFn methods.

  A DoFnInvoker describes a particular way for invoking methods of a DoFn
  represented by a given DoFnSignature."""

  def __init__(self, output_processor, signature):
    self.output_processor = output_processor
    self.signature = signature

  @staticmethod
  def create_invoker(
      signature,
      output_processor=None,
      context=None, side_inputs=None, input_args=None, input_kwargs=None,
      process_invocation=True):
    """ Creates a new DoFnInvoker based on given arguments.

    Args:
        output_processor: an OutputProcessor for receiving elements produced by
                          invoking functions of the DoFn.
        signature: a DoFnSignature for the DoFn being invoked.
        context: Context to be used when invoking the DoFn (deprecated).
        side_inputs: side inputs to be used when invoking th process method.
        input_args: arguments to be used when invoking the process method
        input_kwargs: kwargs to be used when invoking the process method.
        process_invocation: If True, this function may return an invoker that
                            performs extra optimizations for invoking process()
                            method efficiently.
    """
    side_inputs = side_inputs or []
    default_arg_values = signature.process_method.defaults
    use_simple_invoker = not process_invocation or (
        not side_inputs and not input_args and not input_kwargs and
        not default_arg_values)
    if use_simple_invoker:
      return SimpleInvoker(output_processor, signature)
    else:
      return PerWindowInvoker(
          output_processor,
          signature, context, side_inputs, input_args, input_kwargs)

  def invoke_process(self, windowed_value, restriction_tracker=None,
                     output_processor=None):
    """Invokes the DoFn.process() function.

    Args:
      windowed_value: a WindowedValue object that gives the element for which
                      process() method should be invoked along with the window
                      the element belongs to.
      output_procesor: if provided given OutputProcessor will be used.
    """
    raise NotImplementedError

  def invoke_start_bundle(self):
    """Invokes the DoFn.start_bundle() method.
    """
    self.output_processor.start_bundle_outputs(
        self.signature.start_bundle_method.method_value())

  def invoke_finish_bundle(self):
    """Invokes the DoFn.finish_bundle() method.
    """
    self.output_processor.finish_bundle_outputs(
        self.signature.finish_bundle_method.method_value())

  def invoke_split(self, element, restriction):
    return self.signature.split_method.method_value(element, restriction)

  def invoke_initial_restriction(self, element):
    return self.signature.initial_restriction_method.method_value(element)

  def invoke_restriction_coder(self):
    return self.signature.restriction_coder_method.method_value()

  def invoke_create_tracker(self, restriction):
    return self.signature.create_tracker_method.method_value(restriction)


def _find_param_with_default(
    method, default_as_value=None, default_as_type=None):
  if ((default_as_value and default_as_type) or
      not (default_as_value or default_as_type)):
    raise ValueError(
        'Exactly one of \'default_as_value\' and \'default_as_type\' should be '
        'provided. Received %r and %r.', default_as_value, default_as_type)

  defaults = method.defaults
  default_as_value = default_as_value
  default_as_type = default_as_type
  ret = None
  for i, value in enumerate(defaults):
    if default_as_value and value == default_as_value:
      ret = (method.args[len(method.args) - len(defaults) + i], value)
    elif default_as_type and isinstance(value, default_as_type):
      index = len(method.args) - len(defaults) + i
      ret = (method.args[index], value)

  return ret


class SimpleInvoker(DoFnInvoker):
  """An invoker that processes elements ignoring windowing information."""

  def __init__(self, output_processor, signature):
    super(SimpleInvoker, self).__init__(output_processor, signature)
    self.process_method = signature.process_method.method_value

  def invoke_process(self, windowed_value, restriction_tracker=None,
                     output_processor=None):
    if not output_processor:
      output_processor = self.output_processor
    output_processor.process_outputs(
        windowed_value, self.process_method(windowed_value.value))


class PerWindowInvoker(DoFnInvoker):
  """An invoker that processes elements considering windowing information."""

  def __init__(self, output_processor, signature, context,
               side_inputs, input_args, input_kwargs):
    super(PerWindowInvoker, self).__init__(output_processor, signature)
    self.side_inputs = side_inputs
    self.context = context
    self.process_method = signature.process_method.method_value
    default_arg_values = signature.process_method.defaults
    self.has_windowed_inputs = (
        not all(si.is_globally_windowed() for si in side_inputs) or
        (core.DoFn.WindowParam in default_arg_values))

    # Try to prepare all the arguments that can just be filled in
    # without any additional work. in the process function.
    # Also cache all the placeholders needed in the process function.

    # Fill in sideInputs if they are globally windowed
    global_window = GlobalWindow()

    input_args = input_args if input_args else []
    input_kwargs = input_kwargs if input_kwargs else {}

    if not self.has_windowed_inputs:
      input_args, input_kwargs = util.insert_values_in_args(
          input_args, input_kwargs, [si[global_window] for si in side_inputs])

    arguments = signature.process_method.args
    defaults = signature.process_method.defaults

    # Create placeholder for element parameter of DoFn.process() method.
    self_in_args = int(signature.do_fn.is_process_bounded())

    class ArgPlaceholder(object):
      def __init__(self, placeholder):
        self.placeholder = placeholder

    if core.DoFn.ElementParam not in default_arg_values:
      args_to_pick = len(arguments) - len(default_arg_values) - 1 - self_in_args
      args_with_placeholders = (
          [ArgPlaceholder(core.DoFn.ElementParam)] + input_args[:args_to_pick])
    else:
      args_to_pick = len(arguments) - len(defaults) - self_in_args
      args_with_placeholders = input_args[:args_to_pick]

    # Fill the OtherPlaceholders for context, window or timestamp
    remaining_args_iter = iter(input_args[args_to_pick:])
    for a, d in zip(arguments[-len(defaults):], defaults):
      if d == core.DoFn.ElementParam:
        args_with_placeholders.append(ArgPlaceholder(d))
      elif d == core.DoFn.WindowParam:
        args_with_placeholders.append(ArgPlaceholder(d))
      elif d == core.DoFn.TimestampParam:
        args_with_placeholders.append(ArgPlaceholder(d))
      elif d == core.DoFn.SideInputParam:
        # If no more args are present then the value must be passed via kwarg
        try:
          args_with_placeholders.append(next(remaining_args_iter))
        except StopIteration:
          if a not in input_kwargs:
            raise ValueError("Value for sideinput %s not provided" % a)
      else:
        # If no more args are present then the value must be passed via kwarg
        try:
          args_with_placeholders.append(next(remaining_args_iter))
        except StopIteration:
          pass
    args_with_placeholders.extend(list(remaining_args_iter))

    # Stash the list of placeholder positions for performance
    self.placeholders = [(i, x.placeholder) for (i, x) in enumerate(
        args_with_placeholders)
                         if isinstance(x, ArgPlaceholder)]

    self.args_for_process = args_with_placeholders
    self.kwargs_for_process = input_kwargs

  def invoke_process(self, windowed_value, restriction_tracker=None,
                     output_processor=None):
    if not output_processor:
      output_processor = self.output_processor
    self.context.set_element(windowed_value)
    # Call for the process function for each window if has windowed side inputs
    # or if the process accesses the window parameter. We can just call it once
    # otherwise as none of the arguments are changing

    additional_kwargs = {}
    if restriction_tracker:
      restriction_tracker_param = _find_param_with_default(
          self.signature.process_method,
          default_as_type=core.RestrictionProvider)[0]
      if not restriction_tracker_param:
        raise ValueError(
            'A RestrictionTracker %r was provided but DoFn does not have a '
            'RestrictionTrackerParam defined', restriction_tracker)
      additional_kwargs[restriction_tracker_param] = restriction_tracker
    if self.has_windowed_inputs and len(windowed_value.windows) != 1:
      for w in windowed_value.windows:
        self._invoke_per_window(
            WindowedValue(windowed_value.value, windowed_value.timestamp, (w,)),
            additional_kwargs, output_processor)
    else:
      self._invoke_per_window(
          windowed_value, additional_kwargs, output_processor)

  def _invoke_per_window(
      self, windowed_value, additional_kwargs, output_processor):
    if self.has_windowed_inputs:
      window, = windowed_value.windows
      args_for_process, kwargs_for_process = util.insert_values_in_args(
          self.args_for_process, self.kwargs_for_process,
          [si[window] for si in self.side_inputs])
    else:
      args_for_process, kwargs_for_process = (
          self.args_for_process, self.kwargs_for_process)
    # TODO(sourabhbajaj): Investigate why we can't use `is` instead of ==
    for i, p in self.placeholders:
      if p == core.DoFn.ElementParam:
        args_for_process[i] = windowed_value.value
      elif p == core.DoFn.WindowParam:
        args_for_process[i] = window
      elif p == core.DoFn.TimestampParam:
        args_for_process[i] = windowed_value.timestamp

    if additional_kwargs:
      if kwargs_for_process is None:
        kwargs_for_process = additional_kwargs
      else:
        for key in additional_kwargs:
          kwargs_for_process[key] = additional_kwargs[key]

    if kwargs_for_process:
      output_processor.process_outputs(
          windowed_value,
          self.process_method(*args_for_process, **kwargs_for_process))
    else:
      output_processor.process_outputs(
          windowed_value, self.process_method(*args_for_process))


class DoFnRunner(Receiver):
  """For internal use only; no backwards-compatibility guarantees.

  A helper class for executing ParDo operations.
  """

  def __init__(self,
               fn,
               args,
               kwargs,
               side_inputs,
               windowing,
               tagged_receivers=None,
               step_name=None,
               logging_context=None,
               state=None,
               scoped_metrics_container=None):
    """Initializes a DoFnRunner.

    Args:
      fn: user DoFn to invoke
      args: positional side input arguments (static and placeholder), if any
      kwargs: keyword side input arguments (static and placeholder), if any
      side_inputs: list of sideinput.SideInputMaps for deferred side inputs
      windowing: windowing properties of the output PCollection(s)
      tagged_receivers: a dict of tag name to Receiver objects
      step_name: the name of this step
      logging_context: a LoggingContext object
      state: handle for accessing DoFn state
      scoped_metrics_container: Context switcher for metrics container
    """
    # Need to support multiple iterations.
    side_inputs = list(side_inputs)

    self.scoped_metrics_container = (
        scoped_metrics_container or ScopedMetricsContainer())
    self.step_name = step_name
    self.logging_context = logging_context or LoggingContext()
    self.context = DoFnContext(step_name, state=state)

    do_fn_signature = DoFnSignature(fn)

    # Optimize for the common case.
    main_receivers = tagged_receivers[None]
    output_processor = _OutputProcessor(
        windowing.windowfn, main_receivers, tagged_receivers)

    self.do_fn_invoker = DoFnInvoker.create_invoker(
        do_fn_signature, output_processor, self.context, side_inputs, args,
        kwargs)

  def receive(self, windowed_value):
    self.process(windowed_value)

  def process(self, windowed_value):
    try:
      self.logging_context.enter()
      self.scoped_metrics_container.enter()
      self.do_fn_invoker.invoke_process(windowed_value)
    except BaseException as exn:
      self._reraise_augmented(exn)
    finally:
      self.scoped_metrics_container.exit()
      self.logging_context.exit()

  def _invoke_bundle_method(self, bundle_method):
    try:
      self.logging_context.enter()
      self.scoped_metrics_container.enter()
      self.context.set_element(None)
      bundle_method()
    except BaseException as exn:
      self._reraise_augmented(exn)
    finally:
      self.scoped_metrics_container.exit()
      self.logging_context.exit()

  def start(self):
    self._invoke_bundle_method(self.do_fn_invoker.invoke_start_bundle)

  def finish(self):
    self._invoke_bundle_method(self.do_fn_invoker.invoke_finish_bundle)

  def _reraise_augmented(self, exn):
    if getattr(exn, '_tagged_with_step', False) or not self.step_name:
      raise
    step_annotation = " [while running '%s']" % self.step_name
    # To emulate exception chaining (not available in Python 2).
    original_traceback = sys.exc_info()[2]
    try:
      # Attempt to construct the same kind of exception
      # with an augmented message.
      new_exn = type(exn)(exn.args[0] + step_annotation, *exn.args[1:])
      new_exn._tagged_with_step = True  # Could raise attribute error.
    except:  # pylint: disable=bare-except
      # If anything goes wrong, construct a RuntimeError whose message
      # records the original exception's type and message.
      new_exn = RuntimeError(
          traceback.format_exception_only(type(exn), exn)[-1].strip()
          + step_annotation)
      new_exn._tagged_with_step = True
    six.raise_from(new_exn, original_traceback)


class OutputProcessor(object):

  def process_outputs(self, windowed_input_element, results):
    raise NotImplementedError


class _OutputProcessor(OutputProcessor):
  """Processes output produced by DoFn method invocations."""

  def __init__(self, window_fn, main_receivers, tagged_receivers):
    """Initializes ``_OutputProcessor``.

    Args:
      window_fn: a windowing function (WindowFn).
      main_receivers: a dict of tag name to Receiver objects.
      tagged_receivers: main receiver object.
    """
    self.window_fn = window_fn
    self.main_receivers = main_receivers
    self.tagged_receivers = tagged_receivers

  def process_outputs(self, windowed_input_element, results):
    """Dispatch the result of process computation to the appropriate receivers.

    A value wrapped in a TaggedOutput object will be unwrapped and
    then dispatched to the appropriate indexed output.
    """
    if results is None:
      return

    for result in results:
      tag = None
      if isinstance(result, TaggedOutput):
        tag = result.tag
        if not isinstance(tag, basestring):
          raise TypeError('In %s, tag %s is not a string' % (self, tag))
        result = result.value
      if isinstance(result, WindowedValue):
        windowed_value = result
        if (windowed_input_element is not None
            and len(windowed_input_element.windows) != 1):
          windowed_value.windows *= len(windowed_input_element.windows)
      elif isinstance(result, TimestampedValue):
        assign_context = WindowFn.AssignContext(result.timestamp, result.value)
        windowed_value = WindowedValue(
            result.value, result.timestamp,
            self.window_fn.assign(assign_context))
        if len(windowed_input_element.windows) != 1:
          windowed_value.windows *= len(windowed_input_element.windows)
      else:
        windowed_value = windowed_input_element.with_value(result)
      if tag is None:
        self.main_receivers.receive(windowed_value)
      else:
        self.tagged_receivers[tag].receive(windowed_value)

  def start_bundle_outputs(self, results):
    """Validate that start_bundle does not output any elements"""
    if results is None:
      return
    raise RuntimeError(
        'Start Bundle should not output any elements but got %s' % results)

  def finish_bundle_outputs(self, results):
    """Dispatch the result of finish_bundle to the appropriate receivers.

    A value wrapped in a TaggedOutput object will be unwrapped and
    then dispatched to the appropriate indexed output.
    """
    if results is None:
      return

    for result in results:
      tag = None
      if isinstance(result, TaggedOutput):
        tag = result.tag
        if not isinstance(tag, basestring):
          raise TypeError('In %s, tag %s is not a string' % (self, tag))
        result = result.value

      if isinstance(result, WindowedValue):
        windowed_value = result
      else:
        raise RuntimeError('Finish Bundle should only output WindowedValue ' +\
                           'type but got %s' % type(result))

      if tag is None:
        self.main_receivers.receive(windowed_value)
      else:
        self.tagged_receivers[tag].receive(windowed_value)


class _NoContext(WindowFn.AssignContext):
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
  """For internal use only; no backwards-compatibility guarantees.

  Keeps track of state that DoFns want, currently, user counters.
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
  """For internal use only; no backwards-compatibility guarantees."""

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
