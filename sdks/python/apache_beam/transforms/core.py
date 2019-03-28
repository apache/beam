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

"""Core PTransform subclasses, such as FlatMap, GroupByKey, and Map."""

from __future__ import absolute_import

import copy
import logging
import random
import re
import types
from builtins import map
from builtins import object
from builtins import range

from past.builtins import unicode

from apache_beam import coders
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.coders import typecoders
from apache_beam.internal import pickler
from apache_beam.internal import util
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import ptransform
from apache_beam.transforms import userstate
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.ptransform import PTransformWithSideInputs
from apache_beam.transforms.userstate import StateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.typehints import KV
from apache_beam.typehints import Any
from apache_beam.typehints import Iterable
from apache_beam.typehints import Union
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.decorators import TypeCheckError
from apache_beam.typehints.decorators import WithTypeHints
from apache_beam.typehints.decorators import get_type_hints
from apache_beam.typehints.decorators import getfullargspec
from apache_beam.typehints.trivial_inference import element_type
from apache_beam.typehints.typehints import is_consistent_with
from apache_beam.utils import urns

__all__ = [
    'DoFn',
    'CombineFn',
    'PartitionFn',
    'ParDo',
    'FlatMap',
    'Map',
    'Filter',
    'CombineGlobally',
    'CombinePerKey',
    'CombineValues',
    'GroupByKey',
    'Partition',
    'Windowing',
    'WindowInto',
    'Flatten',
    'Create',
    'Impulse',
    ]

# Type variables
T = typehints.TypeVariable('T')
K = typehints.TypeVariable('K')
V = typehints.TypeVariable('V')


class DoFnContext(object):
  """A context available to all methods of DoFn instance."""
  pass


class DoFnProcessContext(DoFnContext):
  """A processing context passed to DoFn process() during execution.

  Experimental; no backwards-compatibility guarantees.

  Most importantly, a DoFn.process method will access context.element
  to get the element it is supposed to process.

  Attributes:
    label: label of the ParDo whose element is being processed.
    element: element being processed
      (in process method only; always None in start_bundle and finish_bundle)
    timestamp: timestamp of the element
      (in process method only; always None in start_bundle and finish_bundle)
    windows: windows of the element
      (in process method only; always None in start_bundle and finish_bundle)
    state: a DoFnState object, which holds the runner's internal state
      for this element.
      Not used by the pipeline code.
  """

  def __init__(self, label, element=None, state=None):
    """Initialize a processing context object with an element and state.

    The element represents one value from a PCollection that will be accessed
    by a DoFn object during pipeline execution, and state is an arbitrary object
    where counters and other pipeline state information can be passed in.

    DoFnProcessContext objects are also used as inputs to PartitionFn instances.

    Args:
      label: label of the PCollection whose element is being processed.
      element: element of a PCollection being processed using this context.
      state: a DoFnState object with state to be passed in to the DoFn object.
    """
    self.label = label
    self.state = state
    if element is not None:
      self.set_element(element)

  def set_element(self, windowed_value):
    if windowed_value is None:
      # Not currently processing an element.
      if hasattr(self, 'element'):
        del self.element
        del self.timestamp
        del self.windows
    else:
      self.element = windowed_value.value
      self.timestamp = windowed_value.timestamp
      self.windows = windowed_value.windows


class ProcessContinuation(object):
  """An object that may be produced as the last element of a process method
    invocation.

  Experimental; no backwards-compatibility guarantees.

  If produced, indicates that there is more work to be done for the current
  input element.
  """

  def __init__(self, resume_delay=0):
    """Initializes a ProcessContinuation object.

    Args:
      resume_delay: indicates the minimum time, in seconds, that should elapse
        before re-invoking process() method for resuming the invocation of the
        current element.
    """
    self.resume_delay = resume_delay

  @staticmethod
  def resume(resume_delay=0):
    """A convenient method that produces a ``ProcessContinuation``.

    Args:
      resume_delay: delay after which processing current element should be
        resumed.
    Returns: a ``ProcessContinuation`` for signalling the runner that current
      input element has not been fully processed and should be resumed later.
    """
    return ProcessContinuation(resume_delay=resume_delay)


class RestrictionProvider(object):
  """Provides methods for generating and manipulating restrictions.

  This class should be implemented to support Splittable ``DoFn``s in Python
  SDK. See https://s.apache.org/splittable-do-fn for more details about
  Splittable ``DoFn``s.

  To denote a ``DoFn`` class to be Splittable ``DoFn``, ``DoFn.process()``
  method of that class should have exactly one parameter whose default value is
  an instance of ``RestrictionProvider``.

  The provided ``RestrictionProvider`` instance must provide suitable overrides
  for the following methods.
  * create_tracker()
  * initial_restriction()

  Optionally, ``RestrictionProvider`` may override default implementations of
  following methods.
  * restriction_coder()
  * split()

  ** Pausing and resuming processing of an element **

  As the last element produced by the iterator returned by the
  ``DoFn.process()`` method, a Splittable ``DoFn`` may return an object of type
  ``ProcessContinuation``.

  If provided, ``ProcessContinuation`` object specifies that runner should
  later re-invoke ``DoFn.process()`` method to resume processing the current
  element and the manner in which the re-invocation should be performed. A
  ``ProcessContinuation`` object must only be specified as the last element of
  the iterator. If a ``ProcessContinuation`` object is not provided the runner
  will assume that the current input element has been fully processed.

  ** Updating output watermark **

  ``DoFn.process()`` method of Splittable ``DoFn``s could contain a parameter
  with default value ``DoFn.WatermarkReporterParam``. If specified this asks the
  runner to provide a function that can be used to give the runner a
  (best-effort) lower bound about the timestamps of future output associated
  with the current element processed by the ``DoFn``. If the ``DoFn`` has
  multiple outputs, the watermark applies to all of them. Provided function must
  be invoked with a single parameter of type ``Timestamp`` or as an integer that
  gives the watermark in number of seconds.
  """

  def create_tracker(self, restriction):
    """Produces a new ``RestrictionTracker`` for the given restriction.

    Args:
      restriction: an object that defines a restriction as identified by a
        Splittable ``DoFn`` that utilizes the current ``RestrictionProvider``.
        For example, a tuple that gives a range of positions for a Splittable
        ``DoFn`` that reads files based on byte positions.
    Returns: an object of type ``RestrictionTracker``.
    """
    raise NotImplementedError

  def initial_restriction(self, element):
    """Produces an initial restriction for the given element."""
    raise NotImplementedError

  def split(self, element, restriction):
    """Splits the given element and restriction.

    Returns an iterator of restrictions. The total set of elements produced by
    reading input element for each of the returned restrictions should be the
    same as the total set of elements produced by reading the input element for
    the input restriction.

    TODO(chamikara): give suitable hints for performing splitting, for example
    number of parts or size in bytes.
    """
    yield restriction

  def restriction_coder(self):
    """Returns a ``Coder`` for restrictions.

    Returned``Coder`` will be used for the restrictions produced by the current
    ``RestrictionProvider``.

    Returns:
      an object of type ``Coder``.
    """
    return coders.registry.get_coder(object)


def get_function_arguments(obj, func):
  """Return the function arguments based on the name provided. If they have
  a _inspect_function attached to the class then use that otherwise default
  to the modified version of python inspect library.
  """
  func_name = '_inspect_%s' % func
  if hasattr(obj, func_name):
    f = getattr(obj, func_name)
    return f()
  f = getattr(obj, func)
  return getfullargspec(f)


class _DoFnParam(object):
  """DoFn parameter."""

  def __init__(self, param_id):
    self.param_id = param_id

  def __eq__(self, other):
    if type(self) == type(other):
      return self.param_id == other.param_id
    return False

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash(self.param_id)

  def __repr__(self):
    return self.param_id


class _StateDoFnParam(_DoFnParam):
  """State DoFn parameter."""

  def __init__(self, state_spec):
    if not isinstance(state_spec, StateSpec):
      raise ValueError("DoFn.StateParam expected StateSpec object.")
    self.state_spec = state_spec
    self.param_id = 'StateParam(%s)' % state_spec.name


class _TimerDoFnParam(_DoFnParam):
  """Timer DoFn parameter."""

  def __init__(self, timer_spec):
    if not isinstance(timer_spec, TimerSpec):
      raise ValueError("DoFn.TimerParam expected TimerSpec object.")
    self.timer_spec = timer_spec
    self.param_id = 'TimerParam(%s)' % timer_spec.name


class _BundleFinalizerParam(_DoFnParam):
  """Bundle Finalization DoFn parameter."""

  def __init__(self):
    self._callbacks = []
    self.param_id = "FinalizeBundle"

  def register(self, callback):
    self._callbacks.append(callback)

  # Log errors when calling callback to make sure all callbacks get called
  # though there are errors. And errors should not fail pipeline.
  def finalize_bundle(self):
    for callback in self._callbacks:
      try:
        callback()
      except Exception as e:
        logging.warn("Got exception from finalization call: %s", e)

  def has_callbacks(self):
    return len(self._callbacks) > 0

  def reset(self):
    del self._callbacks[:]


class DoFn(WithTypeHints, HasDisplayData, urns.RunnerApiFn):
  """A function object used by a transform with custom processing.

  The ParDo transform is such a transform. The ParDo.apply
  method will take an object of type DoFn and apply it to all elements of a
  PCollection object.

  In order to have concrete DoFn objects one has to subclass from DoFn and
  define the desired behavior (start_bundle/finish_bundle and process) or wrap a
  callable object using the CallableWrapperDoFn class.
  """

  # Parameters that can be used in the .process() method.
  ElementParam = _DoFnParam('ElementParam')
  SideInputParam = _DoFnParam('SideInputParam')
  TimestampParam = _DoFnParam('TimestampParam')
  WindowParam = _DoFnParam('WindowParam')
  WatermarkReporterParam = _DoFnParam('WatermarkReporterParam')
  BundleFinalizerParam = _BundleFinalizerParam

  DoFnProcessParams = [ElementParam, SideInputParam, TimestampParam,
                       WindowParam, WatermarkReporterParam,
                       BundleFinalizerParam]

  # Parameters to access state and timers.  Not restricted to use only in the
  # .process() method. Usage: DoFn.StateParam(state_spec),
  # DoFn.TimerParam(timer_spec).
  StateParam = _StateDoFnParam
  TimerParam = _TimerDoFnParam

  @staticmethod
  def from_callable(fn):
    return CallableWrapperDoFn(fn)

  def default_label(self):
    return self.__class__.__name__

  def process(self, element, *args, **kwargs):
    """Method to use for processing elements.

    This is invoked by ``DoFnRunner`` for each element of a input
    ``PCollection``.

    If specified, following default arguments are used by the ``DoFnRunner`` to
    be able to pass the parameters correctly.

    ``DoFn.ElementParam``: element to be processed.
    ``DoFn.SideInputParam``: a side input that may be used when processing.
    ``DoFn.TimestampParam``: timestamp of the input element.
    ``DoFn.WindowParam``: ``Window`` the input element belongs to.
    A ``RestrictionProvider`` instance: an ``iobase.RestrictionTracker`` will be
    provided here to allow treatment as a Splittable `DoFn``.
    ``DoFn.WatermarkReporterParam``: a function that can be used to report
    output watermark of Splittable ``DoFn`` implementations.

    Args:
      element: The element to be processed
      *args: side inputs
      **kwargs: other keyword arguments.
    """
    raise NotImplementedError

  def start_bundle(self):
    """Called before a bundle of elements is processed on a worker.

    Elements to be processed are split into bundles and distributed
    to workers. Before a worker calls process() on the first element
    of its bundle, it calls this method.
    """
    pass

  def finish_bundle(self):
    """Called after a bundle of elements is processed on a worker.
    """
    pass

  def get_function_arguments(self, func):
    return get_function_arguments(self, func)

  # TODO(sourabhbajaj): Do we want to remove the responsibility of these from
  # the DoFn or maybe the runner
  def infer_output_type(self, input_type):
    # TODO(robertwb): Side inputs types.
    # TODO(robertwb): Assert compatibility with input type hint?
    return self._strip_output_annotations(
        trivial_inference.infer_return_type(self.process, [input_type]))

  def _strip_output_annotations(self, type_hint):
    annotations = (TimestampedValue, WindowedValue, pvalue.TaggedOutput)
    # TODO(robertwb): These should be parameterized types that the
    # type inferencer understands.
    if (type_hint in annotations
        or trivial_inference.element_type(type_hint) in annotations):
      return Any
    return type_hint

  def _process_argspec_fn(self):
    """Returns the Python callable that will eventually be invoked.

    This should ideally be the user-level function that is called with
    the main and (if any) side inputs, and is used to relate the type
    hint parameters with the input parameters (e.g., by argument name).
    """
    return self.process

  def is_process_bounded(self):
    """Checks if an object is a bound method on an instance."""
    if not isinstance(self.process, types.MethodType):
      return False # Not a method
    if self.process.__self__ is None:
      return False # Method is not bound
    if issubclass(self.process.__self__.__class__, type) or \
        self.process.__self__.__class__ is type:
      return False # Method is a classmethod
    return True

  urns.RunnerApiFn.register_pickle_urn(python_urns.PICKLED_DOFN)


def _fn_takes_side_inputs(fn):
  try:
    argspec = getfullargspec(fn)
  except TypeError:
    # We can't tell; maybe it does.
    return True
  is_bound = isinstance(fn, types.MethodType) and fn.__self__ is not None

  try:
    varkw = argspec.varkw
    kwonlyargs = argspec.kwonlyargs
  except AttributeError:  # Python 2
    varkw = argspec.keywords
    kwonlyargs = []

  return (len(argspec.args) + len(kwonlyargs) > 1 + is_bound or
          argspec.varargs or varkw)


class CallableWrapperDoFn(DoFn):
  """For internal use only; no backwards-compatibility guarantees.

  A DoFn (function) object wrapping a callable object.

  The purpose of this class is to conveniently wrap simple functions and use
  them in transforms.
  """

  def __init__(self, fn):
    """Initializes a CallableWrapperDoFn object wrapping a callable.

    Args:
      fn: A callable object.

    Raises:
      TypeError: if fn parameter is not a callable type.
    """
    if not callable(fn):
      raise TypeError('Expected a callable object instead of: %r' % fn)

    self._fn = fn
    if isinstance(fn, (
        types.BuiltinFunctionType, types.MethodType, types.FunctionType)):
      self.process = fn
    else:
      # For cases such as set / list where fn is callable but not a function
      self.process = lambda element: fn(element)

    super(CallableWrapperDoFn, self).__init__()

  def display_data(self):
    # If the callable has a name, then it's likely a function, and
    # we show its name.
    # Otherwise, it might be an instance of a callable class. We
    # show its class.
    display_data_value = (self._fn.__name__ if hasattr(self._fn, '__name__')
                          else self._fn.__class__)
    return {'fn': DisplayDataItem(display_data_value,
                                  label='Transform Function')}

  def __repr__(self):
    return 'CallableWrapperDoFn(%s)' % self._fn

  def default_type_hints(self):
    type_hints = get_type_hints(self._fn)
    # If the fn was a DoFn annotated with a type-hint that hinted a return
    # type compatible with Iterable[Any], then we strip off the outer
    # container type due to the 'flatten' portion of FlatMap.
    # TODO(robertwb): Should we require an iterable specification for FlatMap?
    if type_hints.output_types:
      args, kwargs = type_hints.output_types
      if len(args) == 1 and is_consistent_with(args[0], Iterable[Any]):
        type_hints = type_hints.copy()
        type_hints.set_output_types(element_type(args[0]), **kwargs)
    return type_hints

  def infer_output_type(self, input_type):
    return self._strip_output_annotations(
        trivial_inference.infer_return_type(self._fn, [input_type]))

  def _process_argspec_fn(self):
    return getattr(self._fn, '_argspec_fn', self._fn)

  def _inspect_process(self):
    return getfullargspec(self._process_argspec_fn())


class CombineFn(WithTypeHints, HasDisplayData, urns.RunnerApiFn):
  """A function object used by a Combine transform with custom processing.

  A CombineFn specifies how multiple values in all or part of a PCollection can
  be merged into a single value---essentially providing the same kind of
  information as the arguments to the Python "reduce" builtin (except for the
  input argument, which is an instance of CombineFnProcessContext). The
  combining process proceeds as follows:

  1. Input values are partitioned into one or more batches.
  2. For each batch, the create_accumulator method is invoked to create a fresh
     initial "accumulator" value representing the combination of zero values.
  3. For each input value in the batch, the add_input method is invoked to
     combine more values with the accumulator for that batch.
  4. The merge_accumulators method is invoked to combine accumulators from
     separate batches into a single combined output accumulator value, once all
     of the accumulators have had all the input value in their batches added to
     them. This operation is invoked repeatedly, until there is only one
     accumulator value left.
  5. The extract_output operation is invoked on the final accumulator to get
     the output value.

  Note: If this **CombineFn** is used with a transform that has defaults,
  **apply** will be called with an empty list at expansion time to get the
  default value.
  """

  def default_label(self):
    return self.__class__.__name__

  def create_accumulator(self, *args, **kwargs):
    """Return a fresh, empty accumulator for the combine operation.

    Args:
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def add_input(self, accumulator, element, *args, **kwargs):
    """Return result of folding element into accumulator.

    CombineFn implementors must override add_input.

    Args:
      accumulator: the current accumulator
      element: the element to add
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def add_inputs(self, accumulator, elements, *args, **kwargs):
    """Returns the result of folding each element in elements into accumulator.

    This is provided in case the implementation affords more efficient
    bulk addition of elements. The default implementation simply loops
    over the inputs invoking add_input for each one.

    Args:
      accumulator: the current accumulator
      elements: the elements to add
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    for element in elements:
      accumulator = self.add_input(accumulator, element, *args, **kwargs)
    return accumulator

  def merge_accumulators(self, accumulators, *args, **kwargs):
    """Returns the result of merging several accumulators
    to a single accumulator value.

    Args:
      accumulators: the accumulators to merge
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def compact(self, accumulator, *args, **kwargs):
    """Optionally returns a more compact represenation of the accumulator.

    This is called before an accumulator is sent across the wire, and can
    be useful in cases where values are buffered or otherwise lazily
    kept unprocessed when added to the accumulator.  Should return an
    equivalent, though possibly modified, accumulator.

    By default returns the accumulator unmodified.

    Args:
      accumulator: the current accumulator
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    return accumulator

  def extract_output(self, accumulator, *args, **kwargs):
    """Return result of converting accumulator into the output value.

    Args:
      accumulator: the final accumulator value computed by this CombineFn
        for the entire input key or PCollection.
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def apply(self, elements, *args, **kwargs):
    """Returns result of applying this CombineFn to the input values.

    Args:
      elements: the set of values to combine.
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    return self.extract_output(
        self.add_inputs(
            self.create_accumulator(*args, **kwargs), elements,
            *args, **kwargs),
        *args, **kwargs)

  def for_input_type(self, input_type):
    """Returns a specialized implementation of self, if it exists.

    Otherwise, returns self.

    Args:
      input_type: the type of input elements.
    """
    return self

  @staticmethod
  def from_callable(fn):
    return CallableWrapperCombineFn(fn)

  @staticmethod
  def maybe_from_callable(fn, has_side_inputs=True):
    if isinstance(fn, CombineFn):
      return fn
    elif callable(fn) and not has_side_inputs:
      return NoSideInputsCallableWrapperCombineFn(fn)
    elif callable(fn):
      return CallableWrapperCombineFn(fn)
    else:
      raise TypeError('Expected a CombineFn or callable, got %r' % fn)

  def get_accumulator_coder(self):
    return coders.registry.get_coder(object)

  urns.RunnerApiFn.register_pickle_urn(python_urns.PICKLED_COMBINE_FN)


class _ReiterableChain(object):
  """Like itertools.chain, but allowing re-iteration."""
  def __init__(self, iterables):
    self.iterables = iterables

  def __iter__(self):
    for iterable in self.iterables:
      for item in iterable:
        yield item

  def __bool__(self):
    for iterable in self.iterables:
      for _ in iterable:
        return True
    return False


class CallableWrapperCombineFn(CombineFn):
  """For internal use only; no backwards-compatibility guarantees.

  A CombineFn (function) object wrapping a callable object.

  The purpose of this class is to conveniently wrap simple functions and use
  them in Combine transforms.
  """
  _DEFAULT_BUFFER_SIZE = 10

  def __init__(self, fn, buffer_size=_DEFAULT_BUFFER_SIZE):
    """Initializes a CallableFn object wrapping a callable.

    Args:
      fn: A callable object that reduces elements of an iterable to a single
        value (like the builtins sum and max). This callable must be capable of
        receiving the kind of values it generates as output in its input, and
        for best results, its operation must be commutative and associative.

    Raises:
      TypeError: if fn parameter is not a callable type.
    """
    if not callable(fn):
      raise TypeError('Expected a callable object instead of: %r' % fn)

    super(CallableWrapperCombineFn, self).__init__()
    self._fn = fn
    self._buffer_size = buffer_size

  def display_data(self):
    return {'fn_dd': self._fn}

  def __repr__(self):
    return "%s(%s)" % (self.__class__.__name__, self._fn)

  def create_accumulator(self, *args, **kwargs):
    return []

  def add_input(self, accumulator, element, *args, **kwargs):
    accumulator.append(element)
    if len(accumulator) > self._buffer_size:
      accumulator = [self._fn(accumulator, *args, **kwargs)]
    return accumulator

  def add_inputs(self, accumulator, elements, *args, **kwargs):
    accumulator.extend(elements)
    if len(accumulator) > self._buffer_size:
      accumulator = [self._fn(accumulator, *args, **kwargs)]
    return accumulator

  def merge_accumulators(self, accumulators, *args, **kwargs):
    return [self._fn(_ReiterableChain(accumulators), *args, **kwargs)]

  def compact(self, accumulator, *args, **kwargs):
    if len(accumulator) <= 1:
      return accumulator
    else:
      return [self._fn(accumulator, *args, **kwargs)]

  def extract_output(self, accumulator, *args, **kwargs):
    return self._fn(accumulator, *args, **kwargs)

  def default_type_hints(self):
    fn_hints = get_type_hints(self._fn)
    if fn_hints.input_types is None:
      return fn_hints
    else:
      # fn(Iterable[V]) -> V becomes CombineFn(V) -> V
      input_args, input_kwargs = fn_hints.input_types
      if not input_args:
        if len(input_kwargs) == 1:
          input_args, input_kwargs = tuple(input_kwargs.values()), {}
        else:
          raise TypeError('Combiner input type must be specified positionally.')
      if not is_consistent_with(input_args[0], Iterable[Any]):
        raise TypeCheckError(
            'All functions for a Combine PTransform must accept a '
            'single argument compatible with: Iterable[Any]. '
            'Instead a function with input type: %s was received.'
            % input_args[0])
      input_args = (element_type(input_args[0]),) + input_args[1:]
      # TODO(robertwb): Assert output type is consistent with input type?
      hints = fn_hints.copy()
      hints.set_input_types(*input_args, **input_kwargs)
      return hints

  def for_input_type(self, input_type):
    # Avoid circular imports.
    from apache_beam.transforms import cy_combiners
    if self._fn is any:
      return cy_combiners.AnyCombineFn()
    elif self._fn is all:
      return cy_combiners.AllCombineFn()
    else:
      known_types = {
          (sum, int): cy_combiners.SumInt64Fn(),
          (min, int): cy_combiners.MinInt64Fn(),
          (max, int): cy_combiners.MaxInt64Fn(),
          (sum, float): cy_combiners.SumFloatFn(),
          (min, float): cy_combiners.MinFloatFn(),
          (max, float): cy_combiners.MaxFloatFn(),
      }
    return known_types.get((self._fn, input_type), self)


class NoSideInputsCallableWrapperCombineFn(CallableWrapperCombineFn):
  """For internal use only; no backwards-compatibility guarantees.

  A CombineFn (function) object wrapping a callable object with no side inputs.

  This is identical to its parent, but avoids accepting and passing *args
  and **kwargs for efficiency as they are known to be empty.
  """
  def create_accumulator(self):
    return []

  def add_input(self, accumulator, element):
    accumulator.append(element)
    if len(accumulator) > self._buffer_size:
      accumulator = [self._fn(accumulator)]
    return accumulator

  def add_inputs(self, accumulator, elements):
    accumulator.extend(elements)
    if len(accumulator) > self._buffer_size:
      accumulator = [self._fn(accumulator)]
    return accumulator

  def merge_accumulators(self, accumulators):
    return [self._fn(_ReiterableChain(accumulators))]

  def compact(self, accumulator):
    if len(accumulator) <= 1:
      return accumulator
    else:
      return [self._fn(accumulator)]

  def extract_output(self, accumulator):
    return self._fn(accumulator)


class PartitionFn(WithTypeHints):
  """A function object used by a Partition transform.

  A PartitionFn specifies how individual values in a PCollection will be placed
  into separate partitions, indexed by an integer.
  """

  def default_label(self):
    return self.__class__.__name__

  def partition_for(self, element, num_partitions, *args, **kwargs):
    """Specify which partition will receive this element.

    Args:
      element: An element of the input PCollection.
      num_partitions: Number of partitions, i.e., output PCollections.
      *args: optional parameters and side inputs.
      **kwargs: optional parameters and side inputs.

    Returns:
      An integer in [0, num_partitions).
    """
    pass


class CallableWrapperPartitionFn(PartitionFn):
  """For internal use only; no backwards-compatibility guarantees.

  A PartitionFn object wrapping a callable object.

  Instances of this class wrap simple functions for use in Partition operations.
  """

  def __init__(self, fn):
    """Initializes a PartitionFn object wrapping a callable.

    Args:
      fn: A callable object, which should accept the following arguments:
            element - element to assign to a partition.
            num_partitions - number of output partitions.
          and may accept additional arguments and side inputs.

    Raises:
      TypeError: if fn is not a callable type.
    """
    if not callable(fn):
      raise TypeError('Expected a callable object instead of: %r' % fn)
    self._fn = fn

  def partition_for(self, element, num_partitions, *args, **kwargs):
    return self._fn(element, num_partitions, *args, **kwargs)


class ParDo(PTransformWithSideInputs):
  """A :class:`ParDo` transform.

  Processes an input :class:`~apache_beam.pvalue.PCollection` by applying a
  :class:`DoFn` to each element and returning the accumulated results into an
  output :class:`~apache_beam.pvalue.PCollection`. The type of the elements is
  not fixed as long as the :class:`DoFn` can deal with it. In reality the type
  is restrained to some extent because the elements sometimes must be persisted
  to external storage. See the :meth:`.expand()` method comments for a
  detailed description of all possible arguments.

  Note that the :class:`DoFn` must return an iterable for each element of the
  input :class:`~apache_beam.pvalue.PCollection`. An easy way to do this is to
  use the ``yield`` keyword in the process method.

  Args:
    pcoll (~apache_beam.pvalue.PCollection):
      a :class:`~apache_beam.pvalue.PCollection` to be processed.
    fn (DoFn): a :class:`DoFn` object to be applied to each element
      of **pcoll** argument.
    *args: positional arguments passed to the :class:`DoFn` object.
    **kwargs:  keyword arguments passed to the :class:`DoFn` object.

  Note that the positional and keyword arguments will be processed in order
  to detect :class:`~apache_beam.pvalue.PCollection` s that will be computed as
  side inputs to the transform. During pipeline execution whenever the
  :class:`DoFn` object gets executed (its :meth:`DoFn.process()` method gets
  called) the :class:`~apache_beam.pvalue.PCollection` arguments will be
  replaced by values from the :class:`~apache_beam.pvalue.PCollection` in the
  exact positions where they appear in the argument lists.
  """

  def __init__(self, fn, *args, **kwargs):
    super(ParDo, self).__init__(fn, *args, **kwargs)
    # TODO(robertwb): Change all uses of the dofn attribute to use fn instead.
    self.dofn = self.fn
    self.output_tags = set()

    if not isinstance(self.fn, DoFn):
      raise TypeError('ParDo must be called with a DoFn instance.')

    # Validate the DoFn by creating a DoFnSignature
    from apache_beam.runners.common import DoFnSignature
    self._signature = DoFnSignature(self.fn)

  def default_type_hints(self):
    return self.fn.get_type_hints()

  def infer_output_type(self, input_type):
    return trivial_inference.element_type(
        self.fn.infer_output_type(input_type))

  def make_fn(self, fn, has_side_inputs):
    if isinstance(fn, DoFn):
      return fn
    return CallableWrapperDoFn(fn)

  def _process_argspec_fn(self):
    return self.fn._process_argspec_fn()

  def display_data(self):
    return {'fn': DisplayDataItem(self.fn.__class__,
                                  label='Transform Function'),
            'fn_dd': self.fn}

  def expand(self, pcoll):
    # In the case of a stateful DoFn, warn if the key coder is not
    # deterministic.
    if self._signature.is_stateful_dofn():
      kv_type_hint = pcoll.element_type
      if kv_type_hint and kv_type_hint != typehints.Any:
        coder = coders.registry.get_coder(kv_type_hint)
        if not coder.is_kv_coder():
          raise ValueError(
              'Input elements to the transform %s with stateful DoFn must be '
              'key-value pairs.' % self)
        key_coder = coder.key_coder()
      else:
        key_coder = coders.registry.get_coder(typehints.Any)

      if not key_coder.is_deterministic():
        logging.warning(
            'Key coder %s for transform %s with stateful DoFn may not '
            'be deterministic. This may cause incorrect behavior for complex '
            'key types. Consider adding an input type hint for this transform.',
            key_coder, self)

    return pvalue.PCollection(pcoll.pipeline)

  def with_outputs(self, *tags, **main_kw):
    """Returns a tagged tuple allowing access to the outputs of a
    :class:`ParDo`.

    The resulting object supports access to the
    :class:`~apache_beam.pvalue.PCollection` associated with a tag
    (e.g. ``o.tag``, ``o[tag]``) and iterating over the available tags
    (e.g. ``for tag in o: ...``).

    Args:
      *tags: if non-empty, list of valid tags. If a list of valid tags is given,
        it will be an error to use an undeclared tag later in the pipeline.
      **main_kw: dictionary empty or with one key ``'main'`` defining the tag to
        be used for the main output (which will not have a tag associated with
        it).

    Returns:
      ~apache_beam.pvalue.DoOutputsTuple: An object of type
      :class:`~apache_beam.pvalue.DoOutputsTuple` that bundles together all
      the outputs of a :class:`ParDo` transform and allows accessing the
      individual :class:`~apache_beam.pvalue.PCollection` s for each output
      using an ``object.tag`` syntax.

    Raises:
      ~exceptions.TypeError: if the **self** object is not a
        :class:`~apache_beam.pvalue.PCollection` that is the result of a
        :class:`ParDo` transform.
      ~exceptions.ValueError: if **main_kw** contains any key other than
        ``'main'``.
    """
    main_tag = main_kw.pop('main', None)
    if main_kw:
      raise ValueError('Unexpected keyword arguments: %s' %
                       list(main_kw))
    return _MultiParDo(self, tags, main_tag)

  def _pardo_fn_data(self):
    si_tags_and_types = None
    windowing = None
    return self.fn, self.args, self.kwargs, si_tags_and_types, windowing

  def to_runner_api_parameter(self, context):
    assert isinstance(self, ParDo), \
        "expected instance of ParDo, but got %s" % self.__class__
    picked_pardo_fn_data = pickler.dumps(self._pardo_fn_data())
    state_specs, timer_specs = userstate.get_dofn_specs(self.fn)
    from apache_beam.runners.common import DoFnSignature
    is_splittable = DoFnSignature(self.fn).is_splittable_dofn()
    if is_splittable:
      restriction_coder = (
          DoFnSignature(self.fn).get_restriction_provider().restriction_coder())
      restriction_coder_id = context.coders.get_id(restriction_coder)
    else:
      restriction_coder_id = None
    return (
        common_urns.primitives.PAR_DO.urn,
        beam_runner_api_pb2.ParDoPayload(
            do_fn=beam_runner_api_pb2.SdkFunctionSpec(
                environment_id=context.default_environment_id(),
                spec=beam_runner_api_pb2.FunctionSpec(
                    urn=python_urns.PICKLED_DOFN_INFO,
                    payload=picked_pardo_fn_data)),
            splittable=is_splittable,
            restriction_coder_id=restriction_coder_id,
            state_specs={spec.name: spec.to_runner_api(context)
                         for spec in state_specs},
            timer_specs={spec.name: spec.to_runner_api(context)
                         for spec in timer_specs},
            # It'd be nice to name these according to their actual
            # names/positions in the orignal argument list, but such a
            # transformation is currently irreversible given how
            # remove_objects_from_args and insert_values_in_args
            # are currently implemented.
            side_inputs={
                "side%s" % ix: si.to_runner_api(context)
                for ix, si in enumerate(self.side_inputs)}))

  @PTransform.register_urn(
      common_urns.primitives.PAR_DO.urn, beam_runner_api_pb2.ParDoPayload)
  def from_runner_api_parameter(pardo_payload, context):
    assert pardo_payload.do_fn.spec.urn == python_urns.PICKLED_DOFN_INFO
    fn, args, kwargs, si_tags_and_types, windowing = pickler.loads(
        pardo_payload.do_fn.spec.payload)
    if si_tags_and_types:
      raise NotImplementedError('explicit side input data')
    elif windowing:
      raise NotImplementedError('explicit windowing')
    result = ParDo(fn, *args, **kwargs)
    # This is an ordered list stored as a dict (see the comments in
    # to_runner_api_parameter above).
    indexed_side_inputs = [
        (int(re.match('side([0-9]+)(-.*)?$', tag).group(1)),
         pvalue.AsSideInput.from_runner_api(si, context))
        for tag, si in pardo_payload.side_inputs.items()]
    result.side_inputs = [si for _, si in sorted(indexed_side_inputs)]
    return result

  def runner_api_requires_keyed_input(self):
    return userstate.is_stateful_dofn(self.fn)


class _MultiParDo(PTransform):

  def __init__(self, do_transform, tags, main_tag):
    super(_MultiParDo, self).__init__(do_transform.label)
    self._do_transform = do_transform
    self._tags = tags
    self._main_tag = main_tag

  def expand(self, pcoll):
    _ = pcoll | self._do_transform
    return pvalue.DoOutputsTuple(
        pcoll.pipeline, self._do_transform, self._tags, self._main_tag)


def FlatMap(fn, *args, **kwargs):  # pylint: disable=invalid-name
  """:func:`FlatMap` is like :class:`ParDo` except it takes a callable to
  specify the transformation.

  The callable must return an iterable for each element of the input
  :class:`~apache_beam.pvalue.PCollection`. The elements of these iterables will
  be flattened into the output :class:`~apache_beam.pvalue.PCollection`.

  Args:
    fn (callable): a callable object.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`FlatMap` outputs.

  Raises:
    ~exceptions.TypeError: If the **fn** passed as argument is not a callable.
      Typical error is to pass a :class:`DoFn` instance which is supported only
      for :class:`ParDo`.
  """
  label = 'FlatMap(%s)' % ptransform.label_from_callable(fn)
  if not callable(fn):
    raise TypeError(
        'FlatMap can be used only with callable objects. '
        'Received %r instead.' % (fn))

  pardo = ParDo(CallableWrapperDoFn(fn), *args, **kwargs)
  pardo.label = label
  return pardo


def Map(fn, *args, **kwargs):  # pylint: disable=invalid-name
  """:func:`Map` is like :func:`FlatMap` except its callable returns only a
  single element.

  Args:
    fn (callable): a callable object.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`Map` outputs.

  Raises:
    ~exceptions.TypeError: If the **fn** passed as argument is not a callable.
      Typical error is to pass a :class:`DoFn` instance which is supported only
      for :class:`ParDo`.
  """
  if not callable(fn):
    raise TypeError(
        'Map can be used only with callable objects. '
        'Received %r instead.' % (fn))
  if _fn_takes_side_inputs(fn):
    wrapper = lambda x, *args, **kwargs: [fn(x, *args, **kwargs)]
  else:
    wrapper = lambda x: [fn(x)]

  label = 'Map(%s)' % ptransform.label_from_callable(fn)

  # TODO. What about callable classes?
  if hasattr(fn, '__name__'):
    wrapper.__name__ = fn.__name__

  # Proxy the type-hint information from the original function to this new
  # wrapped function.
  get_type_hints(wrapper).input_types = get_type_hints(fn).input_types
  output_hint = get_type_hints(fn).simple_output_type(label)
  if output_hint:
    get_type_hints(wrapper).set_output_types(typehints.Iterable[output_hint])
  # pylint: disable=protected-access
  wrapper._argspec_fn = fn
  # pylint: enable=protected-access

  pardo = FlatMap(wrapper, *args, **kwargs)
  pardo.label = label
  return pardo


def Filter(fn, *args, **kwargs):  # pylint: disable=invalid-name
  """:func:`Filter` is a :func:`FlatMap` with its callable filtering out
  elements.

  Args:
    fn (callable): a callable object.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`Filter` outputs.

  Raises:
    ~exceptions.TypeError: If the **fn** passed as argument is not a callable.
      Typical error is to pass a :class:`DoFn` instance which is supported only
      for :class:`ParDo`.
  """
  if not callable(fn):
    raise TypeError(
        'Filter can be used only with callable objects. '
        'Received %r instead.' % (fn))
  wrapper = lambda x, *args, **kwargs: [x] if fn(x, *args, **kwargs) else []

  label = 'Filter(%s)' % ptransform.label_from_callable(fn)

  # TODO: What about callable classes?
  if hasattr(fn, '__name__'):
    wrapper.__name__ = fn.__name__
  # Proxy the type-hint information from the function being wrapped, setting the
  # output type to be the same as the input type.
  get_type_hints(wrapper).input_types = get_type_hints(fn).input_types
  output_hint = get_type_hints(fn).simple_output_type(label)
  if (output_hint is None
      and get_type_hints(wrapper).input_types
      and get_type_hints(wrapper).input_types[0]):
    output_hint = get_type_hints(wrapper).input_types[0]
  if output_hint:
    get_type_hints(wrapper).set_output_types(typehints.Iterable[output_hint])
  # pylint: disable=protected-access
  wrapper._argspec_fn = fn
  # pylint: enable=protected-access

  pardo = FlatMap(wrapper, *args, **kwargs)
  pardo.label = label
  return pardo


def _combine_payload(combine_fn, context):
  return beam_runner_api_pb2.CombinePayload(
      combine_fn=combine_fn.to_runner_api(context),
      accumulator_coder_id=context.coders.get_id(
          combine_fn.get_accumulator_coder()))


class CombineGlobally(PTransform):
  """A :class:`CombineGlobally` transform.

  Reduces a :class:`~apache_beam.pvalue.PCollection` to a single value by
  progressively applying a :class:`CombineFn` to portions of the
  :class:`~apache_beam.pvalue.PCollection` (and to intermediate values created
  thereby). See documentation in :class:`CombineFn` for details on the specifics
  on how :class:`CombineFn` s are applied.

  Args:
    pcoll (~apache_beam.pvalue.PCollection):
      a :class:`~apache_beam.pvalue.PCollection` to be reduced into a single
      value.
    fn (callable): a :class:`CombineFn` object that will be called to
      progressively reduce the :class:`~apache_beam.pvalue.PCollection` into
      single values, or a callable suitable for wrapping by
      :class:`~apache_beam.transforms.core.CallableWrapperCombineFn`.
    *args: positional arguments passed to the :class:`CombineFn` object.
    **kwargs: keyword arguments passed to the :class:`CombineFn` object.

  Raises:
    ~exceptions.TypeError: If the output type of the input
      :class:`~apache_beam.pvalue.PCollection` is not compatible
      with ``Iterable[A]``.

  Returns:
    ~apache_beam.pvalue.PCollection: A single-element
    :class:`~apache_beam.pvalue.PCollection` containing the main output of
    the :class:`CombineGlobally` transform.

  Note that the positional and keyword arguments will be processed in order
  to detect :class:`~apache_beam.pvalue.PValue` s that will be computed as side
  inputs to the transform.
  During pipeline execution whenever the :class:`CombineFn` object gets executed
  (i.e. any of the :class:`CombineFn` methods get called), the
  :class:`~apache_beam.pvalue.PValue` arguments will be replaced by their
  actual value in the exact position where they appear in the argument lists.
  """
  has_defaults = True
  as_view = False
  fanout = None

  def __init__(self, fn, *args, **kwargs):
    if not (isinstance(fn, CombineFn) or callable(fn)):
      raise TypeError(
          'CombineGlobally can be used only with combineFn objects. '
          'Received %r instead.' % (fn))

    super(CombineGlobally, self).__init__()
    self.fn = fn
    self.args = args
    self.kwargs = kwargs

  def display_data(self):
    return {
        'combine_fn':
            DisplayDataItem(self.fn.__class__, label='Combine Function'),
        'combine_fn_dd':
            self.fn,
    }

  def default_label(self):
    if self.fanout is None:
      return '%s(%s)' % (self.__class__.__name__,
                         ptransform.label_from_callable(self.fn))
    else:
      return '%s(%s, fanout=%s)' % (self.__class__.__name__,
                                    ptransform.label_from_callable(self.fn),
                                    self.fanout)

  def _clone(self, **extra_attributes):
    clone = copy.copy(self)
    clone.__dict__.update(extra_attributes)
    return clone

  def with_fanout(self, fanout):
    return self._clone(fanout=fanout)

  def with_defaults(self, has_defaults=True):
    return self._clone(has_defaults=has_defaults)

  def without_defaults(self):
    return self.with_defaults(False)

  def as_singleton_view(self):
    return self._clone(as_view=True)

  def expand(self, pcoll):
    def add_input_types(transform):
      type_hints = self.get_type_hints()
      if type_hints.input_types:
        return transform.with_input_types(type_hints.input_types[0][0])
      return transform

    combine_per_key = CombinePerKey(self.fn, *self.args, **self.kwargs)
    if self.fanout:
      combine_per_key = combine_per_key.with_hot_key_fanout(self.fanout)

    combined = (pcoll
                | 'KeyWithVoid' >> add_input_types(
                    Map(lambda v: (None, v)).with_output_types(
                        KV[None, pcoll.element_type]))
                | 'CombinePerKey' >> combine_per_key
                | 'UnKey' >> Map(lambda k_v: k_v[1]))

    if not self.has_defaults and not self.as_view:
      return combined

    if self.has_defaults:
      combine_fn = (
          self.fn if isinstance(self.fn, CombineFn)
          else CombineFn.from_callable(self.fn))
      default_value = combine_fn.apply([], *self.args, **self.kwargs)
    else:
      default_value = pvalue.AsSingleton._NO_DEFAULT  # pylint: disable=protected-access
    view = pvalue.AsSingleton(combined, default_value=default_value)
    if self.as_view:
      return view
    else:
      if pcoll.windowing.windowfn != GlobalWindows():
        raise ValueError(
            "Default values are not yet supported in CombineGlobally() if the "
            "output  PCollection is not windowed by GlobalWindows. "
            "Instead, use CombineGlobally().without_defaults() to output "
            "an empty PCollection if the input PCollection is empty, "
            "or CombineGlobally().as_singleton_view() to get the default "
            "output of the CombineFn if the input PCollection is empty.")

      def typed(transform):
        # TODO(robertwb): We should infer this.
        if combined.element_type:
          return transform.with_output_types(combined.element_type)
        return transform
      return (pcoll.pipeline
              | 'DoOnce' >> Create([None])
              | 'InjectDefault' >> typed(Map(lambda _, s: s, view)))


class CombinePerKey(PTransformWithSideInputs):
  """A per-key Combine transform.

  Identifies sets of values associated with the same key in the input
  PCollection, then applies a CombineFn to condense those sets to single
  values. See documentation in CombineFn for details on the specifics on how
  CombineFns are applied.

  Args:
    pcoll: input pcollection.
    fn: instance of CombineFn to apply to all values under the same key in
      pcoll, or a callable whose signature is ``f(iterable, *args, **kwargs)``
      (e.g., sum, max).
    *args: arguments and side inputs, passed directly to the CombineFn.
    **kwargs: arguments and side inputs, passed directly to the CombineFn.

  Returns:
    A PObject holding the result of the combine operation.
  """
  def with_hot_key_fanout(self, fanout):
    """A per-key combine operation like self but with two levels of aggregation.

    If a given key is produced by too many upstream bundles, the final
    reduction can become a bottleneck despite partial combining being lifted
    pre-GroupByKey.  In these cases it can be helpful to perform intermediate
    partial aggregations in parallel and then re-group to peform a final
    (per-key) combine.  This is also useful for high-volume keys in streaming
    where combiners are not generally lifted for latency reasons.

    Note that a fanout greater than 1 requires the data to be sent through
    two GroupByKeys, and a high fanout can also result in more shuffle data
    due to less per-bundle combining. Setting the fanout for a key at 1 or less
    places values on the "cold key" path that skip the intermediate level of
    aggregation.

    Args:
      fanout: either None, for no fanout, an int, for a constant-degree fanout,
          or a callable mapping keys to a key-specific degree of fanout.

    Returns:
      A per-key combining PTransform with the specified fanout.
    """
    from apache_beam.transforms.combiners import curry_combine_fn
    if fanout is None:
      return self
    else:
      return _CombinePerKeyWithHotKeyFanout(
          curry_combine_fn(self.fn, self.args, self.kwargs), fanout)

  def display_data(self):
    return {'combine_fn':
            DisplayDataItem(self.fn.__class__, label='Combine Function'),
            'combine_fn_dd':
            self.fn}

  def make_fn(self, fn, has_side_inputs):
    self._fn_label = ptransform.label_from_callable(fn)
    return CombineFn.maybe_from_callable(fn, has_side_inputs)

  def default_label(self):
    return '%s(%s)' % (self.__class__.__name__, self._fn_label)

  def _process_argspec_fn(self):
    return lambda element, *args, **kwargs: None

  def expand(self, pcoll):
    args, kwargs = util.insert_values_in_args(
        self.args, self.kwargs, self.side_inputs)
    return pcoll | GroupByKey() | 'Combine' >> CombineValues(
        self.fn, *args, **kwargs)

  def default_type_hints(self):
    hints = self.fn.get_type_hints().copy()
    if hints.input_types:
      K = typehints.TypeVariable('K')
      args, kwargs = hints.input_types
      args = (typehints.Tuple[K, args[0]],) + args[1:]
      hints.set_input_types(*args, **kwargs)
    else:
      K = typehints.Any
    if hints.output_types:
      main_output_type = hints.simple_output_type('')
      hints.set_output_types(typehints.Tuple[K, main_output_type])
    return hints

  def to_runner_api_parameter(self, context):
    if self.args or self.kwargs:
      from apache_beam.transforms.combiners import curry_combine_fn
      combine_fn = curry_combine_fn(self.fn, self.args, self.kwargs)
    else:
      combine_fn = self.fn
    return (
        common_urns.composites.COMBINE_PER_KEY.urn,
        _combine_payload(combine_fn, context))

  @PTransform.register_urn(
      common_urns.composites.COMBINE_PER_KEY.urn,
      beam_runner_api_pb2.CombinePayload)
  def from_runner_api_parameter(combine_payload, context):
    return CombinePerKey(
        CombineFn.from_runner_api(combine_payload.combine_fn, context))

  def runner_api_requires_keyed_input(self):
    return True


# TODO(robertwb): Rename to CombineGroupedValues?
class CombineValues(PTransformWithSideInputs):

  def make_fn(self, fn, has_side_inputs):
    return CombineFn.maybe_from_callable(fn, has_side_inputs)

  def expand(self, pcoll):
    args, kwargs = util.insert_values_in_args(
        self.args, self.kwargs, self.side_inputs)

    input_type = pcoll.element_type
    key_type = None
    if input_type is not None:
      key_type, _ = input_type.tuple_types

    runtime_type_check = (
        pcoll.pipeline._options.view_as(TypeOptions).runtime_type_check)
    return pcoll | ParDo(
        CombineValuesDoFn(key_type, self.fn, runtime_type_check),
        *args, **kwargs)

  def to_runner_api_parameter(self, context):
    if self.args or self.kwargs:
      from apache_beam.transforms.combiners import curry_combine_fn
      combine_fn = curry_combine_fn(self.fn, self.args, self.kwargs)
    else:
      combine_fn = self.fn
    return (
        common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
        _combine_payload(combine_fn, context))

  @PTransform.register_urn(
      common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
      beam_runner_api_pb2.CombinePayload)
  def from_runner_api_parameter(combine_payload, context):
    return CombineValues(
        CombineFn.from_runner_api(combine_payload.combine_fn, context))


class CombineValuesDoFn(DoFn):
  """DoFn for performing per-key Combine transforms."""

  def __init__(self, input_pcoll_type, combinefn, runtime_type_check):
    super(CombineValuesDoFn, self).__init__()
    self.combinefn = combinefn
    self.runtime_type_check = runtime_type_check

  def process(self, element, *args, **kwargs):
    # Expected elements input to this DoFn are 2-tuples of the form
    # (key, iter), with iter an iterable of all the values associated with key
    # in the input PCollection.
    if self.runtime_type_check:
      # Apply the combiner in a single operation rather than artificially
      # breaking it up so that output type violations manifest as TypeCheck
      # errors rather than type errors.
      return [
          (element[0],
           self.combinefn.apply(element[1], *args, **kwargs))]

    # Add the elements into three accumulators (for testing of merge).
    elements = list(element[1])
    accumulators = []
    for k in range(3):
      if len(elements) <= k:
        break
      accumulators.append(
          self.combinefn.add_inputs(
              self.combinefn.create_accumulator(*args, **kwargs),
              elements[k::3],
              *args, **kwargs))
    # Merge the accumulators.
    accumulator = self.combinefn.merge_accumulators(
        accumulators, *args, **kwargs)
    # Convert accumulator to the final result.
    return [(element[0],
             self.combinefn.extract_output(accumulator, *args, **kwargs))]

  def default_type_hints(self):
    hints = self.combinefn.get_type_hints().copy()
    if hints.input_types:
      K = typehints.TypeVariable('K')
      args, kwargs = hints.input_types
      args = (typehints.Tuple[K, typehints.Iterable[args[0]]],) + args[1:]
      hints.set_input_types(*args, **kwargs)
    else:
      K = typehints.Any
    if hints.output_types:
      main_output_type = hints.simple_output_type('')
      hints.set_output_types(typehints.Tuple[K, main_output_type])
    return hints


class _CombinePerKeyWithHotKeyFanout(PTransform):

  def __init__(self, combine_fn, fanout):
    self._combine_fn = combine_fn
    self._fanout_fn = (
        (lambda key: fanout) if isinstance(fanout, int) else fanout)

  def default_label(self):
    return '%s(%s, fanout=%s)' % (
        self.__class__.__name__,
        ptransform.label_from_callable(self._combine_fn),
        ptransform.label_from_callable(self._fanout_fn))

  def expand(self, pcoll):

    from apache_beam.transforms.trigger import AccumulationMode
    combine_fn = self._combine_fn
    fanout_fn = self._fanout_fn

    class SplitHotCold(DoFn):

      def start_bundle(self):
        # Spreading a hot key across all possible sub-keys for all bundles
        # would defeat the goal of not overwhelming downstream reducers
        # (as well as making less efficient use of PGBK combining tables).
        # Instead, each bundle independently makes a consistent choice about
        # which "shard" of a key to send its intermediate results.
        self._nonce = int(random.getrandbits(31))

      def process(self, element):
        key, value = element
        fanout = fanout_fn(key)
        if fanout <= 1:
          # Boolean indicates this is not an accumulator.
          yield (key, (False, value))  # cold
        else:
          yield pvalue.TaggedOutput('hot', ((self._nonce % fanout, key), value))

    class PreCombineFn(CombineFn):
      @staticmethod
      def extract_output(accumulator):
        # Boolean indicates this is an accumulator.
        return (True, accumulator)
      create_accumulator = combine_fn.create_accumulator
      add_input = combine_fn.add_input
      merge_accumulators = combine_fn.merge_accumulators
      compact = combine_fn.compact

    class PostCombineFn(CombineFn):
      @staticmethod
      def add_input(accumulator, element):
        is_accumulator, value = element
        if is_accumulator:
          return combine_fn.merge_accumulators([accumulator, value])
        else:
          return combine_fn.add_input(accumulator, value)
      create_accumulator = combine_fn.create_accumulator
      merge_accumulators = combine_fn.merge_accumulators
      compact = combine_fn.compact
      extract_output = combine_fn.extract_output

    def StripNonce(nonce_key_value):
      (_, key), value = nonce_key_value
      return key, value

    cold, hot = pcoll | ParDo(SplitHotCold()).with_outputs('hot', main='cold')
    cold.element_type = typehints.Any  # No multi-output type hints.
    precombined_hot = (
        hot
        # Avoid double counting that may happen with stacked accumulating mode.
        | 'WindowIntoDiscarding' >> WindowInto(
            pcoll.windowing, accumulation_mode=AccumulationMode.DISCARDING)
        | CombinePerKey(PreCombineFn())
        | Map(StripNonce)
        | 'WindowIntoOriginal' >> WindowInto(pcoll.windowing))
    return (
        (cold, precombined_hot)
        | Flatten()
        | CombinePerKey(PostCombineFn()))


@typehints.with_input_types(typehints.KV[K, V])
@typehints.with_output_types(typehints.KV[K, typehints.Iterable[V]])
class GroupByKey(PTransform):
  """A group by key transform.

  Processes an input PCollection consisting of key/value pairs represented as a
  tuple pair. The result is a PCollection where values having a common key are
  grouped together.  For example (a, 1), (b, 2), (a, 3) will result into
  (a, [1, 3]), (b, [2]).

  The implementation here is used only when run on the local direct runner.
  """

  class ReifyWindows(DoFn):

    def process(self, element, window=DoFn.WindowParam,
                timestamp=DoFn.TimestampParam):
      try:
        k, v = element
      except TypeError:
        raise TypeCheckError('Input to GroupByKey must be a PCollection with '
                             'elements compatible with KV[A, B]')

      return [(k, WindowedValue(v, timestamp, [window]))]

    def infer_output_type(self, input_type):
      key_type, value_type = trivial_inference.key_value_types(input_type)
      return Iterable[KV[key_type, typehints.WindowedValue[value_type]]]

  def expand(self, pcoll):
    # This code path is only used in the local direct runner.  For Dataflow
    # runner execution, the GroupByKey transform is expanded on the service.
    input_type = pcoll.element_type
    if input_type is not None:
      # Initialize type-hints used below to enforce type-checking and to pass
      # downstream to further PTransforms.
      key_type, value_type = trivial_inference.key_value_types(input_type)
      # Enforce the input to a GBK has a KV element type.
      pcoll.element_type = KV[key_type, value_type]
      typecoders.registry.verify_deterministic(
          typecoders.registry.get_coder(key_type),
          'GroupByKey operation "%s"' % self.label)

      reify_output_type = KV[key_type, typehints.WindowedValue[value_type]]
      gbk_input_type = (
          KV[key_type, Iterable[typehints.WindowedValue[value_type]]])
      gbk_output_type = KV[key_type, Iterable[value_type]]

      # pylint: disable=bad-continuation
      return (pcoll
              | 'ReifyWindows' >> (ParDo(self.ReifyWindows())
                 .with_output_types(reify_output_type))
              | 'GroupByKey' >> (_GroupByKeyOnly()
                 .with_input_types(reify_output_type)
                 .with_output_types(gbk_input_type))
              | ('GroupByWindow' >> _GroupAlsoByWindow(pcoll.windowing)
                 .with_input_types(gbk_input_type)
                 .with_output_types(gbk_output_type)))
    else:
      # The input_type is None, run the default
      return (pcoll
              | 'ReifyWindows' >> ParDo(self.ReifyWindows())
              | 'GroupByKey' >> _GroupByKeyOnly()
              | 'GroupByWindow' >> _GroupAlsoByWindow(pcoll.windowing))

  def infer_output_type(self, input_type):
    key_type, value_type = trivial_inference.key_value_types(input_type)
    return KV[key_type, Iterable[value_type]]

  def to_runner_api_parameter(self, unused_context):
    return common_urns.primitives.GROUP_BY_KEY.urn, None

  @PTransform.register_urn(common_urns.primitives.GROUP_BY_KEY.urn, None)
  def from_runner_api_parameter(unused_payload, unused_context):
    return GroupByKey()

  def runner_api_requires_keyed_input(self):
    return True


@typehints.with_input_types(typehints.KV[K, V])
@typehints.with_output_types(typehints.KV[K, typehints.Iterable[V]])
class _GroupByKeyOnly(PTransform):
  """A group by key transform, ignoring windows."""
  def infer_output_type(self, input_type):
    key_type, value_type = trivial_inference.key_value_types(input_type)
    return KV[key_type, Iterable[value_type]]

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return pvalue.PCollection(pcoll.pipeline)


@typehints.with_input_types(typehints.KV[K, typehints.Iterable[V]])
@typehints.with_output_types(typehints.KV[K, typehints.Iterable[V]])
class _GroupAlsoByWindow(ParDo):
  """The GroupAlsoByWindow transform."""
  def __init__(self, windowing):
    super(_GroupAlsoByWindow, self).__init__(
        _GroupAlsoByWindowDoFn(windowing))
    self.windowing = windowing

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return pvalue.PCollection(pcoll.pipeline)


class _GroupAlsoByWindowDoFn(DoFn):
  # TODO(robertwb): Support combiner lifting.

  def __init__(self, windowing):
    super(_GroupAlsoByWindowDoFn, self).__init__()
    self.windowing = windowing

  def infer_output_type(self, input_type):
    key_type, windowed_value_iter_type = trivial_inference.key_value_types(
        input_type)
    value_type = windowed_value_iter_type.inner_type.inner_type
    return Iterable[KV[key_type, Iterable[value_type]]]

  def start_bundle(self):
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.trigger import create_trigger_driver
    # pylint: enable=wrong-import-order, wrong-import-position
    self.driver = create_trigger_driver(self.windowing, True)

  def process(self, element):
    k, vs = element
    return self.driver.process_entire_key(k, vs)


class Partition(PTransformWithSideInputs):
  """Split a PCollection into several partitions.

  Uses the specified PartitionFn to separate an input PCollection into the
  specified number of sub-PCollections.

  When apply()d, a Partition() PTransform requires the following:

  Args:
    partitionfn: a PartitionFn, or a callable with the signature described in
      CallableWrapperPartitionFn.
    n: number of output partitions.

  The result of this PTransform is a simple list of the output PCollections
  representing each of n partitions, in order.
  """

  class ApplyPartitionFnFn(DoFn):
    """A DoFn that applies a PartitionFn."""

    def process(self, element, partitionfn, n, *args, **kwargs):
      partition = partitionfn.partition_for(element, n, *args, **kwargs)
      if not 0 <= partition < n:
        raise ValueError(
            'PartitionFn specified out-of-bounds partition index: '
            '%d not in [0, %d)' % (partition, n))
      # Each input is directed into the output that corresponds to the
      # selected partition.
      yield pvalue.TaggedOutput(str(partition), element)

  def make_fn(self, fn, has_side_inputs):
    return fn if isinstance(fn, PartitionFn) else CallableWrapperPartitionFn(fn)

  def expand(self, pcoll):
    n = int(self.args[0])
    return pcoll | ParDo(
        self.ApplyPartitionFnFn(), self.fn, *self.args,
        **self.kwargs).with_outputs(*[str(t) for t in range(n)])


class Windowing(object):
  def __init__(self, windowfn, triggerfn=None, accumulation_mode=None,
               timestamp_combiner=None):
    global AccumulationMode, DefaultTrigger  # pylint: disable=global-variable-not-assigned
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.trigger import AccumulationMode, DefaultTrigger
    # pylint: enable=wrong-import-order, wrong-import-position
    if triggerfn is None:
      triggerfn = DefaultTrigger()
    if accumulation_mode is None:
      if triggerfn == DefaultTrigger():
        accumulation_mode = AccumulationMode.DISCARDING
      else:
        raise ValueError(
            'accumulation_mode must be provided for non-trivial triggers')
    if not windowfn.get_window_coder().is_deterministic():
      raise ValueError(
          'window fn (%s) does not have a determanistic coder (%s)' % (
              windowfn, windowfn.get_window_coder()))
    self.windowfn = windowfn
    self.triggerfn = triggerfn
    self.accumulation_mode = accumulation_mode
    self.timestamp_combiner = (
        timestamp_combiner or TimestampCombiner.OUTPUT_AT_EOW)
    self._is_default = (
        self.windowfn == GlobalWindows() and
        self.triggerfn == DefaultTrigger() and
        self.accumulation_mode == AccumulationMode.DISCARDING and
        self.timestamp_combiner == TimestampCombiner.OUTPUT_AT_EOW)

  def __repr__(self):
    return "Windowing(%s, %s, %s, %s)" % (self.windowfn, self.triggerfn,
                                          self.accumulation_mode,
                                          self.timestamp_combiner)

  def __eq__(self, other):
    if type(self) == type(other):
      if self._is_default and other._is_default:
        return True
      return (
          self.windowfn == other.windowfn
          and self.triggerfn == other.triggerfn
          and self.accumulation_mode == other.accumulation_mode
          and self.timestamp_combiner == other.timestamp_combiner)
    return False

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((self.windowfn, self.accumulation_mode,
                 self.timestamp_combiner))

  def is_default(self):
    return self._is_default

  def to_runner_api(self, context):
    return beam_runner_api_pb2.WindowingStrategy(
        window_fn=self.windowfn.to_runner_api(context),
        # TODO(robertwb): Prohibit implicit multi-level merging.
        merge_status=(beam_runner_api_pb2.MergeStatus.NEEDS_MERGE
                      if self.windowfn.is_merging()
                      else beam_runner_api_pb2.MergeStatus.NON_MERGING),
        window_coder_id=context.coders.get_id(
            self.windowfn.get_window_coder()),
        trigger=self.triggerfn.to_runner_api(context),
        accumulation_mode=self.accumulation_mode,
        output_time=self.timestamp_combiner,
        # TODO(robertwb): Support EMIT_IF_NONEMPTY
        closing_behavior=beam_runner_api_pb2.ClosingBehavior.EMIT_ALWAYS,
        OnTimeBehavior=beam_runner_api_pb2.OnTimeBehavior.FIRE_ALWAYS,
        allowed_lateness=0)

  @staticmethod
  def from_runner_api(proto, context):
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.trigger import TriggerFn
    return Windowing(
        windowfn=WindowFn.from_runner_api(proto.window_fn, context),
        triggerfn=TriggerFn.from_runner_api(proto.trigger, context),
        accumulation_mode=proto.accumulation_mode,
        timestamp_combiner=proto.output_time)


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class WindowInto(ParDo):
  """A window transform assigning windows to each element of a PCollection.

  Transforms an input PCollection by applying a windowing function to each
  element.  Each transformed element in the result will be a WindowedValue
  element with the same input value and timestamp, with its new set of windows
  determined by the windowing function.
  """

  class WindowIntoFn(DoFn):
    """A DoFn that applies a WindowInto operation."""

    def __init__(self, windowing):
      self.windowing = windowing

    def process(self, element, timestamp=DoFn.TimestampParam,
                window=DoFn.WindowParam):
      context = WindowFn.AssignContext(timestamp, element=element,
                                       window=window)
      new_windows = self.windowing.windowfn.assign(context)
      yield WindowedValue(element, context.timestamp, new_windows)

  def __init__(self, windowfn, **kwargs):
    """Initializes a WindowInto transform.

    Args:
      windowfn: Function to be used for windowing
      trigger: (optional) Trigger used for windowing, or None for default.
      accumulation_mode: (optional) Accumulation mode used for windowing,
          required for non-trivial triggers.
      timestamp_combiner: (optional) Timestamp combniner used for windowing,
          or None for default.
    """
    if isinstance(windowfn, Windowing):
      # Overlay windowing with kwargs.
      windowing = windowfn
      windowfn = windowing.windowfn
      # Use windowing to fill in defaults for kwargs.
      kwargs = dict(dict(
          trigger=windowing.triggerfn,
          accumulation_mode=windowing.accumulation_mode,
          timestamp_combiner=windowing.timestamp_combiner), **kwargs)
    # Use kwargs to simulate keyword-only arguments.
    triggerfn = kwargs.pop('trigger', None)
    accumulation_mode = kwargs.pop('accumulation_mode', None)
    timestamp_combiner = kwargs.pop('timestamp_combiner', None)
    if kwargs:
      raise ValueError('Unexpected keyword arguments: %s' % list(kwargs))
    self.windowing = Windowing(
        windowfn, triggerfn, accumulation_mode, timestamp_combiner)
    super(WindowInto, self).__init__(self.WindowIntoFn(self.windowing))

  def get_windowing(self, unused_inputs):
    return self.windowing

  def infer_output_type(self, input_type):
    return input_type

  def expand(self, pcoll):
    input_type = pcoll.element_type

    if input_type is not None:
      output_type = input_type
      self.with_input_types(input_type)
      self.with_output_types(output_type)
    return super(WindowInto, self).expand(pcoll)

  def to_runner_api_parameter(self, context):
    return (
        common_urns.primitives.ASSIGN_WINDOWS.urn,
        self.windowing.to_runner_api(context))

  @staticmethod
  def from_runner_api_parameter(proto, context):
    windowing = Windowing.from_runner_api(proto, context)
    return WindowInto(
        windowing.windowfn,
        trigger=windowing.triggerfn,
        accumulation_mode=windowing.accumulation_mode,
        timestamp_combiner=windowing.timestamp_combiner)


PTransform.register_urn(
    common_urns.primitives.ASSIGN_WINDOWS.urn,
    # TODO(robertwb): Update WindowIntoPayload to include the full strategy.
    # (Right now only WindowFn is used, but we need this to reconstitute the
    # WindowInto transform, and in the future will need it at runtime to
    # support meta-data driven triggers.)
    # TODO(robertwb): Use a reference rather than embedding?
    beam_runner_api_pb2.WindowingStrategy,
    WindowInto.from_runner_api_parameter)


# Python's pickling is broken for nested classes.
WindowIntoFn = WindowInto.WindowIntoFn


class Flatten(PTransform):
  """Merges several PCollections into a single PCollection.

  Copies all elements in 0 or more PCollections into a single output
  PCollection. If there are no input PCollections, the resulting PCollection
  will be empty (but see also kwargs below).

  Args:
    **kwargs: Accepts a single named argument "pipeline", which specifies the
      pipeline that "owns" this PTransform. Ordinarily Flatten can obtain this
      information from one of the input PCollections, but if there are none (or
      if there's a chance there may be none), this argument is the only way to
      provide pipeline information and should be considered mandatory.
  """

  def __init__(self, **kwargs):
    super(Flatten, self).__init__()
    self.pipeline = kwargs.pop('pipeline', None)
    if kwargs:
      raise ValueError('Unexpected keyword arguments: %s' % list(kwargs))

  def _extract_input_pvalues(self, pvalueish):
    try:
      pvalueish = tuple(pvalueish)
    except TypeError:
      raise ValueError('Input to Flatten must be an iterable. '
                       'Got a value of type %s instead.' % type(pvalueish))
    return pvalueish, pvalueish

  def expand(self, pcolls):
    for pcoll in pcolls:
      self._check_pcollection(pcoll)
    result = pvalue.PCollection(self.pipeline)
    result.element_type = typehints.Union[
        tuple(pcoll.element_type for pcoll in pcolls)]
    return result

  def get_windowing(self, inputs):
    if not inputs:
      # TODO(robertwb): Return something compatible with every windowing?
      return Windowing(GlobalWindows())
    return super(Flatten, self).get_windowing(inputs)

  def to_runner_api_parameter(self, context):
    return common_urns.primitives.FLATTEN.urn, None

  @staticmethod
  def from_runner_api_parameter(unused_parameter, unused_context):
    return Flatten()


PTransform.register_urn(
    common_urns.primitives.FLATTEN.urn, None, Flatten.from_runner_api_parameter)


class Create(PTransform):
  """A transform that creates a PCollection from an iterable."""

  def __init__(self, values, reshuffle=True):
    """Initializes a Create transform.

    Args:
      values: An object of values for the PCollection
    """
    super(Create, self).__init__()
    if isinstance(values, (unicode, str, bytes)):
      raise TypeError('PTransform Create: Refusing to treat string as '
                      'an iterable. (string=%r)' % values)
    elif isinstance(values, dict):
      values = values.items()
    self.values = tuple(values)
    self.reshuffle = reshuffle

  def to_runner_api_parameter(self, context):
    # Required as this is identified by type in PTransformOverrides.
    # TODO(BEAM-3812): Use an actual URN here.
    return self.to_runner_api_pickled(context)

  def infer_output_type(self, unused_input_type):
    if not self.values:
      return Any
    return Union[[trivial_inference.instance_to_type(v) for v in self.values]]

  def get_output_type(self):
    return (self.get_type_hints().simple_output_type(self.label) or
            self.infer_output_type(None))

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    # Must guard against this as some legacy runners don't implement impulse.
    debug_options = pbegin.pipeline._options.view_as(DebugOptions)
    fn_api = (debug_options.experiments
              and 'beam_fn_api' in debug_options.experiments)
    if fn_api:
      coder = typecoders.registry.get_coder(self.get_output_type())
      serialized_values = [coder.encode(v) for v in self.values]
      reshuffle = self.reshuffle
      # Avoid the "redistributing" reshuffle for 0 and 1 element Creates.
      # These special cases are often used in building up more complex
      # transforms (e.g. Write).

      class MaybeReshuffle(PTransform):
        def expand(self, pcoll):
          if len(serialized_values) > 1 and reshuffle:
            from apache_beam.transforms.util import Reshuffle
            return pcoll | Reshuffle()
          else:
            return pcoll
      return (
          pbegin
          | Impulse()
          | FlatMap(lambda _: serialized_values)
          | MaybeReshuffle()
          | Map(coder.decode).with_output_types(self.get_output_type()))
    else:
      self.pipeline = pbegin.pipeline
      from apache_beam.io import iobase
      coder = typecoders.registry.get_coder(self.get_output_type())
      source = self._create_source_from_iterable(self.values, coder)
      return (pbegin.pipeline
              | iobase.Read(source).with_output_types(self.get_output_type()))

  def get_windowing(self, unused_inputs):
    return Windowing(GlobalWindows())

  @staticmethod
  def _create_source_from_iterable(values, coder):
    return Create._create_source(list(map(coder.encode, values)), coder)

  @staticmethod
  def _create_source(serialized_values, coder):
    from apache_beam.transforms.create_source import _CreateSource

    return _CreateSource(serialized_values, coder)


class Impulse(PTransform):
  """Impulse primitive."""

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise TypeError(
          'Input to Impulse transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, unused_context):
    return common_urns.primitives.IMPULSE.urn, None

  @PTransform.register_urn(common_urns.primitives.IMPULSE.urn, None)
  def from_runner_api_parameter(unused_parameter, unused_context):
    return Impulse()
