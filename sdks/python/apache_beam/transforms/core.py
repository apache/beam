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

# pytype: skip-file

import concurrent.futures
import copy
import inspect
import logging
import random
import sys
import time
import traceback
import types
import typing
from itertools import dropwhile

from apache_beam import coders
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.coders import typecoders
from apache_beam.internal import pickler
from apache_beam.internal import util
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
from apache_beam.transforms.sideinputs import SIDE_INPUT_PREFIX
from apache_beam.transforms.sideinputs import get_sideinput_index
from apache_beam.transforms.userstate import StateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.typehints import row_type
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.batch import BatchConverter
from apache_beam.typehints.decorators import TypeCheckError
from apache_beam.typehints.decorators import WithTypeHints
from apache_beam.typehints.decorators import get_signature
from apache_beam.typehints.decorators import get_type_hints
from apache_beam.typehints.decorators import with_input_types
from apache_beam.typehints.decorators import with_output_types
from apache_beam.typehints.trivial_inference import element_type
from apache_beam.typehints.typehints import TypeConstraint
from apache_beam.typehints.typehints import is_consistent_with
from apache_beam.typehints.typehints import visit_inner_types
from apache_beam.utils import urns
from apache_beam.utils.timestamp import Duration

if typing.TYPE_CHECKING:
  from google.protobuf import message  # pylint: disable=ungrouped-imports
  from apache_beam.io import iobase
  from apache_beam.pipeline import Pipeline
  from apache_beam.runners.pipeline_context import PipelineContext
  from apache_beam.transforms import create_source
  from apache_beam.transforms.trigger import AccumulationMode
  from apache_beam.transforms.trigger import DefaultTrigger
  from apache_beam.transforms.trigger import TriggerFn

__all__ = [
    'DoFn',
    'CombineFn',
    'PartitionFn',
    'ParDo',
    'FlatMap',
    'FlatMapTuple',
    'Map',
    'MapTuple',
    'Filter',
    'CombineGlobally',
    'CombinePerKey',
    'CombineValues',
    'GroupBy',
    'GroupByKey',
    'Select',
    'Partition',
    'Windowing',
    'WindowInto',
    'Flatten',
    'FlattenWith',
    'Create',
    'Impulse',
    'RestrictionProvider',
    'WatermarkEstimatorProvider',
]

# Type variables
T = typing.TypeVar('T')
K = typing.TypeVar('K')
V = typing.TypeVar('V')

_LOGGER = logging.getLogger(__name__)


class DoFnContext(object):
  """A context available to all methods of DoFn instance."""
  pass


class DoFnProcessContext(DoFnContext):
  """A processing context passed to DoFn process() during execution.

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

  This class should be implemented to support Splittable ``DoFn`` in Python
  SDK. See https://s.apache.org/splittable-do-fn for more details about
  Splittable ``DoFn``.

  To denote a ``DoFn`` class to be Splittable ``DoFn``, ``DoFn.process()``
  method of that class should have exactly one parameter whose default value is
  an instance of ``RestrictionParam``. This ``RestrictionParam`` can either be
  constructed with an explicit ``RestrictionProvider``, or, if no
  ``RestrictionProvider`` is provided, the ``DoFn`` itself must be a
  ``RestrictionProvider``.

  The provided ``RestrictionProvider`` instance must provide suitable overrides
  for the following methods:
  * create_tracker()
  * initial_restriction()
  * restriction_size()

  Optionally, ``RestrictionProvider`` may override default implementations of
  following methods:
  * restriction_coder()
  * split()
  * split_and_size()
  * truncate()

  ** Pausing and resuming processing of an element **

  As the last element produced by the iterator returned by the
  ``DoFn.process()`` method, a Splittable ``DoFn`` may return an object of type
  ``ProcessContinuation``.

  If restriction_tracker.defer_remander is called in the ```DoFn.process()``, it
  means that runner should later re-invoke ``DoFn.process()`` method to resume
  processing the current element and the manner in which the re-invocation
  should be performed.

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
    # type: (...) -> iobase.RestrictionTracker

    """Produces a new ``RestrictionTracker`` for the given restriction.

    This API is required to be implemented.

    Args:
      restriction: an object that defines a restriction as identified by a
        Splittable ``DoFn`` that utilizes the current ``RestrictionProvider``.
        For example, a tuple that gives a range of positions for a Splittable
        ``DoFn`` that reads files based on byte positions.
    Returns: an object of type ``RestrictionTracker``.
    """
    raise NotImplementedError

  def initial_restriction(self, element):
    """Produces an initial restriction for the given element.

    This API is required to be implemented.
    """
    raise NotImplementedError

  def split(self, element, restriction):
    """Splits the given element and restriction initially.

    This method enables runners to perform bulk splitting initially allowing for
    a rapid increase in parallelism. Note that initial split is a different
    concept from the split during element processing time. Please refer to
    ``iobase.RestrictionTracker.try_split`` for details about splitting when the
    current element and restriction are actively being processed.

    Returns an iterator of restrictions. The total set of elements produced by
    reading input element for each of the returned restrictions should be the
    same as the total set of elements produced by reading the input element for
    the input restriction.

    This API is optional if ``split_and_size`` has been implemented.

    If this method is not override, there is no initial splitting happening on
    each restriction.

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

  def restriction_size(self, element, restriction):
    """Returns the size of a restriction with respect to the given element.

    By default, asks a newly-created restriction tracker for the default size
    of the restriction.

    The return value must be non-negative.

    Must be thread safe. Will be invoked concurrently during bundle processing
    due to runner initiated splitting and progress estimation.

    This API is required to be implemented.
    """
    raise NotImplementedError

  def split_and_size(self, element, restriction):
    """Like split, but also does sizing, returning (restriction, size) pairs.

    For each pair, size must be non-negative.

    This API is optional if ``split`` and ``restriction_size`` have been
    implemented.
    """
    for part in self.split(element, restriction):
      yield part, self.restriction_size(element, part)

  def truncate(self, element, restriction):
    """Truncates the provided restriction into a restriction representing a
    finite amount of work when the pipeline is
    `draining <https://docs.google.com/document/d/1NExwHlj-2q2WUGhSO4jTu8XGhDPmm3cllSN8IMmWci8/edit#> for additional details about drain.>_`.  # pylint: disable=line-too-long
    By default, if the restriction is bounded then the restriction will be
    returned otherwise None will be returned.

    This API is optional and should only be implemented if more granularity is
    required.

    Return a truncated finite restriction if further processing is required
    otherwise return None to represent that no further processing of this
    restriction is required.

    The default behavior when a pipeline is being drained is that bounded
    restrictions process entirely while unbounded restrictions process till a
    checkpoint is possible.
    """
    restriction_tracker = self.create_tracker(restriction)
    if restriction_tracker.is_bounded():
      return restriction


def get_function_arguments(obj, func):
  # type: (...) -> typing.Tuple[typing.List[str], typing.List[typing.Any]]

  """Return the function arguments based on the name provided. If they have
  a _inspect_function attached to the class then use that otherwise default
  to the modified version of python inspect library.

  Returns:
    Same as get_function_args_defaults.
  """
  func_name = '_inspect_%s' % func
  if hasattr(obj, func_name):
    f = getattr(obj, func_name)
    return f()
  f = getattr(obj, func)
  return get_function_args_defaults(f)


def get_function_args_defaults(f):
  # type: (...) -> typing.Tuple[typing.List[str], typing.List[typing.Any]]

  """Returns the function arguments of a given function.

  Returns:
    (args: List[str], defaults: List[Any]). The first list names the
    arguments of the method and the second one has the values of the default
    arguments. This is similar to ``inspect.getfullargspec()``'s results, except
    it doesn't include bound arguments and may follow function wrappers.
  """
  signature = get_signature(f)
  parameter = inspect.Parameter
  # TODO(BEAM-5878) support kwonlyargs on Python 3.
  _SUPPORTED_ARG_TYPES = [
      parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD
  ]
  args = [
      name for name,
      p in signature.parameters.items() if p.kind in _SUPPORTED_ARG_TYPES
  ]
  defaults = [
      p.default for p in signature.parameters.values()
      if p.kind in _SUPPORTED_ARG_TYPES and p.default is not p.empty
  ]

  return args, defaults


class WatermarkEstimatorProvider(object):
  """Provides methods for generating WatermarkEstimator.

  This class should be implemented if wanting to providing output_watermark
  information within an SDF.

  In order to make an SDF.process() access to the typical WatermarkEstimator,
  the SDF author should have an argument whose default value is a
  DoFn.WatermarkEstimatorParam instance.  This DoFn.WatermarkEstimatorParam
  can either be constructed with an explicit WatermarkEstimatorProvider,
  or, if no WatermarkEstimatorProvider is provided, the DoFn itself must
  be a WatermarkEstimatorProvider.
  """
  def initial_estimator_state(self, element, restriction):
    """Returns the initial state of the WatermarkEstimator with given element
    and restriction.
    This function is called by the system.
    """
    raise NotImplementedError

  def create_watermark_estimator(self, estimator_state):
    """Create a new WatermarkEstimator based on the state. The state is
    typically useful when resuming processing an element.
    """
    raise NotImplementedError

  def estimator_state_coder(self):
    return coders.registry.get_coder(object)


class _DoFnParam(object):
  """DoFn parameter."""
  def __init__(self, param_id):
    self.param_id = param_id

  def __eq__(self, other):
    if type(self) == type(other):
      return self.param_id == other.param_id
    return False

  def __hash__(self):
    return hash(self.param_id)

  def __repr__(self):
    return self.param_id


class _RestrictionDoFnParam(_DoFnParam):
  """Restriction Provider DoFn parameter."""
  def __init__(self, restriction_provider=None):
    # type: (typing.Optional[RestrictionProvider]) -> None
    if (restriction_provider is not None and
        not isinstance(restriction_provider, RestrictionProvider)):
      raise ValueError(
          'DoFn.RestrictionParam expected RestrictionProvider object.')
    self.restriction_provider = restriction_provider
    self.param_id = (
        'RestrictionParam(%s)' % restriction_provider.__class__.__name__)


class _StateDoFnParam(_DoFnParam):
  """State DoFn parameter."""
  def __init__(self, state_spec):
    # type: (StateSpec) -> None
    if not isinstance(state_spec, StateSpec):
      raise ValueError("DoFn.StateParam expected StateSpec object.")
    self.state_spec = state_spec
    self.param_id = 'StateParam(%s)' % state_spec.name


class _TimerDoFnParam(_DoFnParam):
  """Timer DoFn parameter."""
  def __init__(self, timer_spec):
    # type: (TimerSpec) -> None
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
        _LOGGER.warning("Got exception from finalization call: %s", e)

  def has_callbacks(self):
    # type: () -> bool
    return len(self._callbacks) > 0

  def reset(self):
    # type: () -> None
    del self._callbacks[:]


class _WatermarkEstimatorParam(_DoFnParam):
  """WatermarkEstimator DoFn parameter."""
  def __init__(
      self,
      watermark_estimator_provider: typing.
      Optional[WatermarkEstimatorProvider] = None):
    if (watermark_estimator_provider is not None and not isinstance(
        watermark_estimator_provider, WatermarkEstimatorProvider)):
      raise ValueError(
          'DoFn.WatermarkEstimatorParam expected'
          'WatermarkEstimatorProvider object.')
    self.watermark_estimator_provider = watermark_estimator_provider
    self.param_id = 'WatermarkEstimatorProvider'


class _ContextParam(_DoFnParam):
  def __init__(
      self, context_manager_constructor, args=(), kwargs=None, *, name=None):
    class_name = self.__class__.__name__.strip('_')
    if (not callable(context_manager_constructor) or
        (hasattr(context_manager_constructor, '__enter__') and
         len(inspect.signature(
             context_manager_constructor.__enter__).parameters) == 0)):
      # Context managers constructed with @contextlib.contextmanager can only
      # be used once, and in addition cannot be pickled because they invoke
      # the function on __init__ rather than at __enter__.
      # In addition, other common context managers such as
      # tempfile.TemporaryDirectory perform side-effecting actions in __init__
      # rather than in __enter__.
      raise TypeError(
          "A context manager constructor (not a fully constructed context "
          "manager) must be passed to avoid issues with one-shot managers. "
          "For example, "
          "write {class_name}(tempfile.TemporaryDirectory, args=(...)) "
          "rather than {class_name}(tempfile.TemporaryDirectory(...))")
    super().__init__(f'{class_name}_{name or id(self)}')
    self.context_manager_constructor = context_manager_constructor
    self.args = args
    self.kwargs = kwargs or {}

  def create_and_enter(self):
    cm = self.context_manager_constructor(*self.args, **self.kwargs)
    return cm, cm.__enter__()


class _BundleContextParam(_ContextParam):
  """Allows one to use a context manager to manage bundle-scoped parameters.

  The context will be entered at the start of each bundle and exited at the
  end, equivalent to the `start_bundle` and `finish_bundle` methods on a DoFn.

  The object returned from `__enter__`, if any, will be substituted for this
  parameter in invocations.  Multiple context manager parameters may be
  specified which will all be evaluated (in an unspecified order).

  This can be especially useful for setting up shared context in transforms
  like `Map`, `FlatMap`, and `Filter` where one does not have start_bundle
  and finish_bundle methods.
  """


class _SetupContextParam(_ContextParam):
  """Allows one to use a context manager to manage DoFn-scoped parameters.

  The context will be entered before the DoFn is used and exited when it is
  discarded, equivalent to the `setup` and `teardown` methods of a DoFn.
  (Note, like `teardown`, exiting is best effort, as workers may be killed
  before all DoFns are torn down.)

  The object returned from `__enter__`, if any, will be substituted for this
  parameter in invocations.  Multiple context manager parameters may be
  specified which will all be evaluated (in an unspecified order).

  This can be useful for setting up shared resources like persistent
  connections to external services for transforms like `Map`, `FlatMap`, and
  `Filter` where one does not have setup and teardown methods.
  """


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
  WindowedValueParam = _DoFnParam('WindowedValueParam')
  PaneInfoParam = _DoFnParam('PaneInfoParam')
  WatermarkEstimatorParam = _WatermarkEstimatorParam
  BundleFinalizerParam = _BundleFinalizerParam
  KeyParam = _DoFnParam('KeyParam')
  BundleContextParam = _BundleContextParam
  SetupContextParam = _SetupContextParam

  # Parameters to access state and timers.  Not restricted to use only in the
  # .process() method. Usage: DoFn.StateParam(state_spec),
  # DoFn.TimerParam(timer_spec), DoFn.TimestampParam, DoFn.WindowParam,
  # DoFn.KeyParam
  StateParam = _StateDoFnParam
  TimerParam = _TimerDoFnParam
  DynamicTimerTagParam = _DoFnParam('DynamicTimerTagParam')

  DoFnProcessParams = [
      ElementParam,
      SideInputParam,
      TimestampParam,
      WindowParam,
      WindowedValueParam,
      WatermarkEstimatorParam,
      PaneInfoParam,
      BundleFinalizerParam,
      KeyParam,
      StateParam,
      TimerParam,
      BundleContextParam,
      SetupContextParam,
  ]

  RestrictionParam = _RestrictionDoFnParam

  @staticmethod
  def from_callable(fn):
    return CallableWrapperDoFn(fn)

  @staticmethod
  def unbounded_per_element():
    """A decorator on process fn specifying that the fn performs an unbounded
    amount of work per input element."""
    def wrapper(process_fn):
      process_fn.unbounded_per_element = True
      return process_fn

    return wrapper

  @staticmethod
  def yields_elements(fn):
    """A decorator to apply to ``process_batch`` indicating it yields elements.

    By default ``process_batch`` is assumed to both consume and produce
    "batches", which are collections of multiple logical Beam elements. This
    decorator indicates that ``process_batch`` **produces** individual elements
    at a time. ``process_batch`` is always expected to consume batches.
    """
    if not fn.__name__ in ('process', 'process_batch'):
      raise TypeError(
          "@yields_elements must be applied to a process or "
          f"process_batch method, got {fn!r}.")

    fn._beam_yields_elements = True
    return fn

  @staticmethod
  def yields_batches(fn):
    """A decorator to apply to ``process`` indicating it yields batches.

    By default ``process`` is assumed to both consume and produce
    individual elements at a time. This decorator indicates that ``process``
    **produces** "batches", which are collections of multiple logical Beam
    elements.
    """
    if not fn.__name__ in ('process', 'process_batch'):
      raise TypeError(
          "@yields_elements must be applied to a process or "
          f"process_batch method, got {fn!r}.")

    fn._beam_yields_batches = True
    return fn

  def default_label(self):
    return self.__class__.__name__

  def process(self, element, *args, **kwargs):
    """Method to use for processing elements.

    This is invoked by ``DoFnRunner`` for each element of a input
    ``PCollection``.

    The following parameters can be used as default values on ``process``
    arguments to indicate that a DoFn accepts the corresponding parameters. For
    example, a DoFn might accept the element and its timestamp with the
    following signature::

      def process(element=DoFn.ElementParam, timestamp=DoFn.TimestampParam):
        ...

    The full set of parameters is:

    - ``DoFn.ElementParam``: element to be processed, should not be mutated.
    - ``DoFn.SideInputParam``: a side input that may be used when processing.
    - ``DoFn.TimestampParam``: timestamp of the input element.
    - ``DoFn.WindowParam``: ``Window`` the input element belongs to.
    - ``DoFn.TimerParam``: a ``userstate.RuntimeTimer`` object defined by the
      spec of the parameter.
    - ``DoFn.StateParam``: a ``userstate.RuntimeState`` object defined by the
      spec of the parameter.
    - ``DoFn.KeyParam``: key associated with the element.
    - ``DoFn.RestrictionParam``: an ``iobase.RestrictionTracker`` will be
      provided here to allow treatment as a Splittable ``DoFn``. The restriction
      tracker will be derived from the restriction provider in the parameter.
    - ``DoFn.WatermarkEstimatorParam``: a function that can be used to track
      output watermark of Splittable ``DoFn`` implementations.
    - ``DoFn.BundleContextParam``: allows a shared context manager to be used
      per bundle
    - ``DoFn.SetupContextParam``: allows a shared context manager to be used
      per DoFn

    Args:
      element: The element to be processed
      *args: side inputs
      **kwargs: other keyword arguments.

    Returns:
      An Iterable of output elements or None.
    """
    raise NotImplementedError

  def process_batch(self, batch, *args, **kwargs):
    raise NotImplementedError

  def setup(self):
    """Called to prepare an instance for processing bundles of elements.

    This is a good place to initialize transient in-memory resources, such as
    network connections. The resources can then be disposed in
    ``DoFn.teardown``.
    """
    pass

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

  def teardown(self):
    """Called to use to clean up this instance before it is discarded.

    A runner will do its best to call this method on any given instance to
    prevent leaks of transient resources, however, there may be situations where
    this is impossible (e.g. process crash, hardware failure, etc.) or
    unnecessary (e.g. the pipeline is shutting down and the process is about to
    be killed anyway, so all transient resources will be released automatically
    by the OS). In these cases, the call may not happen. It will also not be
    retried, because in such situations the DoFn instance no longer exists, so
    there's no instance to retry it on.

    Thus, all work that depends on input elements, and all externally important
    side effects, must be performed in ``DoFn.process`` or
    ``DoFn.finish_bundle``.
    """
    pass

  def get_function_arguments(self, func):
    return get_function_arguments(self, func)

  def default_type_hints(self):
    process_type_hints = typehints.decorators.IOTypeHints.from_callable(
        self.process) or typehints.decorators.IOTypeHints.empty()

    if self._process_yields_batches:
      # process() produces batches, don't use it's output typehint
      process_type_hints = process_type_hints.with_output_types_from(
          typehints.decorators.IOTypeHints.empty())

    if self._process_batch_yields_elements:
      # process_batch() produces elements, *do* use it's output typehint

      # First access the typehint
      process_batch_type_hints = typehints.decorators.IOTypeHints.from_callable(
          self.process_batch) or typehints.decorators.IOTypeHints.empty()

      # Then we deconflict with the typehint from process, if it exists
      if (process_batch_type_hints.output_types !=
          typehints.decorators.IOTypeHints.empty().output_types):
        if (process_type_hints.output_types !=
            typehints.decorators.IOTypeHints.empty().output_types and
            process_batch_type_hints.output_types !=
            process_type_hints.output_types):
          raise TypeError(
              f"DoFn {self!r} yields element from both process and "
              "process_batch, but they have mismatched output typehints:\n"
              f" process: {process_type_hints.output_types}\n"
              f" process_batch: {process_batch_type_hints.output_types}")

        process_type_hints = process_type_hints.with_output_types_from(
            process_batch_type_hints)

    try:
      process_type_hints = process_type_hints.strip_iterable()
    except ValueError as e:
      raise ValueError('Return value not iterable: %s: %s' % (self, e))

    # Prefer class decorator type hints for backwards compatibility.
    return get_type_hints(self.__class__).with_defaults(process_type_hints)

  # TODO(sourabhbajaj): Do we want to remove the responsibility of these from
  # the DoFn or maybe the runner
  def infer_output_type(self, input_type):
    # TODO(https://github.com/apache/beam/issues/19824): Side inputs types.
    return trivial_inference.element_type(
        _strip_output_annotations(
            trivial_inference.infer_return_type(self.process, [input_type])))

  @property
  def _process_defined(self) -> bool:
    # Check if this DoFn's process method has been overridden
    # Note that we retrieve the __func__ attribute, if it exists, to get the
    # underlying function from the bound method.
    # If __func__ doesn't exist, self.process was likely overridden with a free
    # function, as in CallableWrapperDoFn.
    return getattr(self.process, '__func__', self.process) != DoFn.process

  @property
  def _process_batch_defined(self) -> bool:
    # Check if this DoFn's process_batch method has been overridden
    # Note that we retrieve the __func__ attribute, if it exists, to get the
    # underlying function from the bound method.
    # If __func__ doesn't exist, self.process_batch was likely overridden with
    # a free function.
    return getattr(
        self.process_batch, '__func__',
        self.process_batch) != DoFn.process_batch

  @property
  def _can_yield_batches(self) -> bool:
    return ((self._process_defined and self._process_yields_batches) or (
        self._process_batch_defined and
        not self._process_batch_yields_elements))

  @property
  def _process_yields_batches(self) -> bool:
    return getattr(self.process, '_beam_yields_batches', False)

  @property
  def _process_batch_yields_elements(self) -> bool:
    return getattr(self.process_batch, '_beam_yields_elements', False)

  def get_input_batch_type(
      self, input_element_type
  ) -> typing.Optional[typing.Union[TypeConstraint, type]]:
    """Determine the batch type expected as input to process_batch.

    The default implementation of ``get_input_batch_type`` simply observes the
    input typehint for the first parameter of ``process_batch``. A Batched DoFn
    may override this method if a dynamic approach is required.

    Args:
      input_element_type: The **element type** of the input PCollection this
        DoFn is being applied to.

    Returns:
      ``None`` if this DoFn cannot accept batches, else a Beam typehint or
      a native Python typehint.
    """
    if not self._process_batch_defined:
      return None
    input_type = list(
        inspect.signature(self.process_batch).parameters.values())[0].annotation
    if input_type == inspect.Signature.empty:
      # TODO(https://github.com/apache/beam/issues/21652): Consider supporting
      # an alternative (dynamic?) approach for declaring input type
      raise TypeError(
          f"Either {self.__class__.__name__}.process_batch() must have a type "
          f"annotation on its first parameter, or {self.__class__.__name__} "
          "must override get_input_batch_type.")
    return input_type

  def _get_input_batch_type_normalized(self, input_element_type):
    return typehints.native_type_compatibility.convert_to_beam_type(
        self.get_input_batch_type(input_element_type))

  def _get_output_batch_type_normalized(self, input_element_type):
    return typehints.native_type_compatibility.convert_to_beam_type(
        self.get_output_batch_type(input_element_type))

  @staticmethod
  def _get_element_type_from_return_annotation(method, input_type):
    return_type = inspect.signature(method).return_annotation
    if return_type == inspect.Signature.empty:
      # output type not annotated, try to infer it
      return_type = trivial_inference.infer_return_type(method, [input_type])

    return_type = typehints.native_type_compatibility.convert_to_beam_type(
        return_type)
    if isinstance(return_type, typehints.typehints.IterableTypeConstraint):
      return return_type.inner_type
    elif isinstance(return_type, typehints.typehints.IteratorTypeConstraint):
      return return_type.yielded_type
    else:
      raise TypeError(
          "Expected Iterator in return type annotation for "
          f"{method!r}, did you mean Iterator[{return_type}]? Note Beam DoFn "
          "process and process_batch methods are expected to produce "
          "generators - they should 'yield' rather than 'return'.")

  def get_output_batch_type(
      self, input_element_type
  ) -> typing.Optional[typing.Union[TypeConstraint, type]]:
    """Determine the batch type produced by this DoFn's ``process_batch``
    implementation and/or its ``process`` implementation with
    ``@yields_batch``.

    The default implementation of this method observes the return type
    annotations on ``process_batch`` and/or ``process``.  A Batched DoFn may
    override this method if a dynamic approach is required.

    Args:
      input_element_type: The **element type** of the input PCollection this
        DoFn is being applied to.

    Returns:
      ``None`` if this DoFn will never yield batches, else a Beam typehint or
      a native Python typehint.
    """
    output_batch_type = None
    if self._process_defined and self._process_yields_batches:
      output_batch_type = self._get_element_type_from_return_annotation(
          self.process, input_element_type)
    if self._process_batch_defined and not self._process_batch_yields_elements:
      process_batch_type = self._get_element_type_from_return_annotation(
          self.process_batch,
          self._get_input_batch_type_normalized(input_element_type))

      # TODO: Consider requiring an inheritance relationship rather than
      # equality
      if (output_batch_type is not None and
          (not process_batch_type == output_batch_type)):
        raise TypeError(
            f"DoFn {self!r} yields batches from both process and "
            "process_batch, but they produce different types:\n"
            f" process: {output_batch_type}\n"
            f" process_batch: {process_batch_type!r}")

      output_batch_type = process_batch_type

    return output_batch_type

  def _process_argspec_fn(self):
    """Returns the Python callable that will eventually be invoked.

    This should ideally be the user-level function that is called with
    the main and (if any) side inputs, and is used to relate the type
    hint parameters with the input parameters (e.g., by argument name).
    """
    return self.process

  urns.RunnerApiFn.register_pickle_urn(python_urns.PICKLED_DOFN)


class CallableWrapperDoFn(DoFn):
  """For internal use only; no backwards-compatibility guarantees.

  A DoFn (function) object wrapping a callable object.

  The purpose of this class is to conveniently wrap simple functions and use
  them in transforms.
  """
  def __init__(self, fn, fullargspec=None):
    """Initializes a CallableWrapperDoFn object wrapping a callable.

    Args:
      fn: A callable object.

    Raises:
      TypeError: if fn parameter is not a callable type.
    """
    if not callable(fn):
      raise TypeError('Expected a callable object instead of: %r' % fn)

    self._fn = fn
    self._fullargspec = fullargspec
    if isinstance(
        fn, (types.BuiltinFunctionType, types.MethodType, types.FunctionType)):
      self.process = fn
    else:
      # For cases such as set / list where fn is callable but not a function
      self.process = lambda element: fn(element)

    super().__init__()

  def display_data(self):
    # If the callable has a name, then it's likely a function, and
    # we show its name.
    # Otherwise, it might be an instance of a callable class. We
    # show its class.
    display_data_value = (
        self._fn.__name__
        if hasattr(self._fn, '__name__') else self._fn.__class__)
    return {
        'fn': DisplayDataItem(display_data_value, label='Transform Function')
    }

  def __repr__(self):
    return 'CallableWrapperDoFn(%s)' % self._fn

  def default_type_hints(self):
    fn_type_hints = typehints.decorators.IOTypeHints.from_callable(self._fn)
    type_hints = get_type_hints(self._fn).with_defaults(fn_type_hints)
    # The fn's output type should be iterable. Strip off the outer
    # container type due to the 'flatten' portion of FlatMap/ParDo.
    try:
      type_hints = type_hints.strip_iterable()
    except ValueError as e:
      raise TypeCheckError(
          'Return value not iterable: %s: %s' %
          (self.display_data()['fn'].value, e))
    return type_hints

  def infer_output_type(self, input_type):
    return trivial_inference.element_type(
        _strip_output_annotations(
            trivial_inference.infer_return_type(self._fn, [input_type])))

  def _process_argspec_fn(self):
    return getattr(self._fn, '_argspec_fn', self._fn)

  def _inspect_process(self):
    if self._fullargspec:
      return self._fullargspec
    else:
      return get_function_args_defaults(self._process_argspec_fn())


class CombineFn(WithTypeHints, HasDisplayData, urns.RunnerApiFn):
  """A function object used by a Combine transform with custom processing.

  A CombineFn specifies how multiple values in all or part of a PCollection can
  be merged into a single value---essentially providing the same kind of
  information as the arguments to the Python "reduce" builtin (except for the
  input argument, which is an instance of CombineFnProcessContext). The
  combining process proceeds as follows:

  1. Input values are partitioned into one or more batches.
  2. For each batch, the setup method is invoked.
  3. For each batch, the create_accumulator method is invoked to create a fresh
     initial "accumulator" value representing the combination of zero values.
  4. For each input value in the batch, the add_input method is invoked to
     combine more values with the accumulator for that batch.
  5. The merge_accumulators method is invoked to combine accumulators from
     separate batches into a single combined output accumulator value, once all
     of the accumulators have had all the input value in their batches added to
     them. This operation is invoked repeatedly, until there is only one
     accumulator value left.
  6. The extract_output operation is invoked on the final accumulator to get
     the output value.
  7. The teardown method is invoked.

  Note: If this **CombineFn** is used with a transform that has defaults,
  **apply** will be called with an empty list at expansion time to get the
  default value.
  """
  def default_label(self):
    return self.__class__.__name__

  def setup(self, *args, **kwargs):
    """Called to prepare an instance for combining.

    This method can be useful if there is some state that needs to be loaded
    before executing any of the other methods. The resources can then be
    disposed of in ``CombineFn.teardown``.

    If you are using Dataflow, you need to enable Dataflow Runner V2
    before using this feature.

    Args:
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    pass

  def create_accumulator(self, *args, **kwargs):
    """Return a fresh, empty accumulator for the combine operation.

    Args:
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def add_input(self, mutable_accumulator, element, *args, **kwargs):
    """Return result of folding element into accumulator.

    CombineFn implementors must override add_input.

    Args:
      mutable_accumulator: the current accumulator,
        may be modified and returned for efficiency
      element: the element to add, should not be mutated
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def add_inputs(self, mutable_accumulator, elements, *args, **kwargs):
    """Returns the result of folding each element in elements into accumulator.

    This is provided in case the implementation affords more efficient
    bulk addition of elements. The default implementation simply loops
    over the inputs invoking add_input for each one.

    Args:
      mutable_accumulator: the current accumulator,
        may be modified and returned for efficiency
      elements: the elements to add, should not be mutated
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    for element in elements:
      mutable_accumulator =\
        self.add_input(mutable_accumulator, element, *args, **kwargs)
    return mutable_accumulator

  def merge_accumulators(self, accumulators, *args, **kwargs):
    """Returns the result of merging several accumulators
    to a single accumulator value.

    Args:
      accumulators: the accumulators to merge.
        Only the first accumulator may be modified and returned for efficiency;
        the other accumulators should not be mutated, because they may be
        shared with other code and mutating them could lead to incorrect
        results or data corruption.
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
        for the entire input key or PCollection. Can be modified for
        efficiency.
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    raise NotImplementedError(str(self))

  def teardown(self, *args, **kwargs):
    """Called to clean up an instance before it is discarded.

    If you are using Dataflow, you need to enable Dataflow Runner V2
    before using this feature.

    Args:
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    pass

  def apply(self, elements, *args, **kwargs):
    """Returns result of applying this CombineFn to the input values.

    Args:
      elements: the set of values to combine.
      *args: Additional arguments and side inputs.
      **kwargs: Additional arguments and side inputs.
    """
    return self.extract_output(
        self.add_inputs(
            self.create_accumulator(*args, **kwargs), elements, *args,
            **kwargs),
        *args,
        **kwargs)

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
    # type: (typing.Union[CombineFn, typing.Callable], bool) -> CombineFn
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

    super().__init__()
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
    fn_type_hints = typehints.decorators.IOTypeHints.from_callable(self._fn)
    type_hints = get_type_hints(self._fn).with_defaults(fn_type_hints)
    if type_hints.input_types is None:
      return type_hints
    else:
      # fn(Iterable[V]) -> V becomes CombineFn(V) -> V
      input_args, input_kwargs = type_hints.input_types
      if not input_args:
        if len(input_kwargs) == 1:
          input_args, input_kwargs = tuple(input_kwargs.values()), {}
        else:
          raise TypeError('Combiner input type must be specified positionally.')
      if not is_consistent_with(input_args[0],
                                typehints.Iterable[typehints.Any]):
        raise TypeCheckError(
            'All functions for a Combine PTransform must accept a '
            'single argument compatible with: Iterable[Any]. '
            'Instead a function with input type: %s was received.' %
            input_args[0])
      input_args = (element_type(input_args[0]), ) + input_args[1:]
      # TODO(robertwb): Assert output type is consistent with input type?
      return type_hints.with_input_types(*input_args, **input_kwargs)

  def infer_output_type(self, input_type):
    return _strip_output_annotations(
        trivial_inference.infer_return_type(self._fn, [input_type]))

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
    # type: (T, int, *typing.Any, **typing.Any) -> int

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
    # type: (T, int, *typing.Any, **typing.Any) -> int
    return self._fn(element, num_partitions, *args, **kwargs)


def _get_function_body_without_inners(func):
  source_lines = inspect.getsourcelines(func)[0]
  source_lines = dropwhile(lambda x: x.startswith("@"), source_lines)
  def_line = next(source_lines).strip()
  if def_line.startswith("def ") and def_line.endswith(":"):
    first_line = next(source_lines)
    indentation = len(first_line) - len(first_line.lstrip())
    final_lines = [first_line[indentation:]]

    skip_inner_def = False
    if first_line[indentation:].startswith("def "):
      skip_inner_def = True
    for line in source_lines:
      line_indentation = len(line) - len(line.lstrip())

      if line[indentation:].startswith("def "):
        skip_inner_def = True
        continue

      if skip_inner_def and line_indentation == indentation:
        skip_inner_def = False

      if skip_inner_def and line_indentation > indentation:
        continue
      final_lines.append(line[indentation:])

    return "".join(final_lines)
  else:
    return def_line.rsplit(":")[-1].strip()


def _check_fn_use_yield_and_return(fn):
  if isinstance(fn, types.BuiltinFunctionType):
    return False
  try:
    source_code = _get_function_body_without_inners(fn)
    has_yield = False
    has_return = False
    for line in source_code.split("\n"):
      if line.lstrip().startswith("yield ") or line.lstrip().startswith(
          "yield("):
        has_yield = True
      if line.lstrip().startswith("return ") or line.lstrip().startswith(
          "return("):
        has_return = True
      if has_yield and has_return:
        return True
    return False
  except Exception as e:
    _LOGGER.debug(str(e))
    return False


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
    fn (`typing.Union[DoFn, typing.Callable]`): a :class:`DoFn` object to be
      applied to each element of **pcoll** argument, or a Callable.
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
    super().__init__(fn, *args, **kwargs)
    # TODO(robertwb): Change all uses of the dofn attribute to use fn instead.
    self.dofn = self.fn
    self.output_tags = set()  # type: typing.Set[str]

    if not isinstance(self.fn, DoFn):
      raise TypeError('ParDo must be called with a DoFn instance.')

    # DoFn.process cannot allow both return and yield
    if _check_fn_use_yield_and_return(self.fn.process):
      _LOGGER.warning(
          'Using yield and return in the process method '
          'of %s can lead to unexpected behavior, see:'
          'https://github.com/apache/beam/issues/22969.',
          self.fn.__class__)

    # Validate the DoFn by creating a DoFnSignature
    from apache_beam.runners.common import DoFnSignature
    self._signature = DoFnSignature(self.fn)

  def with_exception_handling(
      self,
      main_tag='good',
      dead_letter_tag='bad',
      *,
      exc_class=Exception,
      partial=False,
      use_subprocess=False,
      threshold=1,
      threshold_windowing=None,
      timeout=None,
      error_handler=None,
      on_failure_callback: typing.Optional[typing.Callable[
          [Exception, typing.Any], None]] = None):
    """Automatically provides a dead letter output for skipping bad records.
    This can allow a pipeline to continue successfully rather than fail or
    continuously throw errors on retry when bad elements are encountered.

    This returns a tagged output with two PCollections, the first being the
    results of successfully processing the input PCollection, and the second
    being the set of bad records (those which threw exceptions during
    processing) along with information about the errors raised.

    For example, one would write::

        good, bad = Map(maybe_error_raising_function).with_exception_handling()

    and `good` will be a PCollection of mapped records and `bad` will contain
    those that raised exceptions.


    Args:
      main_tag: tag to be used for the main (good) output of the DoFn,
          useful to avoid possible conflicts if this DoFn already produces
          multiple outputs.  Optional, defaults to 'good'.
      dead_letter_tag: tag to be used for the bad records, useful to avoid
          possible conflicts if this DoFn already produces multiple outputs.
          Optional, defaults to 'bad'.
      exc_class: An exception class, or tuple of exception classes, to catch.
          Optional, defaults to 'Exception'.
      partial: Whether to emit outputs for an element as they're produced
          (which could result in partial outputs for a ParDo or FlatMap that
          throws an error part way through execution) or buffer all outputs
          until successful processing of the entire element. Optional,
          defaults to False.
      use_subprocess: Whether to execute the DoFn logic in a subprocess. This
          allows one to recover from errors that can crash the calling process
          (e.g. from an underlying C/C++ library causing a segfault), but is
          slower as elements and results must cross a process boundary.  Note
          that this starts up a long-running process that is used to handle
          all the elements (until hard failure, which should be rare) rather
          than a new process per element, so the overhead should be minimal
          (and can be amortized if there's any per-process or per-bundle
          initialization that needs to be done). Optional, defaults to False.
      threshold: An upper bound on the ratio of records that can be bad before
          aborting the entire pipeline. Optional, defaults to 1.0 (meaning
          up to 100% of records can be bad and the pipeline will still succeed).
      threshold_windowing: Event-time windowing to use for threshold. Optional,
          defaults to the windowing of the input.
      timeout: If the element has not finished processing in timeout seconds,
          raise a TimeoutError.  Defaults to None, meaning no time limit.
      error_handler: An ErrorHandler that should be used to consume the bad
          records, rather than returning the good and bad records as a tuple.
      on_failure_callback: If an element fails or times out,
          on_failure_callback will be invoked. It will receive the exception
          and the element being processed in as args. In case of a timeout,
          the exception will be of type `TimeoutError`. Be careful with this
          callback - if you set a timeout, it will not apply to the callback,
          and if the callback fails it will not be retried.
    """
    args, kwargs = self.raw_side_inputs
    return self.label >> _ExceptionHandlingWrapper(
        self.fn,
        args,
        kwargs,
        main_tag,
        dead_letter_tag,
        exc_class,
        partial,
        use_subprocess,
        threshold,
        threshold_windowing,
        timeout,
        error_handler,
        on_failure_callback)

  def with_error_handler(self, error_handler, **exception_handling_kwargs):
    """An alias for `with_exception_handling(error_handler=error_handler, ...)`

    This is provided to fit the general ErrorHandler conventions.
    """
    if error_handler is None:
      return self
    else:
      return self.with_exception_handling(
          error_handler=error_handler, **exception_handling_kwargs)

  def default_type_hints(self):
    return self.fn.get_type_hints()

  def infer_output_type(self, input_type):
    return self.fn.infer_output_type(input_type)

  def infer_batch_converters(self, input_element_type):
    # TODO: Test this code (in batch_dofn_test)
    if self.fn._process_batch_defined:
      input_batch_type = self.fn._get_input_batch_type_normalized(
          input_element_type)

      if input_batch_type is None:
        raise TypeError(
            "process_batch method on {self.fn!r} does not have "
            "an input type annoation")

      try:
        # Generate a batch converter to convert between the input type and the
        # (batch) input type of process_batch
        self.fn.input_batch_converter = BatchConverter.from_typehints(
            element_type=input_element_type, batch_type=input_batch_type)
      except TypeError as e:
        raise TypeError(
            "Failed to find a BatchConverter for the input types of DoFn "
            f"{self.fn!r} (element_type={input_element_type!r}, "
            f"batch_type={input_batch_type!r}).") from e

    else:
      self.fn.input_batch_converter = None

    if self.fn._can_yield_batches:
      output_batch_type = self.fn._get_output_batch_type_normalized(
          input_element_type)
      if output_batch_type is None:
        # TODO: Mention process method in this error
        raise TypeError(
            f"process_batch method on {self.fn!r} does not have "
            "a return type annoation")

      # Generate a batch converter to convert between the output type and the
      # (batch) output type of process_batch
      output_element_type = self.infer_output_type(input_element_type)

      try:
        self.fn.output_batch_converter = BatchConverter.from_typehints(
            element_type=output_element_type, batch_type=output_batch_type)
      except TypeError as e:
        raise TypeError(
            "Failed to find a BatchConverter for the *output* types of DoFn "
            f"{self.fn!r} (element_type={output_element_type!r}, "
            f"batch_type={output_batch_type!r}). Maybe you need to override "
            "DoFn.infer_output_type to set the output element type?") from e
    else:
      self.fn.output_batch_converter = None

  def make_fn(self, fn, has_side_inputs):
    if isinstance(fn, DoFn):
      return fn
    return CallableWrapperDoFn(fn)

  def _process_argspec_fn(self):
    return self.fn._process_argspec_fn()

  def display_data(self):
    return {
        'fn': DisplayDataItem(self.fn.__class__, label='Transform Function'),
        'fn_dd': self.fn
    }

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
        _LOGGER.warning(
            'Key coder %s for transform %s with stateful DoFn may not '
            'be deterministic. This may cause incorrect behavior for complex '
            'key types. Consider adding an input type hint for this transform.',
            key_coder,
            self)

    if self._signature.is_unbounded_per_element():
      is_bounded = False
    else:
      is_bounded = pcoll.is_bounded

    self.infer_batch_converters(pcoll.element_type)

    return pvalue.PCollection.from_(pcoll, is_bounded=is_bounded)

  def with_outputs(self, *tags, main=None, allow_unknown_tags=None):
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
      TypeError: if the **self** object is not a
        :class:`~apache_beam.pvalue.PCollection` that is the result of a
        :class:`ParDo` transform.
      ValueError: if **main_kw** contains any key other than
        ``'main'``.
    """
    if main in tags:
      raise ValueError(
          'Main output tag %r must be different from side output tags %r.' %
          (main, tags))
    return _MultiParDo(self, tags, main, allow_unknown_tags)

  def _do_fn_info(self):
    return DoFnInfo.create(self.fn, self.args, self.kwargs)

  def _get_key_and_window_coder(self, named_inputs):
    if named_inputs is None or not self._signature.is_stateful_dofn():
      return None, None
    main_input = list(set(named_inputs.keys()) - set(self.side_inputs))[0]
    input_pcoll = named_inputs[main_input]
    kv_type_hint = input_pcoll.element_type
    if kv_type_hint and kv_type_hint != typehints.Any:
      coder = coders.registry.get_coder(kv_type_hint)
      if not coder.is_kv_coder():
        raise ValueError(
            'Input elements to the transform %s with stateful DoFn must be '
            'key-value pairs.' % self)
      key_coder = coder.key_coder()
    else:
      key_coder = coders.registry.get_coder(typehints.Any)
    window_coder = input_pcoll.windowing.windowfn.get_window_coder()
    return key_coder, window_coder

  # typing: PTransform base class does not accept extra_kwargs
  def to_runner_api_parameter(self, context, **extra_kwargs):  # type: ignore[override]
    # type: (PipelineContext, **typing.Any) -> typing.Tuple[str, message.Message]
    assert isinstance(self, ParDo), \
        "expected instance of ParDo, but got %s" % self.__class__
    state_specs, timer_specs = userstate.get_dofn_specs(self.fn)
    if state_specs or timer_specs:
      context.add_requirement(
          common_urns.requirements.REQUIRES_STATEFUL_PROCESSING.urn)
    from apache_beam.runners.common import DoFnSignature
    sig = DoFnSignature(self.fn)
    is_splittable = sig.is_splittable_dofn()
    if is_splittable:
      restriction_coder = sig.get_restriction_coder()
      # restriction_coder will never be None when is_splittable is True
      assert restriction_coder is not None
      restriction_coder_id = context.coders.get_id(
          restriction_coder)  # type: typing.Optional[str]
      context.add_requirement(
          common_urns.requirements.REQUIRES_SPLITTABLE_DOFN.urn)
    else:
      restriction_coder_id = None
    has_bundle_finalization = sig.has_bundle_finalization()
    if has_bundle_finalization:
      context.add_requirement(
          common_urns.requirements.REQUIRES_BUNDLE_FINALIZATION.urn)

    # Get key_coder and window_coder for main_input.
    key_coder, window_coder = self._get_key_and_window_coder(
        extra_kwargs.get('named_inputs', None))
    return (
        common_urns.primitives.PAR_DO.urn,
        beam_runner_api_pb2.ParDoPayload(
            do_fn=self._do_fn_info().to_runner_api(context),
            requests_finalization=has_bundle_finalization,
            restriction_coder_id=restriction_coder_id,
            state_specs={
                spec.name: spec.to_runner_api(context)
                for spec in state_specs
            },
            timer_family_specs={
                spec.name: spec.to_runner_api(context, key_coder, window_coder)
                for spec in timer_specs
            },
            # It'd be nice to name these according to their actual
            # names/positions in the orignal argument list, but such a
            # transformation is currently irreversible given how
            # remove_objects_from_args and insert_values_in_args
            # are currently implemented.
            side_inputs={(SIDE_INPUT_PREFIX + '%s') % ix:
                         si.to_runner_api(context)
                         for ix,
                         si in enumerate(self.side_inputs)}))

  @staticmethod
  @PTransform.register_urn(
      common_urns.primitives.PAR_DO.urn, beam_runner_api_pb2.ParDoPayload)
  def from_runner_api_parameter(unused_ptransform, pardo_payload, context):
    fn, args, kwargs, si_tags_and_types, windowing = pickler.loads(
        DoFnInfo.from_runner_api(
            pardo_payload.do_fn, context).serialized_dofn_data())
    if si_tags_and_types:
      raise NotImplementedError('explicit side input data')
    elif windowing:
      raise NotImplementedError('explicit windowing')
    result = ParDo(fn, *args, **kwargs)
    # This is an ordered list stored as a dict (see the comments in
    # to_runner_api_parameter above).
    indexed_side_inputs = [(
        get_sideinput_index(tag),
        pvalue.AsSideInput.from_runner_api(si, context)) for tag,
                           si in pardo_payload.side_inputs.items()]
    result.side_inputs = [si for _, si in sorted(indexed_side_inputs)]
    return result

  def runner_api_requires_keyed_input(self):
    return userstate.is_stateful_dofn(self.fn)

  def get_restriction_coder(self):
    """Returns `restriction coder if `DoFn` of this `ParDo` is a SDF.

    Returns `None` otherwise.
    """
    from apache_beam.runners.common import DoFnSignature
    return DoFnSignature(self.fn).get_restriction_coder()

  def _add_type_constraint_from_consumer(self, full_label, input_type_hints):
    if not hasattr(self.fn, '_runtime_output_constraints'):
      self.fn._runtime_output_constraints = {}
    self.fn._runtime_output_constraints[full_label] = input_type_hints


class _MultiParDo(PTransform):
  def __init__(self, do_transform, tags, main_tag, allow_unknown_tags=None):
    super().__init__(do_transform.label)
    self._do_transform = do_transform
    self._tags = tags
    self._main_tag = main_tag
    self._allow_unknown_tags = allow_unknown_tags

  def expand(self, pcoll):
    _ = pcoll | self._do_transform
    return pvalue.DoOutputsTuple(
        pcoll.pipeline,
        self._do_transform,
        self._tags,
        self._main_tag,
        self._allow_unknown_tags)


class DoFnInfo(object):
  """This class represents the state in the ParDoPayload's function spec,
  which is the actual DoFn together with some data required for invoking it.
  """
  @staticmethod
  def register_stateless_dofn(urn):
    def wrapper(cls):
      StatelessDoFnInfo.REGISTERED_DOFNS[urn] = cls
      cls._stateless_dofn_urn = urn
      return cls

    return wrapper

  @classmethod
  def create(cls, fn, args, kwargs):
    if hasattr(fn, '_stateless_dofn_urn'):
      assert not args and not kwargs
      return StatelessDoFnInfo(fn._stateless_dofn_urn)
    else:
      return PickledDoFnInfo(cls._pickled_do_fn_info(fn, args, kwargs))

  @staticmethod
  def from_runner_api(spec, unused_context):
    if spec.urn == python_urns.PICKLED_DOFN_INFO:
      return PickledDoFnInfo(spec.payload)
    elif spec.urn in StatelessDoFnInfo.REGISTERED_DOFNS:
      return StatelessDoFnInfo(spec.urn)
    else:
      raise ValueError('Unexpected DoFn type: %s' % spec.urn)

  @staticmethod
  def _pickled_do_fn_info(fn, args, kwargs):
    # This can be cleaned up once all runners move to portability.
    return pickler.dumps((fn, args, kwargs, None, None))

  def serialized_dofn_data(self):
    raise NotImplementedError(type(self))


class PickledDoFnInfo(DoFnInfo):
  def __init__(self, serialized_data):
    self._serialized_data = serialized_data

  def serialized_dofn_data(self):
    return self._serialized_data

  def to_runner_api(self, unused_context):
    return beam_runner_api_pb2.FunctionSpec(
        urn=python_urns.PICKLED_DOFN_INFO, payload=self._serialized_data)


class StatelessDoFnInfo(DoFnInfo):

  REGISTERED_DOFNS = {}  # type: typing.Dict[str, typing.Type[DoFn]]

  def __init__(self, urn):
    # type: (str) -> None
    assert urn in self.REGISTERED_DOFNS
    self._urn = urn

  def serialized_dofn_data(self):
    return self._pickled_do_fn_info(self.REGISTERED_DOFNS[self._urn](), (), {})

  def to_runner_api(self, unused_context):
    return beam_runner_api_pb2.FunctionSpec(urn=self._urn)


def identity(x: T) -> T:
  return x


def FlatMap(fn=identity, *args, **kwargs):  # pylint: disable=invalid-name
  """:func:`FlatMap` is like :class:`ParDo` except it takes a callable to
  specify the transformation.

  The callable must return an iterable for each element of the input
  :class:`~apache_beam.pvalue.PCollection`. The elements of these iterables will
  be flattened into the output :class:`~apache_beam.pvalue.PCollection`. If
  no callable is given, then all elements of the input PCollection must already
  be iterables themselves and will be flattened into the output PCollection.

  Args:
    fn (callable): a callable object.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`FlatMap` outputs.

  Raises:
    TypeError: If the **fn** passed as argument is not a callable.
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
    TypeError: If the **fn** passed as argument is not a callable.
      Typical error is to pass a :class:`DoFn` instance which is supported only
      for :class:`ParDo`.
  """
  if not callable(fn):
    raise TypeError(
        'Map can be used only with callable objects. '
        'Received %r instead.' % (fn))
  from apache_beam.transforms.util import fn_takes_side_inputs
  if fn_takes_side_inputs(fn):
    wrapper = lambda x, *args, **kwargs: [fn(x, *args, **kwargs)]
  else:
    wrapper = lambda x: [fn(x)]

  label = 'Map(%s)' % ptransform.label_from_callable(fn)

  # TODO. What about callable classes?
  if hasattr(fn, '__name__'):
    wrapper.__name__ = fn.__name__

  # Proxy the type-hint information from the original function to this new
  # wrapped function.
  type_hints = get_type_hints(fn).with_defaults(
      typehints.decorators.IOTypeHints.from_callable(fn))
  if type_hints.input_types is not None:
    wrapper = with_input_types(
        *type_hints.input_types[0], **type_hints.input_types[1])(
            wrapper)
  output_hint = type_hints.simple_output_type(label)
  if output_hint:
    wrapper = with_output_types(
        typehints.Iterable[_strip_output_annotations(output_hint)])(
            wrapper)
  # pylint: disable=protected-access
  wrapper._argspec_fn = fn
  # pylint: enable=protected-access

  pardo = FlatMap(wrapper, *args, **kwargs)
  pardo.label = label
  return pardo


def MapTuple(fn, *args, **kwargs):  # pylint: disable=invalid-name
  r""":func:`MapTuple` is like :func:`Map` but expects tuple inputs and
  flattens them into multiple input arguments.

      beam.MapTuple(lambda a, b, ...: ...)

  In other words

      beam.MapTuple(fn)

  is equivalent to

      beam.Map(lambda element, ...: fn(\*element, ...))

  This can be useful when processing a PCollection of tuples
  (e.g. key-value pairs).

  Args:
    fn (callable): a callable object.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`MapTuple` outputs.

  Raises:
    TypeError: If the **fn** passed as argument is not a callable.
      Typical error is to pass a :class:`DoFn` instance which is supported only
      for :class:`ParDo`.
  """
  if not callable(fn):
    raise TypeError(
        'MapTuple can be used only with callable objects. '
        'Received %r instead.' % (fn))

  label = 'MapTuple(%s)' % ptransform.label_from_callable(fn)

  arg_names, defaults = get_function_args_defaults(fn)
  num_defaults = len(defaults)
  if num_defaults < len(args) + len(kwargs):
    raise TypeError('Side inputs must have defaults for MapTuple.')

  if defaults or args or kwargs:
    wrapper = lambda x, *args, **kwargs: [fn(*(tuple(x) + args), **kwargs)]
  else:
    wrapper = lambda x: [fn(*x)]

  # Proxy the type-hint information from the original function to this new
  # wrapped function.
  type_hints = get_type_hints(fn).with_defaults(
      typehints.decorators.IOTypeHints.from_callable(fn))
  if type_hints.input_types is not None:
    # TODO(BEAM-14052): ignore input hints, as we do not have enough
    # information to infer the input type hint of the wrapper function.
    pass
  output_hint = type_hints.simple_output_type(label)
  if output_hint:
    wrapper = with_output_types(
        typehints.Iterable[_strip_output_annotations(output_hint)])(
            wrapper)

  # Replace the first (args) component.
  modified_arg_names = ['tuple_element'] + arg_names[-num_defaults:]
  modified_argspec = (modified_arg_names, defaults)
  pardo = ParDo(
      CallableWrapperDoFn(wrapper, fullargspec=modified_argspec),
      *args,
      **kwargs)
  pardo.label = label
  return pardo


def FlatMapTuple(fn, *args, **kwargs):  # pylint: disable=invalid-name
  r""":func:`FlatMapTuple` is like :func:`FlatMap` but expects tuple inputs and
  flattens them into multiple input arguments.

      beam.FlatMapTuple(lambda a, b, ...: ...)

  is equivalent to Python 2

      beam.FlatMap(lambda (a, b, ...), ...: ...)

  In other words

      beam.FlatMapTuple(fn)

  is equivalent to

      beam.FlatMap(lambda element, ...: fn(\*element, ...))

  This can be useful when processing a PCollection of tuples
  (e.g. key-value pairs).

  Args:
    fn (callable): a callable object.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`FlatMapTuple` outputs.

  Raises:
    TypeError: If the **fn** passed as argument is not a callable.
      Typical error is to pass a :class:`DoFn` instance which is supported only
      for :class:`ParDo`.
  """
  if not callable(fn):
    raise TypeError(
        'FlatMapTuple can be used only with callable objects. '
        'Received %r instead.' % (fn))

  label = 'FlatMapTuple(%s)' % ptransform.label_from_callable(fn)

  arg_names, defaults = get_function_args_defaults(fn)
  num_defaults = len(defaults)
  if num_defaults < len(args) + len(kwargs):
    raise TypeError('Side inputs must have defaults for FlatMapTuple.')

  if defaults or args or kwargs:
    wrapper = lambda x, *args, **kwargs: fn(*(tuple(x) + args), **kwargs)
  else:
    wrapper = lambda x: fn(*x)

  # Proxy the type-hint information from the original function to this new
  # wrapped function.
  type_hints = get_type_hints(fn).with_defaults(
      typehints.decorators.IOTypeHints.from_callable(fn))
  if type_hints.input_types is not None:
    # TODO(BEAM-14052): ignore input hints, as we do not have enough
    # information to infer the input type hint of the wrapper function.
    pass
  output_hint = type_hints.simple_output_type(label)
  if output_hint:
    wrapper = with_output_types(_strip_output_annotations(output_hint))(wrapper)

  # Replace the first (args) component.
  modified_arg_names = ['tuple_element'] + arg_names[-num_defaults:]
  modified_argspec = (modified_arg_names, defaults)
  pardo = ParDo(
      CallableWrapperDoFn(wrapper, fullargspec=modified_argspec),
      *args,
      **kwargs)
  pardo.label = label
  return pardo


class _ExceptionHandlingWrapper(ptransform.PTransform):
  """Implementation of ParDo.with_exception_handling."""
  def __init__(
      self,
      fn,
      args,
      kwargs,
      main_tag,
      dead_letter_tag,
      exc_class,
      partial,
      use_subprocess,
      threshold,
      threshold_windowing,
      timeout,
      error_handler,
      on_failure_callback):
    if partial and use_subprocess:
      raise ValueError('partial and use_subprocess are mutually incompatible.')
    self._fn = fn
    self._args = args
    self._kwargs = kwargs
    self._main_tag = main_tag
    self._dead_letter_tag = dead_letter_tag
    self._exc_class = exc_class
    self._partial = partial
    self._use_subprocess = use_subprocess
    self._threshold = threshold
    self._threshold_windowing = threshold_windowing
    self._timeout = timeout
    self._error_handler = error_handler
    self._on_failure_callback = on_failure_callback

  def expand(self, pcoll):
    if self._use_subprocess:
      wrapped_fn = _SubprocessDoFn(self._fn, timeout=self._timeout)
    elif self._timeout:
      wrapped_fn = _TimeoutDoFn(self._fn, timeout=self._timeout)
    else:
      wrapped_fn = self._fn
    result = pcoll | ParDo(
        _ExceptionHandlingWrapperDoFn(
            wrapped_fn,
            self._dead_letter_tag,
            self._exc_class,
            self._partial,
            self._on_failure_callback),
        *self._args,
        **self._kwargs).with_outputs(
            self._dead_letter_tag, main=self._main_tag, allow_unknown_tags=True)
    #TODO(BEAM-18957): Fix when type inference supports tagged outputs.
    result[self._main_tag].element_type = self._fn.infer_output_type(
        pcoll.element_type)

    if self._threshold < 1.0:

      class MaybeWindow(ptransform.PTransform):
        @staticmethod
        def expand(pcoll):
          if self._threshold_windowing:
            return pcoll | WindowInto(self._threshold_windowing)
          else:
            return pcoll

      input_count_view = pcoll | 'CountTotal' >> (
          MaybeWindow() | Map(lambda _: 1)
          | CombineGlobally(sum).as_singleton_view())
      bad_count_pcoll = result[self._dead_letter_tag] | 'CountBad' >> (
          MaybeWindow() | Map(lambda _: 1)
          | CombineGlobally(sum).without_defaults())

      def check_threshold(bad, total, threshold, window=DoFn.WindowParam):
        if bad > total * threshold:
          raise ValueError(
              'The number of failing elements within the window %r '
              'exceeded threshold: %s / %s = %s > %s' %
              (window, bad, total, bad / total, threshold))

      _ = bad_count_pcoll | Map(
          check_threshold, input_count_view, self._threshold)

    if self._error_handler:
      self._error_handler.add_error_pcollection(result[self._dead_letter_tag])
      return result[self._main_tag]
    else:
      return result


class _ExceptionHandlingWrapperDoFn(DoFn):
  def __init__(
      self, fn, dead_letter_tag, exc_class, partial, on_failure_callback):
    self._fn = fn
    self._dead_letter_tag = dead_letter_tag
    self._exc_class = exc_class
    self._partial = partial
    self._on_failure_callback = on_failure_callback

  def __getattribute__(self, name):
    if (name.startswith('__') or name in self.__dict__ or
        name in _ExceptionHandlingWrapperDoFn.__dict__):
      return object.__getattribute__(self, name)
    else:
      return getattr(self._fn, name)

  def process(self, *args, **kwargs):
    try:
      result = self._fn.process(*args, **kwargs)
      if not self._partial:
        # Don't emit any results until we know there will be no errors.
        result = list(result)
      yield from result
    except self._exc_class as exn:
      if self._on_failure_callback is not None:
        try:
          self._on_failure_callback(exn, args[0])
        except Exception as e:
          logging.warning('on_failure_callback failed with error: %s', e)
      yield pvalue.TaggedOutput(
          self._dead_letter_tag,
          (
              args[0], (
                  type(exn),
                  repr(exn),
                  traceback.format_exception(*sys.exc_info()))))


# Idea adapted from https://github.com/tosun-si/asgarde.
# TODO(robertwb): Consider how this could fit into the public API.
# TODO(robertwb): Generalize to all PValue types.
class _PValueWithErrors(object):
  """This wraps a PCollection such that transforms can be chained in a linear
  manner while still accumulating any errors."""
  def __init__(self, pcoll, exception_handling_args, upstream_errors=()):
    self._pcoll = pcoll
    self._exception_handling_args = exception_handling_args
    self._upstream_errors = upstream_errors

  @property
  def pipeline(self):
    return self._pcoll.pipeline

  @property
  def element_type(self):
    return self._pcoll.element_type

  @element_type.setter
  def element_type(self, value):
    self._pcoll.element_type = value

  def main_output_tag(self):
    return self._exception_handling_args.get('main_tag', 'good')

  def error_output_tag(self):
    return self._exception_handling_args.get('dead_letter_tag', 'bad')

  def __or__(self, transform):
    return self.apply(transform)

  def apply(self, transform):
    if hasattr(transform, 'with_exception_handling'):
      result = self._pcoll | transform.with_exception_handling(
          **self._exception_handling_args)
      if result[self.main_output_tag()].element_type == typehints.Any:
        result[
            self.main_output_tag()].element_type = transform.infer_output_type(
                self._pcoll.element_type)
      # TODO(BEAM-18957): Add support for tagged type hints.
      result[self.error_output_tag()].element_type = typehints.Any
      return _PValueWithErrors(
          result[self.main_output_tag()],
          self._exception_handling_args,
          self._upstream_errors + (result[self.error_output_tag()], ))
    else:
      return _PValueWithErrors(
          self._pcoll | transform,
          self._exception_handling_args,
          self._upstream_errors)

  def accumulated_errors(self):
    if len(self._upstream_errors) == 1:
      return self._upstream_errors[0]
    else:
      return self._upstream_errors | Flatten()

  def as_result(self, error_post_processing=None):
    return {
        self.main_output_tag(): self._pcoll,
        self.error_output_tag(): self.accumulated_errors()
        if error_post_processing is None else self.accumulated_errors()
        | error_post_processing,
    }


class _MaybePValueWithErrors(object):
  """This is like _PValueWithErrors, but only wraps values if
  exception_handling_args is non-trivial.  It is useful for handling
  error-catching and non-error-catching code in a uniform manner.
  """
  def __init__(self, pvalue, exception_handling_args=None):
    if isinstance(pvalue, _PValueWithErrors):
      assert exception_handling_args is None
      self._pvalue = pvalue
    elif exception_handling_args is None:
      self._pvalue = pvalue
    else:
      self._pvalue = _PValueWithErrors(pvalue, exception_handling_args)

  @property
  def pipeline(self):
    return self._pvalue.pipeline

  @property
  def element_type(self):
    return self._pvalue.element_type

  @element_type.setter
  def element_type(self, value):
    self._pvalue.element_type = value

  def __or__(self, transform):
    return self.apply(transform)

  def apply(self, transform):
    return _MaybePValueWithErrors(self._pvalue | transform)

  def as_result(self, error_post_processing=None):
    if isinstance(self._pvalue, _PValueWithErrors):
      return self._pvalue.as_result(error_post_processing)
    else:
      return self._pvalue


class _SubprocessDoFn(DoFn):
  """Process method run in a subprocess, turning hard crashes into exceptions.
  """
  def __init__(self, fn, timeout=None):
    self._fn = fn
    self._serialized_fn = pickler.dumps(fn)
    self._timeout = timeout

  def __getattribute__(self, name):
    if (name.startswith('__') or name in self.__dict__ or
        name in type(self).__dict__):
      return object.__getattribute__(self, name)
    else:
      return getattr(self._fn, name)

  def setup(self):
    self._pool = None

  def start_bundle(self):
    # The pool is initialized lazily, including calls to setup and start_bundle.
    # This allows us to continue processing elements after a crash.
    pass

  def process(self, *args, **kwargs):
    return self._call_remote(self._remote_process, *args, **kwargs)

  def finish_bundle(self):
    self._call_remote(self._remote_finish_bundle)

  def teardown(self):
    self._call_remote(self._remote_teardown)
    self._terminate_pool()

  def _call_remote(self, method, *args, **kwargs):
    if self._pool is None:
      self._pool = concurrent.futures.ProcessPoolExecutor(1)
      self._pool.submit(self._remote_init, self._serialized_fn).result()
    try:
      return self._pool.submit(method, *args, **kwargs).result(
          self._timeout if method == self._remote_process else None)
    except (concurrent.futures.process.BrokenProcessPool,
            TimeoutError,
            concurrent.futures._base.TimeoutError):
      self._terminate_pool()
      raise

  def _terminate_pool(self):
    """Forcibly terminate the pool, not leaving any live subprocesses."""
    pool = self._pool
    self._pool = None
    processes = list(pool._processes.values())
    pool.shutdown(wait=False)
    for p in processes:
      if p.is_alive():
        p.kill()
    time.sleep(1)
    for p in processes:
      if p.is_alive():
        p.terminate()

  # These are classmethods to avoid picking the state of self.
  # They should only be called in an isolated process, so there's no concern
  # about sharing state or thread safety.

  @classmethod
  def _remote_init(cls, serialized_fn):
    cls._serialized_fn = serialized_fn
    cls._fn = None
    cls._started = False

  @classmethod
  def _remote_process(cls, *args, **kwargs):
    if cls._fn is None:
      cls._fn = pickler.loads(cls._serialized_fn)
      cls._fn.setup()
    if not cls._started:
      cls._fn.start_bundle()
      cls._started = True
    result = cls._fn.process(*args, **kwargs)
    if result:
      # Don't return generator objects.
      result = list(result)
    return result

  @classmethod
  def _remote_finish_bundle(cls):
    if cls._started:
      cls._started = False
      if cls._fn.finish_bundle():
        # This is because we restart and re-initialize the pool if it crashed.
        raise RuntimeError(
            "Returning elements from _SubprocessDoFn.finish_bundle not safe.")

  @classmethod
  def _remote_teardown(cls):
    if cls._fn:
      cls._fn.teardown()
    cls._fn = None


class _TimeoutDoFn(DoFn):
  """Process method run in a separate thread allowing timeouts.
  """
  def __init__(self, fn, timeout=None):
    self._fn = fn
    self._timeout = timeout
    self._pool = None

  def __getattribute__(self, name):
    if (name.startswith('__') or name in self.__dict__ or
        name in type(self).__dict__):
      return object.__getattribute__(self, name)
    else:
      return getattr(self._fn, name)

  def process(self, *args, **kwargs):
    if self._pool is None:
      self._pool = concurrent.futures.ThreadPoolExecutor(10)
    # Ensure we iterate over the entire output list in the given amount of time.
    try:
      return self._pool.submit(
          lambda: list(self._fn.process(*args, **kwargs))).result(
              self._timeout)
    except TimeoutError:
      self._pool.shutdown(wait=False)
      self._pool = None
      raise

  def teardown(self):
    try:
      self._fn.teardown()
    finally:
      if self._pool is not None:
        self._pool.shutdown(wait=False)
        self._pool = None


def Filter(fn, *args, **kwargs):  # pylint: disable=invalid-name
  """:func:`Filter` is a :func:`FlatMap` with its callable filtering out
  elements.

  Filter accepts a function that keeps elements that return True, and filters
  out the remaining elements.

  Args:
    fn (``Callable[..., bool]``): a callable object. First argument will be an
      element.
    *args: positional arguments passed to the transform callable.
    **kwargs: keyword arguments passed to the transform callable.

  Returns:
    ~apache_beam.pvalue.PCollection:
    A :class:`~apache_beam.pvalue.PCollection` containing the
    :func:`Filter` outputs.

  Raises:
    TypeError: If the **fn** passed as argument is not a callable.
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

  # Get type hints from this instance or the callable. Do not use output type
  # hints from the callable (which should be bool if set).
  fn_type_hints = typehints.decorators.IOTypeHints.from_callable(fn)
  if fn_type_hints is not None:
    fn_type_hints = fn_type_hints.with_output_types()
  type_hints = get_type_hints(fn).with_defaults(fn_type_hints)

  # Proxy the type-hint information from the function being wrapped, setting the
  # output type to be the same as the input type.
  if type_hints.input_types is not None:
    wrapper = with_input_types(
        *type_hints.input_types[0], **type_hints.input_types[1])(
            wrapper)
  output_hint = type_hints.simple_output_type(label)
  if (output_hint is None and get_type_hints(wrapper).input_types and
      get_type_hints(wrapper).input_types[0]):
    output_hint = get_type_hints(wrapper).input_types[0][0]
  if output_hint:
    wrapper = with_output_types(
        typehints.Iterable[_strip_output_annotations(output_hint)])(
            wrapper)
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
    TypeError: If the output type of the input
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
  fanout = None  # type: typing.Optional[int]

  def __init__(self, fn, *args, **kwargs):
    if not (isinstance(fn, CombineFn) or callable(fn)):
      raise TypeError(
          'CombineGlobally can be used only with combineFn objects. '
          'Received %r instead.' % (fn))

    super().__init__()
    self.fn = fn
    self.args = args
    self.kwargs = kwargs

  def display_data(self):
    return {
        'combine_fn': DisplayDataItem(
            self.fn.__class__, label='Combine Function'),
        'combine_fn_dd': self.fn,
    }

  def default_label(self):
    if self.fanout is None:
      return '%s(%s)' % (
          self.__class__.__name__, ptransform.label_from_callable(self.fn))
    else:
      return '%s(%s, fanout=%s)' % (
          self.__class__.__name__,
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

    combine_fn = CombineFn.maybe_from_callable(
        self.fn, has_side_inputs=self.args or self.kwargs)
    combine_per_key = CombinePerKey(combine_fn, *self.args, **self.kwargs)
    if self.fanout:
      combine_per_key = combine_per_key.with_hot_key_fanout(self.fanout)

    combined = (
        pcoll
        | 'KeyWithVoid' >> add_input_types(
            ParDo(_KeyWithNone()).with_output_types(
                typehints.KV[None, pcoll.element_type]))
        | 'CombinePerKey' >> combine_per_key
        | 'UnKey' >> Map(lambda k_v: k_v[1]))

    if not self.has_defaults and not self.as_view:
      return combined

    elif self.as_view:
      if self.has_defaults:
        try:
          combine_fn.setup(*self.args, **self.kwargs)
          # This is called in the main program, but cannot be avoided
          # in the as_view case as it must be available to all windows.
          default_value = combine_fn.apply([], *self.args, **self.kwargs)
        finally:
          combine_fn.teardown(*self.args, **self.kwargs)
      else:
        default_value = pvalue.AsSingleton._NO_DEFAULT
      return pvalue.AsSingleton(combined, default_value=default_value)

    else:
      if pcoll.windowing.windowfn != GlobalWindows():
        raise ValueError(
            "Default values are not yet supported in CombineGlobally() if the "
            "output  PCollection is not windowed by GlobalWindows. "
            "Instead, use CombineGlobally().without_defaults() to output "
            "an empty PCollection if the input PCollection is empty, "
            "or CombineGlobally().as_singleton_view() to get the default "
            "output of the CombineFn if the input PCollection is empty.")

      # log the error for this ill-defined streaming case now
      if not pcoll.is_bounded and not pcoll.windowing.is_default():
        _LOGGER.error(
            "When combining elements in unbounded collections with "
            "the non-default windowing strategy, you must explicitly "
            "specify how to define the combined result of an empty window. "
            "Please use CombineGlobally().without_defaults() to output "
            "an empty PCollection if the input PCollection is empty.")

      def typed(transform):
        # TODO(robertwb): We should infer this.
        if combined.element_type:
          return transform.with_output_types(combined.element_type)
        return transform

      # Capture in closure (avoiding capturing self).
      args, kwargs = self.args, self.kwargs

      def inject_default(_, combined):
        if combined:
          if len(combined) > 1:
            _LOGGER.error(
                "Multiple combined values unexpectedly provided"
                " for a global combine: %s",
                combined)
          assert len(combined) == 1
          return combined[0]
        else:
          try:
            combine_fn.setup(*args, **kwargs)
            default = combine_fn.apply([], *args, **kwargs)
          finally:
            combine_fn.teardown(*args, **kwargs)
          return default

      return (
          pcoll.pipeline
          | 'DoOnce' >> Create([None])
          | 'InjectDefault' >> typed(
              Map(inject_default, pvalue.AsList(combined))))

  @staticmethod
  @PTransform.register_urn(
      common_urns.composites.COMBINE_GLOBALLY.urn,
      beam_runner_api_pb2.CombinePayload)
  def from_runner_api_parameter(unused_ptransform, combine_payload, context):
    return CombineGlobally(
        CombineFn.from_runner_api(combine_payload.combine_fn, context))


@DoFnInfo.register_stateless_dofn(python_urns.KEY_WITH_NONE_DOFN)
class _KeyWithNone(DoFn):
  def process(self, v):
    yield None, v


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
    return {
        'combine_fn': DisplayDataItem(
            self.fn.__class__, label='Combine Function'),
        'combine_fn_dd': self.fn
    }

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
    result = self.fn.get_type_hints()
    k = typehints.TypeVariable('K')
    if result.input_types:
      args, kwargs = result.input_types
      args = (typehints.Tuple[k, args[0]], ) + args[1:]
      result = result.with_input_types(*args, **kwargs)
    else:
      result = result.with_input_types(typehints.Tuple[k, typehints.Any])
    if result.output_types:
      main_output_type = result.simple_output_type('')
      result = result.with_output_types(typehints.Tuple[k, main_output_type])
    else:
      result = result.with_output_types(typehints.Tuple[k, typehints.Any])
    return result

  def to_runner_api_parameter(
      self,
      context,  # type: PipelineContext
  ):
    # type: (...) -> typing.Tuple[str, beam_runner_api_pb2.CombinePayload]
    if self.args or self.kwargs:
      from apache_beam.transforms.combiners import curry_combine_fn
      combine_fn = curry_combine_fn(self.fn, self.args, self.kwargs)
    else:
      combine_fn = self.fn
    return (
        common_urns.composites.COMBINE_PER_KEY.urn,
        _combine_payload(combine_fn, context))

  @staticmethod
  @PTransform.register_urn(
      common_urns.composites.COMBINE_PER_KEY.urn,
      beam_runner_api_pb2.CombinePayload)
  def from_runner_api_parameter(unused_ptransform, combine_payload, context):
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
        *args,
        **kwargs)

  def to_runner_api_parameter(self, context):
    if self.args or self.kwargs:
      from apache_beam.transforms.combiners import curry_combine_fn
      combine_fn = curry_combine_fn(self.fn, self.args, self.kwargs)
    else:
      combine_fn = self.fn
    return (
        common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
        _combine_payload(combine_fn, context))

  @staticmethod
  @PTransform.register_urn(
      common_urns.combine_components.COMBINE_GROUPED_VALUES.urn,
      beam_runner_api_pb2.CombinePayload)
  def from_runner_api_parameter(unused_ptransform, combine_payload, context):
    return CombineValues(
        CombineFn.from_runner_api(combine_payload.combine_fn, context))


class CombineValuesDoFn(DoFn):
  """DoFn for performing per-key Combine transforms."""

  def __init__(
      self,
      input_pcoll_type,
      combinefn,  # type: CombineFn
      runtime_type_check,  # type: bool
  ):
    super().__init__()
    self.combinefn = combinefn
    self.runtime_type_check = runtime_type_check

  def setup(self):
    self.combinefn.setup()

  def process(self, element, *args, **kwargs):
    # Expected elements input to this DoFn are 2-tuples of the form
    # (key, iter), with iter an iterable of all the values associated with key
    # in the input PCollection.
    if self.runtime_type_check:
      # Apply the combiner in a single operation rather than artificially
      # breaking it up so that output type violations manifest as TypeCheck
      # errors rather than type errors.
      return [(element[0], self.combinefn.apply(element[1], *args, **kwargs))]

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
              *args,
              **kwargs))
    # Merge the accumulators.
    accumulator = self.combinefn.merge_accumulators(
        accumulators, *args, **kwargs)
    # Convert accumulator to the final result.
    return [(
        element[0], self.combinefn.extract_output(accumulator, *args,
                                                  **kwargs))]

  def teardown(self):
    self.combinefn.teardown()

  def default_type_hints(self):
    hints = self.combinefn.get_type_hints()
    if hints.input_types:
      K = typehints.TypeVariable('K')
      args, kwargs = hints.input_types
      args = (typehints.Tuple[K, typehints.Iterable[args[0]]], ) + args[1:]
      hints = hints.with_input_types(*args, **kwargs)
    else:
      K = typehints.Any
    if hints.output_types:
      main_output_type = hints.simple_output_type('')
      hints = hints.with_output_types(typehints.Tuple[K, main_output_type])
    return hints


class _CombinePerKeyWithHotKeyFanout(PTransform):

  def __init__(
      self,
      combine_fn,  # type: CombineFn
      fanout,  # type: typing.Union[int, typing.Callable[[typing.Any], int]]
  ):
    # type: (...) -> None
    self._combine_fn = combine_fn
    self._fanout_fn = ((lambda key: fanout)
                       if isinstance(fanout, int) else fanout)

  def default_label(self):
    return '%s(%s, fanout=%s)' % (
        self.__class__.__name__,
        ptransform.label_from_callable(self._combine_fn),
        ptransform.label_from_callable(self._fanout_fn))

  def expand(self, pcoll):

    from apache_beam.transforms.trigger import AccumulationMode
    combine_fn = self._combine_fn
    fanout_fn = self._fanout_fn

    if isinstance(pcoll.windowing.windowfn, SlidingWindows):
      raise ValueError(
          'CombinePerKey.with_hot_key_fanout does not yet work properly with '
          'SlidingWindows. See: https://github.com/apache/beam/issues/20528')

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

      setup = combine_fn.setup
      create_accumulator = combine_fn.create_accumulator
      add_input = combine_fn.add_input
      merge_accumulators = combine_fn.merge_accumulators
      compact = combine_fn.compact
      teardown = combine_fn.teardown

    class PostCombineFn(CombineFn):
      @staticmethod
      def add_input(accumulator, element):
        is_accumulator, value = element
        if is_accumulator:
          return combine_fn.merge_accumulators([accumulator, value])
        else:
          return combine_fn.add_input(accumulator, value)

      setup = combine_fn.setup
      create_accumulator = combine_fn.create_accumulator
      merge_accumulators = combine_fn.merge_accumulators
      compact = combine_fn.compact
      extract_output = combine_fn.extract_output
      teardown = combine_fn.teardown

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
    return ((cold, precombined_hot)
            | Flatten()
            | CombinePerKey(PostCombineFn()))


@typehints.with_input_types(typing.Tuple[K, V])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class GroupByKey(PTransform):
  """A group by key transform.

  Processes an input PCollection consisting of key/value pairs represented as a
  tuple pair. The result is a PCollection where values having a common key are
  grouped together.  For example (a, 1), (b, 2), (a, 3) will result into
  (a, [1, 3]), (b, [2]).

  The implementation here is used only when run on the local direct runner.
  """
  class ReifyWindows(DoFn):
    def process(
        self, element, window=DoFn.WindowParam, timestamp=DoFn.TimestampParam):
      try:
        k, v = element
      except TypeError:
        raise TypeCheckError(
            'Input to GroupByKey must be a PCollection with '
            'elements compatible with KV[A, B]')

      return [(k, WindowedValue(v, timestamp, [window]))]

    def infer_output_type(self, input_type):
      key_type, value_type = trivial_inference.key_value_types(input_type)
      return typehints.KV[
          key_type, typehints.WindowedValue[value_type]]  # type: ignore[misc]

  def expand(self, pcoll):
    from apache_beam.transforms.trigger import DataLossReason
    from apache_beam.transforms.trigger import DefaultTrigger
    windowing = pcoll.windowing
    trigger = windowing.triggerfn
    if not pcoll.is_bounded and isinstance(
        windowing.windowfn, GlobalWindows) and isinstance(trigger,
                                                          DefaultTrigger):
      if pcoll.pipeline.allow_unsafe_triggers:
        # TODO(BEAM-9487) Change comment for Beam 2.33
        _LOGGER.warning(
            '%s: PCollection passed to GroupByKey is unbounded, has a global '
            'window, and uses a default trigger. This is being allowed '
            'because --allow_unsafe_triggers is set, but it may prevent '
            'data from making it through the pipeline.',
            self.label)
      else:
        raise ValueError(
            'GroupByKey cannot be applied to an unbounded ' +
            'PCollection with global windowing and a default trigger')

    unsafe_reason = trigger.may_lose_data(windowing)
    if unsafe_reason != DataLossReason.NO_POTENTIAL_LOSS:
      reason_msg = str(unsafe_reason).replace('DataLossReason.', '')
      if pcoll.pipeline.allow_unsafe_triggers:
        _LOGGER.warning(
            '%s: Unsafe trigger `%s` detected (reason: %s). This is '
            'being allowed because --allow_unsafe_triggers is set. This could '
            'lead to missing or incomplete groups.',
            self.label,
            trigger,
            reason_msg)
      else:
        msg = '{}: Unsafe trigger: `{}` may lose data. '.format(
            self.label, trigger)
        msg += 'Reason: {}. '.format(reason_msg)
        msg += 'This can be overriden with the --allow_unsafe_triggers flag.'
        raise ValueError(msg)

    return pvalue.PCollection.from_(pcoll)

  def infer_output_type(self, input_type):
    key_type, value_type = (typehints.typehints.coerce_to_kv_type(
        input_type).tuple_types)
    return typehints.KV[key_type, typehints.Iterable[value_type]]

  def to_runner_api_parameter(self, unused_context):
    # type: (PipelineContext) -> typing.Tuple[str, None]
    return common_urns.primitives.GROUP_BY_KEY.urn, None

  @staticmethod
  @PTransform.register_urn(common_urns.primitives.GROUP_BY_KEY.urn, None)
  def from_runner_api_parameter(
      unused_ptransform, unused_payload, unused_context):
    return GroupByKey()

  def runner_api_requires_keyed_input(self):
    return True


def _expr_to_callable(expr, pos):
  if isinstance(expr, str):
    return lambda x: getattr(x, expr)
  elif callable(expr):
    return expr
  else:
    raise TypeError(
        'Field expression %r at %s must be a callable or a string.' %
        (expr, pos))


class GroupBy(PTransform):
  """Groups a PCollection by one or more expressions, used to derive the key.

  `GroupBy(expr)` is roughly equivalent to

      beam.Map(lambda v: (expr(v), v)) | beam.GroupByKey()

  but provides several conveniences, e.g.

      * Several arguments may be provided, as positional or keyword arguments,
        resulting in a tuple-like key. For example `GroupBy(a=expr1, b=expr2)`
        groups by a key with attributes `a` and `b` computed by applying
        `expr1` and `expr2` to each element.

      * Strings can be used as a shorthand for accessing an attribute, e.g.
        `GroupBy('some_field')` is equivalent to
        `GroupBy(lambda v: getattr(v, 'some_field'))`.

  The GroupBy operation can be made into an aggregating operation by invoking
  its `aggregate_field` method.
  """

  def __init__(
      self,
      *fields,  # type: typing.Union[str, typing.Callable]
      **kwargs  # type: typing.Union[str, typing.Callable]
  ):
    if len(fields) == 1 and not kwargs:
      self._force_tuple_keys = False
      name = fields[0] if isinstance(fields[0], str) else 'key'
      key_fields = [(name, _expr_to_callable(fields[0], 0))]
    else:
      self._force_tuple_keys = True
      key_fields = []
      for ix, field in enumerate(fields):
        name = field if isinstance(field, str) else 'key%d' % ix
        key_fields.append((name, _expr_to_callable(field, ix)))
      for name, expr in kwargs.items():
        key_fields.append((name, _expr_to_callable(expr, name)))
    self._key_fields = key_fields
    field_names = tuple(name for name, _ in key_fields)
    self._key_type = lambda *values: _dynamic_named_tuple('Key', field_names)(
        *values)

  def aggregate_field(
      self,
      field,  # type: typing.Union[str, typing.Callable]
      combine_fn,  # type: typing.Union[typing.Callable, CombineFn]
      dest,  # type: str
  ):
    """Returns a grouping operation that also aggregates grouped values.

    Args:
      field: indicates the field to be aggregated
      combine_fn: indicates the aggregation function to be used
      dest: indicates the name that will be used for the aggregate in the output

    May be called repeatedly to aggregate multiple fields, e.g.

        GroupBy('key')
            .aggregate_field('some_attr', sum, 'sum_attr')
            .aggregate_field(lambda v: ..., MeanCombineFn, 'mean')
    """
    return _GroupAndAggregate(self, ()).aggregate_field(field, combine_fn, dest)

  def force_tuple_keys(self, value=True):
    """Forces the keys to always be tuple-like, even if there is only a single
    expression.
    """
    res = copy.copy(self)
    res._force_tuple_keys = value
    return res

  def _key_func(self):
    if not self._force_tuple_keys and len(self._key_fields) == 1:
      return self._key_fields[0][1]
    else:
      key_type = self._key_type
      key_exprs = [expr for _, expr in self._key_fields]
      return lambda element: key_type(*(expr(element) for expr in key_exprs))

  def _key_type_hint(self, input_type):
    if not self._force_tuple_keys and len(self._key_fields) == 1:
      expr = self._key_fields[0][1]
      return trivial_inference.infer_return_type(expr, [input_type])
    else:
      return row_type.RowTypeConstraint.from_fields([
          (name, trivial_inference.infer_return_type(expr, [input_type]))
          for (name, expr) in self._key_fields
      ])

  def default_label(self):
    return 'GroupBy(%s)' % ', '.join(name for name, _ in self._key_fields)

  def expand(self, pcoll):
    input_type = pcoll.element_type or typing.Any
    return (
        pcoll
        | Map(lambda x: (self._key_func()(x), x)).with_output_types(
            typehints.Tuple[self._key_type_hint(input_type), input_type])
        | GroupByKey())


_dynamic_named_tuple_cache = {
}  # type: typing.Dict[typing.Tuple[str, typing.Tuple[str, ...]], typing.Type[tuple]]


def _dynamic_named_tuple(type_name, field_names):
  # type: (str, typing.Tuple[str, ...]) -> typing.Type[tuple]
  cache_key = (type_name, field_names)
  result = _dynamic_named_tuple_cache.get(cache_key)
  if result is None:
    import collections
    result = _dynamic_named_tuple_cache[cache_key] = collections.namedtuple(
        type_name, field_names)
    # typing: can't override a method. also, self type is unknown and can't
    # be cast to tuple
    result.__reduce__ = lambda self: (  # type: ignore[assignment]
        _unpickle_dynamic_named_tuple, (type_name, field_names, tuple(self)))  # type: ignore[arg-type]
  return result


def _unpickle_dynamic_named_tuple(type_name, field_names, values):
  # type: (str, typing.Tuple[str, ...], typing.Iterable[typing.Any]) -> tuple
  return _dynamic_named_tuple(type_name, field_names)(*values)


class _GroupAndAggregate(PTransform):
  def __init__(self, grouping, aggregations):
    self._grouping = grouping
    self._aggregations = aggregations

  def aggregate_field(
      self,
      field,  # type: typing.Union[str, typing.Callable]
      combine_fn,  # type: typing.Union[typing.Callable, CombineFn]
      dest,  # type: str
  ):
    field = _expr_to_callable(field, 0)
    return _GroupAndAggregate(
        self._grouping, list(self._aggregations) + [(field, combine_fn, dest)])

  def expand(self, pcoll):
    from apache_beam.transforms.combiners import TupleCombineFn
    key_func = self._grouping.force_tuple_keys(True)._key_func()
    value_exprs = [expr for expr, _, __ in self._aggregations]
    value_func = lambda element: [expr(element) for expr in value_exprs]
    result_fields = tuple(name
                          for name, _ in self._grouping._key_fields) + tuple(
                              dest for _, __, dest in self._aggregations)
    key_type_hint = self._grouping.force_tuple_keys(True)._key_type_hint(
        pcoll.element_type)

    return (
        pcoll
        | Map(lambda x: (key_func(x), value_func(x))).with_output_types(
            typehints.Tuple[key_type_hint, typing.Any])
        | CombinePerKey(
            TupleCombineFn(
                *[combine_fn for _, combine_fn, __ in self._aggregations]))
        | MapTuple(
            lambda key,
            value: _dynamic_named_tuple('Result', result_fields)
            (*(key + value))))


class Select(PTransform):
  """Converts the elements of a PCollection into a schema'd PCollection of Rows.

  `Select(...)` is roughly equivalent to `Map(lambda x: Row(...))` where each
  argument (which may be a string or callable) of `ToRow` is applied to `x`.
  For example,

      pcoll | beam.Select('a', b=lambda x: foo(x))

  is the same as

      pcoll | beam.Map(lambda x: beam.Row(a=x.a, b=foo(x)))
  """

  def __init__(
      self,
      *args,  # type: typing.Union[str, typing.Callable]
      **kwargs  # type: typing.Union[str, typing.Callable]
  ):
    self._fields = [(
        expr if isinstance(expr, str) else 'arg%02d' % ix,
        _expr_to_callable(expr, ix)) for (ix, expr) in enumerate(args)
                    ] + [(name, _expr_to_callable(expr, name))
                         for (name, expr) in kwargs.items()]
    self._exception_handling_args = None

  def with_exception_handling(self, **kwargs):
    self._exception_handling_args = kwargs
    return self

  def default_label(self):
    return 'ToRows(%s)' % ', '.join(name for name, _ in self._fields)

  def expand(self, pcoll):
    return (
        _MaybePValueWithErrors(pcoll, self._exception_handling_args) | Map(
            lambda x: pvalue.Row(
                **{name: expr(x)
                   for name, expr in self._fields}))).as_result()

  def infer_output_type(self, input_type):
    def extract_return_type(expr):
      expr_hints = get_type_hints(expr)
      if (expr_hints and expr_hints.has_simple_output_type() and
          expr_hints.simple_output_type(None) != typehints.Any):
        return expr_hints.simple_output_type(None)
      else:
        return trivial_inference.infer_return_type(expr, [input_type])

    return row_type.RowTypeConstraint.from_fields([
        (name, extract_return_type(expr)) for (name, expr) in self._fields
    ])


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
    args, kwargs = util.insert_values_in_args(
        self.args, self.kwargs, self.side_inputs)
    return pcoll | ParDo(self.ApplyPartitionFnFn(), self.fn, *args, **
                         kwargs).with_outputs(*[str(t) for t in range(n)])


class Windowing(object):
  def __init__(self,
               windowfn,  # type: WindowFn
               triggerfn=None,  # type: typing.Optional[TriggerFn]
               accumulation_mode=None,  # type: typing.Optional[beam_runner_api_pb2.AccumulationMode.Enum.ValueType]
               timestamp_combiner=None,  # type: typing.Optional[beam_runner_api_pb2.OutputTime.Enum.ValueType]
               allowed_lateness=0, # type: typing.Union[int, float]
               environment_id=None, # type: typing.Optional[str]
               ):
    """Class representing the window strategy.

    Args:
      windowfn: Window assign function.
      triggerfn: Trigger function.
      accumulation_mode: a AccumulationMode, controls what to do with data
        when a trigger fires multiple times.
      timestamp_combiner: a TimestampCombiner, determines how output
        timestamps of grouping operations are assigned.
      allowed_lateness: Maximum delay in seconds after end of window
        allowed for any late data to be processed without being discarded
        directly.
      environment_id: Environment where the current window_fn should be
        applied in.
    """
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
          'window fn (%s) does not have a determanistic coder (%s)' %
          (windowfn, windowfn.get_window_coder()))
    self.windowfn = windowfn
    self.triggerfn = triggerfn
    self.accumulation_mode = accumulation_mode
    self.allowed_lateness = Duration.of(allowed_lateness)
    self.environment_id = environment_id
    self.timestamp_combiner = (
        timestamp_combiner or TimestampCombiner.OUTPUT_AT_EOW)
    self._is_default = (
        self.windowfn == GlobalWindows() and
        self.triggerfn == DefaultTrigger() and
        self.accumulation_mode == AccumulationMode.DISCARDING and
        self.timestamp_combiner == TimestampCombiner.OUTPUT_AT_EOW and
        self.allowed_lateness == 0)

  def __repr__(self):
    return "Windowing(%s, %s, %s, %s, %s)" % (
        self.windowfn,
        self.triggerfn,
        self.accumulation_mode,
        self.timestamp_combiner,
        self.environment_id)

  def __eq__(self, other):
    if type(self) == type(other):
      if self._is_default and other._is_default:
        return True
      return (
          self.windowfn == other.windowfn and
          self.triggerfn == other.triggerfn and
          self.accumulation_mode == other.accumulation_mode and
          self.timestamp_combiner == other.timestamp_combiner and
          self.allowed_lateness == other.allowed_lateness and
          self.environment_id == self.environment_id)
    return False

  def __hash__(self):
    return hash((
        self.windowfn,
        self.triggerfn,
        self.accumulation_mode,
        self.allowed_lateness,
        self.timestamp_combiner,
        self.environment_id))

  def is_default(self):
    return self._is_default

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.WindowingStrategy
    environment_id = self.environment_id or context.default_environment_id()
    return beam_runner_api_pb2.WindowingStrategy(
        window_fn=self.windowfn.to_runner_api(context),
        # TODO(robertwb): Prohibit implicit multi-level merging.
        merge_status=(
            beam_runner_api_pb2.MergeStatus.NEEDS_MERGE
            if self.windowfn.is_merging() else
            beam_runner_api_pb2.MergeStatus.NON_MERGING),
        window_coder_id=context.coders.get_id(self.windowfn.get_window_coder()),
        trigger=self.triggerfn.to_runner_api(context),
        accumulation_mode=self.accumulation_mode,
        output_time=self.timestamp_combiner,
        # TODO(robertwb): Support EMIT_IF_NONEMPTY
        closing_behavior=beam_runner_api_pb2.ClosingBehavior.EMIT_ALWAYS,
        on_time_behavior=beam_runner_api_pb2.OnTimeBehavior.FIRE_ALWAYS,
        allowed_lateness=self.allowed_lateness.micros // 1000,
        environment_id=environment_id)

  @staticmethod
  def from_runner_api(proto, context):
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.trigger import TriggerFn
    return Windowing(
        windowfn=WindowFn.from_runner_api(proto.window_fn, context),
        triggerfn=TriggerFn.from_runner_api(proto.trigger, context),
        accumulation_mode=proto.accumulation_mode,
        timestamp_combiner=proto.output_time,
        allowed_lateness=Duration(micros=proto.allowed_lateness * 1000),
        environment_id=None)


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
      # type: (Windowing) -> None
      self.windowing = windowing

    def process(
        self, element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
      context = WindowFn.AssignContext(
          timestamp, element=element, window=window)
      new_windows = self.windowing.windowfn.assign(context)
      yield WindowedValue(element, context.timestamp, new_windows)

    def infer_output_type(self, input_type):
      return input_type

  def __init__(
      self,
      windowfn,  # type: typing.Union[Windowing, WindowFn]
      trigger=None,  # type: typing.Optional[TriggerFn]
      accumulation_mode=None,
      timestamp_combiner=None,
      allowed_lateness=0):
    """Initializes a WindowInto transform.

    Args:
      windowfn (Windowing, WindowFn): Function to be used for windowing.
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

      # Use windowing to fill in defaults for the extra arguments.
      trigger = trigger or windowing.triggerfn
      accumulation_mode = accumulation_mode or windowing.accumulation_mode
      timestamp_combiner = timestamp_combiner or windowing.timestamp_combiner

    self.windowing = Windowing(
        windowfn,
        trigger,
        accumulation_mode,
        timestamp_combiner,
        allowed_lateness)
    super().__init__(self.WindowIntoFn(self.windowing))

  def get_windowing(self, unused_inputs):
    # type: (typing.Any) -> Windowing
    return self.windowing

  def infer_output_type(self, input_type):
    return input_type

  def expand(self, pcoll):
    input_type = pcoll.element_type

    if input_type is not None:
      output_type = input_type
      self.with_input_types(input_type)
      self.with_output_types(output_type)
    return super().expand(pcoll)

  # typing: PTransform base class does not accept extra_kwargs
  def to_runner_api_parameter(self, context, **extra_kwargs):  # type: ignore[override]
    # type: (PipelineContext, **typing.Any) -> typing.Tuple[str, message.Message]
    return (
        common_urns.primitives.ASSIGN_WINDOWS.urn,
        self.windowing.to_runner_api(context))

  @staticmethod
  def from_runner_api_parameter(unused_ptransform, proto, context):
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
    super().__init__()
    self.pipeline = kwargs.pop(
        'pipeline', None)  # type: typing.Optional[Pipeline]
    if kwargs:
      raise ValueError('Unexpected keyword arguments: %s' % list(kwargs))

  def _extract_input_pvalues(self, pvalueish):
    try:
      pvalueish = tuple(pvalueish)
    except TypeError:
      raise ValueError(
          'Input to Flatten must be an iterable. '
          'Got a value of type %s instead.' % type(pvalueish))
    return pvalueish, pvalueish

  def expand(self, pcolls):
    windowing = self.get_windowing(pcolls)
    for pcoll in pcolls:
      self._check_pcollection(pcoll)
      if pcoll.windowing != windowing:
        _LOGGER.warning(
            'All input pcollections must have the same window. Windowing for '
            'flatten set to %s, windowing of pcoll %s set to %s',
            windowing,
            pcoll,
            pcoll.windowing)
    is_bounded = all(pcoll.is_bounded for pcoll in pcolls)
    return pvalue.PCollection(self.pipeline, is_bounded=is_bounded)

  def infer_output_type(self, input_type):
    return input_type

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> typing.Tuple[str, None]
    return common_urns.primitives.FLATTEN.urn, None

  @staticmethod
  def from_runner_api_parameter(
      unused_ptransform, unused_parameter, unused_context):
    return Flatten()


PTransform.register_urn(
    common_urns.primitives.FLATTEN.urn, None, Flatten.from_runner_api_parameter)


class FlattenWith(PTransform):
  """A PTransform that flattens its input with other PCollections.

  This is equivalent to creating a tuple containing both the input and the
  other PCollection(s), but has the advantage that it can be more easily used
  inline.

  Root PTransforms can be passed as well as PCollections, in which case their
  outputs will be flattened.
  """
  def __init__(self, *others):
    self._others = others

  def expand(self, pcoll):
    pcolls = [pcoll]
    for other in self._others:
      if isinstance(other, pvalue.PCollection):
        pcolls.append(other)
      elif isinstance(other, PTransform):
        pcolls.append(pcoll.pipeline | other)
      else:
        raise TypeError(
            'FlattenWith only takes other PCollections and PTransforms, '
            f'got {other}')
    return tuple(pcolls) | Flatten()


class Create(PTransform):
  """A transform that creates a PCollection from an iterable."""
  def __init__(self, values, reshuffle=True):
    """Initializes a Create transform.

    Args:
      values: An object of values for the PCollection
    """
    super().__init__()
    if isinstance(values, (str, bytes)):
      raise TypeError(
          'PTransform Create: Refusing to treat string as '
          'an iterable. (string=%r)' % values)
    elif isinstance(values, dict):
      values = values.items()
    self.values = tuple(values)
    self.reshuffle = reshuffle
    self._coder = typecoders.registry.get_coder(self.get_output_type())

  def __getstate__(self):
    serialized_values = [self._coder.encode(v) for v in self.values]
    return serialized_values, self.reshuffle, self._coder

  def __setstate__(self, state):
    serialized_values, self.reshuffle, self._coder = state
    self.values = [self._coder.decode(v) for v in serialized_values]

  def to_runner_api_parameter(self, context):
    # type: (PipelineContext) -> typing.Tuple[str, bytes]
    # Required as this is identified by type in PTransformOverrides.
    # TODO(https://github.com/apache/beam/issues/18713): Use an actual URN
    # here.
    return self.to_runner_api_pickled(context)

  def infer_output_type(self, unused_input_type):
    if not self.values:
      return typehints.Any
    return typehints.Union[[
        trivial_inference.instance_to_type(v) for v in self.values
    ]]

  def get_output_type(self):
    return (
        self.get_type_hints().simple_output_type(self.label) or
        self.infer_output_type(None))

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin)
    serialized_values = [self._coder.encode(v) for v in self.values]
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
        | FlatMap(lambda _: serialized_values).with_output_types(bytes)
        | MaybeReshuffle().with_output_types(bytes)
        | Map(self._coder.decode).with_output_types(self.get_output_type()))

  def as_read(self):
    from apache_beam.io import iobase
    source = self._create_source_from_iterable(self.values, self._coder)
    return iobase.Read(source).with_output_types(self.get_output_type())

  def get_windowing(self, unused_inputs):
    # type: (typing.Any) -> Windowing
    return Windowing(GlobalWindows())

  @staticmethod
  def _create_source_from_iterable(values, coder):
    return Create._create_source(list(map(coder.encode, values)), coder)

  @staticmethod
  def _create_source(serialized_values, coder):
    # type: (typing.Any, typing.Any) -> create_source._CreateSource
    from apache_beam.transforms.create_source import _CreateSource

    return _CreateSource(serialized_values, coder)


@typehints.with_output_types(bytes)
class Impulse(PTransform):
  """Impulse primitive."""
  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise TypeError(
          'Input to Impulse transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline, element_type=bytes)

  def get_windowing(self, inputs):
    # type: (typing.Any) -> Windowing
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, unused_context):
    # type: (PipelineContext) -> typing.Tuple[str, None]
    return common_urns.primitives.IMPULSE.urn, None

  @staticmethod
  @PTransform.register_urn(common_urns.primitives.IMPULSE.urn, None)
  def from_runner_api_parameter(
      unused_ptransform, unused_parameter, unused_context):
    return Impulse()


def _strip_output_annotations(type_hint):
  # TODO(robertwb): These should be parameterized types that the
  # type inferencer understands.
  # Then we can replace them with the correct element types instead of
  # using Any. Refer to typehints.WindowedValue when doing this.
  annotations = (TimestampedValue, WindowedValue, pvalue.TaggedOutput)

  contains_annotation = False

  def visitor(t, unused_args):
    if t in annotations or (hasattr(t, '__name__') and
                            t.__name__ == TimestampedValue.__name__):
      raise StopIteration

  try:
    visit_inner_types(type_hint, visitor, [])
  except StopIteration:
    contains_annotation = True

  return typehints.Any if contains_annotation else type_hint
