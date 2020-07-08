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

"""PTransform and descendants.

A PTransform is an object describing (not executing) a computation. The actual
execution semantics for a transform is captured by a runner object. A transform
object always belongs to a pipeline object.

A PTransform derived class needs to define the expand() method that describes
how one or more PValues are created by the transform.

The module defines a few standard transforms: FlatMap (parallel do),
GroupByKey (group by key), etc. Note that the expand() methods for these
classes contain code that will add nodes to the processing graph associated
with a pipeline.

As support for the FlatMap transform, the module also defines a DoFn
class and wrapper class that allows lambda functions to be used as
FlatMap processing functions.
"""

# pytype: skip-file

from __future__ import absolute_import

import copy
import itertools
import operator
import os
import sys
import threading
from builtins import hex
from builtins import object
from builtins import zip
from functools import reduce
from functools import wraps
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union
from typing import overload

from google.protobuf import message

from apache_beam import error
from apache_beam import pvalue
from apache_beam.internal import pickler
from apache_beam.internal import util
from apache_beam.portability import python_urns
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.display import HasDisplayData
from apache_beam.typehints import native_type_compatibility
from apache_beam.typehints import typehints
from apache_beam.typehints.decorators import IOTypeHints
from apache_beam.typehints.decorators import TypeCheckError
from apache_beam.typehints.decorators import WithTypeHints
from apache_beam.typehints.decorators import get_signature
from apache_beam.typehints.decorators import get_type_hints
from apache_beam.typehints.decorators import getcallargs_forhints
from apache_beam.typehints.trivial_inference import instance_to_type
from apache_beam.typehints.typehints import validate_composite_type_param
from apache_beam.utils import proto_utils

if TYPE_CHECKING:
  from apache_beam import coders
  from apache_beam.pipeline import Pipeline
  from apache_beam.runners.pipeline_context import PipelineContext
  from apache_beam.transforms.core import Windowing
  from apache_beam.portability.api import beam_runner_api_pb2

__all__ = [
    'PTransform',
    'ptransform_fn',
    'label_from_callable',
]

T = TypeVar('T')
PTransformT = TypeVar('PTransformT', bound='PTransform')
ConstructorFn = Callable[
    ['beam_runner_api_pb2.PTransform', Optional[Any], 'PipelineContext'], Any]


class _PValueishTransform(object):
  """Visitor for PValueish objects.

  A PValueish is a PValue, or list, tuple, dict of PValuesish objects.

  This visits a PValueish, contstructing a (possibly mutated) copy.
  """
  def visit_nested(self, node, *args):
    if isinstance(node, (tuple, list)):
      args = [self.visit(x, *args) for x in node]
      if isinstance(node, tuple) and hasattr(node.__class__, '_make'):
        # namedtuples require unpacked arguments in their constructor
        return node.__class__(*args)
      else:
        return node.__class__(args)
    elif isinstance(node, dict):
      return node.__class__(
          {key: self.visit(value, *args)
           for (key, value) in node.items()})
    else:
      return node


class _SetInputPValues(_PValueishTransform):
  def visit(self, node, replacements):
    if id(node) in replacements:
      return replacements[id(node)]
    else:
      return self.visit_nested(node, replacements)


# Caches to allow for materialization of values when executing a pipeline
# in-process, in eager mode.  This cache allows the same _MaterializedResult
# object to be accessed and used despite Runner API round-trip serialization.
_pipeline_materialization_cache = {}
_pipeline_materialization_lock = threading.Lock()


def _allocate_materialized_pipeline(pipeline):
  pid = os.getpid()
  with _pipeline_materialization_lock:
    pipeline_id = id(pipeline)
    _pipeline_materialization_cache[(pid, pipeline_id)] = {}


def _allocate_materialized_result(pipeline):
  pid = os.getpid()
  with _pipeline_materialization_lock:
    pipeline_id = id(pipeline)
    if (pid, pipeline_id) not in _pipeline_materialization_cache:
      raise ValueError(
          'Materialized pipeline is not allocated for result '
          'cache.')
    result_id = len(_pipeline_materialization_cache[(pid, pipeline_id)])
    result = _MaterializedResult(pipeline_id, result_id)
    _pipeline_materialization_cache[(pid, pipeline_id)][result_id] = result
    return result


def _get_materialized_result(pipeline_id, result_id):
  pid = os.getpid()
  with _pipeline_materialization_lock:
    if (pid, pipeline_id) not in _pipeline_materialization_cache:
      raise Exception(
          'Materialization in out-of-process and remote runners is not yet '
          'supported.')
    return _pipeline_materialization_cache[(pid, pipeline_id)][result_id]


def _release_materialized_pipeline(pipeline):
  pid = os.getpid()
  with _pipeline_materialization_lock:
    pipeline_id = id(pipeline)
    del _pipeline_materialization_cache[(pid, pipeline_id)]


class _MaterializedResult(object):
  def __init__(self, pipeline_id, result_id):
    self._pipeline_id = pipeline_id
    self._result_id = result_id
    self.elements = []

  def __reduce__(self):
    # When unpickled (during Runner API roundtrip serailization), get the
    # _MaterializedResult object from the cache so that values are written
    # to the original _MaterializedResult when run in eager mode.
    return (_get_materialized_result, (self._pipeline_id, self._result_id))


class _MaterializedDoOutputsTuple(pvalue.DoOutputsTuple):
  def __init__(self, deferred, results_by_tag):
    super(_MaterializedDoOutputsTuple,
          self).__init__(None, None, deferred._tags, deferred._main_tag)
    self._deferred = deferred
    self._results_by_tag = results_by_tag

  def __getitem__(self, tag):
    if tag not in self._results_by_tag:
      raise KeyError(
          'Tag %r is not a a defined output tag of %s.' % (tag, self._deferred))
    return self._results_by_tag[tag].elements


class _AddMaterializationTransforms(_PValueishTransform):
  def _materialize_transform(self, pipeline):
    result = _allocate_materialized_result(pipeline)

    # Need to define _MaterializeValuesDoFn here to avoid circular
    # dependencies.
    from apache_beam import DoFn
    from apache_beam import ParDo

    class _MaterializeValuesDoFn(DoFn):
      def process(self, element):
        result.elements.append(element)

    materialization_label = '_MaterializeValues%d' % result._result_id
    return (materialization_label >> ParDo(_MaterializeValuesDoFn()), result)

  def visit(self, node):
    if isinstance(node, pvalue.PValue):
      transform, result = self._materialize_transform(node.pipeline)
      node | transform
      return result
    elif isinstance(node, pvalue.DoOutputsTuple):
      results_by_tag = {}
      for tag in itertools.chain([node._main_tag], node._tags):
        results_by_tag[tag] = self.visit(node[tag])
      return _MaterializedDoOutputsTuple(node, results_by_tag)
    else:
      return self.visit_nested(node)


class _FinalizeMaterialization(_PValueishTransform):
  def visit(self, node):
    if isinstance(node, _MaterializedResult):
      return node.elements
    elif isinstance(node, _MaterializedDoOutputsTuple):
      return node
    else:
      return self.visit_nested(node)


def get_named_nested_pvalues(pvalueish):
  if isinstance(pvalueish, tuple):
    # Check to see if it's a named tuple.
    fields = getattr(pvalueish, '_fields', None)
    if fields and len(fields) == len(pvalueish):
      tagged_values = zip(fields, pvalueish)
    else:
      tagged_values = enumerate(pvalueish)
  elif isinstance(pvalueish, list):
    tagged_values = enumerate(pvalueish)
  elif isinstance(pvalueish, dict):
    tagged_values = pvalueish.items()
  else:
    if isinstance(pvalueish, (pvalue.PValue, pvalue.DoOutputsTuple)):
      yield None, pvalueish
    return

  for tag, subvalue in tagged_values:
    for subtag, subsubvalue in get_named_nested_pvalues(subvalue):
      if subtag is None:
        yield tag, subsubvalue
      else:
        yield '%s.%s' % (tag, subtag), subsubvalue


class _ZipPValues(object):
  """Pairs each PValue in a pvalueish with a value in a parallel out sibling.

  Sibling should have the same nested structure as pvalueish.  Leaves in
  sibling are expanded across nested pvalueish lists, tuples, and dicts.
  For example

      ZipPValues().visit({'a': pc1, 'b': (pc2, pc3)},
                         {'a': 'A', 'b', 'B'})

  will return

      [('a', pc1, 'A'), ('b', pc2, 'B'), ('b', pc3, 'B')]
  """
  def visit(self, pvalueish, sibling, pairs=None, context=None):
    if pairs is None:
      pairs = []
      self.visit(pvalueish, sibling, pairs, context)
      return pairs
    elif isinstance(pvalueish, (pvalue.PValue, pvalue.DoOutputsTuple)):
      pairs.append((context, pvalueish, sibling))
    elif isinstance(pvalueish, (list, tuple)):
      self.visit_sequence(pvalueish, sibling, pairs, context)
    elif isinstance(pvalueish, dict):
      self.visit_dict(pvalueish, sibling, pairs, context)

  def visit_sequence(self, pvalueish, sibling, pairs, context):
    if isinstance(sibling, (list, tuple)):
      for ix, (p, s) in enumerate(zip(pvalueish,
                                      list(sibling) + [None] * len(pvalueish))):
        self.visit(p, s, pairs, 'position %s' % ix)
    else:
      for p in pvalueish:
        self.visit(p, sibling, pairs, context)

  def visit_dict(self, pvalueish, sibling, pairs, context):
    if isinstance(sibling, dict):
      for key, p in pvalueish.items():
        self.visit(p, sibling.get(key), pairs, key)
    else:
      for p in pvalueish.values():
        self.visit(p, sibling, pairs, context)


class PTransform(WithTypeHints, HasDisplayData):
  """A transform object used to modify one or more PCollections.

  Subclasses must define an expand() method that will be used when the transform
  is applied to some arguments. Typical usage pattern will be:

    input | CustomTransform(...)

  The expand() method of the CustomTransform object passed in will be called
  with input as an argument.
  """
  # By default, transforms don't have any side inputs.
  side_inputs = ()  # type: Sequence[pvalue.AsSideInput]

  # Used for nullary transforms.
  pipeline = None  # type: Optional[Pipeline]

  # Default is unset.
  _user_label = None  # type: Optional[str]

  def __init__(self, label=None):
    # type: (Optional[str]) -> None
    super(PTransform, self).__init__()
    self.label = label  # type: ignore # https://github.com/python/mypy/issues/3004

  @property
  def label(self):
    # type: () -> str
    return self._user_label or self.default_label()

  @label.setter
  def label(self, value):
    # type: (Optional[str]) -> None
    self._user_label = value

  def default_label(self):
    # type: () -> str
    return self.__class__.__name__

  def default_type_hints(self):
    fn_type_hints = IOTypeHints.from_callable(self.expand)
    if fn_type_hints is not None:
      fn_type_hints = fn_type_hints.strip_pcoll()

    # Prefer class decorator type hints for backwards compatibility.
    return get_type_hints(self.__class__).with_defaults(fn_type_hints)

  def with_input_types(self, input_type_hint):
    """Annotates the input type of a :class:`PTransform` with a type-hint.

    Args:
      input_type_hint (type): An instance of an allowed built-in type, a custom
        class, or an instance of a
        :class:`~apache_beam.typehints.typehints.TypeConstraint`.

    Raises:
      TypeError: If **input_type_hint** is not a valid type-hint.
        See
        :obj:`apache_beam.typehints.typehints.validate_composite_type_param()`
        for further details.

    Returns:
      PTransform: A reference to the instance of this particular
      :class:`PTransform` object. This allows chaining type-hinting related
      methods.
    """
    input_type_hint = native_type_compatibility.convert_to_beam_type(
        input_type_hint)
    validate_composite_type_param(
        input_type_hint, 'Type hints for a PTransform')
    return super(PTransform, self).with_input_types(input_type_hint)

  def with_output_types(self, type_hint):
    """Annotates the output type of a :class:`PTransform` with a type-hint.

    Args:
      type_hint (type): An instance of an allowed built-in type, a custom class,
        or a :class:`~apache_beam.typehints.typehints.TypeConstraint`.

    Raises:
      TypeError: If **type_hint** is not a valid type-hint. See
        :obj:`~apache_beam.typehints.typehints.validate_composite_type_param()`
        for further details.

    Returns:
      PTransform: A reference to the instance of this particular
      :class:`PTransform` object. This allows chaining type-hinting related
      methods.
    """
    type_hint = native_type_compatibility.convert_to_beam_type(type_hint)
    validate_composite_type_param(type_hint, 'Type hints for a PTransform')
    return super(PTransform, self).with_output_types(type_hint)

  def type_check_inputs(self, pvalueish):
    self.type_check_inputs_or_outputs(pvalueish, 'input')

  def infer_output_type(self, unused_input_type):
    return self.get_type_hints().simple_output_type(self.label) or typehints.Any

  def type_check_outputs(self, pvalueish):
    self.type_check_inputs_or_outputs(pvalueish, 'output')

  def type_check_inputs_or_outputs(self, pvalueish, input_or_output):
    type_hints = self.get_type_hints()
    hints = getattr(type_hints, input_or_output + '_types')
    if hints is None or not any(hints):
      return
    arg_hints, kwarg_hints = hints
    if arg_hints and kwarg_hints:
      raise TypeCheckError(
          'PTransform cannot have both positional and keyword type hints '
          'without overriding %s._type_check_%s()' %
          (self.__class__, input_or_output))
    root_hint = (
        arg_hints[0] if len(arg_hints) == 1 else arg_hints or kwarg_hints)
    for context, pvalue_, hint in _ZipPValues().visit(pvalueish, root_hint):
      if pvalue_.element_type is None:
        # TODO(robertwb): It's a bug that we ever get here. (typecheck)
        continue
      if hint and not typehints.is_consistent_with(pvalue_.element_type, hint):
        at_context = ' %s %s' % (input_or_output, context) if context else ''
        raise TypeCheckError(
            '{type} type hint violation at {label}{context}: expected {hint}, '
            'got {actual_type}\nFull type hint:\n{debug_str}'.format(
                type=input_or_output.title(),
                label=self.label,
                context=at_context,
                hint=hint,
                actual_type=pvalue_.element_type,
                debug_str=type_hints.debug_str()))

  def _infer_output_coder(self, input_type=None, input_coder=None):
    # type: (...) -> Optional[coders.Coder]

    """Returns the output coder to use for output of this transform.

    Note: this API is experimental and is subject to change; please do not rely
    on behavior induced by this method.

    The Coder returned here should not be wrapped in a WindowedValueCoder
    wrapper.

    Args:
      input_type: An instance of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint for the input type, or None if not available.
      input_coder: Coder object for encoding input to this PTransform, or None
        if not available.

    Returns:
      Coder object for encoding output of this PTransform or None if unknown.
    """
    # TODO(ccy): further refine this API.
    return None

  def _clone(self, new_label):
    """Clones the current transform instance under a new label."""
    transform = copy.copy(self)
    transform.label = new_label
    return transform

  def expand(self, input_or_inputs):
    raise NotImplementedError

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s(PTransform)%s%s%s' % (
        self.__class__.__name__,
        ' label=[%s]' % self.label if
        (hasattr(self, 'label') and self.label) else '',
        ' inputs=%s' % str(self.inputs) if
        (hasattr(self, 'inputs') and self.inputs) else '',
        ' side_inputs=%s' % str(self.side_inputs) if self.side_inputs else '')

  def _check_pcollection(self, pcoll):
    # type: (pvalue.PCollection) -> None
    if not isinstance(pcoll, pvalue.PCollection):
      raise error.TransformError('Expecting a PCollection argument.')
    if not pcoll.pipeline:
      raise error.TransformError('PCollection not part of a pipeline.')

  def get_windowing(self, inputs):
    # type: (Any) -> Windowing

    """Returns the window function to be associated with transform's output.

    By default most transforms just return the windowing function associated
    with the input PCollection (or the first input if several).
    """
    # TODO(robertwb): Assert all input WindowFns compatible.
    return inputs[0].windowing

  def __rrshift__(self, label):
    return _NamedPTransform(self, label)

  def __or__(self, right):
    """Used to compose PTransforms, e.g., ptransform1 | ptransform2."""
    if isinstance(right, PTransform):
      return _ChainedPTransform(self, right)
    return NotImplemented

  def __ror__(self, left, label=None):
    """Used to apply this PTransform to non-PValues, e.g., a tuple."""
    pvalueish, pvalues = self._extract_input_pvalues(left)
    pipelines = [v.pipeline for v in pvalues if isinstance(v, pvalue.PValue)]
    if pvalues and not pipelines:
      deferred = False
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam import pipeline
      from apache_beam.options.pipeline_options import PipelineOptions
      # pylint: enable=wrong-import-order, wrong-import-position
      p = pipeline.Pipeline('DirectRunner', PipelineOptions(sys.argv))
    else:
      if not pipelines:
        if self.pipeline is not None:
          p = self.pipeline
        else:
          raise ValueError(
              '"%s" requires a pipeline to be specified '
              'as there are no deferred inputs.' % self.label)
      else:
        p = self.pipeline or pipelines[0]
        for pp in pipelines:
          if p != pp:
            raise ValueError(
                'Mixing value from different pipelines not allowed.')
      deferred = not getattr(p.runner, 'is_eager', False)
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.core import Create
    # pylint: enable=wrong-import-order, wrong-import-position
    replacements = {
        id(v): p | 'CreatePInput%s' % ix >> Create(v, reshuffle=False)
        for ix,
        v in enumerate(pvalues)
        if not isinstance(v, pvalue.PValue) and v is not None
    }
    pvalueish = _SetInputPValues().visit(pvalueish, replacements)
    self.pipeline = p
    result = p.apply(self, pvalueish, label)
    if deferred:
      return result
    _allocate_materialized_pipeline(p)
    materialized_result = _AddMaterializationTransforms().visit(result)
    p.run().wait_until_finish()
    _release_materialized_pipeline(p)
    return _FinalizeMaterialization().visit(materialized_result)

  def _extract_input_pvalues(self, pvalueish):
    """Extract all the pvalues contained in the input pvalueish.

    Returns pvalueish as well as the flat inputs list as the input may have to
    be copied as inspection may be destructive.

    By default, recursively extracts tuple components and dict values.

    Generally only needs to be overriden for multi-input PTransforms.
    """
    # pylint: disable=wrong-import-order
    from apache_beam import pipeline
    # pylint: enable=wrong-import-order
    if isinstance(pvalueish, pipeline.Pipeline):
      pvalueish = pvalue.PBegin(pvalueish)

    def _dict_tuple_leaves(pvalueish):
      if isinstance(pvalueish, tuple):
        for a in pvalueish:
          for p in _dict_tuple_leaves(a):
            yield p
      elif isinstance(pvalueish, dict):
        for a in pvalueish.values():
          for p in _dict_tuple_leaves(a):
            yield p
      else:
        yield pvalueish

    return pvalueish, tuple(_dict_tuple_leaves(pvalueish))

  def _pvaluish_from_dict(self, input_dict):
    if len(input_dict) == 1:
      return next(iter(input_dict.values()))
    else:
      return input_dict

  _known_urns = {}  # type: Dict[str, Tuple[Optional[type], ConstructorFn]]

  @classmethod
  @overload
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type,  # type: Type[T]
  ):
    # type: (...) -> Callable[[Union[type, Callable[[beam_runner_api_pb2.PTransform, T, PipelineContext], Any]]], Callable[[T, PipelineContext], Any]]
    pass

  @classmethod
  @overload
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type,  # type: None
  ):
    # type: (...) -> Callable[[Union[type, Callable[[beam_runner_api_pb2.PTransform, bytes, PipelineContext], Any]]], Callable[[bytes, PipelineContext], Any]]
    pass

  @classmethod
  @overload
  def register_urn(cls,
                   urn,  # type: str
                   parameter_type,  # type: Type[T]
                   constructor  # type: Callable[[beam_runner_api_pb2.PTransform, T, PipelineContext], Any]
                  ):
    # type: (...) -> None
    pass

  @classmethod
  @overload
  def register_urn(cls,
                   urn,  # type: str
                   parameter_type,  # type: None
                   constructor  # type: Callable[[beam_runner_api_pb2.PTransform, bytes, PipelineContext], Any]
                  ):
    # type: (...) -> None
    pass

  @classmethod
  def register_urn(cls, urn, parameter_type, constructor=None):
    def register(constructor):
      if isinstance(constructor, type):
        constructor.from_runner_api_parameter = register(
            constructor.from_runner_api_parameter)
      else:
        cls._known_urns[urn] = parameter_type, constructor
      return constructor

    if constructor:
      # Used as a statement.
      register(constructor)
    else:
      # Used as a decorator.
      return register

  def to_runner_api(self, context, has_parts=False, **extra_kwargs):
    # type: (PipelineContext, bool, Any) -> beam_runner_api_pb2.FunctionSpec
    from apache_beam.portability.api import beam_runner_api_pb2
    urn, typed_param = self.to_runner_api_parameter(context, **extra_kwargs)
    if urn == python_urns.GENERIC_COMPOSITE_TRANSFORM and not has_parts:
      # TODO(BEAM-3812): Remove this fallback.
      urn, typed_param = self.to_runner_api_pickled(context)
    return beam_runner_api_pb2.FunctionSpec(
        urn=urn,
        payload=typed_param.SerializeToString() if isinstance(
            typed_param, message.Message) else typed_param.encode('utf-8')
        if isinstance(typed_param, str) else typed_param)

  @classmethod
  def from_runner_api(cls,
                      proto,  # type: Optional[beam_runner_api_pb2.PTransform]
                      context  # type: PipelineContext
                     ):
    # type: (...) -> Optional[PTransform]
    if proto is None or proto.spec is None or not proto.spec.urn:
      return None
    parameter_type, constructor = cls._known_urns[proto.spec.urn]

    try:
      return constructor(
          proto,
          proto_utils.parse_Bytes(proto.spec.payload, parameter_type),
          context)
    except Exception:
      if context.allow_proto_holders:
        # For external transforms we cannot build a Python ParDo object so
        # we build a holder transform instead.
        from apache_beam.transforms.core import RunnerAPIPTransformHolder
        return RunnerAPIPTransformHolder(proto.spec, context)
      raise

  def to_runner_api_parameter(
      self,
      unused_context  # type: PipelineContext
  ):
    # type: (...) -> Tuple[str, Optional[Union[message.Message, bytes, str]]]
    # The payload here is just to ease debugging.
    return (
        python_urns.GENERIC_COMPOSITE_TRANSFORM,
        getattr(self, '_fn_api_payload', str(self)))

  def to_runner_api_pickled(self, unused_context):
    # type: (PipelineContext) -> Tuple[str, bytes]
    return (python_urns.PICKLED_TRANSFORM, pickler.dumps(self))

  def runner_api_requires_keyed_input(self):
    return False


@PTransform.register_urn(python_urns.GENERIC_COMPOSITE_TRANSFORM, None)
def _create_transform(unused_ptransform, payload, unused_context):
  empty_transform = PTransform()
  empty_transform._fn_api_payload = payload
  return empty_transform


@PTransform.register_urn(python_urns.PICKLED_TRANSFORM, None)
def _unpickle_transform(unused_ptransform, pickled_bytes, unused_context):
  return pickler.loads(pickled_bytes)


class _ChainedPTransform(PTransform):
  def __init__(self, *parts):
    # type: (*PTransform) -> None
    super(_ChainedPTransform, self).__init__(label=self._chain_label(parts))
    self._parts = parts

  def _chain_label(self, parts):
    return '|'.join(p.label for p in parts)

  def __or__(self, right):
    if isinstance(right, PTransform):
      # Create a flat list rather than a nested tree of composite
      # transforms for better monitoring, etc.
      return _ChainedPTransform(*(self._parts + (right, )))
    return NotImplemented

  def expand(self, pval):
    return reduce(operator.or_, self._parts, pval)


class PTransformWithSideInputs(PTransform):
  """A superclass for any :class:`PTransform` (e.g.
  :func:`~apache_beam.transforms.core.FlatMap` or
  :class:`~apache_beam.transforms.core.CombineFn`)
  invoking user code.

  :class:`PTransform` s like :func:`~apache_beam.transforms.core.FlatMap`
  invoke user-supplied code in some kind of package (e.g. a
  :class:`~apache_beam.transforms.core.DoFn`) and optionally provide arguments
  and side inputs to that code. This internal-use-only class contains common
  functionality for :class:`PTransform` s that fit this model.
  """
  def __init__(self, fn, *args, **kwargs):
    # type: (WithTypeHints, *Any, **Any) -> None
    if isinstance(fn, type) and issubclass(fn, WithTypeHints):
      # Don't treat Fn class objects as callables.
      raise ValueError('Use %s() not %s.' % (fn.__name__, fn.__name__))
    self.fn = self.make_fn(fn, bool(args or kwargs))
    # Now that we figure out the label, initialize the super-class.
    super(PTransformWithSideInputs, self).__init__()

    if (any([isinstance(v, pvalue.PCollection) for v in args]) or
        any([isinstance(v, pvalue.PCollection) for v in kwargs.values()])):
      raise error.SideInputError(
          'PCollection used directly as side input argument. Specify '
          'AsIter(pcollection) or AsSingleton(pcollection) to indicate how the '
          'PCollection is to be used.')
    self.args, self.kwargs, self.side_inputs = util.remove_objects_from_args(
        args, kwargs, pvalue.AsSideInput)
    self.raw_side_inputs = args, kwargs

    # Prevent name collisions with fns of the form '<function <lambda> at ...>'
    self._cached_fn = self.fn

    # Ensure fn and side inputs are picklable for remote execution.
    try:
      self.fn = pickler.loads(pickler.dumps(self.fn))
    except RuntimeError as e:
      raise RuntimeError('Unable to pickle fn %s: %s' % (self.fn, e))

    self.args = pickler.loads(pickler.dumps(self.args))
    self.kwargs = pickler.loads(pickler.dumps(self.kwargs))

    # For type hints, because loads(dumps(class)) != class.
    self.fn = self._cached_fn

  def with_input_types(
      self, input_type_hint, *side_inputs_arg_hints, **side_input_kwarg_hints):
    """Annotates the types of main inputs and side inputs for the PTransform.

    Args:
      input_type_hint: An instance of an allowed built-in type, a custom class,
        or an instance of a typehints.TypeConstraint.
      *side_inputs_arg_hints: A variable length argument composed of
        of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint.
      **side_input_kwarg_hints: A dictionary argument composed of
        of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint.

    Example of annotating the types of side-inputs::

      FlatMap().with_input_types(int, int, bool)

    Raises:
      :class:`TypeError`: If **type_hint** is not a valid type-hint.
        See
        :func:`~apache_beam.typehints.typehints.validate_composite_type_param`
        for further details.

    Returns:
      :class:`PTransform`: A reference to the instance of this particular
      :class:`PTransform` object. This allows chaining type-hinting related
      methods.
    """
    super(PTransformWithSideInputs, self).with_input_types(input_type_hint)

    side_inputs_arg_hints = native_type_compatibility.convert_to_beam_types(
        side_inputs_arg_hints)
    side_input_kwarg_hints = native_type_compatibility.convert_to_beam_types(
        side_input_kwarg_hints)

    for si in side_inputs_arg_hints:
      validate_composite_type_param(si, 'Type hints for a PTransform')
    for si in side_input_kwarg_hints.values():
      validate_composite_type_param(si, 'Type hints for a PTransform')

    self.side_inputs_types = side_inputs_arg_hints
    return WithTypeHints.with_input_types(
        self, input_type_hint, *side_inputs_arg_hints, **side_input_kwarg_hints)

  def type_check_inputs(self, pvalueish):
    type_hints = self.get_type_hints()
    input_types = type_hints.input_types
    if input_types:
      args, kwargs = self.raw_side_inputs

      def element_type(side_input):
        if isinstance(side_input, pvalue.AsSideInput):
          return side_input.element_type
        return instance_to_type(side_input)

      arg_types = [pvalueish.element_type] + [element_type(v) for v in args]
      kwargs_types = {k: element_type(v) for (k, v) in kwargs.items()}
      argspec_fn = self._process_argspec_fn()
      bindings = getcallargs_forhints(argspec_fn, *arg_types, **kwargs_types)
      hints = getcallargs_forhints(
          argspec_fn, *input_types[0], **input_types[1])
      for arg, hint in hints.items():
        if arg.startswith('__unknown__'):
          continue
        if hint is None:
          continue
        if not typehints.is_consistent_with(bindings.get(arg, typehints.Any),
                                            hint):
          raise TypeCheckError(
              'Type hint violation for \'{label}\': requires {hint} but got '
              '{actual_type} for {arg}\nFull type hint:\n{debug_str}'.format(
                  label=self.label,
                  hint=hint,
                  actual_type=bindings[arg],
                  arg=arg,
                  debug_str=type_hints.debug_str()))

  def _process_argspec_fn(self):
    """Returns an argspec of the function actually consuming the data.
    """
    raise NotImplementedError

  def make_fn(self, fn, has_side_inputs):
    # TODO(silviuc): Add comment describing that this is meant to be overriden
    # by methods detecting callables and wrapping them in DoFns.
    return fn

  def default_label(self):
    return '%s(%s)' % (self.__class__.__name__, self.fn.default_label())


class _PTransformFnPTransform(PTransform):
  """A class wrapper for a function-based transform."""
  def __init__(self, fn, *args, **kwargs):
    super(_PTransformFnPTransform, self).__init__()
    self._fn = fn
    self._args = args
    self._kwargs = kwargs

  def display_data(self):
    res = {
        'fn': (
            self._fn.__name__
            if hasattr(self._fn, '__name__') else self._fn.__class__),
        'args': DisplayDataItem(str(self._args)).drop_if_default('()'),
        'kwargs': DisplayDataItem(str(self._kwargs)).drop_if_default('{}')
    }
    return res

  def expand(self, pcoll):
    # Since the PTransform will be implemented entirely as a function
    # (once called), we need to pass through any type-hinting information that
    # may have been annotated via the .with_input_types() and
    # .with_output_types() methods.
    kwargs = dict(self._kwargs)
    args = tuple(self._args)

    # TODO(BEAM-5878) Support keyword-only arguments.
    try:
      if 'type_hints' in get_signature(self._fn).parameters:
        args = (self.get_type_hints(), ) + args
    except TypeError:
      # Might not be a function.
      pass
    return self._fn(pcoll, *args, **kwargs)

  def default_label(self):
    if self._args:
      return '%s(%s)' % (
          label_from_callable(self._fn), label_from_callable(self._args[0]))
    return label_from_callable(self._fn)


def ptransform_fn(fn):
  """A decorator for a function-based PTransform.

  Experimental; no backwards-compatibility guarantees.

  Args:
    fn: A function implementing a custom PTransform.

  Returns:
    A CallablePTransform instance wrapping the function-based PTransform.

  This wrapper provides an alternative, simpler way to define a PTransform.
  The standard method is to subclass from PTransform and override the expand()
  method. An equivalent effect can be obtained by defining a function that
  accepts an input PCollection and additional optional arguments and returns a
  resulting PCollection. For example::

    @ptransform_fn
    def CustomMapper(pcoll, mapfn):
      return pcoll | ParDo(mapfn)

  The equivalent approach using PTransform subclassing::

    class CustomMapper(PTransform):

      def __init__(self, mapfn):
        super(CustomMapper, self).__init__()
        self.mapfn = mapfn

      def expand(self, pcoll):
        return pcoll | ParDo(self.mapfn)

  With either method the custom PTransform can be used in pipelines as if
  it were one of the "native" PTransforms::

    result_pcoll = input_pcoll | 'Label' >> CustomMapper(somefn)

  Note that for both solutions the underlying implementation of the pipe
  operator (i.e., `|`) will inject the pcoll argument in its proper place
  (first argument if no label was specified and second argument otherwise).
  """
  # TODO(robertwb): Consider removing staticmethod to allow for self parameter.
  @wraps(fn)
  def callable_ptransform_factory(*args, **kwargs):
    return _PTransformFnPTransform(fn, *args, **kwargs)

  return callable_ptransform_factory


def label_from_callable(fn):
  if hasattr(fn, 'default_label'):
    return fn.default_label()
  elif hasattr(fn, '__name__'):
    if fn.__name__ == '<lambda>':
      return '<lambda at %s:%s>' % (
          os.path.basename(fn.__code__.co_filename), fn.__code__.co_firstlineno)
    return fn.__name__
  return str(fn)


class _NamedPTransform(PTransform):
  def __init__(self, transform, label):
    super(_NamedPTransform, self).__init__(label)
    self.transform = transform

  def __ror__(self, pvalueish, _unused=None):
    return self.transform.__ror__(pvalueish, self.label)

  def expand(self, pvalue):
    raise RuntimeError("Should never be expanded directly.")
