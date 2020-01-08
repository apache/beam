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

"""PValue, PCollection: one node of a dataflow graph.

A node of a dataflow processing graph is a PValue. Currently, there is only
one type: PCollection (a potentially very large set of arbitrary values).
Once created, a PValue belongs to a pipeline and has an associated
transform (of type PTransform), which describes how the value will be
produced when the pipeline gets executed.
"""

from __future__ import absolute_import

import collections
import itertools
from builtins import hex
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Generic
from typing import Iterator
from typing import Optional
from typing import Sequence
from typing import TypeVar
from typing import Union

from past.builtins import unicode

from apache_beam import typehints
from apache_beam.internal import pickler
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2

if TYPE_CHECKING:
  from apache_beam.transforms import sideinputs
  from apache_beam.transforms.core import ParDo
  from apache_beam.transforms.core import Windowing
  from apache_beam.pipeline import AppliedPTransform
  from apache_beam.pipeline import Pipeline
  from apache_beam.runners.pipeline_context import PipelineContext

__all__ = [
    'PCollection',
    'TaggedOutput',
    'AsSingleton',
    'AsIter',
    'AsList',
    'AsDict',
    'EmptySideInput',
]

T = TypeVar('T')


class PValue(object):
  """Base class for PCollection.

  Dataflow users should not construct PValue objects directly in their
  pipelines.

  A PValue has the following main characteristics:
    (1) Belongs to a pipeline. Added during object initialization.
    (2) Has a transform that can compute the value if executed.
    (3) Has a value which is meaningful if the transform was executed.
  """

  def __init__(self,
               pipeline,  # type: Pipeline
               tag=None,  # type: Optional[str]
               element_type=None,  # type: Optional[type]
               windowing=None,  # type: Optional[Windowing]
               is_bounded=True,
              ):
    """Initializes a PValue with all arguments hidden behind keyword arguments.

    Args:
      pipeline: Pipeline object for this PValue.
      tag: Tag of this PValue.
      element_type: The type of this PValue.
    """
    self.pipeline = pipeline
    self.tag = tag
    self.element_type = element_type
    # The AppliedPTransform instance for the application of the PTransform
    # generating this PValue. The field gets initialized when a transform
    # gets applied.
    self.producer = None  # type: Optional[AppliedPTransform]
    self.is_bounded = is_bounded
    if windowing:
      self._windowing = windowing

  def __str__(self):
    return self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return "%s[%s.%s]" % (self.__class__.__name__,
                          self.producer.full_label if self.producer else None,
                          self.tag)

  def apply(self, *args, **kwargs):
    """Applies a transform or callable to a PValue.

    Args:
      *args: positional arguments.
      **kwargs: keyword arguments.

    The method will insert the pvalue as the next argument following an
    optional first label and a transform/callable object. It will call the
    pipeline.apply() method with this modified argument list.
    """
    arglist = list(args)
    arglist.insert(1, self)
    return self.pipeline.apply(*arglist, **kwargs)

  def __or__(self, ptransform):
    return self.pipeline.apply(ptransform, self)


class PCollection(PValue, Generic[T]):
  """A multiple values (potentially huge) container.

  Dataflow users should not construct PCollection objects directly in their
  pipelines.
  """

  def __eq__(self, other):
    if isinstance(other, PCollection):
      return self.tag == other.tag and self.producer == other.producer

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other

  def __hash__(self):
    return hash((self.tag, self.producer))

  @property
  def windowing(self):
    # type: () -> Windowing
    if not hasattr(self, '_windowing'):
      self._windowing = self.producer.transform.get_windowing(
          self.producer.inputs)
    return self._windowing

  def __reduce_ex__(self, unused_version):
    # Pickling a PCollection is almost always the wrong thing to do, but we
    # can't prohibit it as it often gets implicitly picked up (e.g. as part
    # of a closure).
    return _InvalidUnpickledPCollection, ()

  @staticmethod
  def from_(pcoll):
    # type: (PValue) -> PCollection
    """Create a PCollection, using another PCollection as a starting point.

    Transfers relevant attributes.
    """
    return PCollection(pcoll.pipeline, is_bounded=pcoll.is_bounded)

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.PCollection
    return beam_runner_api_pb2.PCollection(
        unique_name=self._unique_name(),
        coder_id=context.coder_id_from_element_type(self.element_type),
        is_bounded=beam_runner_api_pb2.IsBounded.BOUNDED
        if self.is_bounded
        else beam_runner_api_pb2.IsBounded.UNBOUNDED,
        windowing_strategy_id=context.windowing_strategies.get_id(
            self.windowing))

  def _unique_name(self):
    # type: () -> str
    if self.producer:
      return '%d%s.%s' % (
          len(self.producer.full_label), self.producer.full_label, self.tag)
    else:
      return 'PCollection%s' % id(self)

  @staticmethod
  def from_runner_api(proto, context):
    # type: (beam_runner_api_pb2.PCollection, PipelineContext) -> PCollection
    # Producer and tag will be filled in later, the key point is that the
    # same object is returned for the same pcollection id.
    return PCollection(
        None,
        element_type=context.element_type_from_coder_id(proto.coder_id),
        windowing=context.windowing_strategies.get_by_id(
            proto.windowing_strategy_id),
        is_bounded=proto.is_bounded == beam_runner_api_pb2.IsBounded.BOUNDED)


class _InvalidUnpickledPCollection(object):
  pass


class PBegin(PValue):
  """A pipeline begin marker used as input to create/read transforms.

  The class is used internally to represent inputs to Create and Read
  transforms. This allows us to have transforms that uniformly take PValue(s)
  as inputs.
  """
  pass


class PDone(PValue):
  """PDone is the output of a transform that has a trivial result such as Write.
  """
  pass


class DoOutputsTuple(object):
  """An object grouping the multiple outputs of a ParDo or FlatMap transform."""

  def __init__(self,
               pipeline,  # type: Pipeline
               transform,  # type: ParDo
               tags,  # type: Sequence[str]
               main_tag  # type: Optional[str]
              ):
    self._pipeline = pipeline
    self._tags = tags
    self._main_tag = main_tag
    self._transform = transform
    # The ApplyPTransform instance for the application of the multi FlatMap
    # generating this value. The field gets initialized when a transform
    # gets applied.
    self.producer = None  # type: Optional[AppliedPTransform]
    # Dictionary of PCollections already associated with tags.
    self._pcolls = {}  # type: Dict[Optional[str], PValue]

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s main_tag=%s tags=%s transform=%s' % (
        self.__class__.__name__, self._main_tag, self._tags, self._transform)

  def __iter__(self):
    # type: () -> Iterator[PValue]
    """Iterates over tags returning for each call a (tag, pvalue) pair."""
    if self._main_tag is not None:
      yield self[self._main_tag]
    for tag in self._tags:
      yield self[tag]

  def __getattr__(self, tag):
    # type: (str) -> PValue
    # Special methods which may be accessed before the object is
    # fully constructed (e.g. in unpickling).
    if tag[:2] == tag[-2:] == '__':
      return object.__getattr__(self, tag)  # type: ignore
    return self[tag]

  def __getitem__(self, tag):
    # type: (Union[int, str, None]) -> PValue
    # Accept int tags so that we can look at Partition tags with the
    # same ints that we used in the partition function.
    # TODO(gildea): Consider requiring string-based tags everywhere.
    # This will require a partition function that does not return ints.
    if isinstance(tag, int):
      tag = str(tag)
    if tag == self._main_tag:
      tag = None
    elif self._tags and tag not in self._tags:
      raise ValueError(
          "Tag '%s' is neither the main tag '%s' "
          "nor any of the tags %s" % (
              tag, self._main_tag, self._tags))
    # Check if we accessed this tag before.
    if tag in self._pcolls:
      return self._pcolls[tag]

    assert self.producer is not None
    if tag is not None:
      self._transform.output_tags.add(tag)
      pcoll = PCollection(self._pipeline, tag=tag, element_type=typehints.Any)  # type: PValue
      # Transfer the producer from the DoOutputsTuple to the resulting
      # PCollection.
      pcoll.producer = self.producer.parts[0]
      # Add this as an output to both the inner ParDo and the outer _MultiParDo
      # PTransforms.
      if tag not in self.producer.parts[0].outputs:
        self.producer.parts[0].add_output(pcoll, tag)
        self.producer.add_output(pcoll, tag)
    else:
      # Main output is output of inner ParDo.
      pcoll = self.producer.parts[0].outputs[None]
    self._pcolls[tag] = pcoll
    return pcoll


class TaggedOutput(object):
  """An object representing a tagged value.

  ParDo, Map, and FlatMap transforms can emit values on multiple outputs which
  are distinguished by string tags. The DoFn will return plain values
  if it wants to emit on the main output and TaggedOutput objects
  if it wants to emit a value on a specific tagged output.
  """

  def __init__(self, tag, value):
    # type: (str, Any) -> None
    if not isinstance(tag, (str, unicode)):
      raise TypeError(
          'Attempting to create a TaggedOutput with non-string tag %s' % (tag,))
    self.tag = tag
    self.value = value


class AsSideInput(object):
  """Marker specifying that a PCollection will be used as a side input.

  When a PCollection is supplied as a side input to a PTransform, it is
  necessary to indicate how the PCollection should be made available
  as a PTransform side argument (e.g. in the form of an iterable, mapping,
  or single value).  This class is the superclass of all the various
  options, and should not be instantiated directly. (See instead AsSingleton,
  AsIter, etc.)
  """

  def __init__(self, pcoll):
    # type: (PCollection) -> None
    from apache_beam.transforms import sideinputs
    self.pvalue = pcoll
    self._window_mapping_fn = sideinputs.default_window_mapping_fn(
        pcoll.windowing.windowfn)

  def _view_options(self):
    """Internal options corresponding to specific view.

    Intended for internal use by runner implementations.

    Returns:
      Tuple of options for the given view.
    """
    return {'window_mapping_fn': self._window_mapping_fn}

  @property
  def element_type(self):
    return typehints.Any

  # TODO(robertwb): Get rid of _from_runtime_iterable and _view_options
  # in favor of _side_input_data().
  def _side_input_data(self):
    # type: () -> SideInputData
    view_options = self._view_options()
    from_runtime_iterable = type(self)._from_runtime_iterable
    return SideInputData(
        common_urns.side_inputs.ITERABLE.urn,
        self._window_mapping_fn,
        lambda iterable: from_runtime_iterable(iterable, view_options))

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.SideInput
    return self._side_input_data().to_runner_api(context)

  @staticmethod
  def from_runner_api(proto,  # type: beam_runner_api_pb2.SideInput
                      context  # type: PipelineContext
                     ):
    # type: (...) -> _UnpickledSideInput
    return _UnpickledSideInput(
        SideInputData.from_runner_api(proto, context))

  @staticmethod
  def _from_runtime_iterable(it, options):
    raise NotImplementedError

  def requires_keyed_input(self):
    return False


class _UnpickledSideInput(AsSideInput):
  def __init__(self, side_input_data):
    # type: (SideInputData) -> None
    self._data = side_input_data
    self._window_mapping_fn = side_input_data.window_mapping_fn

  @staticmethod
  def _from_runtime_iterable(it, options):
    return options['data'].view_fn(it)

  def _view_options(self):
    return {
        'data': self._data,
        # For non-fn-api runners.
        'window_mapping_fn': self._data.window_mapping_fn,
    }

  def _side_input_data(self):
    return self._data


class SideInputData(object):
  """All of the data about a side input except for the bound PCollection."""
  def __init__(self,
               access_pattern,  # type: str
               window_mapping_fn,  # type: sideinputs.WindowMappingFn
               view_fn
              ):
    self.access_pattern = access_pattern
    self.window_mapping_fn = window_mapping_fn
    self.view_fn = view_fn

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.SideInput
    return beam_runner_api_pb2.SideInput(
        access_pattern=beam_runner_api_pb2.FunctionSpec(
            urn=self.access_pattern),
        view_fn=beam_runner_api_pb2.FunctionSpec(
            urn=python_urns.PICKLED_VIEWFN,
            payload=pickler.dumps(self.view_fn)),
        window_mapping_fn=beam_runner_api_pb2.FunctionSpec(
            urn=python_urns.PICKLED_WINDOW_MAPPING_FN,
            payload=pickler.dumps(self.window_mapping_fn)))

  @staticmethod
  def from_runner_api(proto, unused_context):
    # type: (beam_runner_api_pb2.SideInput, PipelineContext) -> SideInputData
    assert proto.view_fn.urn == python_urns.PICKLED_VIEWFN
    assert (proto.window_mapping_fn.urn ==
            python_urns.PICKLED_WINDOW_MAPPING_FN)
    return SideInputData(
        proto.access_pattern.urn,
        pickler.loads(proto.window_mapping_fn.payload),
        pickler.loads(proto.view_fn.payload))


class AsSingleton(AsSideInput):
  """Marker specifying that an entire PCollection is to be used as a side input.

  When a PCollection is supplied as a side input to a PTransform, it is
  necessary to indicate whether the entire PCollection should be made available
  as a PTransform side argument (in the form of an iterable), or whether just
  one value should be pulled from the PCollection and supplied as the side
  argument (as an ordinary value).

  Wrapping a PCollection side input argument to a PTransform in this container
  (e.g., data.apply('label', MyPTransform(), AsSingleton(my_side_input) )
  selects the latter behavior.

  The input PCollection must contain exactly one value per window, unless a
  default is given, in which case it may be empty.
  """
  _NO_DEFAULT = object()

  def __init__(self, pcoll, default_value=_NO_DEFAULT):
    # type: (PCollection, Any) -> None
    super(AsSingleton, self).__init__(pcoll)
    self.default_value = default_value

  def __repr__(self):
    return 'AsSingleton(%s)' % self.pvalue

  def _view_options(self):
    base = super(AsSingleton, self)._view_options()
    if self.default_value != AsSingleton._NO_DEFAULT:
      return dict(base, default=self.default_value)
    return base

  @staticmethod
  def _from_runtime_iterable(it, options):
    head = list(itertools.islice(it, 2))
    if not head:
      return options.get('default', EmptySideInput())
    elif len(head) == 1:
      return head[0]
    raise ValueError(
        'PCollection of size %d with more than one element accessed as a '
        'singleton view. First two elements encountered are "%s", "%s".' % (
            len(head), str(head[0]), str(head[1])))

  @property
  def element_type(self):
    return self.pvalue.element_type


class AsIter(AsSideInput):
  """Marker specifying that an entire PCollection is to be used as a side input.

  When a PCollection is supplied as a side input to a PTransform, it is
  necessary to indicate whether the entire PCollection should be made available
  as a PTransform side argument (in the form of an iterable), or whether just
  one value should be pulled from the PCollection and supplied as the side
  argument (as an ordinary value).

  Wrapping a PCollection side input argument to a PTransform in this container
  (e.g., data.apply('label', MyPTransform(), AsIter(my_side_input) ) selects the
  former behavor.
  """

  def __repr__(self):
    return 'AsIter(%s)' % self.pvalue

  @staticmethod
  def _from_runtime_iterable(it, options):
    return it

  def _side_input_data(self):
    # type: () -> SideInputData
    return SideInputData(
        common_urns.side_inputs.ITERABLE.urn,
        self._window_mapping_fn,
        lambda iterable: iterable)

  @property
  def element_type(self):
    return typehints.Iterable[self.pvalue.element_type]


class AsList(AsSideInput):
  """Marker specifying that an entire PCollection is to be used as a side input.

  Intended for use in side-argument specification---the same places where
  AsSingleton and AsIter are used, but forces materialization of this
  PCollection as a list.

  Args:
    pcoll: Input pcollection.

  Returns:
    An AsList-wrapper around a PCollection whose one element is a list
    containing all elements in pcoll.
  """

  @staticmethod
  def _from_runtime_iterable(it, options):
    return list(it)

  def _side_input_data(self):
    # type: () -> SideInputData
    return SideInputData(
        common_urns.side_inputs.ITERABLE.urn,
        self._window_mapping_fn,
        list)


class AsDict(AsSideInput):
  """Marker specifying a PCollection to be used as an indexable side input.

  Intended for use in side-argument specification---the same places where
  AsSingleton and AsIter are used, but returns an interface that allows
  key lookup.

  Args:
    pcoll: Input pcollection. All elements should be key-value pairs (i.e.
       2-tuples) with unique keys.

  Returns:
    An AsDict-wrapper around a PCollection whose one element is a dict with
      entries for uniquely-keyed pairs in pcoll.
  """

  @staticmethod
  def _from_runtime_iterable(it, options):
    return dict(it)

  def _side_input_data(self):
    # type: () -> SideInputData
    return SideInputData(
        common_urns.side_inputs.ITERABLE.urn,
        self._window_mapping_fn,
        dict)


class AsMultiMap(AsSideInput):
  """Marker specifying a PCollection to be used as an indexable side input.

  Similar to AsDict, but multiple values may be associated per key, and
  the keys are fetched lazily rather than all having to fit in memory.

  Intended for use in side-argument specification---the same places where
  AsSingleton and AsIter are used, but returns an interface that allows
  key lookup.
  """

  @staticmethod
  def _from_runtime_iterable(it, options):
    # Legacy implementation.
    result = collections.defaultdict(list)
    for k, v in it:
      result[k].append(v)
    return result

  def _side_input_data(self):
    # type: () -> SideInputData
    return SideInputData(
        common_urns.side_inputs.MULTIMAP.urn,
        self._window_mapping_fn,
        lambda x: x)

  def requires_keyed_input(self):
    return True


class EmptySideInput(object):
  """Value indicating when a singleton side input was empty.

  If a PCollection was furnished as a singleton side input to a PTransform, and
  that PCollection was empty, then this value is supplied to the DoFn in the
  place where a value from a non-empty PCollection would have gone. This alerts
  the DoFn that the side input PCollection was empty. Users may want to check
  whether side input values are EmptySideInput, but they will very likely never
  want to create new instances of this class themselves.
  """
  pass
