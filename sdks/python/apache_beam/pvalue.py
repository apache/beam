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

import itertools

from apache_beam import typehints


__all__ = [
    'PValue',
    'PCollection',
    'TaggedOutput',
    'AsSideInput',
    'AsSingleton',
    'AsIter',
    'AsList',
    'AsDict',
    'EmptySideInput',
]


class PValue(object):
  """Base class for PCollection.

  Dataflow users should not construct PValue objects directly in their
  pipelines.

  A PValue has the following main characteristics:
    (1) Belongs to a pipeline. Added during object initialization.
    (2) Has a transform that can compute the value if executed.
    (3) Has a value which is meaningful if the transform was executed.
  """

  def __init__(self, pipeline, tag=None, element_type=None):
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
    self.producer = None

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


class PCollection(PValue):
  """A multiple values (potentially huge) container.

  Dataflow users should not construct PCollection objects directly in their
  pipelines.
  """

  def __eq__(self, other):
    if isinstance(other, PCollection):
      return self.tag == other.tag and self.producer == other.producer

  def __hash__(self):
    return hash((self.tag, self.producer))

  @property
  def windowing(self):
    if not hasattr(self, '_windowing'):
      self._windowing = self.producer.transform.get_windowing(
          self.producer.inputs)
    return self._windowing

  def __reduce_ex__(self, unused_version):
    # Pickling a PCollection is almost always the wrong thing to do, but we
    # can't prohibit it as it often gets implicitly picked up (e.g. as part
    # of a closure).
    return _InvalidUnpickledPCollection, ()

  def to_runner_api(self, context):
    from apache_beam.portability.api import beam_runner_api_pb2
    from apache_beam.internal import pickler
    return beam_runner_api_pb2.PCollection(
        unique_name='%d%s.%s' % (
            len(self.producer.full_label), self.producer.full_label, self.tag),
        coder_id=pickler.dumps(self.element_type),
        is_bounded=beam_runner_api_pb2.BOUNDED,
        windowing_strategy_id=context.windowing_strategies.get_id(
            self.windowing))

  @staticmethod
  def from_runner_api(proto, context):
    from apache_beam.internal import pickler
    # Producer and tag will be filled in later, the key point is that the
    # same object is returned for the same pcollection id.
    return PCollection(None, element_type=pickler.loads(proto.coder_id))


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

  def __init__(self, pipeline, transform, tags, main_tag):
    self._pipeline = pipeline
    self._tags = tags
    self._main_tag = main_tag
    self._transform = transform
    # The ApplyPTransform instance for the application of the multi FlatMap
    # generating this value. The field gets initialized when a transform
    # gets applied.
    self.producer = None
    # Dictionary of PCollections already associated with tags.
    self._pcolls = {}

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s main_tag=%s tags=%s transform=%s' % (
        self.__class__.__name__, self._main_tag, self._tags, self._transform)

  def __iter__(self):
    """Iterates over tags returning for each call a (tag, pvalue) pair."""
    if self._main_tag is not None:
      yield self[self._main_tag]
    for tag in self._tags:
      yield self[tag]

  def __getattr__(self, tag):
    # Special methods which may be accessed before the object is
    # fully constructed (e.g. in unpickling).
    if tag[:2] == tag[-2:] == '__':
      return object.__getattr__(self, tag)
    return self[tag]

  def __getitem__(self, tag):
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

    if tag is not None:
      self._transform.output_tags.add(tag)
      pcoll = PCollection(self._pipeline, tag=tag)
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
    if not isinstance(tag, basestring):
      raise TypeError(
          'Attempting to create a TaggedOutput with non-string tag %s' % tag)
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


class AsSingleton(AsSideInput):
  """Marker specifying that an entire PCollection is to be used as a side input.

  When a PCollection is supplied as a side input to a PTransform, it is
  necessary to indicate whether the entire PCollection should be made available
  as a PTransform side argument (in the form of an iterable), or whether just
  one value should be pulled from the PCollection and supplied as the side
  argument (as an ordinary value).

  Wrapping a PCollection side input argument to a PTransform in this container
  (e.g., data.apply('label', MyPTransform(), AsSingleton(my_side_input) )
  selects the latter behavor.

  The input PCollection must contain exactly one  value per window, unless a
  default is given, in which case it may be empty.
  """
  _NO_DEFAULT = object()

  def __init__(self, pcoll, default_value=_NO_DEFAULT):
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
        'PCollection with more than one element accessed as '
        'a singleton view.')

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
