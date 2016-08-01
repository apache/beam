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
    if isinstance(args[0], basestring):
      kwargs['label'], args = args[0], args[1:]
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

  def __init__(self, pipeline, **kwargs):
    """Initializes a PCollection. Do not call directly."""
    super(PCollection, self).__init__(pipeline, **kwargs)

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
          'Tag %s is neither the main tag %s nor any of the side tags %s' % (
              tag, self._main_tag, self._tags))
    # Check if we accessed this tag before.
    if tag in self._pcolls:
      return self._pcolls[tag]

    if tag is not None:
      self._transform.side_output_tags.add(tag)
      pcoll = PCollection(self._pipeline, tag=tag)
      # Transfer the producer from the DoOutputsTuple to the resulting
      # PCollection.
      pcoll.producer = self.producer
      # Add this as an output to both the inner ParDo and the outer _MultiParDo
      # PTransforms.
      self.producer.parts[0].add_output(pcoll, tag)
      self.producer.add_output(pcoll, tag)
    else:
      # Main output is output of inner ParDo.
      pcoll = self.producer.parts[0].outputs[0]
    self._pcolls[tag] = pcoll
    return pcoll


class SideOutputValue(object):
  """An object representing a tagged value.

  ParDo, Map, and FlatMap transforms can emit values on multiple outputs which
  are distinguished by string tags. The DoFn will return plain values
  if it wants to emit on the main output and SideOutputValue objects
  if it wants to emit a value on a specific tagged output.
  """

  def __init__(self, tag, value):
    if not isinstance(tag, basestring):
      raise TypeError(
          'Attempting to create a SideOutputValue with non-string tag %s' % tag)
    self.tag = tag
    self.value = value


class PCollectionView(PValue):
  """An immutable view of a PCollection that can be used as a side input."""

  def __init__(self, pipeline):
    """Initializes a PCollectionView. Do not call directly."""
    super(PCollectionView, self).__init__(pipeline)

  @property
  def windowing(self):
    if not hasattr(self, '_windowing'):
      self._windowing = self.producer.transform.get_windowing(
          self.producer.inputs)
    return self._windowing

  def _view_options(self):
    """Internal options corresponding to specific view.

    Intended for internal use by runner implementations.

    Returns:
      Tuple of options for the given view.
    """
    return ()


class SingletonPCollectionView(PCollectionView):
  """A PCollectionView that contains a single object."""

  def __init__(self, pipeline, has_default, default_value):
    super(SingletonPCollectionView, self).__init__(pipeline)
    self.has_default = has_default
    self.default_value = default_value

  def _view_options(self):
    return (self.has_default, self.default_value)


class IterablePCollectionView(PCollectionView):
  """A PCollectionView that can be treated as an iterable."""
  pass


class ListPCollectionView(PCollectionView):
  """A PCollectionView that can be treated as a list."""
  pass


class DictPCollectionView(PCollectionView):
  """A PCollectionView that can be treated as a dict."""
  pass


def _get_cached_view(pipeline, key):
  return pipeline._view_cache.get(key, None)  # pylint: disable=protected-access


def _cache_view(pipeline, key, view):
  pipeline._view_cache[key] = view  # pylint: disable=protected-access


def can_take_label_as_first_argument(callee):
  """Decorator to allow the "label" kwarg to be passed as the first argument.

  For example, since AsSingleton is annotated with this decorator, this allows
  the call "AsSingleton(pcoll, label='label1')" to be written more succinctly
  as "AsSingleton('label1', pcoll)".

  Args:
    callee: The callable to be called with an optional label argument.

  Returns:
    Callable that allows (but does not require) a string label as its first
    argument.
  """
  def _inner(maybe_label, *args, **kwargs):
    if isinstance(maybe_label, basestring):
      return callee(*args, label=maybe_label, **kwargs)
    return callee(*((maybe_label,) + args), **kwargs)
  return _inner


def _format_view_label(pcoll):
  # The monitoring UI doesn't like '/' character in transform labels.
  if not pcoll.producer:
    return str(pcoll.tag)
  return '%s.%s' % (pcoll.producer.full_label.replace('/', '|'),
                    pcoll.tag)


_SINGLETON_NO_DEFAULT = object()


@can_take_label_as_first_argument
def AsSingleton(pcoll, default_value=_SINGLETON_NO_DEFAULT, label=None):  # pylint: disable=invalid-name
  """Create a SingletonPCollectionView from the contents of input PCollection.

  The input PCollection should contain at most one element (per window) and the
  resulting PCollectionView can then be used as a side input to PTransforms. If
  the PCollectionView is empty (for a given window), the side input value will
  be the default_value, if specified; otherwise, it will be an EmptySideInput
  object.

  Args:
    pcoll: Input pcollection.
    default_value: Default value for the singleton view.
    label: Label to be specified if several AsSingleton's with different
      defaults for the same PCollection.

  Returns:
    A singleton PCollectionView containing the element as above.
  """
  label = label or _format_view_label(pcoll)
  has_default = default_value is not _SINGLETON_NO_DEFAULT
  if not has_default:
    default_value = None

  # Don't recreate the view if it was already created.
  hashable_default_value = ('val', default_value)
  if not isinstance(default_value, collections.Hashable):
    # Massage default value to treat as hash key.
    hashable_default_value = ('id', id(default_value))
  cache_key = (pcoll, AsSingleton, has_default, hashable_default_value)
  cached_view = _get_cached_view(pcoll.pipeline, cache_key)
  if cached_view:
    return cached_view

  # Local import is required due to dependency loop; even though the
  # implementation of this function requires concepts defined in modules that
  # depend on pvalue, it lives in this module to reduce user workload.
  from apache_beam.transforms import sideinputs  # pylint: disable=wrong-import-order, wrong-import-position
  view = (pcoll | sideinputs.ViewAsSingleton(has_default, default_value,
                                             label=label))
  _cache_view(pcoll.pipeline, cache_key, view)
  return view


@can_take_label_as_first_argument
def AsIter(pcoll, label=None):  # pylint: disable=invalid-name
  """Create an IterablePCollectionView from the elements of input PCollection.

  The contents of the given PCollection will be available as an iterable in
  PTransforms that use the returned PCollectionView as a side input.

  Args:
    pcoll: Input pcollection.
    label: Label to be specified if several AsIter's for the same PCollection.

  Returns:
    An iterable PCollectionView containing the elements as above.
  """
  label = label or _format_view_label(pcoll)

  # Don't recreate the view if it was already created.
  cache_key = (pcoll, AsIter)
  cached_view = _get_cached_view(pcoll.pipeline, cache_key)
  if cached_view:
    return cached_view

  # Local import is required due to dependency loop; even though the
  # implementation of this function requires concepts defined in modules that
  # depend on pvalue, it lives in this module to reduce user workload.
  from apache_beam.transforms import sideinputs  # pylint: disable=wrong-import-order, wrong-import-position
  view = (pcoll | sideinputs.ViewAsIterable(label=label))
  _cache_view(pcoll.pipeline, cache_key, view)
  return view


@can_take_label_as_first_argument
def AsList(pcoll, label=None):  # pylint: disable=invalid-name
  """Create a ListPCollectionView from the elements of input PCollection.

  The contents of the given PCollection will be available as a list-like object
  in PTransforms that use the returned PCollectionView as a side input.

  Args:
    pcoll: Input pcollection.
    label: Label to be specified if several AsList's for the same PCollection.

  Returns:
    A list PCollectionView containing the elements as above.
  """
  label = label or _format_view_label(pcoll)

  # Don't recreate the view if it was already created.
  cache_key = (pcoll, AsList)
  cached_view = _get_cached_view(pcoll.pipeline, cache_key)
  if cached_view:
    return cached_view

  # Local import is required due to dependency loop; even though the
  # implementation of this function requires concepts defined in modules that
  # depend on pvalue, it lives in this module to reduce user workload.
  from apache_beam.transforms import sideinputs  # pylint: disable=wrong-import-order, wrong-import-position
  view = (pcoll | sideinputs.ViewAsList(label=label))
  _cache_view(pcoll.pipeline, cache_key, view)
  return view


@can_take_label_as_first_argument
def AsDict(pcoll, label=None):  # pylint: disable=invalid-name
  """Create a DictPCollectionView from the elements of input PCollection.

  The contents of the given PCollection whose elements are 2-tuples of key and
  value will be available as a dict-like object in PTransforms that use the
  returned PCollectionView as a side input.

  Args:
    pcoll: Input pcollection containing 2-tuples of key and value.
    label: Label to be specified if several AsDict's for the same PCollection.

  Returns:
    A dict PCollectionView containing the dict as above.
  """
  label = label or _format_view_label(pcoll)

  # Don't recreate the view if it was already created.
  cache_key = (pcoll, AsDict)
  cached_view = _get_cached_view(pcoll.pipeline, cache_key)
  if cached_view:
    return cached_view

  # Local import is required due to dependency loop; even though the
  # implementation of this function requires concepts defined in modules that
  # depend on pvalue, it lives in this module to reduce user workload.
  from apache_beam.transforms import sideinputs  # pylint: disable=wrong-import-order, wrong-import-position
  view = (pcoll | sideinputs.ViewAsDict(label=label))
  _cache_view(pcoll.pipeline, cache_key, view)
  return view


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
