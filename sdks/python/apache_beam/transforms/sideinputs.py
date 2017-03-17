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

"""Internal side input transforms and implementations.

Important: this module is an implementation detail and should not be used
directly by pipeline writers. Instead, users should use the helper methods
AsSingleton, AsIter, AsList and AsDict in apache_beam.pvalue.
"""

from __future__ import absolute_import

from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms import window

# Type variables
K = typehints.TypeVariable('K')
V = typehints.TypeVariable('V')


class CreatePCollectionView(PTransform):
  """Transform to materialize a given PCollectionView in the pipeline.

  Important: this transform is an implementation detail and should not be used
  directly by pipeline writers.
  """

  def __init__(self, view):
    self.view = view
    super(CreatePCollectionView, self).__init__()

  def infer_output_type(self, input_type):
    # TODO(ccy): Figure out if we want to create a new type of type hint, i.e.,
    # typehints.View[...].
    return input_type

  def expand(self, pcoll):
    return self.view


class ViewAsSingleton(PTransform):
  """Transform to view PCollection as a singleton PCollectionView.

  Important: this transform is an implementation detail and should not be used
  directly by pipeline writers. Use pvalue.AsSingleton(...) instead.
  """

  def __init__(self, has_default, default_value, label=None):
    if label:
      label = 'ViewAsSingleton(%s)' % label
    super(ViewAsSingleton, self).__init__(label=label)
    self.has_default = has_default
    self.default_value = default_value

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    input_type = pcoll.element_type
    output_type = input_type
    return (pcoll
            | CreatePCollectionView(
                pvalue.SingletonPCollectionView(
                    pcoll.pipeline, self.has_default, self.default_value,
                    default_window_mapping_fn(pcoll.windowing.windowfn)))
            .with_input_types(input_type)
            .with_output_types(output_type))


class ViewAsIterable(PTransform):
  """Transform to view PCollection as an iterable PCollectionView.

  Important: this transform is an implementation detail and should not be used
  directly by pipeline writers. Use pvalue.AsIter(...) instead.
  """

  def __init__(self, label=None):
    if label:
      label = 'ViewAsIterable(%s)' % label
    super(ViewAsIterable, self).__init__(label=label)

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    input_type = pcoll.element_type
    output_type = typehints.Iterable[input_type]
    return (pcoll
            | CreatePCollectionView(
                pvalue.IterablePCollectionView(
                    pcoll.pipeline,
                    default_window_mapping_fn(pcoll.windowing.windowfn)))
            .with_input_types(input_type)
            .with_output_types(output_type))


class ViewAsList(PTransform):
  """Transform to view PCollection as a list PCollectionView.

  Important: this transform is an implementation detail and should not be used
  directly by pipeline writers. Use pvalue.AsList(...) instead.
  """

  def __init__(self, label=None):
    if label:
      label = 'ViewAsList(%s)' % label
    super(ViewAsList, self).__init__(label=label)

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    input_type = pcoll.element_type
    output_type = typehints.List[input_type]
    return (pcoll
            | CreatePCollectionView(pvalue.ListPCollectionView(
                pcoll.pipeline,
                default_window_mapping_fn(pcoll.windowing.windowfn)))
            .with_input_types(input_type)
            .with_output_types(output_type))


@typehints.with_input_types(typehints.Tuple[K, V])
@typehints.with_output_types(typehints.Dict[K, V])
class ViewAsDict(PTransform):
  """Transform to view PCollection as a dict PCollectionView.

  Important: this transform is an implementation detail and should not be used
  directly by pipeline writers. Use pvalue.AsDict(...) instead.
  """

  def __init__(self, label=None):
    if label:
      label = 'ViewAsDict(%s)' % label
    super(ViewAsDict, self).__init__(label=label)

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    input_type = pcoll.element_type
    key_type, value_type = (
        typehints.trivial_inference.key_value_types(input_type))
    output_type = typehints.Dict[key_type, value_type]
    return (pcoll
            | CreatePCollectionView(
                pvalue.DictPCollectionView(
                    pcoll.pipeline,
                    default_window_mapping_fn(pcoll.windowing.windowfn)))
            .with_input_types(input_type)
            .with_output_types(output_type))


# Top-level function so we can identify it later.
def _global_window_mapping_fn(w, global_window=window.GlobalWindow()):
  return global_window


def default_window_mapping_fn(target_window_fn):
  if target_window_fn == window.GlobalWindows():
    return _global_window_mapping_fn
  else:
    def map_via_end(source_window):
      return list(target_window_fn.assign(
          window.WindowFn.AssignContext(source_window.max_timestamp())))[-1]
    return map_via_end


class SideInputMap(object):
  """Represents a mapping of windows to side input values."""

  def __init__(self, view_class, view_options, iterable):
    self._window_mapping_fn = view_options.get(
        'window_mapping_fn', _global_window_mapping_fn)
    self._view_class = view_class
    self._view_options = view_options
    self._iterable = iterable
    self._cache = {}

  def __getitem__(self, window):
    if window not in self._cache:
      target_window = self._window_mapping_fn(window)
      self._cache[window] = self._view_class._from_runtime_iterable(
          _FilteringIterable(self._iterable, target_window), self._view_options)
    return self._cache[window]

  def is_globally_windowed(self):
    return self._window_mapping_fn == _global_window_mapping_fn


class _FilteringIterable(object):
  """An iterable containing only those values in the given window.
  """

  def __init__(self, iterable, target_window):
    self._iterable = iterable
    self._target_window = target_window

  def __iter__(self):
    for wv in self._iterable:
      if self._target_window in wv.windows:
        yield wv.value

  def __reduce__(self):
    # Pickle self as an already filtered list.
    return list, (list(self),)
