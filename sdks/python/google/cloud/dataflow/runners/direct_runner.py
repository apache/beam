# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DirectPipelineRunner, executing on the local machine.

The DirectPipelineRunner class implements what is called in Dataflow
parlance the "direct runner". Such a runner executes the entire graph
of transformations belonging to a pipeline on the local machine.
"""

from __future__ import absolute_import

import collections
import itertools

from google.cloud.dataflow import coders
from google.cloud.dataflow import error
from google.cloud.dataflow.pvalue import AsIter
from google.cloud.dataflow.pvalue import AsSingleton
from google.cloud.dataflow.pvalue import EmptySideInput
from google.cloud.dataflow.runners.common import DoFnRunner
from google.cloud.dataflow.runners.common import DoFnState
from google.cloud.dataflow.runners.runner import PipelineRunner
from google.cloud.dataflow.runners.runner import PValueCache
from google.cloud.dataflow.transforms import DoFnProcessContext
from google.cloud.dataflow.transforms.window import GlobalWindows
from google.cloud.dataflow.transforms.window import WindowedValue
from google.cloud.dataflow.typehints.typecheck import OutputCheckWrapperDoFn
from google.cloud.dataflow.typehints.typecheck import TypeCheckError
from google.cloud.dataflow.typehints.typecheck import TypeCheckWrapperDoFn


class DirectPipelineRunner(PipelineRunner):
  """A local pipeline runner.

  The runner computes everything locally and does not make any attempt to
  optimize for time or space.
  """

  def __init__(self, cache=None):
    # Cache of values computed while the runner executes a pipeline.
    self._cache = cache if cache is not None else PValueCache()

  def get_pvalue(self, pvalue):
    """Gets the PValue's computed value from the runner's cache."""
    try:
      return self._cache.get_pvalue(pvalue)
    except KeyError:
      raise error.PValueError('PValue is not computed.')

  def clear_pvalue(self, pvalue):
    """Removes a PValue from the runner's cache."""
    self._cache.clear_pvalue(pvalue)

  def skip_if_cached(func):  # pylint: disable=no-self-argument
    """Decorator to skip execution of a transform if value is cached."""

    def func_wrapper(self, pvalue, *args, **kwargs):
      if self._cache.is_cached(pvalue):  # pylint: disable=protected-access
        return
      else:
        func(self, pvalue, *args, **kwargs)
    return func_wrapper

  @skip_if_cached
  def run_ParDo(self, transform_node):
    transform = transform_node.transform
    # TODO(gildea): what is the appropriate object to attach the state to?
    context = DoFnProcessContext(label=transform.label, state=DoFnState())

    # Construct the list of values from side-input PCollections that we'll
    # substitute into the arguments for DoFn methods.
    def get_side_input_value(si):
      if isinstance(si, AsSingleton):
        # User wants one item from the PCollection as side input, or an
        # EmptySideInput if no value exists.
        pcoll_vals = self._cache.get_pvalue(si.pvalue)
        if len(pcoll_vals) == 1:
          return pcoll_vals[0].value
        elif len(pcoll_vals) > 1:
          raise ValueError("PCollection with more than one element "
                           "accessed as a singleton view.")
        elif si.default_value != si._NO_DEFAULT:
          return si.default_value
        else:
          # TODO(robertwb): Should be an error like Java.
          return EmptySideInput()
      if isinstance(si, AsIter):
        # User wants the entire PCollection as side input. List permits
        # repeatable iteration.
        return [v.value for v in self._cache.get_pvalue(si.pvalue)]
    side_inputs = [get_side_input_value(e) for e in transform_node.side_inputs]

    # TODO(robertwb): Do this type checking inside DoFnRunner to get it on
    # remote workers as well?
    options = transform_node.inputs[0].pipeline.options
    if options is not None and options.runtime_type_check:
      transform.dofn = TypeCheckWrapperDoFn(
          transform.dofn, transform.get_type_hints())

    # TODO(robertwb): Should this be conditionally done on the workers as well?
    transform.dofn = OutputCheckWrapperDoFn(
        transform.dofn, transform_node.full_label)

    class NoOpCounters(object):
      def update(self, element):
        pass

    class RecordingReciever(object):
      def __init__(self, tag):
        self.tag = tag
      def process(self, element):
        results[self.tag].append(element)

    class TaggedRecievers(dict):
      def __missing__(self, key):
        return [RecordingReciever(key)]

    results = collections.defaultdict(list)
    # Some tags may be empty.
    for tag in transform.side_output_tags:
      results[tag] = []

    runner = DoFnRunner(transform.dofn, transform.args, transform.kwargs,
                        side_inputs, transform_node.inputs[0].windowing,
                        context, TaggedRecievers(),
                        collections.defaultdict(NoOpCounters))
    runner.start()
    for v in self._cache.get_pvalue(transform_node.inputs[0]):
      runner.process(v)
    runner.finish()

    self._cache.cache_output(transform_node, [])
    for tag, value in results.items():
      self._cache.cache_output(transform_node, tag, value)

  @skip_if_cached
  def run_GroupByKeyOnly(self, transform_node):
    result_dict = collections.defaultdict(list)
    # The input type of a GroupByKey will be KV[Any, Any] or more specific.
    kv_type_hint = transform_node.transform.get_type_hints().input_types[0]
    key_coder = coders.registry.get_coder(kv_type_hint[0].tuple_types[0])

    for wv in self._cache.get_pvalue(transform_node.inputs[0]):
      if (isinstance(wv, WindowedValue) and
          isinstance(wv.value, collections.Iterable) and len(wv.value) == 2):
        k, v = wv.value
        # We use as key a string encoding of the key object to support keys
        # that are based on custom classes. This mimics also the remote
        # execution behavior where key objects are encoded before being written
        # to the shuffler system responsible for grouping.
        result_dict[key_coder.encode(k)].append(v)
      else:
        raise TypeCheckError('Input to GroupByKeyOnly must be a PCollection of '
                             'windowed key-value pairs. Instead received: %r.'
                             % wv)

    self._cache.cache_output(
        transform_node,
        map(GlobalWindows.WindowedValue,
            ((key_coder.decode(k), v) for k, v in result_dict.iteritems())))

  @skip_if_cached
  def run_Create(self, transform_node):
    transform = transform_node.transform
    self._cache.cache_output(
        transform_node,
        [GlobalWindows.WindowedValue(v) for v in transform.value])

  @skip_if_cached
  def run_Flatten(self, transform_node):
    self._cache.cache_output(
        transform_node,
        list(itertools.chain.from_iterable(
            self._cache.get_pvalue(pc) for pc in transform_node.inputs)))

  @skip_if_cached
  def run_Read(self, transform_node):
    transform = transform_node.transform
    with transform.source.reader() as reader:
      self._cache.cache_output(
          transform_node, [GlobalWindows.WindowedValue(e) for e in reader])

  @skip_if_cached
  def run__NativeWrite(self, transform_node):
    transform = transform_node.transform
    with transform.sink.writer() as writer:
      for v in self._cache.get_pvalue(transform_node.inputs[0]):
        writer.Write(v.value)
