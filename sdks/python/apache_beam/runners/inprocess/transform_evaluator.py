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

"""An evaluator of a specific application of a transform."""

from __future__ import absolute_import

import collections
import copy

from apache_beam import coders
from apache_beam import pvalue
import apache_beam.io as io
from apache_beam.runners.common import DoFnRunner
from apache_beam.runners.common import DoFnState
from apache_beam.runners.inprocess.inprocess_watermark_manager import InProcessWatermarkManager
from apache_beam.runners.inprocess.inprocess_transform_result import InProcessTransformResult
from apache_beam.transforms import core
from apache_beam.transforms import sideinputs
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import WindowedValue
from apache_beam.typehints.typecheck import OutputCheckWrapperDoFn
from apache_beam.typehints.typecheck import TypeCheckError
from apache_beam.typehints.typecheck import TypeCheckWrapperDoFn
from apache_beam.utils import counters
from apache_beam.utils.options import TypeOptions


class TransformEvaluatorRegistry(object):
  """Creates instances of TransformEvaluator for the application of a transform.
  """

  def __init__(self, evaluation_context):
    assert evaluation_context
    self._evaluation_context = evaluation_context
    self._evaluators = {
        io.Read: _BoundedReadEvaluator,
        core.Create: _CreateEvaluator,
        core.Flatten: _FlattenEvaluator,
        core.ParDo: _ParDoEvaluator,
        core.GroupByKeyOnly: _GroupByKeyOnlyEvaluator,
        sideinputs.CreatePCollectionView: _CreatePCollectionViewEvaluator,
        io.iobase._NativeWrite: _NativeWriteEvaluator,  # pylint: disable=protected-access
    }

  def for_application(
      self, applied_ptransform, input_committed_bundle, side_inputs):
    """Returns a TransformEvaluator suitable for processing given inputs."""
    assert applied_ptransform
    assert bool(applied_ptransform.side_inputs) == bool(side_inputs)

    # Walk up the class hierarchy to find an evaluable type. This is necessary
    # for supporting sub-classes of core transforms.
    for cls in applied_ptransform.transform.__class__.mro():
      evaluator = self._evaluators.get(cls)
      if evaluator:
        break

    if not evaluator:
      raise NotImplementedError(
          'Execution of [%s] not implemented in runner %s.' % (
              type(applied_ptransform.transform), self))
    return evaluator(self._evaluation_context, applied_ptransform,
                     input_committed_bundle, side_inputs)

  def should_execute_serially(self, applied_ptransform):
    """Returns True if this applied_ptransform should run one bundle at a time.

    Some TransformEvaluators use a global state object to keep track of their
    global execution state. For example evaluator for GroupByKeyOnly uses this
    state as an in memory dictionary to buffer keys.

    Serially executed evaluators will act as syncing point in the graph and
    execution will not move forward until they receive all of their inputs. Once
    they receive all of their input, they will release the combined output.
    Their output may consist of multiple bundles as they may divide their output
    into pieces before releasing.

    Args:
      applied_ptransform: Transform to be used for execution.

    Returns:
      True if executor should execute applied_ptransform serially.
    """
    return isinstance(applied_ptransform.transform,
                      (core.GroupByKeyOnly, sideinputs.CreatePCollectionView,
                       io.iobase._NativeWrite))  # pylint: disable=protected-access


class _TransformEvaluator(object):
  """An evaluator of a specific application of a transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    self._evaluation_context = evaluation_context
    self._applied_ptransform = applied_ptransform
    self._input_committed_bundle = input_committed_bundle
    self._side_inputs = side_inputs
    self._expand_outputs()
    self._execution_context = evaluation_context.get_execution_context(
        applied_ptransform)
    self.start_bundle()

  def _expand_outputs(self):
    outputs = set()
    for pval in self._applied_ptransform.outputs.values():
      if isinstance(pval, pvalue.DoOutputsTuple):
        pvals = (v for v in pval)
      else:
        pvals = (pval,)
      for v in pvals:
        outputs.add(v)
    self._outputs = frozenset(outputs)

  def _split_list_into_bundles(
      self, output_pcollection, elements, max_element_per_bundle,
      element_size_fn):
    """Splits elements, an iterable, into multiple output bundles.

    Args:
      output_pcollection: PCollection that the elements belong to.
      elements: elements to be chunked into bundles.
      max_element_per_bundle: (approximately) the maximum element per bundle.
        If it is None, only a single bundle will be produced.
      element_size_fn: Function to return the size of a given element.

    Returns:
      List of output uncommitted bundles with at least one bundle.
    """
    bundle = self._evaluation_context.create_bundle(output_pcollection)
    bundle_size = 0
    bundles = [bundle]
    for element in elements:
      if max_element_per_bundle and bundle_size >= max_element_per_bundle:
        bundle = self._evaluation_context.create_bundle(output_pcollection)
        bundle_size = 0
        bundles.append(bundle)

      bundle.output(element)
      bundle_size += element_size_fn(element)
    return bundles

  def start_bundle(self):
    """Starts a new bundle."""
    pass

  def process_element(self, element):
    """Processes a new element as part of the current bundle."""
    raise NotImplementedError('%s do not process elements.', type(self))

  def finish_bundle(self):
    """Finishes the bundle and produces output."""
    pass


class _BoundedReadEvaluator(_TransformEvaluator):
  """TransformEvaluator for bounded Read transform."""

  MAX_ELEMENT_PER_BUNDLE = 100

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not input_committed_bundle
    assert not side_inputs
    self._source = applied_ptransform.transform.source
    self._source.pipeline_options = evaluation_context.pipeline_options
    super(_BoundedReadEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def finish_bundle(self):
    assert len(self._outputs) == 1
    output_pcollection = list(self._outputs)[0]

    def _read_values_to_bundles(reader):
      read_result = [GlobalWindows.windowed_value(e) for e in reader]
      return self._split_list_into_bundles(
          output_pcollection, read_result,
          _BoundedReadEvaluator.MAX_ELEMENT_PER_BUNDLE, lambda _: 1)

    if isinstance(self._source, io.iobase.BoundedSource):
      # Getting a RangeTracker for the default range of the source and reading
      # the full source using that.
      range_tracker = self._source.get_range_tracker(None, None)
      reader = self._source.read(range_tracker)
      bundles = _read_values_to_bundles(reader)
    else:
      with self._source.reader() as reader:
        bundles = _read_values_to_bundles(reader)

    return InProcessTransformResult(
        self._applied_ptransform, bundles, None, None, None, None)


class _FlattenEvaluator(_TransformEvaluator):
  """TransformEvaluator for Flatten transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_FlattenEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def start_bundle(self):
    assert len(self._outputs) == 1
    output_pcollection = list(self._outputs)[0]
    self.bundle = self._evaluation_context.create_bundle(output_pcollection)

  def process_element(self, element):
    self.bundle.output(element)

  def finish_bundle(self):
    bundles = [self.bundle]
    return InProcessTransformResult(
        self._applied_ptransform, bundles, None, None, None, None)


class _CreateEvaluator(_TransformEvaluator):
  """TransformEvaluator for Create transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not input_committed_bundle
    assert not side_inputs
    super(_CreateEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def start_bundle(self):
    assert len(self._outputs) == 1
    output_pcollection = list(self._outputs)[0]
    self.bundle = self._evaluation_context.create_bundle(output_pcollection)

  def finish_bundle(self):
    bundles = []
    transform = self._applied_ptransform.transform

    assert transform.value is not None
    create_result = [GlobalWindows.windowed_value(v) for v in transform.value]
    for result in create_result:
      self.bundle.output(result)
    bundles.append(self.bundle)

    return InProcessTransformResult(
        self._applied_ptransform, bundles, None, None, None, None)


class _TaggedReceivers(dict):
  """Received ParDo output and redirect to the associated output bundle."""

  def __init__(self, evaluation_context):
    self._evaluation_context = evaluation_context
    self._null_receiver = None
    self._undeclared_in_memory_tag_values = None
    super(_TaggedReceivers, self).__init__()

  @property
  def undeclared_in_memory_tag_values(self):
    assert (not self._undeclared_in_memory_tag_values
            or self._evaluation_context.has_cache)
    return self._undeclared_in_memory_tag_values

  class NullReceiver(object):
    """Ignores undeclared side outputs, default execution mode."""

    def output(self, element):
      pass

  class InMemoryReceiver(object):
    """Buffers undeclared side outputs to the given dictionary."""

    def __init__(self, target, tag):
      self._target = target
      self._tag = tag

    def output(self, element):
      self._target[self._tag].append(element)

  def __missing__(self, key):
    if self._evaluation_context.has_cache:
      if not self._undeclared_in_memory_tag_values:
        self._undeclared_in_memory_tag_values = collections.defaultdict(list)
      receiver = _TaggedReceivers.InMemoryReceiver(
          self._undeclared_in_memory_tag_values, key)
    else:
      if not self._null_receiver:
        self._null_receiver = _TaggedReceivers.NullReceiver()
      receiver = self._null_receiver
    return receiver


class _ParDoEvaluator(_TransformEvaluator):
  """TransformEvaluator for ParDo transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    super(_ParDoEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def start_bundle(self):
    transform = self._applied_ptransform.transform

    self._tagged_receivers = _TaggedReceivers(self._evaluation_context)
    if isinstance(self._applied_ptransform.parent.transform, core._MultiParDo):  # pylint: disable=protected-access
      do_outputs_tuple = self._applied_ptransform.parent.outputs[0]
      assert isinstance(do_outputs_tuple, pvalue.DoOutputsTuple)
      main_output_pcollection = do_outputs_tuple[do_outputs_tuple._main_tag]  # pylint: disable=protected-access

      for side_output_tag in transform.side_output_tags:
        output_pcollection = do_outputs_tuple[side_output_tag]
        self._tagged_receivers[side_output_tag] = (
            self._evaluation_context.create_bundle(output_pcollection))
        self._tagged_receivers[side_output_tag].tag = side_output_tag
    else:
      assert len(self._outputs) == 1
      main_output_pcollection = list(self._outputs)[0]

    self._tagged_receivers[None] = self._evaluation_context.create_bundle(
        main_output_pcollection)
    self._tagged_receivers[None].tag = None  # main_tag is None.

    self._counter_factory = counters.CounterFactory()

    dofn = copy.deepcopy(transform.dofn)

    pipeline_options = self._evaluation_context.pipeline_options
    if (pipeline_options is not None
        and pipeline_options.view_as(TypeOptions).runtime_type_check):
      dofn = TypeCheckWrapperDoFn(dofn, transform.get_type_hints())

    dofn = OutputCheckWrapperDoFn(dofn, self._applied_ptransform.full_label)
    self.runner = DoFnRunner(dofn, transform.args, transform.kwargs,
                             self._side_inputs,
                             self._applied_ptransform.inputs[0].windowing,
                             tagged_receivers=self._tagged_receivers,
                             step_name=self._applied_ptransform.full_label,
                             state=DoFnState(self._counter_factory))
    self.runner.start()

  def process_element(self, element):
    self.runner.process(element)

  def finish_bundle(self):
    self.runner.finish()
    bundles = self._tagged_receivers.values()
    result_counters = self._counter_factory.get_counters()
    return InProcessTransformResult(
        self._applied_ptransform, bundles, None, None, result_counters, None,
        self._tagged_receivers.undeclared_in_memory_tag_values)


class _GroupByKeyOnlyEvaluator(_TransformEvaluator):
  """TransformEvaluator for GroupByKeyOnly transform."""

  MAX_ELEMENT_PER_BUNDLE = None

  class _GroupByKeyOnlyEvaluatorState(object):

    def __init__(self):
      # output: {} key -> [values]
      self.output = collections.defaultdict(list)
      self.completed = False

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_GroupByKeyOnlyEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  @property
  def _is_final_bundle(self):
    return (self._execution_context.watermarks.input_watermark
            == InProcessWatermarkManager.WATERMARK_POS_INF)

  def start_bundle(self):
    self.state = (self._execution_context.existing_state
                  if self._execution_context.existing_state
                  else _GroupByKeyOnlyEvaluator._GroupByKeyOnlyEvaluatorState())

    assert len(self._outputs) == 1
    self.output_pcollection = list(self._outputs)[0]

    # The input type of a GroupByKey will be KV[Any, Any] or more specific.
    kv_type_hint = (
        self._applied_ptransform.transform.get_type_hints().input_types[0])
    self.key_coder = coders.registry.get_coder(kv_type_hint[0].tuple_types[0])

  def process_element(self, element):
    assert not self.state.completed
    if (isinstance(element, WindowedValue)
        and isinstance(element.value, collections.Iterable)
        and len(element.value) == 2):
      k, v = element.value
      self.state.output[self.key_coder.encode(k)].append(v)
    else:
      raise TypeCheckError('Input to GroupByKeyOnly must be a PCollection of '
                           'windowed key-value pairs. Instead received: %r.'
                           % element)

  def finish_bundle(self):
    if self._is_final_bundle:
      if self.state.completed:
        # Ignore empty bundles after emitting output. (This may happen because
        # empty bundles do not affect input watermarks.)
        bundles = []
      else:
        gbk_result = (
            map(GlobalWindows.windowed_value, (
                (self.key_coder.decode(k), v)
                for k, v in self.state.output.iteritems())))

        def len_element_fn(element):
          _, v = element.value
          return len(v)

        bundles = self._split_list_into_bundles(
            self.output_pcollection, gbk_result,
            _GroupByKeyOnlyEvaluator.MAX_ELEMENT_PER_BUNDLE, len_element_fn)

      self.state.completed = True
      state = self.state
      hold = InProcessWatermarkManager.WATERMARK_POS_INF
    else:
      bundles = []
      state = self.state
      hold = InProcessWatermarkManager.WATERMARK_NEG_INF

    return InProcessTransformResult(
        self._applied_ptransform, bundles, state, None, None, hold)


class _CreatePCollectionViewEvaluator(_TransformEvaluator):
  """TransformEvaluator for CreatePCollectionView transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_CreatePCollectionViewEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  @property
  def _is_final_bundle(self):
    return (self._execution_context.watermarks.input_watermark
            == InProcessWatermarkManager.WATERMARK_POS_INF)

  def start_bundle(self):
    # state: [values]
    self.state = (self._execution_context.existing_state
                  if self._execution_context.existing_state else [])

    assert len(self._outputs) == 1
    self.output_pcollection = list(self._outputs)[0]

  def process_element(self, element):
    self.state.append(element)

  def finish_bundle(self):
    if self._is_final_bundle:
      bundle = self._evaluation_context.create_bundle(self.output_pcollection)

      view_result = self.state
      for result in view_result:
        bundle.output(result)

      bundles = [bundle]
      state = None
      hold = InProcessWatermarkManager.WATERMARK_POS_INF
    else:
      bundles = []
      state = self.state
      hold = InProcessWatermarkManager.WATERMARK_NEG_INF

    return InProcessTransformResult(
        self._applied_ptransform, bundles, state, None, None, hold)


class _NativeWriteEvaluator(_TransformEvaluator):
  """TransformEvaluator for _NativeWrite transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_NativeWriteEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

    assert applied_ptransform.transform.sink
    self._sink = copy.deepcopy(applied_ptransform.transform.sink)

  @property
  def _is_final_bundle(self):
    return (self._execution_context.watermarks.input_watermark
            == InProcessWatermarkManager.WATERMARK_POS_INF)

  def start_bundle(self):
    # state: [values]
    self.state = (self._execution_context.existing_state
                  if self._execution_context.existing_state else [])

  def process_element(self, element):
    self.state.append(element)

  def finish_bundle(self):
    # TODO(altay): Do not wait until the last bundle to write in a single shard.
    if self._is_final_bundle:
      if isinstance(self._sink, io.fileio.NativeTextFileSink):
        assert self._sink.num_shards in (0, 1)
        if self._sink.shard_name_template:
          self._sink.file_path += '-00000-of-00001'
          self._sink.file_path += self._sink.file_name_suffix
      self._sink.pipeline_options = self._evaluation_context.pipeline_options
      with self._sink.writer() as writer:
        for v in self.state:
          writer.Write(v.value)

      state = None
      hold = InProcessWatermarkManager.WATERMARK_POS_INF
    else:
      state = self.state
      hold = InProcessWatermarkManager.WATERMARK_NEG_INF

    return InProcessTransformResult(
        self._applied_ptransform, [], state, None, None, hold)
