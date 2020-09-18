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

"""DirectRunner, executing on the local machine.

The DirectRunner is a runner implementation that executes the entire
graph of transformations belonging to a pipeline on the local machine.
"""

# pytype: skip-file

from __future__ import absolute_import

import itertools
import logging
import time
import typing

from google.protobuf import wrappers_pb2

import apache_beam as beam
from apache_beam import coders
from apache_beam import typehints
from apache_beam.internal.util import ArgumentPlaceholder
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.pvalue import PCollection
from apache_beam.runners.direct.bundle_factory import BundleFactory
from apache_beam.runners.direct.clock import RealClock
from apache_beam.runners.direct.clock import TestClock
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.transforms.core import CombinePerKey
from apache_beam.transforms.core import CombineValuesDoFn
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.ptransform import PTransform
from apache_beam.typehints import trivial_inference

# Note that the BundleBasedDirectRunner and SwitchingDirectRunner names are
# experimental and have no backwards compatibility guarantees.
__all__ = ['BundleBasedDirectRunner', 'DirectRunner', 'SwitchingDirectRunner']

_LOGGER = logging.getLogger(__name__)


class SwitchingDirectRunner(PipelineRunner):
  """Executes a single pipeline on the local machine.

  This implementation switches between using the FnApiRunner (which has
  high throughput for batch jobs) and using the BundleBasedDirectRunner,
  which supports streaming execution and certain primitives not yet
  implemented in the FnApiRunner.
  """
  def is_fnapi_compatible(self):
    return BundleBasedDirectRunner.is_fnapi_compatible()

  def run_pipeline(self, pipeline, options):

    from apache_beam.pipeline import PipelineVisitor
    from apache_beam.runners.dataflow.native_io.iobase import NativeSource
    from apache_beam.runners.dataflow.native_io.iobase import _NativeWrite
    from apache_beam.testing.test_stream import TestStream

    class _FnApiRunnerSupportVisitor(PipelineVisitor):
      """Visitor determining if a Pipeline can be run on the FnApiRunner."""
      def accept(self, pipeline):
        self.supported_by_fnapi_runner = True
        pipeline.visit(self)
        return self.supported_by_fnapi_runner

      def visit_transform(self, applied_ptransform):
        transform = applied_ptransform.transform
        # The FnApiRunner does not support streaming execution.
        if isinstance(transform, TestStream):
          self.supported_by_fnapi_runner = False
        # The FnApiRunner does not support reads from NativeSources.
        if (isinstance(transform, beam.io.Read) and
            isinstance(transform.source, NativeSource)):
          self.supported_by_fnapi_runner = False
        # The FnApiRunner does not support the use of _NativeWrites.
        if isinstance(transform, _NativeWrite):
          self.supported_by_fnapi_runner = False
        if isinstance(transform, beam.ParDo):
          dofn = transform.dofn
          # The FnApiRunner does not support execution of CombineFns with
          # deferred side inputs.
          if isinstance(dofn, CombineValuesDoFn):
            args, kwargs = transform.raw_side_inputs
            args_to_check = itertools.chain(args, kwargs.values())
            if any(isinstance(arg, ArgumentPlaceholder)
                   for arg in args_to_check):
              self.supported_by_fnapi_runner = False

    # Check whether all transforms used in the pipeline are supported by the
    # FnApiRunner, and the pipeline was not meant to be run as streaming.
    if _FnApiRunnerSupportVisitor().accept(pipeline):
      from apache_beam.runners.portability.fn_api_runner import FnApiRunner
      runner = FnApiRunner()
    else:
      runner = BundleBasedDirectRunner()

    return runner.run_pipeline(pipeline, options)


# Type variables.
K = typing.TypeVar('K')
V = typing.TypeVar('V')


@typehints.with_input_types(typing.Tuple[K, V])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _GroupByKeyOnly(PTransform):
  """A group by key transform, ignoring windows."""
  def infer_output_type(self, input_type):
    key_type, value_type = trivial_inference.key_value_types(input_type)
    return typehints.KV[key_type, typehints.Iterable[value_type]]

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return PCollection.from_(pcoll)


@typehints.with_input_types(typing.Tuple[K, typing.Iterable[V]])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _GroupAlsoByWindow(ParDo):
  """The GroupAlsoByWindow transform."""
  def __init__(self, windowing):
    super(_GroupAlsoByWindow, self).__init__(_GroupAlsoByWindowDoFn(windowing))
    self.windowing = windowing

  def expand(self, pcoll):
    self._check_pcollection(pcoll)
    return PCollection.from_(pcoll)


class _GroupAlsoByWindowDoFn(DoFn):
  # TODO(robertwb): Support combiner lifting.

  def __init__(self, windowing):
    super(_GroupAlsoByWindowDoFn, self).__init__()
    self.windowing = windowing

  def infer_output_type(self, input_type):
    key_type, windowed_value_iter_type = trivial_inference.key_value_types(
        input_type)
    value_type = windowed_value_iter_type.inner_type.inner_type
    return typehints.Iterable[typehints.KV[key_type,
                                           typehints.Iterable[value_type]]]

  def start_bundle(self):
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.trigger import create_trigger_driver
    # pylint: enable=wrong-import-order, wrong-import-position
    self.driver = create_trigger_driver(self.windowing, True)

  def process(self, element):
    k, vs = element
    return self.driver.process_entire_key(k, vs)


@typehints.with_input_types(typing.Tuple[K, V])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _StreamingGroupByKeyOnly(_GroupByKeyOnly):
  """Streaming GroupByKeyOnly placeholder for overriding in DirectRunner."""
  urn = "direct_runner:streaming_gbko:v0.1"

  # These are needed due to apply overloads.
  def to_runner_api_parameter(self, unused_context):
    return _StreamingGroupByKeyOnly.urn, None

  @staticmethod
  @PTransform.register_urn(urn, None)
  def from_runner_api_parameter(
      unused_ptransform, unused_payload, unused_context):
    return _StreamingGroupByKeyOnly()


@typehints.with_input_types(typing.Tuple[K, typing.Iterable[V]])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _StreamingGroupAlsoByWindow(_GroupAlsoByWindow):
  """Streaming GroupAlsoByWindow placeholder for overriding in DirectRunner."""
  urn = "direct_runner:streaming_gabw:v0.1"

  # These are needed due to apply overloads.
  def to_runner_api_parameter(self, context):
    return (
        _StreamingGroupAlsoByWindow.urn,
        wrappers_pb2.BytesValue(
            value=context.windowing_strategies.get_id(self.windowing)))

  @staticmethod
  @PTransform.register_urn(urn, wrappers_pb2.BytesValue)
  def from_runner_api_parameter(unused_ptransform, payload, context):
    return _StreamingGroupAlsoByWindow(
        context.windowing_strategies.get_by_id(payload.value))


@typehints.with_input_types(typing.Tuple[K, typing.Iterable[V]])
@typehints.with_output_types(typing.Tuple[K, typing.Iterable[V]])
class _GroupByKey(PTransform):
  """The DirectRunner GroupByKey implementation."""
  def expand(self, pcoll):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.coders import typecoders

    input_type = pcoll.element_type
    if input_type is not None:
      # Initialize type-hints used below to enforce type-checking and to
      # pass downstream to further PTransforms.
      key_type, value_type = trivial_inference.key_value_types(input_type)
      # Enforce the input to a GBK has a KV element type.
      pcoll.element_type = typehints.typehints.coerce_to_kv_type(
          pcoll.element_type)
      typecoders.registry.verify_deterministic(
          typecoders.registry.get_coder(key_type),
          'GroupByKey operation "%s"' % self.label)

      reify_output_type = typehints.KV[
          key_type, typehints.WindowedValue[value_type]]  # type: ignore[misc]
      gbk_input_type = (
          typehints.KV[
              key_type,
              typehints.Iterable[typehints.WindowedValue[  # type: ignore[misc]
                  value_type]]])
      gbk_output_type = typehints.KV[key_type, typehints.Iterable[value_type]]

      # pylint: disable=bad-continuation
      return (
          pcoll
          | 'ReifyWindows' >> (
              ParDo(beam.GroupByKey.ReifyWindows()).with_output_types(
                  reify_output_type))
          | 'GroupByKey' >> (
              _GroupByKeyOnly().with_input_types(
                  reify_output_type).with_output_types(gbk_input_type))
          | (
              'GroupByWindow' >>
              _GroupAlsoByWindow(pcoll.windowing).with_input_types(
                  gbk_input_type).with_output_types(gbk_output_type)))
    else:
      # The input_type is None, run the default
      return (
          pcoll
          | 'ReifyWindows' >> ParDo(beam.GroupByKey.ReifyWindows())
          | 'GroupByKey' >> _GroupByKeyOnly()
          | 'GroupByWindow' >> _GroupAlsoByWindow(pcoll.windowing))


def _get_transform_overrides(pipeline_options):
  # A list of PTransformOverride objects to be applied before running a pipeline
  # using DirectRunner.
  # Currently this only works for overrides where the input and output types do
  # not change.
  # For internal use only; no backwards-compatibility guarantees.

  # Importing following locally to avoid a circular dependency.
  from apache_beam.pipeline import PTransformOverride
  from apache_beam.runners.direct.helper_transforms import LiftedCombinePerKey
  from apache_beam.runners.direct.sdf_direct_runner import ProcessKeyedElementsViaKeyedWorkItemsOverride
  from apache_beam.runners.direct.sdf_direct_runner import SplittableParDoOverride

  class CombinePerKeyOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      if isinstance(applied_ptransform.transform, CombinePerKey):
        return applied_ptransform.inputs[0].windowing.is_default()

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # TODO: Move imports to top. Pipeline <-> Runner dependency cause problems
      # with resolving imports when they are at top.
      # pylint: disable=wrong-import-position
      try:
        transform = applied_ptransform.transform
        return LiftedCombinePerKey(
            transform.fn, transform.args, transform.kwargs)
      except NotImplementedError:
        return transform

  class StreamingGroupByKeyOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      # Note: we match the exact class, since we replace it with a subclass.
      return applied_ptransform.transform.__class__ == _GroupByKeyOnly

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # Use specialized streaming implementation.
      transform = _StreamingGroupByKeyOnly()
      return transform

  class StreamingGroupAlsoByWindowOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      # Note: we match the exact class, since we replace it with a subclass.
      transform = applied_ptransform.transform
      return (
          isinstance(applied_ptransform.transform, ParDo) and
          isinstance(transform.dofn, _GroupAlsoByWindowDoFn) and
          transform.__class__ != _StreamingGroupAlsoByWindow)

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      # Use specialized streaming implementation.
      transform = _StreamingGroupAlsoByWindow(
          applied_ptransform.transform.dofn.windowing)
      return transform

  class TestStreamOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      from apache_beam.testing.test_stream import TestStream
      self.applied_ptransform = applied_ptransform
      return isinstance(applied_ptransform.transform, TestStream)

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      from apache_beam.runners.direct.test_stream_impl import _ExpandableTestStream
      return _ExpandableTestStream(applied_ptransform.transform)

  class GroupByKeyPTransformOverride(PTransformOverride):
    """A ``PTransformOverride`` for ``GroupByKey``.

    This replaces the Beam implementation as a primitive.
    """
    def matches(self, applied_ptransform):
      # Imported here to avoid circular dependencies.
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam.transforms.core import GroupByKey
      return isinstance(applied_ptransform.transform, GroupByKey)

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      return _GroupByKey()

  overrides = [
      # This needs to be the first and the last override. Other overrides depend
      # on the GroupByKey implementation to be composed of _GroupByKeyOnly and
      # _GroupAlsoByWindow.
      GroupByKeyPTransformOverride(),
      SplittableParDoOverride(),
      ProcessKeyedElementsViaKeyedWorkItemsOverride(),
      CombinePerKeyOverride(),
      TestStreamOverride(),
  ]

  # Add streaming overrides, if necessary.
  if pipeline_options.view_as(StandardOptions).streaming:
    overrides.append(StreamingGroupByKeyOverride())
    overrides.append(StreamingGroupAlsoByWindowOverride())

  # Add PubSub overrides, if PubSub is available.
  try:
    from apache_beam.io.gcp import pubsub as unused_pubsub
    overrides += _get_pubsub_transform_overrides(pipeline_options)
  except ImportError:
    pass

  # This also needs to be last because other transforms apply GBKs which need to
  # be translated into a DirectRunner-compatible transform.
  overrides.append(GroupByKeyPTransformOverride())

  return overrides


class _DirectReadFromPubSub(PTransform):
  def __init__(self, source):
    self._source = source

  def _infer_output_coder(
      self, unused_input_type=None, unused_input_coder=None):
    # type: (...) -> typing.Optional[coders.Coder]
    return coders.BytesCoder()

  def get_windowing(self, inputs):
    return beam.Windowing(beam.window.GlobalWindows())

  def expand(self, pvalue):
    # This is handled as a native transform.
    return PCollection(self.pipeline, is_bounded=self._source.is_bounded())


class _DirectWriteToPubSubFn(DoFn):
  BUFFER_SIZE_ELEMENTS = 100
  FLUSH_TIMEOUT_SECS = BUFFER_SIZE_ELEMENTS * 0.5

  def __init__(self, sink):
    self.project = sink.project
    self.short_topic_name = sink.topic_name
    self.id_label = sink.id_label
    self.timestamp_attribute = sink.timestamp_attribute
    self.with_attributes = sink.with_attributes

    # TODO(BEAM-4275): Add support for id_label and timestamp_attribute.
    if sink.id_label:
      raise NotImplementedError(
          'DirectRunner: id_label is not supported for '
          'PubSub writes')
    if sink.timestamp_attribute:
      raise NotImplementedError(
          'DirectRunner: timestamp_attribute is not '
          'supported for PubSub writes')

  def start_bundle(self):
    self._buffer = []

  def process(self, elem):
    self._buffer.append(elem)
    if len(self._buffer) >= self.BUFFER_SIZE_ELEMENTS:
      self._flush()

  def finish_bundle(self):
    self._flush()

  def _flush(self):
    from google.cloud import pubsub
    pub_client = pubsub.PublisherClient()
    topic = pub_client.topic_path(self.project, self.short_topic_name)

    if self.with_attributes:
      futures = [
          pub_client.publish(topic, elem.data, **elem.attributes)
          for elem in self._buffer
      ]
    else:
      futures = [pub_client.publish(topic, elem) for elem in self._buffer]

    timer_start = time.time()
    for future in futures:
      remaining = self.FLUSH_TIMEOUT_SECS - (time.time() - timer_start)
      future.result(remaining)
    self._buffer = []


def _get_pubsub_transform_overrides(pipeline_options):
  from apache_beam.io.gcp import pubsub as beam_pubsub
  from apache_beam.pipeline import PTransformOverride

  class ReadFromPubSubOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      return isinstance(
          applied_ptransform.transform, beam_pubsub.ReadFromPubSub)

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      if not pipeline_options.view_as(StandardOptions).streaming:
        raise Exception(
            'PubSub I/O is only available in streaming mode '
            '(use the --streaming flag).')
      return _DirectReadFromPubSub(applied_ptransform.transform._source)

  class WriteToPubSubOverride(PTransformOverride):
    def matches(self, applied_ptransform):
      return isinstance(
          applied_ptransform.transform,
          (beam_pubsub.WriteToPubSub, beam_pubsub._WriteStringsToPubSub))

    def get_replacement_transform_for_applied_ptransform(
        self, applied_ptransform):
      if not pipeline_options.view_as(StandardOptions).streaming:
        raise Exception(
            'PubSub I/O is only available in streaming mode '
            '(use the --streaming flag).')
      return beam.ParDo(
          _DirectWriteToPubSubFn(applied_ptransform.transform._sink))

  return [ReadFromPubSubOverride(), WriteToPubSubOverride()]


class BundleBasedDirectRunner(PipelineRunner):
  """Executes a single pipeline on the local machine."""
  @staticmethod
  def is_fnapi_compatible():
    return False

  def run_pipeline(self, pipeline, options):
    """Execute the entire pipeline and returns an DirectPipelineResult."""

    # TODO: Move imports to top. Pipeline <-> Runner dependency cause problems
    # with resolving imports when they are at top.
    # pylint: disable=wrong-import-position
    from apache_beam.pipeline import PipelineVisitor
    from apache_beam.runners.direct.consumer_tracking_pipeline_visitor import \
      ConsumerTrackingPipelineVisitor
    from apache_beam.runners.direct.evaluation_context import EvaluationContext
    from apache_beam.runners.direct.executor import Executor
    from apache_beam.runners.direct.transform_evaluator import \
      TransformEvaluatorRegistry
    from apache_beam.testing.test_stream import TestStream

    # If the TestStream I/O is used, use a mock test clock.
    class TestStreamUsageVisitor(PipelineVisitor):
      """Visitor determining whether a Pipeline uses a TestStream."""
      def __init__(self):
        self.uses_test_stream = False

      def visit_transform(self, applied_ptransform):
        if isinstance(applied_ptransform.transform, TestStream):
          self.uses_test_stream = True

    visitor = TestStreamUsageVisitor()
    pipeline.visit(visitor)
    clock = TestClock() if visitor.uses_test_stream else RealClock()

    # Performing configured PTransform overrides.
    pipeline.replace_all(_get_transform_overrides(options))

    _LOGGER.info('Running pipeline with DirectRunner.')
    self.consumer_tracking_visitor = ConsumerTrackingPipelineVisitor()
    pipeline.visit(self.consumer_tracking_visitor)

    evaluation_context = EvaluationContext(
        options,
        BundleFactory(
            stacked=options.view_as(
                DirectOptions).direct_runner_use_stacked_bundle),
        self.consumer_tracking_visitor.root_transforms,
        self.consumer_tracking_visitor.value_to_consumers,
        self.consumer_tracking_visitor.step_names,
        self.consumer_tracking_visitor.views,
        clock)

    executor = Executor(
        self.consumer_tracking_visitor.value_to_consumers,
        TransformEvaluatorRegistry(evaluation_context),
        evaluation_context)
    # DirectRunner does not support injecting
    # PipelineOptions values at runtime
    RuntimeValueProvider.set_runtime_options({})
    # Start the executor. This is a non-blocking call, it will start the
    # execution in background threads and return.
    executor.start(self.consumer_tracking_visitor.root_transforms)
    result = DirectPipelineResult(executor, evaluation_context)

    return result


# Use the SwitchingDirectRunner as the default.
DirectRunner = SwitchingDirectRunner


class DirectPipelineResult(PipelineResult):
  """A DirectPipelineResult provides access to info about a pipeline."""
  def __init__(self, executor, evaluation_context):
    super(DirectPipelineResult, self).__init__(PipelineState.RUNNING)
    self._executor = executor
    self._evaluation_context = evaluation_context

  def __del__(self):
    if self._state == PipelineState.RUNNING:
      _LOGGER.warning(
          'The DirectPipelineResult is being garbage-collected while the '
          'DirectRunner is still running the corresponding pipeline. This may '
          'lead to incomplete execution of the pipeline if the main thread '
          'exits before pipeline completion. Consider using '
          'result.wait_until_finish() to wait for completion of pipeline '
          'execution.')

  def wait_until_finish(self, duration=None):
    if not PipelineState.is_terminal(self.state):
      if duration:
        raise NotImplementedError(
            'DirectRunner does not support duration argument.')
      try:
        self._executor.await_completion()
        self._state = PipelineState.DONE
      except:  # pylint: disable=broad-except
        self._state = PipelineState.FAILED
        raise
    return self._state

  def aggregated_values(self, aggregator_or_name):
    return self._evaluation_context.get_aggregator_values(aggregator_or_name)

  def metrics(self):
    return self._evaluation_context.metrics()

  def cancel(self):
    """Shuts down pipeline workers.

    For testing use only. Does not properly wait for pipeline workers to shut
    down.
    """
    self._state = PipelineState.CANCELLING
    self._executor.shutdown()
    self._state = PipelineState.CANCELLED
