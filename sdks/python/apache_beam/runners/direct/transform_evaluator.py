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
import logging
import random
import time
from builtins import object

from future.utils import iteritems

import apache_beam.io as io
from apache_beam import coders
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.internal import pickler
from apache_beam.runners import common
from apache_beam.runners.common import DoFnRunner
from apache_beam.runners.common import DoFnState
from apache_beam.runners.dataflow.native_io.iobase import _NativeWrite  # pylint: disable=protected-access
from apache_beam.runners.direct.direct_runner import _DirectReadFromPubSub
from apache_beam.runners.direct.direct_runner import _StreamingGroupAlsoByWindow
from apache_beam.runners.direct.direct_runner import _StreamingGroupByKeyOnly
from apache_beam.runners.direct.direct_userstate import DirectUserStateContext
from apache_beam.runners.direct.sdf_direct_runner import ProcessElements
from apache_beam.runners.direct.sdf_direct_runner import ProcessFn
from apache_beam.runners.direct.sdf_direct_runner import SDFProcessElementInvoker
from apache_beam.runners.direct.util import KeyedWorkItem
from apache_beam.runners.direct.util import TransformResult
from apache_beam.runners.direct.watermark_manager import WatermarkManager
from apache_beam.testing.test_stream import ElementEvent
from apache_beam.testing.test_stream import ProcessingTimeEvent
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_stream import WatermarkEvent
from apache_beam.transforms import core
from apache_beam.transforms.trigger import TimeDomain
from apache_beam.transforms.trigger import _CombiningValueStateTag
from apache_beam.transforms.trigger import _ListStateTag
from apache_beam.transforms.trigger import create_trigger_driver
from apache_beam.transforms.userstate import get_dofn_specs
from apache_beam.transforms.userstate import is_stateful_dofn
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import WindowedValue
from apache_beam.typehints.typecheck import TypeCheckError
from apache_beam.utils import counters
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


class TransformEvaluatorRegistry(object):
  """For internal use only; no backwards-compatibility guarantees.

  Creates instances of TransformEvaluator for the application of a transform.
  """

  _test_evaluators_overrides = {}

  def __init__(self, evaluation_context):
    assert evaluation_context
    self._evaluation_context = evaluation_context
    self._evaluators = {
        io.Read: _BoundedReadEvaluator,
        _DirectReadFromPubSub: _PubSubReadEvaluator,
        core.Flatten: _FlattenEvaluator,
        core.ParDo: _ParDoEvaluator,
        core._GroupByKeyOnly: _GroupByKeyOnlyEvaluator,
        _StreamingGroupByKeyOnly: _StreamingGroupByKeyOnlyEvaluator,
        _StreamingGroupAlsoByWindow: _StreamingGroupAlsoByWindowEvaluator,
        _NativeWrite: _NativeWriteEvaluator,
        TestStream: _TestStreamEvaluator,
        ProcessElements: _ProcessElementsEvaluator
    }
    self._evaluators.update(self._test_evaluators_overrides)
    self._root_bundle_providers = {
        core.PTransform: DefaultRootBundleProvider,
        TestStream: _TestStreamRootBundleProvider,
    }

  def get_evaluator(
      self, applied_ptransform, input_committed_bundle,
      side_inputs):
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

  def get_root_bundle_provider(self, applied_ptransform):
    provider_cls = None
    for cls in applied_ptransform.transform.__class__.mro():
      provider_cls = self._root_bundle_providers.get(cls)
      if provider_cls:
        break
    if not provider_cls:
      raise NotImplementedError(
          'Root provider for [%s] not implemented in runner %s' % (
              type(applied_ptransform.transform), self))
    return provider_cls(self._evaluation_context, applied_ptransform)

  def should_execute_serially(self, applied_ptransform):
    """Returns True if this applied_ptransform should run one bundle at a time.

    Some TransformEvaluators use a global state object to keep track of their
    global execution state. For example evaluator for _GroupByKeyOnly uses this
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
    if isinstance(applied_ptransform.transform,
                  (core._GroupByKeyOnly,
                   _StreamingGroupByKeyOnly,
                   _StreamingGroupAlsoByWindow,
                   _NativeWrite)):
      return True
    elif (isinstance(applied_ptransform.transform, core.ParDo) and
          is_stateful_dofn(applied_ptransform.transform.dofn)):
      return True
    return False


class RootBundleProvider(object):
  """Provides bundles for the initial execution of a root transform."""

  def __init__(self, evaluation_context, applied_ptransform):
    self._evaluation_context = evaluation_context
    self._applied_ptransform = applied_ptransform

  def get_root_bundles(self):
    raise NotImplementedError


class DefaultRootBundleProvider(RootBundleProvider):
  """Provides an empty bundle by default for root transforms."""

  def get_root_bundles(self):
    input_node = pvalue.PBegin(self._applied_ptransform.transform.pipeline)
    empty_bundle = (
        self._evaluation_context.create_empty_committed_bundle(input_node))
    return [empty_bundle]


class _TestStreamRootBundleProvider(RootBundleProvider):
  """Provides an initial bundle for the TestStream evaluator."""

  def get_root_bundles(self):
    test_stream = self._applied_ptransform.transform
    bundles = []
    if len(test_stream.events) > 0:
      bundle = self._evaluation_context.create_bundle(
          pvalue.PBegin(self._applied_ptransform.transform.pipeline))
      # Explicitly set timestamp to MIN_TIMESTAMP to ensure that we hold the
      # watermark.
      bundle.add(GlobalWindows.windowed_value(0, timestamp=MIN_TIMESTAMP))
      bundle.commit(None)
      bundles.append(bundle)
    return bundles


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
    self._step_context = self._execution_context.get_step_context()

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

  def process_timer_wrapper(self, timer_firing):
    """Process timer by clearing and then calling process_timer().

    This method is called with any timer firing and clears the delivered
    timer from the keyed state and then calls process_timer().  The default
    process_timer() implementation emits a KeyedWorkItem for the particular
    timer and passes it to process_element().  Evaluator subclasses which
    desire different timer delivery semantics can override process_timer().
    """
    state = self._step_context.get_keyed_state(timer_firing.encoded_key)
    state.clear_timer(
        timer_firing.window, timer_firing.name, timer_firing.time_domain)
    self.process_timer(timer_firing)

  def process_timer(self, timer_firing):
    """Default process_timer() impl. generating KeyedWorkItem element."""
    self.process_element(
        GlobalWindows.windowed_value(
            KeyedWorkItem(timer_firing.encoded_key,
                          timer_firings=[timer_firing])))

  def process_element(self, element):
    """Processes a new element as part of the current bundle."""
    raise NotImplementedError('%s do not process elements.' % type(self))

  def finish_bundle(self):
    """Finishes the bundle and produces output."""
    pass


class _BoundedReadEvaluator(_TransformEvaluator):
  """TransformEvaluator for bounded Read transform."""

  # After some benchmarks, 1000 was optimal among {100,1000,10000}
  MAX_ELEMENT_PER_BUNDLE = 1000

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
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

    return TransformResult(self, bundles, [], None, None)


class _TestStreamEvaluator(_TransformEvaluator):
  """TransformEvaluator for the TestStream transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    self.test_stream = applied_ptransform.transform
    super(_TestStreamEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def start_bundle(self):
    self.current_index = -1
    self.watermark = MIN_TIMESTAMP
    self.bundles = []

  def process_element(self, element):
    index = element.value
    self.watermark = element.timestamp
    assert isinstance(index, int)
    assert 0 <= index <= len(self.test_stream.events)
    self.current_index = index
    event = self.test_stream.events[self.current_index]
    if isinstance(event, ElementEvent):
      assert len(self._outputs) == 1
      output_pcollection = list(self._outputs)[0]
      bundle = self._evaluation_context.create_bundle(output_pcollection)
      for tv in event.timestamped_values:
        bundle.output(
            GlobalWindows.windowed_value(tv.value, timestamp=tv.timestamp))
      self.bundles.append(bundle)
    elif isinstance(event, WatermarkEvent):
      assert event.new_watermark >= self.watermark
      self.watermark = event.new_watermark
    elif isinstance(event, ProcessingTimeEvent):
      self._evaluation_context._watermark_manager._clock.advance_time(
          event.advance_by)
    else:
      raise ValueError('Invalid TestStream event: %s.' % event)

  def finish_bundle(self):
    unprocessed_bundles = []
    hold = None
    if self.current_index < len(self.test_stream.events) - 1:
      unprocessed_bundle = self._evaluation_context.create_bundle(
          pvalue.PBegin(self._applied_ptransform.transform.pipeline))
      unprocessed_bundle.add(GlobalWindows.windowed_value(
          self.current_index + 1, timestamp=self.watermark))
      unprocessed_bundles.append(unprocessed_bundle)
      hold = self.watermark

    return TransformResult(
        self, self.bundles, unprocessed_bundles, None, {None: hold})


class _PubSubSubscriptionWrapper(object):
  """Wrapper for managing temporary PubSub subscriptions."""

  def __init__(self, project, short_topic_name, short_sub_name):
    """Initialize subscription wrapper.

    If sub_name is None, will create a temporary subscription to topic_name.

    Args:
      project: GCP project name for topic and subscription. May be None.
        Required if sub_name is None.
      short_topic_name: Valid topic name without
        'projects/{project}/topics/' prefix. May be None.
        Required if sub_name is None.
      short_sub_name: Valid subscription name without
        'projects/{project}/subscriptions/' prefix. May be None.
    """
    from google.cloud import pubsub
    self.sub_client = pubsub.SubscriberClient()

    if short_sub_name is None:
      self.sub_name = self.sub_client.subscription_path(
          project, 'beam_%d_%x' % (int(time.time()), random.randrange(1 << 32)))
      topic_name = self.sub_client.topic_path(project, short_topic_name)
      self.sub_client.create_subscription(self.sub_name, topic_name)
      self._should_cleanup = True
    else:
      self.sub_name = self.sub_client.subscription_path(project, short_sub_name)
      self._should_cleanup = False

  def __del__(self):
    if self._should_cleanup:
      self.sub_client.delete_subscription(self.sub_name)


class _PubSubReadEvaluator(_TransformEvaluator):
  """TransformEvaluator for PubSub read."""

  # A mapping of transform to _PubSubSubscriptionWrapper.
  _subscription_cache = {}

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_PubSubReadEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

    self.source = self._applied_ptransform.transform._source
    if self.source.id_label:
      raise NotImplementedError(
          'DirectRunner: id_label is not supported for PubSub reads')
    self._sub_name = _PubSubReadEvaluator.get_subscription(
        self._applied_ptransform, self.source.project, self.source.topic_name,
        self.source.subscription_name)

  @classmethod
  def get_subscription(cls, transform, project, topic, short_sub_name):
    if transform not in cls._subscription_cache:
      wrapper = _PubSubSubscriptionWrapper(project, topic, short_sub_name)
      cls._subscription_cache[transform] = wrapper
    return cls._subscription_cache[transform].sub_name

  def start_bundle(self):
    pass

  def process_element(self, element):
    pass

  def _read_from_pubsub(self, timestamp_attribute):
    from apache_beam.io.gcp.pubsub import PubsubMessage
    from google.cloud import pubsub
    # Because of the AutoAck, we are not able to reread messages if this
    # evaluator fails with an exception before emitting a bundle. However,
    # the DirectRunner currently doesn't retry work items anyway, so the
    # pipeline would enter an inconsistent state on any error.
    sub_client = pubsub.SubscriberClient()
    response = sub_client.pull(self._sub_name, max_messages=10,
                               return_immediately=True)

    def _get_element(message):
      parsed_message = PubsubMessage._from_message(message)
      if (timestamp_attribute and
          timestamp_attribute in parsed_message.attributes):
        rfc3339_or_milli = parsed_message.attributes[timestamp_attribute]
        try:
          timestamp = Timestamp.from_rfc3339(rfc3339_or_milli)
        except ValueError:
          try:
            timestamp = Timestamp(micros=int(rfc3339_or_milli) * 1000)
          except ValueError as e:
            raise ValueError('Bad timestamp value: %s' % e)
      else:
        timestamp = Timestamp(message.publish_time.seconds,
                              message.publish_time.nanos // 1000)

      return timestamp, parsed_message

    results = [_get_element(rm.message) for rm in response.received_messages]
    ack_ids = [rm.ack_id for rm in response.received_messages]
    if ack_ids:
      sub_client.acknowledge(self._sub_name, ack_ids)

    return results

  def finish_bundle(self):
    data = self._read_from_pubsub(self.source.timestamp_attribute)
    if data:
      output_pcollection = list(self._outputs)[0]
      bundle = self._evaluation_context.create_bundle(output_pcollection)
      # TODO(ccy): Respect the PubSub source's id_label field.
      for timestamp, message in data:
        if self.source.with_attributes:
          element = message
        else:
          element = message.data
        bundle.output(
            GlobalWindows.windowed_value(element, timestamp=timestamp))
      bundles = [bundle]
    else:
      bundles = []
    if self._applied_ptransform.inputs:
      input_pvalue = self._applied_ptransform.inputs[0]
    else:
      input_pvalue = pvalue.PBegin(self._applied_ptransform.transform.pipeline)
    unprocessed_bundle = self._evaluation_context.create_bundle(
        input_pvalue)

    # TODO(udim): Correct value for watermark hold.
    return TransformResult(self, bundles, [unprocessed_bundle], None,
                           {None: Timestamp.of(time.time())})


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
    return TransformResult(self, bundles, [], None, None)


class _TaggedReceivers(dict):
  """Received ParDo output and redirect to the associated output bundle."""

  def __init__(self, evaluation_context):
    self._evaluation_context = evaluation_context
    self._null_receiver = None
    super(_TaggedReceivers, self).__init__()

  class NullReceiver(common.Receiver):
    """Ignores undeclared outputs, default execution mode."""

    def receive(self, element):
      pass

  class _InMemoryReceiver(common.Receiver):
    """Buffers undeclared outputs to the given dictionary."""

    def __init__(self, target, tag):
      self._target = target
      self._tag = tag

    def receive(self, element):
      self._target[self._tag].append(element)

  def __missing__(self, key):
    if not self._null_receiver:
      self._null_receiver = _TaggedReceivers.NullReceiver()
    return self._null_receiver


class _ParDoEvaluator(_TransformEvaluator):
  """TransformEvaluator for ParDo transform."""

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs,
               perform_dofn_pickle_test=True):
    super(_ParDoEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)
    # This is a workaround for SDF implementation. SDF implementation adds state
    # to the SDF that is not picklable.
    self._perform_dofn_pickle_test = perform_dofn_pickle_test

  def start_bundle(self):
    transform = self._applied_ptransform.transform

    self._tagged_receivers = _TaggedReceivers(self._evaluation_context)
    for output_tag in self._applied_ptransform.outputs:
      output_pcollection = pvalue.PCollection(None, tag=output_tag)
      output_pcollection.producer = self._applied_ptransform
      self._tagged_receivers[output_tag] = (
          self._evaluation_context.create_bundle(output_pcollection))
      self._tagged_receivers[output_tag].tag = output_tag

    self._counter_factory = counters.CounterFactory()

    # TODO(aaltay): Consider storing the serialized form as an optimization.
    dofn = (pickler.loads(pickler.dumps(transform.dofn))
            if self._perform_dofn_pickle_test else transform.dofn)

    args = transform.args if hasattr(transform, 'args') else []
    kwargs = transform.kwargs if hasattr(transform, 'kwargs') else {}

    self.user_state_context = None
    self.user_timer_map = {}
    if is_stateful_dofn(dofn):
      kv_type_hint = self._applied_ptransform.inputs[0].element_type
      if kv_type_hint and kv_type_hint != typehints.Any:
        coder = coders.registry.get_coder(kv_type_hint)
        self.key_coder = coder.key_coder()
      else:
        self.key_coder = coders.registry.get_coder(typehints.Any)

      self.user_state_context = DirectUserStateContext(
          self._step_context, dofn, self.key_coder)
      _, all_timer_specs = get_dofn_specs(dofn)
      for timer_spec in all_timer_specs:
        self.user_timer_map['user/%s' % timer_spec.name] = timer_spec

    self.runner = DoFnRunner(
        dofn, args, kwargs,
        self._side_inputs,
        self._applied_ptransform.inputs[0].windowing,
        tagged_receivers=self._tagged_receivers,
        step_name=self._applied_ptransform.full_label,
        state=DoFnState(self._counter_factory),
        user_state_context=self.user_state_context)
    self.runner.start()

  def process_timer(self, timer_firing):
    if timer_firing.name not in self.user_timer_map:
      logging.warning('Unknown timer fired: %s', timer_firing)
    timer_spec = self.user_timer_map[timer_firing.name]
    self.runner.process_user_timer(
        timer_spec, self.key_coder.decode(timer_firing.encoded_key),
        timer_firing.window, timer_firing.timestamp)

  def process_element(self, element):
    self.runner.process(element)

  def finish_bundle(self):
    self.runner.finish()
    bundles = list(self._tagged_receivers.values())
    result_counters = self._counter_factory.get_counters()
    if self.user_state_context:
      self.user_state_context.commit()
    return TransformResult(
        self, bundles, [], result_counters, None)


class _GroupByKeyOnlyEvaluator(_TransformEvaluator):
  """TransformEvaluator for _GroupByKeyOnly transform."""

  MAX_ELEMENT_PER_BUNDLE = None
  ELEMENTS_TAG = _ListStateTag('elements')
  COMPLETION_TAG = _CombiningValueStateTag('completed', any)

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_GroupByKeyOnlyEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def _is_final_bundle(self):
    return (self._execution_context.watermarks.input_watermark
            == WatermarkManager.WATERMARK_POS_INF)

  def start_bundle(self):
    self.global_state = self._step_context.get_keyed_state(None)

    assert len(self._outputs) == 1
    self.output_pcollection = list(self._outputs)[0]

    # The output type of a GroupByKey will be KV[Any, Any] or more specific.
    # TODO(BEAM-2717): Infer coders earlier.
    kv_type_hint = (
        self._applied_ptransform.outputs[None].element_type
        or
        self._applied_ptransform.transform.get_type_hints().input_types[0][0])
    self.key_coder = coders.registry.get_coder(kv_type_hint.tuple_types[0])

  def process_timer(self, timer_firing):
    # We do not need to emit a KeyedWorkItem to process_element().
    pass

  def process_element(self, element):
    assert not self.global_state.get_state(
        None, _GroupByKeyOnlyEvaluator.COMPLETION_TAG)
    if (isinstance(element, WindowedValue)
        and isinstance(element.value, collections.Iterable)
        and len(element.value) == 2):
      k, v = element.value
      encoded_k = self.key_coder.encode(k)
      state = self._step_context.get_keyed_state(encoded_k)
      state.add_state(None, _GroupByKeyOnlyEvaluator.ELEMENTS_TAG, v)
    else:
      raise TypeCheckError('Input to _GroupByKeyOnly must be a PCollection of '
                           'windowed key-value pairs. Instead received: %r.'
                           % element)

  def finish_bundle(self):
    if self._is_final_bundle():
      if self.global_state.get_state(
          None, _GroupByKeyOnlyEvaluator.COMPLETION_TAG):
        # Ignore empty bundles after emitting output. (This may happen because
        # empty bundles do not affect input watermarks.)
        bundles = []
      else:
        gbk_result = []
        # TODO(ccy): perhaps we can clean this up to not use this
        # internal attribute of the DirectStepContext.
        for encoded_k in self._step_context.existing_keyed_state:
          # Ignore global state.
          if encoded_k is None:
            continue
          k = self.key_coder.decode(encoded_k)
          state = self._step_context.get_keyed_state(encoded_k)
          vs = state.get_state(None, _GroupByKeyOnlyEvaluator.ELEMENTS_TAG)
          gbk_result.append(GlobalWindows.windowed_value((k, vs)))

        def len_element_fn(element):
          _, v = element.value
          return len(v)

        bundles = self._split_list_into_bundles(
            self.output_pcollection, gbk_result,
            _GroupByKeyOnlyEvaluator.MAX_ELEMENT_PER_BUNDLE, len_element_fn)

      self.global_state.add_state(
          None, _GroupByKeyOnlyEvaluator.COMPLETION_TAG, True)
      hold = WatermarkManager.WATERMARK_POS_INF
    else:
      bundles = []
      hold = WatermarkManager.WATERMARK_NEG_INF
      self.global_state.set_timer(
          None, '', TimeDomain.WATERMARK, WatermarkManager.WATERMARK_POS_INF)

    return TransformResult(self, bundles, [], None, {None: hold})


class _StreamingGroupByKeyOnlyEvaluator(_TransformEvaluator):
  """TransformEvaluator for _StreamingGroupByKeyOnly transform.

  The _GroupByKeyOnlyEvaluator buffers elements until its input watermark goes
  to infinity, which is suitable for batch mode execution. During streaming
  mode execution, we emit each bundle as it comes to the next transform.
  """

  MAX_ELEMENT_PER_BUNDLE = None

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_StreamingGroupByKeyOnlyEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def start_bundle(self):
    self.gbk_items = collections.defaultdict(list)

    assert len(self._outputs) == 1
    self.output_pcollection = list(self._outputs)[0]

    # The input type of a GroupByKey will be KV[Any, Any] or more specific.
    kv_type_hint = self._applied_ptransform.inputs[0].element_type
    key_type_hint = (kv_type_hint.tuple_types[0] if kv_type_hint
                     else typehints.Any)
    self.key_coder = coders.registry.get_coder(key_type_hint)

  def process_element(self, element):
    if (isinstance(element, WindowedValue)
        and isinstance(element.value, collections.Iterable)
        and len(element.value) == 2):
      k, v = element.value
      self.gbk_items[self.key_coder.encode(k)].append(v)
    else:
      raise TypeCheckError('Input to _GroupByKeyOnly must be a PCollection of '
                           'windowed key-value pairs. Instead received: %r.'
                           % element)

  def finish_bundle(self):
    bundles = []
    bundle = None
    for encoded_k, vs in iteritems(self.gbk_items):
      if not bundle:
        bundle = self._evaluation_context.create_bundle(
            self.output_pcollection)
        bundles.append(bundle)
      kwi = KeyedWorkItem(encoded_k, elements=vs)
      bundle.add(GlobalWindows.windowed_value(kwi))

    return TransformResult(self, bundles, [], None, None)


class _StreamingGroupAlsoByWindowEvaluator(_TransformEvaluator):
  """TransformEvaluator for the _StreamingGroupAlsoByWindow transform.

  This evaluator is only used in streaming mode.  In batch mode, the
  GroupAlsoByWindow operation is evaluated as a normal DoFn, as defined
  in transforms/core.py.
  """

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_StreamingGroupAlsoByWindowEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

  def start_bundle(self):
    assert len(self._outputs) == 1
    self.output_pcollection = list(self._outputs)[0]
    self.driver = create_trigger_driver(
        self._applied_ptransform.transform.windowing,
        clock=self._evaluation_context._watermark_manager._clock)
    self.gabw_items = []
    self.keyed_holds = {}

    # The input type (which is the same as the output type) of a
    # GroupAlsoByWindow will be KV[Any, Iter[Any]] or more specific.
    kv_type_hint = self._applied_ptransform.outputs[None].element_type
    key_type_hint = (kv_type_hint.tuple_types[0] if kv_type_hint
                     else typehints.Any)
    self.key_coder = coders.registry.get_coder(key_type_hint)

  def process_element(self, element):
    kwi = element.value
    assert isinstance(kwi, KeyedWorkItem), kwi
    encoded_k, timer_firings, vs = (
        kwi.encoded_key, kwi.timer_firings, kwi.elements)
    k = self.key_coder.decode(encoded_k)
    state = self._step_context.get_keyed_state(encoded_k)

    for timer_firing in timer_firings:
      for wvalue in self.driver.process_timer(
          timer_firing.window, timer_firing.name, timer_firing.time_domain,
          timer_firing.timestamp, state):
        self.gabw_items.append(wvalue.with_value((k, wvalue.value)))
    if vs:
      for wvalue in self.driver.process_elements(state, vs, MIN_TIMESTAMP):
        self.gabw_items.append(wvalue.with_value((k, wvalue.value)))

    self.keyed_holds[encoded_k] = state.get_earliest_hold()

  def finish_bundle(self):
    bundles = []
    if self.gabw_items:
      bundle = self._evaluation_context.create_bundle(self.output_pcollection)
      for item in self.gabw_items:
        bundle.add(item)
      bundles.append(bundle)

    return TransformResult(self, bundles, [], None, self.keyed_holds)


class _NativeWriteEvaluator(_TransformEvaluator):
  """TransformEvaluator for _NativeWrite transform."""

  ELEMENTS_TAG = _ListStateTag('elements')

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    assert not side_inputs
    super(_NativeWriteEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

    assert applied_ptransform.transform.sink
    self._sink = applied_ptransform.transform.sink

  @property
  def _is_final_bundle(self):
    return (self._execution_context.watermarks.input_watermark
            == WatermarkManager.WATERMARK_POS_INF)

  @property
  def _has_already_produced_output(self):
    return (self._execution_context.watermarks.output_watermark
            == WatermarkManager.WATERMARK_POS_INF)

  def start_bundle(self):
    self.global_state = self._step_context.get_keyed_state(None)

  def process_timer(self, timer_firing):
    # We do not need to emit a KeyedWorkItem to process_element().
    pass

  def process_element(self, element):
    self.global_state.add_state(
        None, _NativeWriteEvaluator.ELEMENTS_TAG, element)

  def finish_bundle(self):
    # finish_bundle will append incoming bundles in memory until all the bundles
    # carrying data is processed. This is done to produce only a single output
    # shard (some tests depends on this behavior). It is possible to have
    # incoming empty bundles after the output is produced, these bundles will be
    # ignored and would not generate additional output files.
    # TODO(altay): Do not wait until the last bundle to write in a single shard.
    if self._is_final_bundle:
      elements = self.global_state.get_state(
          None, _NativeWriteEvaluator.ELEMENTS_TAG)
      if self._has_already_produced_output:
        # Ignore empty bundles that arrive after the output is produced.
        assert elements == []
      else:
        self._sink.pipeline_options = self._evaluation_context.pipeline_options
        with self._sink.writer() as writer:
          for v in elements:
            writer.Write(v.value)
      hold = WatermarkManager.WATERMARK_POS_INF
    else:
      hold = WatermarkManager.WATERMARK_NEG_INF
      self.global_state.set_timer(
          None, '', TimeDomain.WATERMARK, WatermarkManager.WATERMARK_POS_INF)

    return TransformResult(self, [], [], None, {None: hold})


class _ProcessElementsEvaluator(_TransformEvaluator):
  """An evaluator for sdf_direct_runner.ProcessElements transform."""

  # Maximum number of elements that will be produced by a Splittable DoFn before
  # a checkpoint is requested by the runner.
  DEFAULT_MAX_NUM_OUTPUTS = 100
  # Maximum duration a Splittable DoFn will process an element before a
  # checkpoint is requested by the runner.
  DEFAULT_MAX_DURATION = 1

  def __init__(self, evaluation_context, applied_ptransform,
               input_committed_bundle, side_inputs):
    super(_ProcessElementsEvaluator, self).__init__(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs)

    process_elements_transform = applied_ptransform.transform
    assert isinstance(process_elements_transform, ProcessElements)

    # Replacing the do_fn of the transform with a wrapper do_fn that performs
    # SDF magic.
    transform = applied_ptransform.transform
    sdf = transform.sdf
    self._process_fn = transform.new_process_fn(sdf)
    transform.dofn = self._process_fn

    assert isinstance(self._process_fn, ProcessFn)

    self._process_fn.step_context = self._step_context

    process_element_invoker = (
        SDFProcessElementInvoker(
            max_num_outputs=self.DEFAULT_MAX_NUM_OUTPUTS,
            max_duration=self.DEFAULT_MAX_DURATION))
    self._process_fn.set_process_element_invoker(process_element_invoker)

    self._par_do_evaluator = _ParDoEvaluator(
        evaluation_context, applied_ptransform, input_committed_bundle,
        side_inputs, perform_dofn_pickle_test=False)
    self.keyed_holds = {}

  def start_bundle(self):
    self._par_do_evaluator.start_bundle()

  def process_element(self, element):
    assert isinstance(element, WindowedValue)
    assert len(element.windows) == 1
    window = element.windows[0]
    if isinstance(element.value, KeyedWorkItem):
      key = element.value.encoded_key
    else:
      # If not a `KeyedWorkItem`, this must be a tuple where key is a randomly
      # generated key and the value is a `WindowedValue` that contains an
      # `ElementAndRestriction` object.
      assert isinstance(element.value, tuple)
      key = element.value[0]

    self._par_do_evaluator.process_element(element)

    state = self._step_context.get_keyed_state(key)
    self.keyed_holds[key] = state.get_state(
        window, self._process_fn.watermark_hold_tag)

  def finish_bundle(self):
    par_do_result = self._par_do_evaluator.finish_bundle()

    transform_result = TransformResult(
        self, par_do_result.uncommitted_output_bundles,
        par_do_result.unprocessed_bundles, par_do_result.counters,
        par_do_result.keyed_watermark_holds,
        par_do_result.undeclared_tag_values)
    for key in self.keyed_holds:
      transform_result.keyed_watermark_holds[key] = self.keyed_holds[key]
    return transform_result
