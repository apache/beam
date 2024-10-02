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

"""Unit tests for bundle processing."""
# pytype: skip-file

import random
import unittest

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders.coders import FastPrimitivesCoder
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners import common
from apache_beam.runners.portability.fn_api_runner.worker_handlers import StateServicer
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import operations
from apache_beam.runners.worker.bundle_processor import BeamTransformFactory
from apache_beam.runners.worker.bundle_processor import BundleProcessor
from apache_beam.runners.worker.bundle_processor import DataInputOperation
from apache_beam.runners.worker.bundle_processor import FnApiUserStateContext
from apache_beam.runners.worker.bundle_processor import SynchronousOrderedListRuntimeState
from apache_beam.runners.worker.bundle_processor import TimerInfo
from apache_beam.runners.worker.data_plane import SizeBasedBufferingClosableOutputStream
from apache_beam.runners.worker.data_sampler import DataSampler
from apache_beam.runners.worker.sdk_worker import GlobalCachingStateHandler
from apache_beam.runners.worker.statecache import StateCache
from apache_beam.transforms import userstate
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue


class FnApiUserStateContextTest(unittest.TestCase):
  def testOutputTimerTimestamp(self):
    class Coder(object):
      """Dummy coder to capture the timer result befor encoding."""
      def encode_to_stream(self, timer, *args, **kwargs):
        self.timer = timer

    coder = Coder()

    ctx = FnApiUserStateContext(None, 'transform_id', None, None)
    ctx.add_timer_info(
        'ts-event-timer',
        TimerInfo(coder, SizeBasedBufferingClosableOutputStream()))
    ctx.add_timer_info(
        'ts-proc-timer',
        TimerInfo(coder, SizeBasedBufferingClosableOutputStream()))

    timer_spec1 = userstate.TimerSpec(
        'event-timer', userstate.TimeDomain.WATERMARK)
    timer_spec2 = userstate.TimerSpec(
        'proc-timer', userstate.TimeDomain.REAL_TIME)

    # Set event time timer
    event_timer = ctx.get_timer(timer_spec1, 'key', GlobalWindow, 23, None)
    event_timer.set(42)
    # Output timestamp should be equal to the fire timestamp
    self.assertEqual(coder.timer.hold_timestamp, 42)

    # Set processing time timer
    proc_timer = ctx.get_timer(timer_spec2, 'key', GlobalWindow, 23, None)
    proc_timer.set(42)
    # Output timestamp should be equal to the input timestamp
    self.assertEqual(coder.timer.hold_timestamp, 23)


class SplitTest(unittest.TestCase):
  def split(
      self,
      index,
      current_element_progress,
      fraction_of_remainder,
      buffer_size,
      allowed=(),
      sdf=False):
    return DataInputOperation._compute_split(
        index,
        current_element_progress,
        float('inf'),
        fraction_of_remainder,
        buffer_size,
        allowed_split_points=allowed,
        try_split=lambda frac: element_split(frac, 0)[1:3] if sdf else None)

  def sdf_split(self, *args, **kwargs):
    return self.split(*args, sdf=True, **kwargs)

  def test_simple_split(self):
    # Split as close to the beginning as possible.
    self.assertEqual(self.split(0, 0, 0, 16), simple_split(1))
    # The closest split is at 4, even when just above or below it.
    self.assertEqual(self.split(0, 0, 0.24, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.25, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.26, 16), simple_split(4))
    # Split the *remainder* in half.
    self.assertEqual(self.split(0, 0, 0.5, 16), simple_split(8))
    self.assertEqual(self.split(2, 0, 0.5, 16), simple_split(9))
    self.assertEqual(self.split(6, 0, 0.5, 16), simple_split(11))

  def test_split_with_element_progress(self):
    # Progress into the active element influences where the split of the
    # remainder falls.
    self.assertEqual(self.split(0, 0.5, 0.25, 4), simple_split(1))
    self.assertEqual(self.split(0, 0.9, 0.25, 4), simple_split(2))
    self.assertEqual(self.split(1, 0.0, 0.25, 4), simple_split(2))
    self.assertEqual(self.split(1, 0.1, 0.25, 4), simple_split(2))

  def test_split_with_element_allowed_splits(self):
    # The desired split point is at 4.
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 4, 5)), simple_split(4))
    # If we can't split at 4, choose the closest possible split point.
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 5)), simple_split(5))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 6)), simple_split(3))

    # Also test the case where all possible split points lie above or below
    # the desired split point.
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(5, 6, 7)), simple_split(5))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(1, 2, 3)), simple_split(3))

    # We have progressed beyond all possible split points, so can't split.
    self.assertEqual(self.split(5, 0, 0.25, 16, allowed=(1, 2, 3)), None)

  def test_sdf_split(self):
    # Split between future elements at element boundaries.
    self.assertEqual(self.sdf_split(0, 0, 0.51, 4), simple_split(2))
    self.assertEqual(self.sdf_split(0, 0, 0.49, 4), simple_split(2))
    self.assertEqual(self.sdf_split(0, 0, 0.26, 4), simple_split(1))
    self.assertEqual(self.sdf_split(0, 0, 0.25, 4), simple_split(1))

    # If the split falls inside the first, splittable element, split there.
    self.assertEqual(
        self.sdf_split(0, 0, 0.20, 4),
        (-1, ['Primary(0.8)'], ['Residual(0.2)'], 1))
    # The choice of split depends on the progress into the first element.
    self.assertEqual(
        self.sdf_split(0, 0, .125, 4),
        (-1, ['Primary(0.5)'], ['Residual(0.5)'], 1))
    # Here we are far enough into the first element that splitting at 0.2 of the
    # remainder falls outside the first element.
    self.assertEqual(self.sdf_split(0, .5, 0.2, 4), simple_split(1))

    # Verify the above logic when we are partially through the stream.
    self.assertEqual(self.sdf_split(2, 0, 0.6, 4), simple_split(3))
    self.assertEqual(self.sdf_split(2, 0.9, 0.6, 4), simple_split(4))
    self.assertEqual(
        self.sdf_split(2, 0.5, 0.2, 4),
        (1, ['Primary(0.6)'], ['Residual(0.4)'], 3))

  def test_sdf_split_with_allowed_splits(self):
    # This is where we would like to split, when all split points are available.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 3, 4, 5)),
        (1, ['Primary(0.6)'], ['Residual(0.4)'], 3))
    # We can't split element at index 2, because 3 is not a split point.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 4, 5)), simple_split(4))
    # We can't even split element at index 4 as above, because 4 is also not a
    # split point.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 5)), simple_split(5))
    # We can't split element at index 2, because 2 is not a split point.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 3, 4, 5)), simple_split(3))


def simple_split(first_residual_index):
  return first_residual_index - 1, [], [], first_residual_index


def element_split(frac, index):
  return (
      index - 1, ['Primary(%0.1f)' % frac], ['Residual(%0.1f)' % (1 - frac)],
      index + 1)


class TestOperation(operations.Operation):
  """Test operation that forwards its payload to consumers."""
  class Spec:
    def __init__(self, transform_proto):
      self.output_coders = [
          FastPrimitivesCoder() for _ in transform_proto.outputs
      ]

  def __init__(
      self,
      transform_proto,
      name_context,
      counter_factory,
      state_sampler,
      consumers,
      payload,
  ):
    super().__init__(
        name_context,
        self.Spec(transform_proto),
        counter_factory,
        state_sampler)
    self.payload = payload

    for _, consumer_ops in consumers.items():
      for consumer in consumer_ops:
        self.add_receiver(consumer, 0)

  def start(self):
    super().start()

    # Not using windowing logic, so just using simple defaults here.
    if self.payload:
      self.process(
          WindowedValue(self.payload, timestamp=0, windows=[GlobalWindow()]))

  def process(self, windowed_value):
    self.output(windowed_value)


@BeamTransformFactory.register_urn('beam:internal:testop:v1', bytes)
def create_test_op(factory, transform_id, transform_proto, payload, consumers):
  return TestOperation(
      transform_proto,
      common.NameContext(transform_proto.unique_name, transform_id),
      factory.counter_factory,
      factory.state_sampler,
      consumers,
      payload)


@BeamTransformFactory.register_urn('beam:internal:testexn:v1', bytes)
def create_exception_dofn(
    factory, transform_id, transform_proto, payload, consumers):
  """Returns a test DoFn that raises the given exception."""
  class RaiseException(beam.DoFn):
    def __init__(self, msg):
      self.msg = msg.decode()

    def process(self, _):
      raise RuntimeError(self.msg)

  return bundle_processor._create_simple_pardo_operation(
      factory,
      transform_id,
      transform_proto,
      consumers,
      RaiseException(payload))


class DataSamplingTest(unittest.TestCase):
  def test_disabled_by_default(self):
    """Test that not providing the sampler does not enable Data Sampling.

    Note that data sampling is enabled by providing the sampler to the
    processor.
    """
    descriptor = beam_fn_api_pb2.ProcessBundleDescriptor()
    descriptor.pcollections['a'].unique_name = 'a'
    _ = BundleProcessor(set(), descriptor, None, None)
    self.assertEqual(len(descriptor.transforms), 0)

  def test_can_sample(self):
    """Test that elements are sampled.

    This is a small integration test with the BundleProcessor and the
    DataSampler. It ensures that samples are taken from in-flight elements.
    These elements are then finally queried.
    """
    data_sampler = DataSampler(sample_every_sec=0.1)
    descriptor = beam_fn_api_pb2.ProcessBundleDescriptor()

    # Create the PCollection to sample from.
    PCOLLECTION_ID = 'pc'
    CODER_ID = 'c'
    descriptor.pcollections[PCOLLECTION_ID].unique_name = PCOLLECTION_ID
    descriptor.pcollections[PCOLLECTION_ID].coder_id = CODER_ID
    descriptor.coders[
        CODER_ID].spec.urn = common_urns.StandardCoders.Enum.BYTES.urn

    # Add a simple transform to inject an element into the data sampler. This
    # doesn't use the FnApi, so this uses a simple operation to forward its
    # payload to consumers.
    TRANSFORM_ID = 'test_transform'
    test_transform = descriptor.transforms[TRANSFORM_ID]
    test_transform.outputs['None'] = PCOLLECTION_ID
    test_transform.spec.urn = 'beam:internal:testop:v1'
    test_transform.spec.payload = b'hello, world!'

    try:
      # Create and process a fake bundle. The instruction id doesn't matter
      # here.
      processor = BundleProcessor(
          set(), descriptor, None, None, data_sampler=data_sampler)
      processor.process_bundle('instruction_id')

      samples = data_sampler.wait_for_samples([PCOLLECTION_ID])
      expected = beam_fn_api_pb2.SampleDataResponse(
          element_samples={
              PCOLLECTION_ID: beam_fn_api_pb2.SampleDataResponse.ElementList(
                  elements=[
                      beam_fn_api_pb2.SampledElement(
                          element=b'\rhello, world!')
                  ])
          })
      self.assertEqual(samples, expected)
    finally:
      data_sampler.stop()

  def test_can_sample_exceptions(self):
    """Test that exceptions are sampled."""
    data_sampler = DataSampler(sample_every_sec=0.1)
    descriptor = beam_fn_api_pb2.ProcessBundleDescriptor()

    # Boiler plate for the DoFn.
    WINDOWING_ID = 'window'
    WINDOW_CODER_ID = 'cw'
    window = descriptor.windowing_strategies[WINDOWING_ID]
    window.window_fn.urn = common_urns.global_windows.urn
    window.window_coder_id = WINDOW_CODER_ID
    window.trigger.default.SetInParent()
    window_coder = descriptor.coders[WINDOW_CODER_ID]
    window_coder.spec.urn = common_urns.StandardCoders.Enum.GLOBAL_WINDOW.urn

    # Input collection to the exception raising DoFn.
    INPUT_PCOLLECTION_ID = 'pc-in'
    INPUT_CODER_ID = 'c-in'
    descriptor.pcollections[
        INPUT_PCOLLECTION_ID].unique_name = INPUT_PCOLLECTION_ID
    descriptor.pcollections[INPUT_PCOLLECTION_ID].coder_id = INPUT_CODER_ID
    descriptor.pcollections[
        INPUT_PCOLLECTION_ID].windowing_strategy_id = WINDOWING_ID
    descriptor.coders[
        INPUT_CODER_ID].spec.urn = common_urns.StandardCoders.Enum.BYTES.urn

    # Output collection to the exception raising DoFn. Because the transform
    # "failed" to process the input element, we do NOT expect to see a sample in
    # this PCollection.
    OUTPUT_PCOLLECTION_ID = 'pc-out'
    OUTPUT_CODER_ID = 'c-out'
    descriptor.pcollections[
        OUTPUT_PCOLLECTION_ID].unique_name = OUTPUT_PCOLLECTION_ID
    descriptor.pcollections[OUTPUT_PCOLLECTION_ID].coder_id = OUTPUT_CODER_ID
    descriptor.pcollections[
        OUTPUT_PCOLLECTION_ID].windowing_strategy_id = WINDOWING_ID
    descriptor.coders[
        OUTPUT_CODER_ID].spec.urn = common_urns.StandardCoders.Enum.BYTES.urn

    # Add a simple transform to inject an element into the data sampler. This
    # doesn't use the FnApi, so this uses a simple operation to forward its
    # payload to consumers.
    TEST_OP_TRANSFORM_ID = 'test_op'
    test_transform = descriptor.transforms[TEST_OP_TRANSFORM_ID]
    test_transform.outputs['None'] = INPUT_PCOLLECTION_ID
    test_transform.spec.urn = 'beam:internal:testop:v1'
    test_transform.spec.payload = b'hello, world!'

    # Add the DoFn to create an exception to sample from.
    TEST_EXCEPTION_TRANSFORM_ID = 'test_transform'
    test_transform = descriptor.transforms[TEST_EXCEPTION_TRANSFORM_ID]
    test_transform.inputs['0'] = INPUT_PCOLLECTION_ID
    test_transform.outputs['None'] = OUTPUT_PCOLLECTION_ID
    test_transform.spec.urn = 'beam:internal:testexn:v1'
    test_transform.spec.payload = b'expected exception'

    try:
      # Create and process a fake bundle. The instruction id doesn't matter
      # here.
      processor = BundleProcessor(
          set(), descriptor, None, None, data_sampler=data_sampler)

      with self.assertRaisesRegex(RuntimeError, 'expected exception'):
        processor.process_bundle('instruction_id')

      # NOTE: The expected sample comes from the input PCollection. This is very
      # important because there can be coder issues if the sample is put in the
      # wrong PCollection.
      samples = data_sampler.wait_for_samples([INPUT_PCOLLECTION_ID])
      self.assertEqual(len(samples.element_samples), 1)

      element = samples.element_samples[INPUT_PCOLLECTION_ID].elements[0]
      self.assertEqual(element.element, b'\rhello, world!')
      self.assertTrue(element.HasField('exception'))

      exception = element.exception
      self.assertEqual(exception.instruction_id, 'instruction_id')
      self.assertEqual(exception.transform_id, TEST_EXCEPTION_TRANSFORM_ID)
      self.assertRegex(
          exception.error, 'Traceback(\n|.)*RuntimeError: expected exception')

    finally:
      data_sampler.stop()


class EnvironmentCompatibilityTest(unittest.TestCase):
  def test_rc_environments_are_compatible_with_released_images(self):
    # TODO(https://github.com/apache/beam/issues/28084): remove when
    # resolved.
    self.assertTrue(
        bundle_processor._environments_compatible(
            "beam:version:sdk_base:apache/beam_python3.5_sdk:2.1.0rc1",
            "beam:version:sdk_base:apache/beam_python3.5_sdk:2.1.0"))

  def test_user_modified_sdks_need_to_be_installed_in_runtime_env(self):
    self.assertFalse(
        bundle_processor._environments_compatible(
            "beam:version:sdk_base:apache/beam_python3.5_sdk:2.1.0-custom",
            "beam:version:sdk_base:apache/beam_python3.5_sdk:2.1.0"))
    self.assertTrue(
        bundle_processor._environments_compatible(
            "beam:version:sdk_base:apache/beam_python3.5_sdk:2.1.0-custom",
            "beam:version:sdk_base:apache/beam_python3.5_sdk:2.1.0-custom"))


class OrderedListStateTest(unittest.TestCase):
  class NoStateCache(StateCache):
    def __init__(self):
      super().__init__(max_weight=0)

  @staticmethod
  def _create_state(window=b"my_window", key=b"my_key", coder=StrUtf8Coder()):
    state_handler = GlobalCachingStateHandler(
        OrderedListStateTest.NoStateCache(), StateServicer())
    state_key = beam_fn_api_pb2.StateKey(
        ordered_list_user_state=beam_fn_api_pb2.StateKey.OrderedListUserState(
            window=window, key=key))
    return SynchronousOrderedListRuntimeState(state_handler, state_key, coder)

  def setUp(self):
    self.state = self._create_state()

  def test_read_range(self):
    A1, B1, A4 = [(1, "a1"), (1, "b1"), (4, "a4")]
    self.assertEqual([], list(self.state.read_range(0, 5)))

    self.state.add(A1)
    self.assertEqual([A1], list(self.state.read_range(0, 5)))

    self.state.add(B1)
    self.assertEqual([A1, B1], list(self.state.read_range(0, 5)))

    self.state.add(A4)
    self.assertEqual([A1, B1, A4], list(self.state.read_range(0, 5)))

    self.assertEqual([], list(self.state.read_range(0, 1)))
    self.assertEqual([], list(self.state.read_range(5, 10)))
    self.assertEqual([A1, B1], list(self.state.read_range(1, 2)))
    self.assertEqual([], list(self.state.read_range(2, 3)))
    self.assertEqual([], list(self.state.read_range(2, 4)))
    self.assertEqual([A4], list(self.state.read_range(4, 5)))

  def test_read(self):
    A1, B1, A4 = [(1, "a1"), (1, "b1"), (4, "a4")]
    self.assertEqual([], list(self.state.read()))

    self.state.add(A1)
    self.assertEqual([A1], list(self.state.read()))

    self.state.add(A1)
    self.assertEqual([A1, A1], list(self.state.read()))

    self.state.add(B1)
    self.assertEqual([A1, A1, B1], list(self.state.read()))

    self.state.add(A4)
    self.assertEqual([A1, A1, B1, A4], list(self.state.read()))

  def test_clear_range(self):
    A1, B1, A4, A5 = [(1, "a1"), (1, "b1"), (4, "a4"), (5, "a5")]
    self.state.clear_range(0, 1)
    self.assertEqual([], list(self.state.read()))

    self.state.add(A1)
    self.state.add(B1)
    self.state.add(A4)
    self.state.add(A5)
    self.assertEqual([A1, B1, A4, A5], list(self.state.read()))

    self.state.clear_range(0, 1)
    self.assertEqual([A1, B1, A4, A5], list(self.state.read()))

    self.state.clear_range(1, 2)
    self.assertEqual([A4, A5], list(self.state.read()))

    # no side effect on clearing the same range twice
    self.state.clear_range(1, 2)
    self.assertEqual([A4, A5], list(self.state.read()))

    self.state.clear_range(3, 4)
    self.assertEqual([A4, A5], list(self.state.read()))

    self.state.clear_range(3, 5)
    self.assertEqual([A5], list(self.state.read()))

  def test_add_and_clear_range_after_commit(self):
    A1, B1, C1, A4, A5, A6 = [(1, "a1"), (1, "b1"), (1, "c1"),
                              (4, "a4"), (5, "a5"), (6, "a6")]
    self.state.add(A1)
    self.state.add(B1)
    self.state.add(A4)
    self.state.add(A5)
    self.state.clear_range(4, 5)
    self.assertEqual([A1, B1, A5], list(self.state.read()))

    self.state.commit()
    self.assertEqual(len(self.state._pending_adds), 0)
    self.assertEqual(len(self.state._pending_removes), 0)
    self.assertEqual([A1, B1, A5], list(self.state.read()))

    self.state.add(C1)
    self.state.add(A6)
    self.assertEqual([A1, B1, C1, A5, A6], list(self.state.read()))

    self.state.clear_range(5, 6)
    self.assertEqual([A1, B1, C1, A6], list(self.state.read()))

    self.state.commit()
    self.assertEqual(len(self.state._pending_adds), 0)
    self.assertEqual(len(self.state._pending_removes), 0)
    self.assertEqual([A1, B1, C1, A6], list(self.state.read()))

  def test_clear(self):
    A1, B1, C1, A4, A5, B5 = [(1, "a1"), (1, "b1"), (1, "c1"),
                              (4, "a4"), (5, "a5"), (5, "b5")]
    self.state.add(A1)
    self.state.add(B1)
    self.state.add(A4)
    self.state.add(A5)
    self.state.clear_range(4, 5)
    self.assertEqual([A1, B1, A5], list(self.state.read()))
    self.state.commit()

    self.state.add(C1)
    self.state.clear_range(5, 10)
    self.assertEqual([A1, B1, C1], list(self.state.read()))
    self.state.clear()
    self.assertEqual(len(self.state._pending_adds), 0)
    self.assertEqual(len(self.state._pending_removes), 1)

    self.state.add(B5)
    self.assertEqual([B5], list(self.state.read()))
    self.state.commit()

    self.assertEqual(len(self.state._pending_adds), 0)
    self.assertEqual(len(self.state._pending_removes), 0)

    self.assertEqual([B5], list(self.state.read()))

  def test_multiple_iterators(self):
    A1, B1, A3, B3 = [(1, "a1"), (1, "b1"), (3, "a3"), (3, "b3")]
    self.state.add(A1)
    self.state.add(A3)
    self.state.commit()

    iter_before_b1 = iter(self.state.read())
    self.assertEqual(A1, next(iter_before_b1))

    self.state.add(B1)
    self.assertEqual(A3, next(iter_before_b1))
    self.assertRaises(StopIteration, lambda: next(iter_before_b1))

    self.state.add(B3)
    iter_before_clear_range = iter(self.state.read())
    self.assertEqual(A1, next(iter_before_clear_range))
    self.state.clear_range(3, 10)
    self.assertEqual(B1, next(iter_before_clear_range))
    self.assertEqual(A3, next(iter_before_clear_range))
    self.assertEqual(B3, next(iter_before_clear_range))
    self.assertRaises(StopIteration, lambda: next(iter_before_clear_range))
    self.assertEqual([A1, B1], list(self.state.read()))

    iter_before_clear = iter(self.state.read())
    self.assertEqual(A1, next(iter_before_clear))
    self.state.clear()
    self.assertEqual(B1, next(iter_before_clear))
    self.assertRaises(StopIteration, lambda: next(iter_before_clear))

    self.assertEqual([], list(self.state.read()))

  def fuzz_test_helper(self, seed=0, lower=0, upper=20):
    class NaiveState:
      def __init__(self):
        self._data = [[] for i in range((upper - lower + 1))]
        self._logs = []

      def add(self, elem):
        k, v = elem
        self._data[k - lower].append(v)
        self._logs.append("add(%d, %s)" % (k, v))

      def clear_range(self, lo, hi):
        for i in range(lo, hi):
          self._data[i - lower] = []
        self._logs.append("clear_range(%d, %d)" % (lo, hi))

      def clear(self):
        for i in range(len(self._data)):
          self._data[i] = []
        self._logs.append("clear()")

      def read(self):
        self._logs.append("read()")
        for i in range(len(self._data)):
          for v in self._data[i]:
            yield (i + lower, v)

    random.seed(seed)

    state = self._create_state()
    bench_state = NaiveState()

    steps = random.randint(20, 50)
    for i in range(steps):
      op = random.randint(1, 100)
      if 1 <= op < 70:
        num = random.randint(lower, upper)
        state.add((num, "a%d" % num))
        bench_state.add((num, "a%d" % num))
      elif 70 <= op < 95:
        num1 = random.randint(lower, upper)
        num2 = random.randint(lower, upper)
        state.clear_range(min(num1, num2), max(num1, num2))
        bench_state.clear_range(min(num1, num2), max(num1, num2))
      elif op >= 95:
        state.clear()
        bench_state.clear()

      op = random.randint(1, 10)
      if 1 <= op <= 9:
        pass
      else:
        state.commit()

      a = list(bench_state.read())
      b = list(state.read())
      self.assertEqual(
          a,
          b,
          "Mismatch occurred on seed=%d, step=%d, logs=%s" %
          (seed, i, ';'.join(bench_state._logs)))

  def test_fuzz(self):
    for _ in range(1000):
      seed = random.randint(0, 0xffffffffffffffff)
      try:
        self.fuzz_test_helper(seed=seed)
      except Exception as e:
        raise RuntimeError("Exception occurred on seed=%d: %s" % (seed, e))

  def test_min_max(self):
    INT64_MIN, INT64_MAX_MINUS_ONE, INT64_MAX = [(-(1 << 63), "min"),
                                                 ((1 << 63) - 2, "max"),
                                                 ((1 << 63) - 1, "err")]
    self.state.add(INT64_MIN)
    self.state.add(INT64_MAX_MINUS_ONE)
    self.assertRaises(ValueError, lambda: self.state.add(INT64_MAX))

    self.assertEqual([INT64_MIN, INT64_MAX_MINUS_ONE], list(self.state.read()))
    self.assertEqual([INT64_MIN], list(self.state.read_range(-(1 << 63), 0)))
    self.assertEqual([INT64_MAX_MINUS_ONE],
                     list(self.state.read_range(0, (1 << 63) - 1)))

  def test_continuation_token(self):
    A1, A2, A7, B7, A8 = [(1, "a1"), (2, "a2"), (7, "a7"), (7, "b7"), (8, "a8")]
    self.state._state_handler._underlying._use_continuation_tokens = True
    self.assertEqual([], list(self.state.read_range(1, 8)))

    self.state.add(A1)
    self.state.add(A2)
    self.state.add(A7)
    self.state.add(B7)
    self.state.add(A8)

    self.assertEqual([A2, A7, B7], list(self.state.read_range(2, 8)))

    self.state.commit()
    self.assertEqual([A2, A7, B7], list(self.state.read_range(2, 8)))

    self.assertEqual([A1, A2, A7, B7, A8], list(self.state.read()))


if __name__ == '__main__':
  unittest.main()
