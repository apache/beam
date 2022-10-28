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

"""Tests for apache_beam.runners.worker.sdk_worker."""

# pytype: skip-file

import contextlib
import logging
import unittest
from collections import namedtuple

import grpc
import hamcrest as hc
import mock

from apache_beam.coders import VarIntCoder
from apache_beam.internal.metrics.metric import Metrics as InternalMetrics
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import metrics_pb2
from apache_beam.runners.worker import sdk_worker
from apache_beam.runners.worker import statecache
from apache_beam.runners.worker.sdk_worker import BundleProcessorCache
from apache_beam.runners.worker.sdk_worker import GlobalCachingStateHandler
from apache_beam.runners.worker.sdk_worker import SdkWorker
from apache_beam.utils import thread_pool_executor

_LOGGER = logging.getLogger(__name__)


class BeamFnControlServicer(beam_fn_api_pb2_grpc.BeamFnControlServicer):
  def __init__(self, requests, raise_errors=True):
    self.requests = requests
    self.instruction_ids = set(r.instruction_id for r in requests)
    self.responses = {}
    self.raise_errors = raise_errors

  def Control(self, response_iterator, context):
    for request in self.requests:
      _LOGGER.info("Sending request %s", request)
      yield request
    for response in response_iterator:
      _LOGGER.info("Got response %s", response)
      if response.instruction_id != -1:
        assert response.instruction_id in self.instruction_ids
        assert response.instruction_id not in self.responses
        self.responses[response.instruction_id] = response
        if self.raise_errors and response.error:
          raise RuntimeError(response.error)
        elif len(self.responses) == len(self.requests):
          _LOGGER.info("All %s instructions finished.", len(self.requests))
          return
    raise RuntimeError(
        "Missing responses: %s" %
        (self.instruction_ids - set(self.responses.keys())))


class SdkWorkerTest(unittest.TestCase):
  def _get_process_bundles(self, prefix, size):
    return [
        beam_fn_api_pb2.ProcessBundleDescriptor(
            id=str(str(prefix) + "-" + str(ix)),
            transforms={
                str(ix): beam_runner_api_pb2.PTransform(unique_name=str(ix))
            }) for ix in range(size)
    ]

  def _check_fn_registration_multi_request(self, *args):
    """Check the function registration calls to the sdk_harness.

    Args:
     tuple of request_count, number of process_bundles per request and workers
     counts to process the request.
    """
    for (request_count, process_bundles_per_request) in args:
      requests = []
      process_bundle_descriptors = []

      for i in range(request_count):
        pbd = self._get_process_bundles(i, process_bundles_per_request)
        process_bundle_descriptors.extend(pbd)
        requests.append(
            beam_fn_api_pb2.InstructionRequest(
                instruction_id=str(i),
                register=beam_fn_api_pb2.RegisterRequest(
                    process_bundle_descriptor=process_bundle_descriptors)))

      test_controller = BeamFnControlServicer(requests)

      server = grpc.server(thread_pool_executor.shared_unbounded_instance())
      beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
          test_controller, server)
      test_port = server.add_insecure_port("[::]:0")
      server.start()

      harness = sdk_worker.SdkHarness(
          "localhost:%s" % test_port, state_cache_size=100)
      harness.run()

      self.assertEqual(
          harness._bundle_processor_cache.fns,
          {item.id: item
           for item in process_bundle_descriptors})

  def test_fn_registration(self):
    self._check_fn_registration_multi_request((1, 4), (4, 4))

  def test_inactive_bundle_processor_returns_empty_progress_response(self):
    bundle_processor = mock.MagicMock()
    bundle_processor_cache = BundleProcessorCache(None, None, {})
    bundle_processor_cache.activate('instruction_id')
    worker = SdkWorker(bundle_processor_cache)
    split_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id='progress_instruction_id',
        process_bundle_progress=beam_fn_api_pb2.ProcessBundleProgressRequest(
            instruction_id='instruction_id'))
    self.assertEqual(
        worker.do_instruction(split_request),
        beam_fn_api_pb2.InstructionResponse(
            instruction_id='progress_instruction_id',
            process_bundle_progress=beam_fn_api_pb2.
            ProcessBundleProgressResponse()))

    # Add a mock bundle processor as if it was running before it's released
    bundle_processor_cache.active_bundle_processors['instruction_id'] = (
        'descriptor_id', bundle_processor)
    bundle_processor_cache.release('instruction_id')
    self.assertEqual(
        worker.do_instruction(split_request),
        beam_fn_api_pb2.InstructionResponse(
            instruction_id='progress_instruction_id',
            process_bundle_progress=beam_fn_api_pb2.
            ProcessBundleProgressResponse()))

  def test_failed_bundle_processor_returns_failed_progress_response(self):
    bundle_processor = mock.MagicMock()
    bundle_processor_cache = BundleProcessorCache(None, None, {})
    bundle_processor_cache.activate('instruction_id')
    worker = SdkWorker(bundle_processor_cache)

    # Add a mock bundle processor as if it was running before it's discarded
    bundle_processor_cache.active_bundle_processors['instruction_id'] = (
        'descriptor_id', bundle_processor)
    bundle_processor_cache.discard('instruction_id')
    split_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id='progress_instruction_id',
        process_bundle_progress=beam_fn_api_pb2.ProcessBundleProgressRequest(
            instruction_id='instruction_id'))
    hc.assert_that(
        worker.do_instruction(split_request).error,
        hc.contains_string(
            'Bundle processing associated with instruction_id has failed'))

  def test_inactive_bundle_processor_returns_empty_split_response(self):
    bundle_processor = mock.MagicMock()
    bundle_processor_cache = BundleProcessorCache(None, None, {})
    bundle_processor_cache.activate('instruction_id')
    worker = SdkWorker(bundle_processor_cache)
    split_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id='split_instruction_id',
        process_bundle_split=beam_fn_api_pb2.ProcessBundleSplitRequest(
            instruction_id='instruction_id'))
    self.assertEqual(
        worker.do_instruction(split_request),
        beam_fn_api_pb2.InstructionResponse(
            instruction_id='split_instruction_id',
            process_bundle_split=beam_fn_api_pb2.ProcessBundleSplitResponse()))

    # Add a mock bundle processor as if it was running before it's released
    bundle_processor_cache.active_bundle_processors['instruction_id'] = (
        'descriptor_id', bundle_processor)
    bundle_processor_cache.release('instruction_id')
    self.assertEqual(
        worker.do_instruction(split_request),
        beam_fn_api_pb2.InstructionResponse(
            instruction_id='split_instruction_id',
            process_bundle_split=beam_fn_api_pb2.ProcessBundleSplitResponse()))

  def get_responses(self, instruction_requests):
    """Evaluates and returns {id: InstructionResponse} for the requests."""
    test_controller = BeamFnControlServicer(instruction_requests)

    server = grpc.server(thread_pool_executor.shared_unbounded_instance())
    beam_fn_api_pb2_grpc.add_BeamFnControlServicer_to_server(
        test_controller, server)
    test_port = server.add_insecure_port("[::]:0")
    server.start()

    harness = sdk_worker.SdkHarness(
        "localhost:%s" % test_port, state_cache_size=100)
    harness.run()
    return test_controller.responses

  def test_harness_monitoring_infos_and_metadata(self):
    # Clear the process wide metric container.
    MetricsEnvironment.process_wide_container().metrics = {}
    # Create a process_wide metric.
    urn = 'my.custom.urn'
    labels = {'key': 'value'}
    InternalMetrics.counter(urn=urn, labels=labels, process_wide=True).inc(10)

    harness_monitoring_infos_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id="monitoring_infos",
        harness_monitoring_infos=beam_fn_api_pb2.HarnessMonitoringInfosRequest(
        ))

    responses = self.get_responses([harness_monitoring_infos_request])

    expected_monitoring_info = monitoring_infos.int64_counter(
        urn, 10, labels=labels)
    monitoring_data = (
        responses['monitoring_infos'].harness_monitoring_infos.monitoring_data)

    # Request the full MonitoringInfo metadata for the returned short_ids.
    short_ids = list(monitoring_data.keys())
    monitoring_infos_metadata_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id="monitoring_infos_metadata",
        monitoring_infos=beam_fn_api_pb2.MonitoringInfosMetadataRequest(
            monitoring_info_id=short_ids))

    responses = self.get_responses([monitoring_infos_metadata_request])

    # Request the full MonitoringInfo metadata to be returned now.
    expected_monitoring_info.ClearField("payload")

    # Verify that one of the returned monitoring infos is our expected
    # monitoring info.
    short_id_to_mi = (
        responses['monitoring_infos_metadata'].monitoring_infos.monitoring_info)
    found = False
    for mi in short_id_to_mi.values():
      # Clear the timestamp before comparing
      mi.ClearField("start_time")
      if mi == expected_monitoring_info:
        found = True
    self.assertTrue(found, str(responses['monitoring_infos_metadata']))

  def test_failed_bundle_processor_returns_failed_split_response(self):
    bundle_processor = mock.MagicMock()
    bundle_processor_cache = BundleProcessorCache(None, None, {})
    bundle_processor_cache.activate('instruction_id')
    worker = SdkWorker(bundle_processor_cache)

    # Add a mock bundle processor as if it was running before it's discarded
    bundle_processor_cache.active_bundle_processors['instruction_id'] = (
        'descriptor_id', bundle_processor)
    bundle_processor_cache.discard('instruction_id')
    split_request = beam_fn_api_pb2.InstructionRequest(
        instruction_id='split_instruction_id',
        process_bundle_split=beam_fn_api_pb2.ProcessBundleSplitRequest(
            instruction_id='instruction_id'))
    hc.assert_that(
        worker.do_instruction(split_request).error,
        hc.contains_string(
            'Bundle processing associated with instruction_id has failed'))


class CachingStateHandlerTest(unittest.TestCase):
  def test_caching(self):

    coder = VarIntCoder()
    coder_impl = coder.get_impl()

    class FakeUnderlyingState(object):
      """Simply returns an incremented counter as the state "value."
      """
      def set_counter(self, n):
        self._counter = n

      def get_raw(self, *args):
        self._counter += 1
        return coder.encode(self._counter), None

      @contextlib.contextmanager
      def process_instruction_id(self, bundle_id):
        yield

    underlying_state = FakeUnderlyingState()
    state_cache = statecache.StateCache(100 << 20)
    caching_state_hander = GlobalCachingStateHandler(
        state_cache, underlying_state)

    state1 = beam_fn_api_pb2.StateKey(
        bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
            user_state_id='state1'))
    state2 = beam_fn_api_pb2.StateKey(
        bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
            user_state_id='state2'))
    side1 = beam_fn_api_pb2.StateKey(
        multimap_side_input=beam_fn_api_pb2.StateKey.MultimapSideInput(
            transform_id='transform', side_input_id='side1'))
    side2 = beam_fn_api_pb2.StateKey(
        iterable_side_input=beam_fn_api_pb2.StateKey.IterableSideInput(
            transform_id='transform', side_input_id='side2'))

    state_token1 = beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
        token=b'state_token1',
        user_state=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.UserState())
    state_token2 = beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
        token=b'state_token2',
        user_state=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.UserState())
    side1_token1 = beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
        token=b'side1_token1',
        side_input=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.SideInput(
            transform_id='transform', side_input_id='side1'))
    side1_token2 = beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
        token=b'side1_token2',
        side_input=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.SideInput(
            transform_id='transform', side_input_id='side1'))

    def get_as_list(key):
      return list(caching_state_hander.blocking_get(key, coder_impl))

    underlying_state.set_counter(100)
    with caching_state_hander.process_instruction_id('bundle1', []):
      self.assertEqual(get_as_list(state1), [101])  # uncached
      self.assertEqual(get_as_list(state2), [102])  # uncached
      self.assertEqual(get_as_list(state1), [101])  # cached on bundle
      self.assertEqual(get_as_list(side1), [103])  # uncached
      self.assertEqual(get_as_list(side2), [104])  # uncached

    underlying_state.set_counter(200)
    with caching_state_hander.process_instruction_id(
        'bundle2', [state_token1, side1_token1]):
      self.assertEqual(get_as_list(state1), [201])  # uncached
      self.assertEqual(get_as_list(state2), [202])  # uncached
      self.assertEqual(get_as_list(state1), [201])  # cached on state token1
      self.assertEqual(get_as_list(side1), [203])  # uncached
      self.assertEqual(get_as_list(side1), [203])  # cached on side1_token1
      self.assertEqual(get_as_list(side2), [204])  # uncached
      self.assertEqual(get_as_list(side2), [204])  # cached on bundle

    underlying_state.set_counter(300)
    with caching_state_hander.process_instruction_id(
        'bundle3', [state_token1, side1_token1]):
      self.assertEqual(get_as_list(state1), [201])  # cached on state token1
      self.assertEqual(get_as_list(state2), [202])  # cached on state token1
      self.assertEqual(get_as_list(state1), [201])  # cached on state token1
      self.assertEqual(get_as_list(side1), [203])  # cached on side1_token1
      self.assertEqual(get_as_list(side1), [203])  # cached on side1_token1
      self.assertEqual(get_as_list(side2), [301])  # uncached
      self.assertEqual(get_as_list(side2), [301])  # cached on bundle

    underlying_state.set_counter(400)
    with caching_state_hander.process_instruction_id(
        'bundle4', [state_token2, side1_token1]):
      self.assertEqual(get_as_list(state1), [401])  # uncached
      self.assertEqual(get_as_list(state2), [402])  # uncached
      self.assertEqual(get_as_list(state1), [401])  # cached on state token2
      self.assertEqual(get_as_list(side1), [203])  # cached on side1_token1
      self.assertEqual(get_as_list(side1), [203])  # cached on side1_token1
      self.assertEqual(get_as_list(side2), [403])  # uncached
      self.assertEqual(get_as_list(side2), [403])  # cached on bundle

    underlying_state.set_counter(500)
    with caching_state_hander.process_instruction_id(
        'bundle5', [state_token2, side1_token2]):
      self.assertEqual(get_as_list(state1), [401])  # cached on state token2
      self.assertEqual(get_as_list(state2), [402])  # cached on state token2
      self.assertEqual(get_as_list(state1), [401])  # cached on state token2
      self.assertEqual(get_as_list(side1), [501])  # uncached
      self.assertEqual(get_as_list(side1), [501])  # cached on side1_token2
      self.assertEqual(get_as_list(side2), [502])  # uncached
      self.assertEqual(get_as_list(side2), [502])  # cached on bundle

  class UnderlyingStateHandler(object):
    """Simply returns an incremented counter as the state "value."
    """
    def __init__(self):
      self._encoded_values = []
      self._continuations = False

    def set_value(self, value, coder):
      self._encoded_values = [coder.encode(value)]

    def set_values(self, values, coder):
      self._encoded_values = [coder.encode(value) for value in values]

    def set_continuations(self, continuations):
      self._continuations = continuations

    def get_raw(self, _state_key, continuation_token=None):
      if self._continuations and len(self._encoded_values) > 0:
        if not continuation_token:
          continuation_token = '0'
        idx = int(continuation_token)
        next_token = str(idx +
                         1) if idx + 1 < len(self._encoded_values) else None
        return self._encoded_values[idx], next_token
      else:
        return b''.join(self._encoded_values), None

    def append_raw(self, _key, bytes):
      self._encoded_values.append(bytes)

    def clear(self, *args):
      self._encoded_values = []

    @contextlib.contextmanager
    def process_instruction_id(self, bundle_id):
      yield

  def test_append_clear_with_preexisting_state(self):
    state = beam_fn_api_pb2.StateKey(
        bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
            user_state_id='state1'))

    cache_token = beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
        token=b'state_token1',
        user_state=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.UserState())

    coder = VarIntCoder()

    underlying_state_handler = self.UnderlyingStateHandler()
    state_cache = statecache.StateCache(100 << 20)
    handler = GlobalCachingStateHandler(state_cache, underlying_state_handler)

    def get():
      return handler.blocking_get(state, coder.get_impl())

    def append(iterable):
      handler.extend(state, coder.get_impl(), iterable)

    def clear():
      handler.clear(state)

    # Initialize state
    underlying_state_handler.set_value(42, coder)
    with handler.process_instruction_id('bundle', [cache_token]):
      # Append without reading beforehand
      append([43])
      self.assertEqual(get(), [42, 43])
      clear()
      self.assertEqual(get(), [])
      append([44, 45])
      self.assertEqual(get(), [44, 45])
      append((46, 47))
      self.assertEqual(get(), [44, 45, 46, 47])
      clear()
      append(range(1000))
      self.assertEqual(get(), list(range(1000)))

  def test_continuation_token(self):
    underlying_state_handler = self.UnderlyingStateHandler()
    state_cache = statecache.StateCache(100 << 20)
    handler = GlobalCachingStateHandler(state_cache, underlying_state_handler)

    coder = VarIntCoder()

    state = beam_fn_api_pb2.StateKey(
        bag_user_state=beam_fn_api_pb2.StateKey.BagUserState(
            user_state_id='state1'))

    cache_token = beam_fn_api_pb2.ProcessBundleRequest.CacheToken(
        token=b'state_token1',
        user_state=beam_fn_api_pb2.ProcessBundleRequest.CacheToken.UserState())

    def get(materialize=True):
      result = handler.blocking_get(state, coder.get_impl())
      return list(result) if materialize else result

    def get_type():
      return type(get(materialize=False))

    def append(*values):
      handler.extend(state, coder.get_impl(), values)

    def clear():
      handler.clear(state)

    underlying_state_handler.set_continuations(True)
    underlying_state_handler.set_values([45, 46, 47], coder)
    with handler.process_instruction_id('bundle', [cache_token]):
      self.assertEqual(
          get_type(), GlobalCachingStateHandler.ContinuationIterable)
      self.assertEqual(get(), [45, 46, 47])
      append(48, 49)
      self.assertEqual(
          get_type(), GlobalCachingStateHandler.ContinuationIterable)
      self.assertEqual(get(), [45, 46, 47, 48, 49])
      clear()
      self.assertEqual(get_type(), list)
      self.assertEqual(get(), [])
      append(1)
      self.assertEqual(get(), [1])
      append(2, 3)
      self.assertEqual(get(), [1, 2, 3])
      clear()
      for i in range(1000):
        append(i)
      self.assertEqual(get_type(), list)
      self.assertEqual(get(), [i for i in range(1000)])


class ShortIdCacheTest(unittest.TestCase):
  def testShortIdAssignment(self):
    TestCase = namedtuple('TestCase', ['expected_short_id', 'info'])
    test_cases = [
        TestCase(*args) for args in [
            (
                "1",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:user:distribution_int64:v1",
                    type="beam:metrics:distribution_int64:v1")),
            (
                "2",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:element_count:v1",
                    type="beam:metrics:sum_int64:v1")),
            (
                "3",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:ptransform_progress:completed:v1",
                    type="beam:metrics:progress:v1")),
            (
                "4",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:user:distribution_double:v1",
                    type="beam:metrics:distribution_double:v1")),
            (
                "5",
                metrics_pb2.MonitoringInfo(
                    urn="TestingSentinelUrn", type="TestingSentinelType")),
            (
                "6",
                metrics_pb2.MonitoringInfo(
                    urn=
                    "beam:metric:pardo_execution_time:finish_bundle_msecs:v1",
                    type="beam:metrics:sum_int64:v1")),
            # This case and the next one validates that different labels
            # with the same urn are in fact assigned different short ids.
            (
                "7",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:user:sum_int64:v1",
                    type="beam:metrics:sum_int64:v1",
                    labels={
                        "PTRANSFORM": "myT",
                        "NAMESPACE": "harness",
                        "NAME": "metricNumber7"
                    })),
            (
                "8",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:user:sum_int64:v1",
                    type="beam:metrics:sum_int64:v1",
                    labels={
                        "PTRANSFORM": "myT",
                        "NAMESPACE": "harness",
                        "NAME": "metricNumber8"
                    })),
            (
                "9",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:user:top_n_double:v1",
                    type="beam:metrics:top_n_double:v1",
                    labels={
                        "PTRANSFORM": "myT",
                        "NAMESPACE": "harness",
                        "NAME": "metricNumber7"
                    })),
            (
                "a",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:element_count:v1",
                    type="beam:metrics:sum_int64:v1",
                    labels={"PCOLLECTION": "myPCol"})),
            # validate payload is ignored for shortId assignment
            (
                "3",
                metrics_pb2.MonitoringInfo(
                    urn="beam:metric:ptransform_progress:completed:v1",
                    type="beam:metrics:progress:v1",
                    payload=b"this is ignored!"))
        ]
    ]

    cache = sdk_worker.ShortIdCache()

    for case in test_cases:
      self.assertEqual(
          case.expected_short_id,
          cache.get_short_id(case.info),
          "Got incorrect short id for monitoring info:\n%s" % case.info)

    # Retrieve all of the monitoring infos by short id, and verify that the
    # metadata (everything but the payload) matches the originals
    actual_recovered_infos = cache.get_infos(
        case.expected_short_id for case in test_cases).values()
    for recoveredInfo, case in zip(actual_recovered_infos, test_cases):
      self.assertEqual(
          monitoringInfoMetadata(case.info),
          monitoringInfoMetadata(recoveredInfo))

    # Retrieve short ids one more time in reverse
    for case in reversed(test_cases):
      self.assertEqual(
          case.expected_short_id,
          cache.get_short_id(case.info),
          "Got incorrect short id on second retrieval for monitoring info:\n%s"
          % case.info)


def monitoringInfoMetadata(info):
  return {
      descriptor.name: value
      for descriptor,
      value in info.ListFields() if not descriptor.name == "payload"
  }


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
