# -*- coding: utf-8 -*-
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
from __future__ import absolute_import

import gc
import logging
import sys
import time
import unittest
from queue import Queue

import mock

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.runners.interactive.caching import file_based_cache
from apache_beam.runners.interactive.caching import file_based_cache_test
from apache_beam.runners.interactive.caching import stream_based_cache
from apache_beam.testing import datatype_inference
from apache_beam.testing import test_utils
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin

# Protect against environments where the PubSub library is not available.
try:
  from google.cloud import pubsub  # pylint: disable=ungrouped-imports
  from google.cloud import pubsub_v1  # pylint: disable=ungrouped-imports
  from google.api_core import exceptions as gexc
except ImportError:
  pubsub = None
  pubsub_v1 = None
  gexc = None


class FakePublisherClient(object):
  """A class to mock basic Google Cloud PubSub CRUD operations."""

  def __init__(self, topics, subscriptions, snapshots):
    self._topics = topics
    self._subscriptions = subscriptions
    self._snapshots = snapshots

  def __setattr__(self, item, value):
    # These attributes should be shared with the parent class.
    protected_attributes = ["_topics", "_subscriptions", "_snapshots"]
    if item in protected_attributes and hasattr(self, item):
      raise ValueError("Attributes {} should not be overwritten.".format(
          protected_attributes))
    super(FakePublisherClient, self).__setattr__(item, value)

  @staticmethod
  def topic_path(*args, **kwargs):
    return pubsub.PublisherClient.topic_path(*args, **kwargs)

  def create_topic(self, name, *args, **kwargs):
    if name in self._topics:
      raise gexc.AlreadyExists("Topic '{}' already exists.".format(name))
    self._topics.append(name)
    topic_proto = pubsub.types.Topic()
    topic_proto.name = name
    return topic_proto

  def delete_topic(self, topic, *args, **kwargs):
    if topic not in self._topics:
      raise gexc.NotFound("Topic '{}' was not found.".format(topic))
    self._topics.remove(topic)

  def get_topic(self, topic, *args, **kwargs):
    if topic not in self._topics:
      raise gexc.NotFound("Topic '{}' was not found.".format(topic))
    topic_proto = pubsub.types.Topic()
    topic_proto.name = topic
    return topic_proto

  def list_topics(self, project, *args, **kwargs):
    topic_protos = []
    for topic in self._topics:
      topic_project = "/".join(topic.split("/")[:2])
      if topic_project == project:
        topic_proto = pubsub.types.Topic()
        topic_proto.name = topic
        topic_protos.append(topic_proto)
    return topic_protos

  def list_topic_subscriptions(self, topic, *args, **kwargs):
    topic_project = topic.split("/")[1]
    subscriptions = []
    for sub_name, _ in self._subscriptions:
      sub_project = sub_name.split("/")[1]
      if sub_project == topic_project:
        subscriptions.append(sub_name)
    return subscriptions

  def topic_path(self, project, topic_name):
    return "projects/{}/topics/{}".format(project, topic_name)

  def publish(self, *args, **kwargs):

    class Future(object):

      def __init__(self, result):
        self._result = result

      def result(self, *args, **kwargs):
        return self._result

    return Future(None)


class FakeSubscriberClient(object):

  def __init__(self, topics, subscriptions, snapshots):
    self._topics = topics
    self._subscriptions = subscriptions
    self._snapshots = snapshots

  def __setattr__(self, item, value):
    # These attributes should be shared with the parent class.
    protected_attributes = ["_topics", "_subscriptions", "_snapshots"]
    if item in protected_attributes and hasattr(self, item):
      raise ValueError("Attributes {} should not be overwritten.".format(
          protected_attributes))
    super(FakeSubscriberClient, self).__setattr__(item, value)

  @staticmethod
  def subscription_path(*args, **kwargs):
    return pubsub.SubscriberClient.subscription_path(*args, **kwargs)

  @staticmethod
  def snapshot_path(*args, **kwargs):
    return pubsub.SubscriberClient.snapshot_path(*args, **kwargs)

  def create_subscription(self, name, topic, *args, **kwargs):
    if name in [s for s, _ in self._subscriptions]:
      raise gexc.AlreadyExists("Subscription '{}' already exists.".format(name))
    self._subscriptions.append((name, topic))
    sub_proto = pubsub.types.Subscription()
    sub_proto.name = name
    sub_proto.topic = topic
    return sub_proto

  def delete_subscription(self, subscription, *args, **kwargs):
    if subscription not in [s for s, _ in self._subscriptions]:
      raise gexc.NotFound(
          "Subscription '{}' does not exist.".format(subscription))
    topic = next(t for s, t in self._subscriptions if s == subscription)
    self._subscriptions.remove((subscription, topic))

  def get_subscription(self, subscription, *args, **kwargs):
    for sub_name, sub_topic in self._subscriptions:
      if sub_name == subscription:
        sub_proto = pubsub.types.Subscription()
        sub_proto.name = sub_name
        sub_proto.topic = sub_topic
        return sub_proto
    raise gexc.NotFound(
        "Subscription '{}' does not exist.".format(subscription))

  def list_subscriptions(self, project, *args, **kwargs):
    sub_protos = []
    for sub_name, sub_topic in self._subscriptions:
      sub_project = "/".join(sub_name.split("/")[:2])
      if sub_project == project:
        sub_proto = pubsub.types.Subscription()
        sub_proto.name = sub_name
        sub_proto.topic = sub_topic
        sub_proto.append(sub_proto)
    return sub_protos

  def create_snapshot(self, name, subscription, *args, **kwargs):
    if name in [s for s, _ in self._snapshots]:
      raise gexc.AlreadyExists("Snapshot '{}' already exists.".format(name))
    topic = self.get_subscription(subscription).topic
    self._snapshots.append((name, topic))
    snap_proto = pubsub.types.Snapshot()
    snap_proto.name = name
    snap_proto.topic = topic
    return snap_proto

  def delete_snapshot(self, snapshot, *args, **kwargs):
    if snapshot not in [s for s, _ in self._snapshots]:
      raise gexc.NotFound("Snapshot '{}' does not exist.".format(snapshot))
    topic = next(t for s, t in self._snapshots if s == snapshot)
    self._snapshots.remove((snapshot, topic))

  def get_snapshot(self, snapshot, *args, **kwargs):
    # Blocked by: https://github.com/googleapis/google-cloud-python/issues/8554
    raise NotImplementedError

  def list_snapshots(self, project, *args, **kwargs):
    snap_protos = []
    for snap_name, snap_topic in self._snapshots:
      snap_project = "/".join(snap_name.split("/")[:2])
      if snap_project == project:
        snap_proto = pubsub.types.Snapshot()
        snap_proto.name = snap_name
        snap_proto.topic = snap_topic
        snap_protos.append(snap_proto)
    return snap_protos

  def seek(self, *args, **kwargs):
    pass


class FakePubSub(object):

  def __init__(self):
    self._topics = []
    self._subscriptions = []
    self._snapshots = []
    self.types = pubsub.types if pubsub is not None else None

  def PublisherClient(self):
    return FakePublisherClient(self._topics, self._subscriptions,
                               self._snapshots)

  def SubscriberClient(self):
    return FakeSubscriberClient(self._topics, self._subscriptions,
                                self._snapshots)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
@mock.patch(
    "apache_beam.runners.interactive.caching.stream_based_cache.pubsub",
    new_callable=FakePubSub)
class PubSubBasedCacheTest(ExtraAssertionsMixin, unittest.TestCase):

  cache_class = stream_based_cache.PubSubBasedCache

  def setUp(self):
    project_id = "test-project-id"
    topic_name = "test-topic-name"
    self.location = "projects/{}/topics/{}".format(project_id, topic_name)

  def tearDown(self):
    pass

  def test_init(self, mock_pubsub):
    _, project_id, _, topic_name = self.location.split("/")
    subscription_path = "projects/{}/subscriptions/{}".format(
        project_id, topic_name)
    snapshot_path = "projects/{}/snapshots/{}".format(project_id, topic_name)

    cache = self.cache_class(self.location)

    self.assertEqual(cache.location, self.location)
    self.assertEqual(cache.subscription.name, subscription_path)
    self.assertEqual(cache.snapshot.name, snapshot_path)

    self.assertCountEqual(mock_pubsub._topics, [self.location])
    self.assertCountEqual(mock_pubsub._subscriptions,
                          [(subscription_path, self.location)])
    self.assertCountEqual(mock_pubsub._snapshots,
                          [(snapshot_path, self.location)])

  def test_init_mode_invalid(self, unused_mock_pubsub):
    with self.assertRaises(ValueError):
      _ = self.cache_class(self.location, mode=None)

    with self.assertRaises(ValueError):
      _ = self.cache_class(self.location, mode="")

    with self.assertRaises(ValueError):
      _ = self.cache_class(self.location, mode="be happy")

  def test_init_mode_error(self, mock_pubsub):
    _ = self.cache_class(self.location)
    with self.assertRaises(IOError):
      _ = self.cache_class(self.location, mode="error")

  def test_init_mode_overwrite(self, mock_pubsub):
    location_copy = "".join(self.location)
    self.assertEqual(self.location, location_copy)
    self.assertNotEqual(id(self.location), id(location_copy))

    _ = self.cache_class(self.location, persist=True)
    topic = mock_pubsub._topics[0]
    subscription = mock_pubsub._subscriptions[0]
    snapshot = mock_pubsub._snapshots[0]

    # cache2 needs to be kept so that resources are not gc-ed immediately.
    cache2 = self.cache_class(location_copy, mode="overwrite")  # pylint: disable=unused-variable
    self.assertCountEqual([topic], mock_pubsub._topics)
    self.assertCountEqual([subscription], mock_pubsub._subscriptions)
    self.assertCountEqual([snapshot], mock_pubsub._snapshots)
    self.assertNotEqual(id(topic), id(mock_pubsub._topics[0]))
    self.assertNotEqual(id(subscription), id(mock_pubsub._subscriptions[0]))
    self.assertNotEqual(id(snapshot), id(mock_pubsub._snapshots[0]))

  def test_init_mode_append(self, mock_pubsub):
    location_copy = "".join(self.location)
    self.assertEqual(self.location, location_copy)
    self.assertNotEqual(id(self.location), id(location_copy))

    _ = self.cache_class(self.location, persist=True)
    topic = mock_pubsub._topics[0]
    subscription = mock_pubsub._subscriptions[0]
    snapshot = mock_pubsub._snapshots[0]

    # cache2 needs to be kept so that resources are not gc-ed immediately.
    cache2 = self.cache_class(location_copy, mode="append")  # pylint: disable=unused-variable
    self.assertCountEqual([topic], mock_pubsub._topics)
    self.assertCountEqual([subscription], mock_pubsub._subscriptions)
    self.assertCountEqual([snapshot], mock_pubsub._snapshots)
    self.assertEqual(id(topic), id(mock_pubsub._topics[0]))
    self.assertEqual(id(subscription), id(mock_pubsub._subscriptions[0]))
    self.assertEqual(id(snapshot), id(mock_pubsub._snapshots[0]))

  def test_persist_false_0(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=False)
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)
    cache.remove()
    self.assertEqual(len(mock_pubsub._topics), 0)
    self.assertEqual(len(mock_pubsub._subscriptions), 0)
    self.assertEqual(len(mock_pubsub._snapshots), 0)

  def test_persist_false_1(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=False)
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)
    del cache
    gc.collect()
    self.assertEqual(len(mock_pubsub._topics), 0)
    self.assertEqual(len(mock_pubsub._subscriptions), 0)
    self.assertEqual(len(mock_pubsub._snapshots), 0)

  def test_persist_setter_false(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=True)
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)
    cache.persist = False
    del cache
    gc.collect()
    self.assertEqual(len(mock_pubsub._topics), 0)
    self.assertEqual(len(mock_pubsub._subscriptions), 0)
    self.assertEqual(len(mock_pubsub._snapshots), 0)

  def test_persist_true_0(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=True)
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)
    cache.remove()
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)

  def test_persist_true_1(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=True)
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)
    del cache
    gc.collect()
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)

  def test_persist_setter_true(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=False)
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)
    cache.persist = True
    del cache
    gc.collect()
    self.assertGreater(len(mock_pubsub._topics), 0)
    self.assertGreater(len(mock_pubsub._subscriptions), 0)
    self.assertGreater(len(mock_pubsub._snapshots), 0)

  def test_timestamp(self, unused_mock_pubsub):
    cache = self.cache_class(self.location)
    self.assertEqual(cache.timestamp, 0)
    cache.writer()
    timestamp1 = cache.timestamp
    self.assertGreater(timestamp1, 0)
    time.sleep(0.01)
    cache.writer()
    timestamp2 = cache.timestamp
    self.assertGreater(timestamp2, timestamp1)

  def test_writer_arguments(self, unused_mock_pubsub):
    kwargs = {"a": 10, "b": "hello"}
    dummy_ptransfrom = beam.Map(lambda e: e)
    dummy_pcoll = PCollection(pipeline=Pipeline(), element_type=str)
    with mock.patch("apache_beam.runners.interactive.caching.stream_based_cache"
                    ".WriteToPubSub") as mock_writer:
      mock_writer.return_value = dummy_ptransfrom
      cache = self.cache_class(self.location, **kwargs)
      cache.writer().expand(dummy_pcoll)
    _, kwargs_out = list(mock_writer.call_args)
    # Arguments known to be assigned by the cache
    kwargs_out.pop("with_attributes", None)
    self.assertEqual(kwargs_out, kwargs)

  def test_reader_arguments(self, unused_mock_pubsub):

    def get_reader_kwargs(kwargs, passthrough):
      dummy_ptransfrom = beam.Map(lambda e: e)
      with mock.patch(
          "apache_beam.runners.interactive.caching.stream_based_cache"
          ".ReadFromPubSub") as mock_reader:
        mock_reader.return_value = dummy_ptransfrom
        cache = self.cache_class(
            self.location, mode="overwrite", coder=DummyCoder(), **kwargs)
        cache._reader_passthrough_arguments = (
            cache._reader_passthrough_arguments | passthrough)
        cache.reader().expand(PBegin(Pipeline()))
      _, kwargs_out = list(mock_reader.call_args)
      # Arguments known to be assigned by the cache
      kwargs_out.pop("subscription", None)
      kwargs_out.pop("timestamp_attribute", None)
      return kwargs_out

    kwargs = {"a": 10, "b": "hello world", "c": b"xxx"}
    kwargs_out = get_reader_kwargs(kwargs, set())
    self.assertEqual(kwargs_out, {})

    kwargs_out = get_reader_kwargs(kwargs, {"a", "b"})
    self.assertEqual(kwargs_out, {"a": 10, "b": "hello world"})

  def test_infer_element_type_with_write(self, unused_mock_pubsub):
    cache = self.cache_class(self.location)
    self.assertEqual(cache.element_type, None)
    for data in file_based_cache_test.GENERIC_TEST_DATA:
      element_type = datatype_inference.infer_element_type(data)
      cache.truncate()
      self.assertEqual(cache.element_type, None)
      with mock.patch("google.cloud.pubsub"):
        cache.write(data)
      self.assertEqual(cache.element_type, element_type)

  def test_infer_element_type_with_writer(self, unused_mock_pubsub):
    cache = self.cache_class(self.location)
    self.assertEqual(cache.element_type, None)
    for data in file_based_cache_test.GENERIC_TEST_DATA:
      element_type = datatype_inference.infer_element_type(data)
      cache.truncate()
      self.assertEqual(cache.element_type, None)
      cache.writer().expand(PCollection(Pipeline(), element_type=element_type))
      self.assertEqual(cache.element_type, element_type)

  def test_default_coder(self, unused_mock_pubsub):
    cache = self.cache_class(
        self.location, coder=file_based_cache.SafeFastPrimitivesCoder)
    for element_type in file_based_cache_test.GENERIC_ELEMENT_TYPES:
      cache.truncate()
      cache.element_type = element_type

      # We need to specify new=DummyEncoder instead of using MagicMock
      # to prevent StackOverflow errors caused by recursive pickling.
      with mock.patch(
          "apache_beam.runners.interactive.caching.stream_based_cache"
          ".EncodeToPubSub",
          new=generate_dummy_encoder(file_based_cache.SafeFastPrimitivesCoder)):
        cache.writer().expand(PCollection(Pipeline(), element_type=str))

  def test_inferred_coder(self, unused_mock_pubsub):
    cache = self.cache_class(self.location)
    for element_type in file_based_cache_test.GENERIC_ELEMENT_TYPES:
      cache.truncate()
      cache.element_type = element_type

      # We need to specify new=DummyEncoder instead of using MagicMock
      # to prevent StackOverflow errors caused by recursive pickling.
      with mock.patch(
          "apache_beam.runners.interactive.caching.stream_based_cache"
          ".EncodeToPubSub",
          new=generate_dummy_encoder(coders.registry.get_coder(element_type))):
        cache.writer().expand(PCollection(Pipeline(), element_type=str))

  @mock.patch("apache_beam.runners.interactive.caching.stream_based_cache"
              ".WriteToPubSub")
  def test_writer(self, mock_writer, unused_mock_pubsub):
    dummy_ptransfrom = beam.Map(lambda e: e)
    mock_writer.return_value = dummy_ptransfrom
    cache = self.cache_class(self.location)
    self.assertEqual(mock_writer.call_count, 0)
    cache.writer().expand(PCollection(Pipeline(), element_type=str))
    self.assertEqual(mock_writer.call_count, 1)
    cache.writer().expand(PCollection(Pipeline(), element_type=str))
    self.assertEqual(mock_writer.call_count, 2)

  @mock.patch("apache_beam.runners.interactive.caching.stream_based_cache"
              ".ReadFromPubSub")
  def test_reader(self, mock_reader, unused_mock_pubsub):
    dummy_ptransfrom = beam.Map(lambda e: e)
    mock_reader.return_value = dummy_ptransfrom
    cache = self.cache_class(self.location, coder=DummyCoder())
    self.assertEqual(mock_reader.call_count, 0)
    cache.reader().expand(PBegin(Pipeline()))
    self.assertEqual(mock_reader.call_count, 1)
    cache.reader().expand(PBegin(Pipeline()))
    self.assertEqual(mock_reader.call_count, 2)

  def test_write(self, unused_mock_pubsub):
    num_elements = 10
    data_list = [
        'data {}'.format(i).encode("utf-8") for i in range(num_elements)
    ]
    cache = self.cache_class(self.location)
    with mock.patch("google.cloud.pubsub") as mock_publisher:
      cache.write(data_list)
    expected_attributes = {
        self.cache_class._default_timestamp_attribute: mock.ANY
    }
    call_list = ([
        mock.call(mock.ANY, data, **expected_attributes) for data in data_list
    ] + [mock.call().result(mock.ANY) for _ in data_list])
    mock_publisher.PublisherClient().publish.assert_has_calls(call_list)

  def test_write_with_attributes(self, unused_mock_pubsub):
    num_elements = 10
    data_list = [
        'data {}'.format(i).encode("utf-8") for i in range(num_elements)
    ]
    attributes_list = [{
        'key': 'value {}'.format(i), 'custom_timestmap': '{}'.format(i)
    } for i in range(num_elements)]
    messages = [
        PubsubMessage(data, attributes)
        for data, attributes in zip(data_list, attributes_list)
    ]
    cache = self.cache_class(
        self.location,
        with_attributes=True,
        timestamp_attribute="custom_timestmap")
    with mock.patch("google.cloud.pubsub") as mock_publisher:
      cache.write(messages)
    call_list = ([
        mock.call(mock.ANY, data, **attributes)
        for data, attributes in zip(data_list, attributes_list)
    ] + [mock.call().result(mock.ANY) for _ in data_list])
    mock_publisher.PublisherClient().publish.assert_has_calls(call_list)

  def test_read_to_queue(self, mock_pubsub):

    num_elements = 10
    data_list = [
        'data {}'.format(i).encode("utf-8") for i in range(num_elements)
    ]
    attributes_list = [{
        'key': 'value {}'.format(i),
        self.cache_class._default_timestamp_attribute: '0'
    } for i in range(num_elements)]
    ack_ids = ['ack_id_{}'.format(i) for i in range(num_elements)]
    response_messages = [
        test_utils.PullResponseMessage(data, attributes, ack_id=ack_id)
        for data, attributes, ack_id in zip(data_list, attributes_list, ack_ids)
    ]
    response = test_utils.create_pull_response(response_messages)
    request_queue = Queue()
    mock_subscribe_future = mock.MagicMock()

    def push_test_items(subscription, callback):
      for msg in response.received_messages:
        msg = pubsub_v1.subscriber.message.Message(msg.message, msg.ack_id,
                                                   request_queue)
        callback(msg)
      return mock_subscribe_future

    FakeSubscriberClient.subscribe = mock.MagicMock(side_effect=push_test_items)

    cache = self.cache_class(self.location, coder=DummyCoder())
    self.assertEqual(len(mock_pubsub._subscriptions), 1)
    with cache.read_to_queue() as message_queue:
      self.assertEqual(len(mock_pubsub._subscriptions), 2)
      received_messages = []
      for _ in range(num_elements):
        timestamped_value = message_queue.get()
        received_messages.append(timestamped_value.value)
    self.assertEqual(len(mock_pubsub._subscriptions), 1)
    self.assertCountEqual(received_messages, data_list)
    mock_subscribe_future.cancel.assert_called_once_with()

  def test_read(self, mock_pubsub):
    input_data = [0, 1, 2, 3, "4"]

    message_queue = Queue()
    for element in input_data:
      message_queue.put(element)

    mock_queue_context = mock.MagicMock()
    mock_queue_context.__enter__.side_effect = lambda: message_queue
    mock_read_to_queue = mock.MagicMock(
        side_effect=lambda *args, **kwargs: mock_queue_context)

    cache = self.cache_class(self.location)
    cache.read_to_queue = mock_read_to_queue
    produced_data = []
    for element in cache.read(delay=0, timeout=0):
      produced_data.append(element)
    mock_queue_context.__exit__.assert_called_once()
    self.assertEqual(input_data, produced_data)

  def test_truncate(self, mock_pubsub):
    cache = self.cache_class(self.location, persist=True)
    subscriptions = mock_pubsub._subscriptions[:]
    snapshots = mock_pubsub._snapshots[:]
    cache.truncate()
    self.assertEqual(subscriptions, mock_pubsub._subscriptions)
    self.assertNotEqual([id(t) for t in subscriptions],
                        [id(t) for t in mock_pubsub._subscriptions])
    self.assertEqual(snapshots, mock_pubsub._snapshots)
    self.assertNotEqual([id(t) for t in snapshots],
                        [id(t) for t in mock_pubsub._snapshots])

  def test_remove(self, mock_pubsub):
    cache = self.cache_class(self.location)
    cache.remove()
    self.assertCountEqual([], mock_pubsub._topics)
    self.assertCountEqual([], mock_pubsub._subscriptions)
    self.assertCountEqual([], mock_pubsub._snapshots)


def generate_dummy_encoder(default_coder):

  class DummyEncoder(beam.DoFn, unittest.TestCase):

    default_coder = None

    def __init__(self, coder, *args, **kwargs):
      unittest.TestCase.__init__(self)
      self.coder = coder
      self.assertEqual(self.coder, self.default_coder)

    def process(self, element):
      yield element

    if sys.version_info < (3,):

      def runTest(self):
        pass

  DummyEncoder.default_coder = default_coder

  return DummyEncoder


class DummyCoder(object):

  def decode(self, element):
    return element


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
