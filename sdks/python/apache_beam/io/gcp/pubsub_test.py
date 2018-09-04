# coding=utf-8
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

"""Unit tests for PubSub sources and sinks."""

from __future__ import absolute_import

import functools
import logging
import unittest
from builtins import object
from builtins import range
from builtins import zip

import hamcrest as hc
import mock

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import ReadStringsFromPubSub
from apache_beam.io.gcp.pubsub import WriteStringsToPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.io.gcp.pubsub import _PubSubSink
from apache_beam.io.gcp.pubsub import _PubSubSource
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.direct import transform_evaluator
from apache_beam.runners.direct.direct_runner import _DirectReadFromPubSub
from apache_beam.runners.direct.direct_runner import _get_transform_overrides
from apache_beam.runners.direct.transform_evaluator import _PubSubReadEvaluator
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window
from apache_beam.transforms.core import Create
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.utils import timestamp

# Protect against environments where the PubSub library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position

# The protobuf library is only used for running on Dataflow.
try:
  from google.cloud.proto.pubsub.v1 import pubsub_pb2
except ImportError:
  pubsub_pb2 = None


class TestPubsubMessage(unittest.TestCase):

  def test_payload_valid(self):
    _ = PubsubMessage('', None)
    _ = PubsubMessage('data', None)
    _ = PubsubMessage(None, {'k': 'v'})

  def test_payload_invalid(self):
    with self.assertRaisesRegexp(ValueError, r'data.*attributes.*must be set'):
      _ = PubsubMessage(None, None)
    with self.assertRaisesRegexp(ValueError, r'data.*attributes.*must be set'):
      _ = PubsubMessage(None, {})

  @unittest.skipIf(pubsub_pb2 is None,
                   'PubSub proto dependencies are not installed')
  def test_proto_conversion(self):
    data = 'data'
    attributes = {'k1': 'v1', 'k2': 'v2'}
    m = PubsubMessage(data, attributes)
    m_converted = PubsubMessage._from_proto_str(m._to_proto_str())
    self.assertEqual(m_converted.data, data)
    self.assertEqual(m_converted.attributes, attributes)

  def test_eq(self):
    a = PubsubMessage('abc', {1: 2, 3: 4})
    b = PubsubMessage('abc', {1: 2, 3: 4})
    c = PubsubMessage('abc', {1: 2})
    self.assertTrue(a == b)
    self.assertTrue(a != c)
    self.assertTrue(b != c)

  def test_hash(self):
    a = PubsubMessage('abc', {1: 2, 3: 4})
    b = PubsubMessage('abc', {1: 2, 3: 4})
    c = PubsubMessage('abc', {1: 2})
    self.assertTrue(hash(a) == hash(b))
    self.assertTrue(hash(a) != hash(c))
    self.assertTrue(hash(b) != hash(c))

  def test_repr(self):
    a = PubsubMessage('abc', {1: 2, 3: 4})
    b = PubsubMessage('abc', {1: 2, 3: 4})
    c = PubsubMessage('abc', {1: 2})
    self.assertTrue(repr(a) == repr(b))
    self.assertTrue(repr(a) != repr(c))
    self.assertTrue(repr(b) != repr(c))


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestReadFromPubSubOverride(unittest.TestCase):

  def test_expand_with_topic(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub('projects/fakeprj/topics/a_topic',
                              None, 'a_label', with_attributes=False,
                              timestamp_attribute=None)
             | beam.Map(lambda x: x))
    self.assertEqual(bytes, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertEqual('a_topic', source.topic_name)
    self.assertEqual('a_label', source.id_label)

  def test_expand_with_subscription(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub(
                 None, 'projects/fakeprj/subscriptions/a_subscription',
                 'a_label', with_attributes=False, timestamp_attribute=None)
             | beam.Map(lambda x: x))
    self.assertEqual(bytes, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertEqual('a_subscription', source.subscription_name)
    self.assertEqual('a_label', source.id_label)

  def test_expand_with_no_topic_or_subscription(self):
    with self.assertRaisesRegexp(
        ValueError, "Either a topic or subscription must be provided."):
      ReadFromPubSub(None, None, 'a_label', with_attributes=False,
                     timestamp_attribute=None)

  def test_expand_with_both_topic_and_subscription(self):
    with self.assertRaisesRegexp(
        ValueError, "Only one of topic or subscription should be provided."):
      ReadFromPubSub('a_topic', 'a_subscription', 'a_label',
                     with_attributes=False, timestamp_attribute=None)

  def test_expand_with_other_options(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub('projects/fakeprj/topics/a_topic',
                              None, 'a_label', with_attributes=True,
                              timestamp_attribute='time')
             | beam.Map(lambda x: x))
    self.assertEqual(PubsubMessage, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertTrue(source.with_attributes)
    self.assertEqual('time', source.timestamp_attribute)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestWriteStringsToPubSubOverride(unittest.TestCase):
  def test_expand_deprecated(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub('projects/fakeprj/topics/baz')
             | WriteStringsToPubSub('projects/fakeprj/topics/a_topic')
             | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    write_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    self.assertEqual('a_topic', write_transform.dofn.topic_name)

  def test_expand(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub('projects/fakeprj/topics/baz')
             | WriteToPubSub('projects/fakeprj/topics/a_topic',
                             with_attributes=True)
             | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    write_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    self.assertEqual('a_topic', write_transform.dofn.topic_name)
    self.assertEqual(True, write_transform.dofn.with_attributes)
    # TODO(BEAM-4275): These properties aren't supported yet in direct runner.
    self.assertEqual(None, write_transform.dofn.id_label)
    self.assertEqual(None, write_transform.dofn.timestamp_attribute)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestPubSubSource(unittest.TestCase):
  def test_display_data_topic(self):
    source = _PubSubSource(
        'projects/fakeprj/topics/a_topic',
        None,
        'a_label')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher(
            'topic', 'projects/fakeprj/topics/a_topic'),
        DisplayDataItemMatcher('id_label', 'a_label'),
        DisplayDataItemMatcher('with_attributes', False),
    ]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_display_data_subscription(self):
    source = _PubSubSource(
        None,
        'projects/fakeprj/subscriptions/a_subscription',
        'a_label')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher(
            'subscription', 'projects/fakeprj/subscriptions/a_subscription'),
        DisplayDataItemMatcher('id_label', 'a_label'),
        DisplayDataItemMatcher('with_attributes', False),
    ]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_display_data_no_subscription(self):
    source = _PubSubSource('projects/fakeprj/topics/a_topic')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('topic', 'projects/fakeprj/topics/a_topic'),
        DisplayDataItemMatcher('with_attributes', False),
    ]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestPubSubSink(unittest.TestCase):
  def test_display_data(self):
    sink = _PubSubSink('projects/fakeprj/topics/a_topic',
                       id_label='id', with_attributes=False,
                       timestamp_attribute='time')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('topic', 'projects/fakeprj/topics/a_topic'),
        DisplayDataItemMatcher('id_label', 'id'),
        DisplayDataItemMatcher('with_attributes', False),
        DisplayDataItemMatcher('timestamp_attribute', 'time'),
    ]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


class TestPubSubReadEvaluator(object):
  """Wrapper of _PubSubReadEvaluator that makes it bounded."""

  _pubsub_read_evaluator = _PubSubReadEvaluator

  def __init__(self, *args, **kwargs):
    self._evaluator = self._pubsub_read_evaluator(*args, **kwargs)

  def start_bundle(self):
    return self._evaluator.start_bundle()

  def process_element(self, element):
    return self._evaluator.process_element(element)

  def finish_bundle(self):
    result = self._evaluator.finish_bundle()
    result.unprocessed_bundles = []
    result.keyed_watermark_holds = {None: None}
    return result


transform_evaluator.TransformEvaluatorRegistry._test_evaluators_overrides = {
    _DirectReadFromPubSub: TestPubSubReadEvaluator,
}


class FakePubsubTopic(object):

  def __init__(self, name, client):
    self.name = name
    self.client = client

  def subscription(self, name):
    return FakePubsubSubscription(name, self.name, self.client)

  def batch(self):
    if self.client.batch is None:
      self.client.batch = FakeBatch(self.client)
    return self.client.batch


class FakePubsubSubscription(object):

  def __init__(self, name, topic, client):
    self.name = name
    self.topic = topic
    self.client = client

  def create(self):
    pass


class FakeAutoAck(object):

  def __init__(self, sub, **unused_kwargs):
    self.sub = sub

  def __enter__(self):
    messages = self.sub.client.messages_read
    self.ack_id_to_msg = dict(zip(range(len(messages)), messages))
    return self.ack_id_to_msg

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass


class FakeBatch(object):
  """Context manager that accept Pubsub client writes via publish().

  Verifies writes on exit.
  """

  def __init__(self, client):
    self.client = client
    self.published = []

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type is not None:
      return  # Exception will be raised.
    hc.assert_that(self.published,
                   hc.only_contains(*self.client.messages_write))

  def publish(self, message, **attrs):
    self.published.append([message, attrs])


class FakePubsubClient(object):

  def __init__(self, messages_read=None, messages_write=None, project=None,
               **unused_kwargs):
    """Creates a Pubsub client fake.

    Args:
      messages_read: List of PubsubMessage objects to return.
      messages_write: List of [data, attributes] pairs, corresponding to
        messages expected to be written to the client.
      project: Name of GCP project.
    """
    self.messages_read = messages_read
    self.messages_write = messages_write
    self.project = project
    self.batch = None

  def topic(self, name):
    return FakePubsubTopic(name, self)


def create_client_message(data, message_id, attributes, publish_time):
  """Returns a message as it would be returned from Cloud Pub/Sub client.

  This is what the reader sees.
  """
  msg = pubsub.message.Message(data, message_id, attributes)
  msg._service_timestamp = publish_time
  return msg


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestReadFromPubSub(unittest.TestCase):

  @mock.patch('google.cloud.pubsub')
  def test_read_messages_success(self, mock_pubsub):
    data = 'data'
    message_id = 'message_id'
    publish_time = '2018-03-12T13:37:01.234567Z'
    attributes = {'key': 'value'}
    payloads = [create_client_message(
        data, message_id, attributes, publish_time)]
    expected_elements = [
        TestWindowedValue(PubsubMessage(data, attributes),
                          timestamp.Timestamp(1520861821.234567),
                          [window.GlobalWindow()])]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub('projects/fakeprj/topics/a_topic',
                              None, None, with_attributes=True))
    assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_strings_success(self, mock_pubsub):
    data = u'🤷 ¯\\_(ツ)_/¯'
    data_encoded = data.encode('utf-8')
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [create_client_message(data_encoded, None, None, publish_time)]
    expected_elements = [data]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadStringsFromPubSub('projects/fakeprj/topics/a_topic',
                                     None, None))
    assert_that(pcoll, equal_to(expected_elements))
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_data_success(self, mock_pubsub):
    data_encoded = u'🤷 ¯\\_(ツ)_/¯'.encode('utf-8')
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [create_client_message(data_encoded, None, None, publish_time)]
    expected_elements = [data_encoded]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub('projects/fakeprj/topics/a_topic', None, None))
    assert_that(pcoll, equal_to(expected_elements))
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_messages_timestamp_attribute_milli_success(self, mock_pubsub):
    data = 'data'
    message_id = 'message_id'
    attributes = {'time': '1337'}
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [
        create_client_message(data, message_id, attributes, publish_time)]
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp(micros=int(attributes['time']) * 1000),
            [window.GlobalWindow()]),
    ]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub(
                 'projects/fakeprj/topics/a_topic', None, None,
                 with_attributes=True, timestamp_attribute='time'))
    assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_messages_timestamp_attribute_rfc3339_success(self, mock_pubsub):
    data = 'data'
    message_id = 'message_id'
    attributes = {'time': '2018-03-12T13:37:01.234567Z'}
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [
        create_client_message(data, message_id, attributes, publish_time)]
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp.from_rfc3339(attributes['time']),
            [window.GlobalWindow()]),
    ]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub(
                 'projects/fakeprj/topics/a_topic', None, None,
                 with_attributes=True, timestamp_attribute='time'))
    assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_messages_timestamp_attribute_missing(self, mock_pubsub):
    data = 'data'
    message_id = 'message_id'
    attributes = {}
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [
        create_client_message(data, message_id, attributes, publish_time)]
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp.from_rfc3339(publish_time),
            [window.GlobalWindow()]),
    ]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadFromPubSub(
                 'projects/fakeprj/topics/a_topic', None, None,
                 with_attributes=True, timestamp_attribute='nonexistent'))
    assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_messages_timestamp_attribute_fail_parse(self, mock_pubsub):
    data = 'data'
    message_id = 'message_id'
    attributes = {'time': '1337 unparseable'}
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [
        create_client_message(data, message_id, attributes, publish_time)]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | ReadFromPubSub(
             'projects/fakeprj/topics/a_topic', None, None,
             with_attributes=True, timestamp_attribute='time'))
    with self.assertRaisesRegexp(ValueError, r'parse'):
      p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_message_id_label_unsupported(self, mock_pubsub):
    # id_label is unsupported in DirectRunner.
    data = 'data'
    message_id = 'message_id'
    attributes = {'time': '1337 unparseable'}
    publish_time = '2018-03-12T13:37:01.234567Z'
    payloads = [
        create_client_message(data, message_id, attributes, publish_time)]

    mock_pubsub.Client = functools.partial(FakePubsubClient, payloads)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p | ReadFromPubSub('projects/fakeprj/topics/a_topic', None, 'a_label'))
    with self.assertRaisesRegexp(NotImplementedError,
                                 r'id_label is not supported'):
      p.run()


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestWriteToPubSub(unittest.TestCase):

  @mock.patch('google.cloud.pubsub')
  def test_write_messages_success(self, mock_pubsub):
    data = 'data'
    payloads = [data]
    expected_payloads = [[data, {}]]

    mock_pubsub.Client = functools.partial(FakePubsubClient,
                                           messages_write=expected_payloads)

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | Create(payloads)
         | WriteToPubSub('projects/fakeprj/topics/a_topic',
                         with_attributes=False))
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_write_messages_deprecated(self, mock_pubsub):
    data = 'data'
    payloads = [data]
    expected_payloads = [[data, {}]]

    mock_pubsub.Client = functools.partial(FakePubsubClient,
                                           messages_write=expected_payloads)

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | Create(payloads)
         | WriteStringsToPubSub('projects/fakeprj/topics/a_topic'))
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_write_messages_with_attributes_success(self, mock_pubsub):
    data = 'data'
    attributes = {'key': 'value'}
    payloads = [PubsubMessage(data, attributes)]
    expected_payloads = [[data, attributes]]

    mock_pubsub.Client = functools.partial(FakePubsubClient,
                                           messages_write=expected_payloads)

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | Create(payloads)
         | WriteToPubSub('projects/fakeprj/topics/a_topic',
                         with_attributes=True))
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_write_messages_with_attributes_error(self, mock_pubsub):
    data = 'data'
    # Sending raw data when WriteToPubSub expects a PubsubMessage object.
    payloads = [data]

    mock_pubsub.Client = functools.partial(FakePubsubClient)

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | Create(payloads)
         | WriteToPubSub('projects/fakeprj/topics/a_topic',
                         with_attributes=True))
    with self.assertRaisesRegexp(AttributeError,
                                 r'str.*has no attribute.*data'):
      p.run()

  @mock.patch('google.cloud.pubsub')
  def test_write_messages_unsupported_features(self, mock_pubsub):
    data = 'data'
    attributes = {'key': 'value'}
    payloads = [PubsubMessage(data, attributes)]
    expected_payloads = [[data, attributes]]

    mock_pubsub.Client = functools.partial(FakePubsubClient,
                                           messages_write=expected_payloads)

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | Create(payloads)
         | WriteToPubSub('projects/fakeprj/topics/a_topic',
                         id_label='a_label'))
    with self.assertRaisesRegexp(NotImplementedError,
                                 r'id_label is not supported'):
      p.run()
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    _ = (p
         | Create(payloads)
         | WriteToPubSub('projects/fakeprj/topics/a_topic',
                         timestamp_attribute='timestamp'))
    with self.assertRaisesRegexp(NotImplementedError,
                                 r'timestamp_attribute is not supported'):
      p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
