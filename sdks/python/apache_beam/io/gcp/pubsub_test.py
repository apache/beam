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

# pytype: skip-file

import logging
import unittest

import hamcrest as hc
import mock

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.io import Write
from apache_beam.io.gcp.pubsub import MultipleReadFromPubSub
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import PubSubSourceDescriptor
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import ReadStringsFromPubSub
from apache_beam.io.gcp.pubsub import WriteStringsToPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.io.gcp.pubsub import _PubSubSink
from apache_beam.io.gcp.pubsub import _PubSubSource
from apache_beam.metrics.metric import Lineage
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import pipeline_context
from apache_beam.runners.direct import transform_evaluator
from apache_beam.runners.direct.direct_runner import _DirectReadFromPubSub
from apache_beam.runners.direct.direct_runner import _get_transform_overrides
from apache_beam.runners.direct.transform_evaluator import _PubSubReadEvaluator
from apache_beam.testing import test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window
from apache_beam.transforms.core import Create
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.utils import proto_utils
from apache_beam.utils import timestamp

# Protect against environments where the PubSub library is not available.
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None


class TestPubsubMessage(unittest.TestCase):
  def test_payload_valid(self):
    _ = PubsubMessage('', None)
    _ = PubsubMessage('data', None)
    _ = PubsubMessage(None, {'k': 'v'})

  def test_payload_invalid(self):
    with self.assertRaisesRegex(ValueError, r'data.*attributes.*must be set'):
      _ = PubsubMessage(None, None)
    with self.assertRaisesRegex(ValueError, r'data.*attributes.*must be set'):
      _ = PubsubMessage(None, {})

  @unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
  def test_proto_conversion(self):
    data = b'data'
    attributes = {'k1': 'v1', 'k2': 'v2'}
    m = PubsubMessage(data, attributes)
    m_converted = PubsubMessage._from_proto_str(m._to_proto_str())
    self.assertEqual(m_converted.data, data)
    self.assertEqual(m_converted.attributes, attributes)

  @unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
  def test_payload_publish_invalid(self):
    with self.assertRaisesRegex(ValueError, r'data field.*10MB'):
      msg = PubsubMessage(b'0' * 1024 * 1024 * 11, None)
      msg._to_proto_str(for_publish=True)
    with self.assertRaisesRegex(ValueError, 'attribute key'):
      msg = PubsubMessage(b'0', {'0' * 257: '0'})
      msg._to_proto_str(for_publish=True)
    with self.assertRaisesRegex(ValueError, 'attribute value'):
      msg = PubsubMessage(b'0', {'0' * 100: '0' * 1025})
      msg._to_proto_str(for_publish=True)
    with self.assertRaisesRegex(ValueError, '100 attributes'):
      attributes = {}
      for i in range(0, 101):
        attributes[str(i)] = str(i)
      msg = PubsubMessage(b'0', attributes)
      msg._to_proto_str(for_publish=True)
    with self.assertRaisesRegex(ValueError, 'ordering key'):
      msg = PubsubMessage(b'0', None, ordering_key='0' * 1301)
      msg._to_proto_str(for_publish=True)

  def test_eq(self):
    a = PubsubMessage(b'abc', {1: 2, 3: 4})
    b = PubsubMessage(b'abc', {1: 2, 3: 4})
    c = PubsubMessage(b'abc', {1: 2})
    self.assertTrue(a == b)
    self.assertTrue(a != c)
    self.assertTrue(b != c)

  def test_hash(self):
    a = PubsubMessage(b'abc', {1: 2, 3: 4})
    b = PubsubMessage(b'abc', {1: 2, 3: 4})
    c = PubsubMessage(b'abc', {1: 2})
    self.assertTrue(hash(a) == hash(b))
    self.assertTrue(hash(a) != hash(c))
    self.assertTrue(hash(b) != hash(c))

  def test_repr(self):
    a = PubsubMessage(b'abc', {1: 2, 3: 4})
    b = PubsubMessage(b'abc', {1: 2, 3: 4})
    c = PubsubMessage(b'abc', {1: 2})
    self.assertTrue(repr(a) == repr(b))
    self.assertTrue(repr(a) != repr(c))
    self.assertTrue(repr(b) != repr(c))


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestReadFromPubSubOverride(unittest.TestCase):
  def test_expand_with_topic(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    pcoll = (
        p
        | ReadFromPubSub(
            'projects/fakeprj/topics/a_topic',
            None,
            'a_label',
            with_attributes=False,
            timestamp_attribute=None)
        | beam.Map(lambda x: x))
    self.assertEqual(bytes, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertEqual('a_topic', source.topic_name)
    self.assertEqual('a_label', source.id_label)

  def test_expand_with_subscription(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    pcoll = (
        p
        | ReadFromPubSub(
            None,
            'projects/fakeprj/subscriptions/a_subscription',
            'a_label',
            with_attributes=False,
            timestamp_attribute=None)
        | beam.Map(lambda x: x))
    self.assertEqual(bytes, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertEqual('a_subscription', source.subscription_name)
    self.assertEqual('a_label', source.id_label)

  def test_expand_with_no_topic_or_subscription(self):
    with self.assertRaisesRegex(
        ValueError, "Either a topic or subscription must be provided."):
      ReadFromPubSub(
          None,
          None,
          'a_label',
          with_attributes=False,
          timestamp_attribute=None)

  def test_expand_with_both_topic_and_subscription(self):
    with self.assertRaisesRegex(
        ValueError, "Only one of topic or subscription should be provided."):
      ReadFromPubSub(
          'a_topic',
          'a_subscription',
          'a_label',
          with_attributes=False,
          timestamp_attribute=None)

  def test_expand_with_other_options(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    pcoll = (
        p
        | ReadFromPubSub(
            'projects/fakeprj/topics/a_topic',
            None,
            'a_label',
            with_attributes=True,
            timestamp_attribute='time')
        | beam.Map(lambda x: x))
    self.assertEqual(PubsubMessage, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertTrue(source.with_attributes)
    self.assertEqual('time', source.timestamp_attribute)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestMultiReadFromPubSubOverride(unittest.TestCase):
  def test_expand_with_multiple_sources(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    topics = [
        'projects/fakeprj/topics/a_topic', 'projects/fakeprj2/topics/b_topic'
    ]
    subscriptions = ['projects/fakeprj/subscriptions/a_subscription']

    pubsub_sources = [
        PubSubSourceDescriptor(descriptor)
        for descriptor in topics + subscriptions
    ]
    pcoll = (p | MultipleReadFromPubSub(pubsub_sources) | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    self.assertEqual(bytes, pcoll.element_type)

    # Ensure that the sources are passed through correctly
    read_transforms = pcoll.producer.inputs[0].producer.inputs
    topics_list = []
    subscription_list = []
    for read_transform in read_transforms:
      source = read_transform.producer.transform._source
      if source.full_topic:
        topics_list.append(source.full_topic)
      else:
        subscription_list.append(source.full_subscription)
    self.assertEqual(topics_list, topics)
    self.assertEqual(subscription_list, subscriptions)

  def test_expand_with_multiple_sources_and_attributes(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    topics = [
        'projects/fakeprj/topics/a_topic', 'projects/fakeprj2/topics/b_topic'
    ]
    subscriptions = ['projects/fakeprj/subscriptions/a_subscription']

    pubsub_sources = [
        PubSubSourceDescriptor(descriptor)
        for descriptor in topics + subscriptions
    ]
    pcoll = (
        p | MultipleReadFromPubSub(pubsub_sources, with_attributes=True)
        | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    self.assertEqual(PubsubMessage, pcoll.element_type)

    # Ensure that the sources are passed through correctly
    read_transforms = pcoll.producer.inputs[0].producer.inputs
    topics_list = []
    subscription_list = []
    for read_transform in read_transforms:
      source = read_transform.producer.transform._source
      if source.full_topic:
        topics_list.append(source.full_topic)
      else:
        subscription_list.append(source.full_subscription)
    self.assertEqual(topics_list, topics)
    self.assertEqual(subscription_list, subscriptions)

  def test_expand_with_multiple_sources_and_other_options(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    sources = [
        'projects/fakeprj/topics/a_topic',
        'projects/fakeprj2/topics/b_topic',
        'projects/fakeprj/subscriptions/a_subscription'
    ]
    id_labels = ['a_label_topic', 'b_label_topic', 'a_label_subscription']
    timestamp_attributes = ['a_ta_topic', 'b_ta_topic', 'a_ta_subscription']

    pubsub_sources = [
        PubSubSourceDescriptor(
            source=source,
            id_label=id_label,
            timestamp_attribute=timestamp_attribute) for source,
        id_label,
        timestamp_attribute in zip(sources, id_labels, timestamp_attributes)
    ]

    pcoll = (p | MultipleReadFromPubSub(pubsub_sources) | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    self.assertEqual(bytes, pcoll.element_type)

    # Ensure that the sources are passed through correctly
    read_transforms = pcoll.producer.inputs[0].producer.inputs
    for i, read_transform in enumerate(read_transforms):
      id_label = id_labels[i]
      timestamp_attribute = timestamp_attributes[i]

      source = read_transform.producer.transform._source
      self.assertEqual(source.id_label, id_label)
      self.assertEqual(source.with_attributes, False)
      self.assertEqual(source.timestamp_attribute, timestamp_attribute)

  def test_expand_with_wrong_source(self):
    with self.assertRaisesRegex(
        ValueError,
        r'PubSub source descriptor must be in the form '
        r'"projects/<project>/topics/<topic>"'
        ' or "projects/<project>/subscription/<subscription>".*'):
      MultipleReadFromPubSub([PubSubSourceDescriptor('not_a_proper_source')])


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestWriteStringsToPubSubOverride(unittest.TestCase):
  def test_expand_deprecated(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    pcoll = (
        p
        | ReadFromPubSub('projects/fakeprj/topics/baz')
        | WriteStringsToPubSub('projects/fakeprj/topics/a_topic')
        | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    write_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    self.assertEqual('a_topic', write_transform.dofn.short_topic_name)

  def test_expand(self):
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    pcoll = (
        p
        | ReadFromPubSub('projects/fakeprj/topics/baz')
        | beam.Map(lambda x: PubsubMessage(x))
        | WriteToPubSub(
            'projects/fakeprj/topics/a_topic', with_attributes=True)
        | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(options)
    p.replace_all(overrides)

    # Note that the direct output of ReadFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    write_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    self.assertEqual('a_topic', write_transform.dofn.short_topic_name)
    self.assertEqual(True, write_transform.dofn.with_attributes)
    # TODO(https://github.com/apache/beam/issues/18939): These properties
    # aren't supported yet in direct runner.
    self.assertEqual(None, write_transform.dofn.id_label)
    self.assertEqual(None, write_transform.dofn.timestamp_attribute)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestPubSubSource(unittest.TestCase):
  def test_display_data_topic(self):
    source = _PubSubSource('projects/fakeprj/topics/a_topic', None, 'a_label')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('topic', 'projects/fakeprj/topics/a_topic'),
        DisplayDataItemMatcher('id_label', 'a_label'),
        DisplayDataItemMatcher('with_attributes', False),
    ]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_display_data_subscription(self):
    source = _PubSubSource(
        None, 'projects/fakeprj/subscriptions/a_subscription', 'a_label')
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
    sink = WriteToPubSub(
        'projects/fakeprj/topics/a_topic',
        id_label='id',
        timestamp_attribute='time')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('topic', 'projects/fakeprj/topics/a_topic'),
        DisplayDataItemMatcher('id_label', 'id'),
        DisplayDataItemMatcher('with_attributes', True),
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
    _DirectReadFromPubSub: TestPubSubReadEvaluator,  # type: ignore[dict-item]
}


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
@mock.patch('google.cloud.pubsub.SubscriberClient')
class TestReadFromPubSub(unittest.TestCase):
  def test_read_messages_success(self, mock_pubsub):
    data = b'data'
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    attributes = {'key': 'value'}
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response([
        test_utils.PullResponseMessage(
            data, attributes, publish_time_secs, publish_time_nanos, ack_id)
    ])
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp(1520861821.234567), [window.GlobalWindow()])
    ]
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = (
          p
          | ReadFromPubSub(
              'projects/fakeprj/topics/a_topic',
              None,
              None,
              with_attributes=True))
      assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    mock_pubsub.return_value.acknowledge.assert_has_calls(
        [mock.call(subscription=mock.ANY, ack_ids=[ack_id])])

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_strings_success(self, mock_pubsub):
    data = '🤷 ¯\\_(ツ)_/¯'
    data_encoded = data.encode('utf-8')
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response(
        [test_utils.PullResponseMessage(data_encoded, ack_id=ack_id)])
    expected_elements = [data]
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = (
          p
          | ReadStringsFromPubSub(
              'projects/fakeprj/topics/a_topic', None, None))
      assert_that(pcoll, equal_to(expected_elements))
    mock_pubsub.return_value.acknowledge.assert_has_calls(
        [mock.call(subscription=mock.ANY, ack_ids=[ack_id])])

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_data_success(self, mock_pubsub):
    data_encoded = '🤷 ¯\\_(ツ)_/¯'.encode('utf-8')
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response(
        [test_utils.PullResponseMessage(data_encoded, ack_id=ack_id)])
    expected_elements = [data_encoded]
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = (
          p
          | ReadFromPubSub('projects/fakeprj/topics/a_topic', None, None))
      assert_that(pcoll, equal_to(expected_elements))
    mock_pubsub.return_value.acknowledge.assert_has_calls(
        [mock.call(subscription=mock.ANY, ack_ids=[ack_id])])

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_messages_timestamp_attribute_milli_success(self, mock_pubsub):
    data = b'data'
    attributes = {'time': '1337'}
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response([
        test_utils.PullResponseMessage(
            data, attributes, publish_time_secs, publish_time_nanos, ack_id)
    ])
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp(micros=int(attributes['time']) * 1000),
            [window.GlobalWindow()]),
    ]
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = (
          p
          | ReadFromPubSub(
              'projects/fakeprj/topics/a_topic',
              None,
              None,
              with_attributes=True,
              timestamp_attribute='time'))
      assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    mock_pubsub.return_value.acknowledge.assert_has_calls(
        [mock.call(subscription=mock.ANY, ack_ids=[ack_id])])

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_messages_timestamp_attribute_rfc3339_success(self, mock_pubsub):
    data = b'data'
    attributes = {'time': '2018-03-12T13:37:01.234567Z'}
    publish_time_secs = 1337000000
    publish_time_nanos = 133700000
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response([
        test_utils.PullResponseMessage(
            data, attributes, publish_time_secs, publish_time_nanos, ack_id)
    ])
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp.from_rfc3339(attributes['time']),
            [window.GlobalWindow()]),
    ]
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = (
          p
          | ReadFromPubSub(
              'projects/fakeprj/topics/a_topic',
              None,
              None,
              with_attributes=True,
              timestamp_attribute='time'))
      assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    mock_pubsub.return_value.acknowledge.assert_has_calls(
        [mock.call(subscription=mock.ANY, ack_ids=[ack_id])])

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_messages_timestamp_attribute_missing(self, mock_pubsub):
    data = b'data'
    attributes = {}
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    publish_time = '2018-03-12T13:37:01.234567Z'
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response([
        test_utils.PullResponseMessage(
            data, attributes, publish_time_secs, publish_time_nanos, ack_id)
    ])
    expected_elements = [
        TestWindowedValue(
            PubsubMessage(data, attributes),
            timestamp.Timestamp.from_rfc3339(publish_time),
            [window.GlobalWindow()]),
    ]
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = (
          p
          | ReadFromPubSub(
              'projects/fakeprj/topics/a_topic',
              None,
              None,
              with_attributes=True,
              timestamp_attribute='nonexistent'))
      assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
    mock_pubsub.return_value.acknowledge.assert_has_calls(
        [mock.call(subscription=mock.ANY, ack_ids=[ack_id])])

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_messages_timestamp_attribute_fail_parse(self, mock_pubsub):
    data = b'data'
    attributes = {'time': '1337 unparseable'}
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    ack_id = 'ack_id'
    pull_response = test_utils.create_pull_response([
        test_utils.PullResponseMessage(
            data, attributes, publish_time_secs, publish_time_nanos, ack_id)
    ])
    mock_pubsub.return_value.pull.return_value = pull_response

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    p = TestPipeline(options=options)
    _ = (
        p
        | ReadFromPubSub(
            'projects/fakeprj/topics/a_topic',
            None,
            None,
            with_attributes=True,
            timestamp_attribute='time'))
    with self.assertRaisesRegex(ValueError, r'parse'):
      p.run()
    mock_pubsub.return_value.acknowledge.assert_not_called()

    mock_pubsub.return_value.close.assert_has_calls([mock.call()])

  def test_read_message_id_label_unsupported(self, unused_mock_pubsub):
    # id_label is unsupported in DirectRunner.
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with self.assertRaisesRegex(NotImplementedError,
                                r'id_label is not supported'):
      with TestPipeline(options=options) as p:
        _ = (
            p | ReadFromPubSub(
                'projects/fakeprj/topics/a_topic', None, 'a_label'))

  def test_runner_api_transformation_with_topic(self, unused_mock_pubsub):
    source = _PubSubSource(
        topic='projects/fakeprj/topics/a_topic',
        subscription=None,
        id_label='a_label',
        timestamp_attribute='b_label',
        with_attributes=True)
    transform = Read(source)

    context = pipeline_context.PipelineContext()
    proto_transform_spec = transform.to_runner_api(context)
    self.assertEqual(
        common_urns.composites.PUBSUB_READ.urn, proto_transform_spec.urn)

    pubsub_read_payload = (
        proto_utils.parse_Bytes(
            proto_transform_spec.payload,
            beam_runner_api_pb2.PubSubReadPayload))
    self.assertEqual(
        'projects/fakeprj/topics/a_topic', pubsub_read_payload.topic)
    self.assertEqual('a_label', pubsub_read_payload.id_attribute)
    self.assertEqual('b_label', pubsub_read_payload.timestamp_attribute)
    self.assertEqual('', pubsub_read_payload.subscription)
    self.assertTrue(pubsub_read_payload.with_attributes)

    proto_transform = beam_runner_api_pb2.PTransform(
        unique_name="dummy_label", spec=proto_transform_spec)

    transform_from_proto = Read.from_runner_api_parameter(
        proto_transform, pubsub_read_payload, None)
    self.assertTrue(isinstance(transform_from_proto, Read))
    self.assertTrue(isinstance(transform_from_proto.source, _PubSubSource))
    self.assertEqual(
        'projects/fakeprj/topics/a_topic',
        transform_from_proto.source.full_topic)
    self.assertTrue(transform_from_proto.source.with_attributes)

  def test_runner_api_transformation_properties_none(self, unused_mock_pubsub):
    # Confirming that properties stay None after a runner API transformation.
    source = _PubSubSource(
        topic='projects/fakeprj/topics/a_topic', with_attributes=True)
    transform = Read(source)

    context = pipeline_context.PipelineContext()
    proto_transform_spec = transform.to_runner_api(context)
    self.assertEqual(
        common_urns.composites.PUBSUB_READ.urn, proto_transform_spec.urn)

    pubsub_read_payload = (
        proto_utils.parse_Bytes(
            proto_transform_spec.payload,
            beam_runner_api_pb2.PubSubReadPayload))

    proto_transform = beam_runner_api_pb2.PTransform(
        unique_name="dummy_label", spec=proto_transform_spec)

    transform_from_proto = Read.from_runner_api_parameter(
        proto_transform, pubsub_read_payload, None)
    self.assertIsNone(transform_from_proto.source.full_subscription)
    self.assertIsNone(transform_from_proto.source.id_label)
    self.assertIsNone(transform_from_proto.source.timestamp_attribute)

  def test_runner_api_transformation_with_subscription(
      self, unused_mock_pubsub):
    source = _PubSubSource(
        topic=None,
        subscription='projects/fakeprj/subscriptions/a_subscription',
        id_label='a_label',
        timestamp_attribute='b_label',
        with_attributes=True)
    transform = Read(source)

    context = pipeline_context.PipelineContext()
    proto_transform_spec = transform.to_runner_api(context)
    self.assertEqual(
        common_urns.composites.PUBSUB_READ.urn, proto_transform_spec.urn)

    pubsub_read_payload = (
        proto_utils.parse_Bytes(
            proto_transform_spec.payload,
            beam_runner_api_pb2.PubSubReadPayload))
    self.assertEqual(
        'projects/fakeprj/subscriptions/a_subscription',
        pubsub_read_payload.subscription)
    self.assertEqual('a_label', pubsub_read_payload.id_attribute)
    self.assertEqual('b_label', pubsub_read_payload.timestamp_attribute)
    self.assertEqual('', pubsub_read_payload.topic)
    self.assertTrue(pubsub_read_payload.with_attributes)

    proto_transform = beam_runner_api_pb2.PTransform(
        unique_name="dummy_label", spec=proto_transform_spec)

    transform_from_proto = Read.from_runner_api_parameter(
        proto_transform, pubsub_read_payload, None)
    self.assertTrue(isinstance(transform_from_proto, Read))
    self.assertTrue(isinstance(transform_from_proto.source, _PubSubSource))
    self.assertTrue(transform_from_proto.source.with_attributes)
    self.assertEqual(
        'projects/fakeprj/subscriptions/a_subscription',
        transform_from_proto.source.full_subscription)

  def test_read_from_pubsub_no_overwrite(self, unused_mock):
    expected_elements = [
        TestWindowedValue(
            b'apache',
            timestamp.Timestamp(1520861826.234567), [window.GlobalWindow()]),
        TestWindowedValue(
            b'beam',
            timestamp.Timestamp(1520861824.234567), [window.GlobalWindow()])
    ]
    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    for test_case in ('topic', 'subscription'):
      with TestPipeline(options=options) as p:
        # Direct runner currently overwrites the whole ReadFromPubSub transform.
        # This test part of composite transform without overwrite.
        pcoll = p | beam.Create([b'apache', b'beam']) | beam.Map(
            lambda x: window.TimestampedValue(x, 1520861820.234567 + len(x)))
        args = {test_case: f'projects/fakeprj/{test_case}s/topic_or_sub'}
        pcoll = ReadFromPubSub(**args).expand_continued(pcoll)
        assert_that(pcoll, equal_to(expected_elements), reify_windows=True)
      self.assertSetEqual(
          Lineage.query(p.result.metrics(), Lineage.SOURCE),
          set([f"pubsub:{test_case}:fakeprj.topic_or_sub"]))


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
@mock.patch('google.cloud.pubsub.PublisherClient')
class TestWriteToPubSub(unittest.TestCase):
  def test_write_messages_success(self, mock_pubsub):
    data = 'data'
    payloads = [data]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      _ = (
          p
          | Create(payloads)
          | WriteToPubSub(
              'projects/fakeprj/topics/a_topic', with_attributes=False))
    mock_pubsub.return_value.publish.assert_has_calls(
        [mock.call(mock.ANY, data)])

  def test_write_messages_deprecated(self, mock_pubsub):
    data = 'data'
    data_bytes = b'data'
    payloads = [data]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      _ = (
          p
          | Create(payloads)
          | WriteStringsToPubSub('projects/fakeprj/topics/a_topic'))
    mock_pubsub.return_value.publish.assert_has_calls(
        [mock.call(mock.ANY, data_bytes)])

  def test_write_messages_with_attributes_success(self, mock_pubsub):
    data = b'data'
    attributes = {'key': 'value'}
    payloads = [PubsubMessage(data, attributes)]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      _ = (
          p
          | Create(payloads)
          | WriteToPubSub(
              'projects/fakeprj/topics/a_topic', with_attributes=True))
    mock_pubsub.return_value.publish.assert_has_calls(
        [mock.call(mock.ANY, data, **attributes)])

  def test_write_messages_with_attributes_error(self, mock_pubsub):
    data = 'data'
    # Sending raw data when WriteToPubSub expects a PubsubMessage object.
    payloads = [data]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with self.assertRaisesRegex(Exception, r'Type hint violation'):
      with TestPipeline(options=options) as p:
        _ = (
            p
            | Create(payloads)
            | WriteToPubSub(
                'projects/fakeprj/topics/a_topic', with_attributes=True))

  def test_write_messages_unsupported_features(self, mock_pubsub):
    data = b'data'
    attributes = {'key': 'value'}
    payloads = [PubsubMessage(data, attributes)]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with self.assertRaisesRegex(NotImplementedError,
                                r'id_label is not supported'):
      with TestPipeline(options=options) as p:
        _ = (
            p
            | Create(payloads)
            | WriteToPubSub(
                'projects/fakeprj/topics/a_topic',
                id_label='a_label',
                with_attributes=True))

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with self.assertRaisesRegex(NotImplementedError,
                                r'timestamp_attribute is not supported'):
      with TestPipeline(options=options) as p:
        _ = (
            p
            | Create(payloads)
            | WriteToPubSub(
                'projects/fakeprj/topics/a_topic',
                timestamp_attribute='timestamp',
                with_attributes=True))

  def test_runner_api_transformation(self, unused_mock_pubsub):
    sink = _PubSubSink(
        topic='projects/fakeprj/topics/a_topic',
        id_label=None,
        # We expect encoded PubSub write transform to always return attributes.
        timestamp_attribute=None)
    transform = Write(sink)

    context = pipeline_context.PipelineContext()
    proto_transform_spec = transform.to_runner_api(context)
    self.assertEqual(
        common_urns.composites.PUBSUB_WRITE.urn, proto_transform_spec.urn)

    pubsub_write_payload = (
        proto_utils.parse_Bytes(
            proto_transform_spec.payload,
            beam_runner_api_pb2.PubSubWritePayload))

    self.assertEqual(
        'projects/fakeprj/topics/a_topic', pubsub_write_payload.topic)

    proto_transform = beam_runner_api_pb2.PTransform(
        unique_name="dummy_label", spec=proto_transform_spec)

    transform_from_proto = Write.from_runner_api_parameter(
        proto_transform, pubsub_write_payload, None)
    self.assertTrue(isinstance(transform_from_proto, Write))
    self.assertTrue(isinstance(transform_from_proto.sink, _PubSubSink))
    self.assertEqual(
        'projects/fakeprj/topics/a_topic', transform_from_proto.sink.full_topic)

  def test_runner_api_transformation_properties_none(self, unused_mock_pubsub):
    # Confirming that properties stay None after a runner API transformation.
    sink = _PubSubSink(
        topic='projects/fakeprj/topics/a_topic',
        id_label=None,
        # We expect encoded PubSub write transform to always return attributes.
        timestamp_attribute=None)
    transform = Write(sink)

    context = pipeline_context.PipelineContext()
    proto_transform_spec = transform.to_runner_api(context)
    self.assertEqual(
        common_urns.composites.PUBSUB_WRITE.urn, proto_transform_spec.urn)

    pubsub_write_payload = (
        proto_utils.parse_Bytes(
            proto_transform_spec.payload,
            beam_runner_api_pb2.PubSubWritePayload))
    proto_transform = beam_runner_api_pb2.PTransform(
        unique_name="dummy_label", spec=proto_transform_spec)
    transform_from_proto = Write.from_runner_api_parameter(
        proto_transform, pubsub_write_payload, None)

    self.assertTrue(isinstance(transform_from_proto, Write))
    self.assertTrue(isinstance(transform_from_proto.sink, _PubSubSink))
    self.assertIsNone(transform_from_proto.sink.id_label)
    self.assertIsNone(transform_from_proto.sink.timestamp_attribute)

  def test_write_to_pubsub_no_overwrite(self, unused_mock):
    data = 'data'
    payloads = [data]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = p | Create(payloads)
      WriteToPubSub(
          'projects/fakeprj/topics/a_topic',
          with_attributes=False).expand(pcoll)
    self.assertSetEqual(
        Lineage.query(p.result.metrics(), Lineage.SINK),
        set(["pubsub:topic:fakeprj.a_topic"]))

  def test_write_to_pubsub_with_attributes_no_overwrite(self, unused_mock):
    data = b'data'
    attributes = {'key': 'value'}
    payloads = [PubsubMessage(data, attributes)]

    options = PipelineOptions([])
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      pcoll = p | Create(payloads)
      # Avoid direct runner overwrites WriteToPubSub
      WriteToPubSub(
          'projects/fakeprj/topics/a_topic',
          with_attributes=True).expand(pcoll)
    self.assertSetEqual(
        Lineage.query(p.result.metrics(), Lineage.SINK),
        set(["pubsub:topic:fakeprj.a_topic"]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
