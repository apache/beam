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

import functools
import logging
import unittest

import hamcrest as hc
import mock

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import ReadMessagesFromPubSub
from apache_beam.io.gcp.pubsub import ReadStringsFromPubSub
from apache_beam.io.gcp.pubsub import WriteStringsToPubSub
from apache_beam.io.gcp.pubsub import _PubSubPayloadSink
from apache_beam.io.gcp.pubsub import _PubSubSource
from apache_beam.io.gcp.pubsub import _ReadFromPubSub
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.direct import transform_evaluator
from apache_beam.runners.direct.direct_runner import _DirectReadFromPubSub
from apache_beam.runners.direct.direct_runner import _get_transform_overrides
from apache_beam.runners.direct.transform_evaluator import _PubSubReadEvaluator
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher

# Protect against environments where the PubSub library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestReadFromPubSubOverride(unittest.TestCase):

  def test_expand_with_topic(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | _ReadFromPubSub('projects/fakeprj/topics/a_topic',
                               None, 'a_label', with_attributes=False)
             | beam.Map(lambda x: x))
    self.assertEqual(str, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadMessagesFromPubSub will be replaced
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
             | _ReadFromPubSub(
                 None, 'projects/fakeprj/subscriptions/a_subscription',
                 'a_label', with_attributes=False)
             | beam.Map(lambda x: x))
    self.assertEqual(str, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadMessagesFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertEqual('a_subscription', source.subscription_name)
    self.assertEqual('a_label', source.id_label)

  def test_expand_with_no_topic_or_subscription(self):
    with self.assertRaisesRegexp(
        ValueError, "Either a topic or subscription must be provided."):
      _ReadFromPubSub(None, None, 'a_label', with_attributes=False)

  def test_expand_with_both_topic_and_subscription(self):
    with self.assertRaisesRegexp(
        ValueError, "Only one of topic or subscription should be provided."):
      _ReadFromPubSub('a_topic', 'a_subscription', 'a_label',
                      with_attributes=False)

  def test_expand_with_attributes(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | _ReadFromPubSub('projects/fakeprj/topics/a_topic',
                               None, 'a_label', with_attributes=True)
             | beam.Map(lambda x: x))
    self.assertEqual(PubsubMessage, pcoll.element_type)

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadMessagesFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    read_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    source = read_transform._source
    self.assertEqual('a_topic', source.topic_name)
    self.assertEqual('a_label', source.id_label)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestWriteStringsToPubSub(unittest.TestCase):
  def test_expand(self):
    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadStringsFromPubSub('projects/fakeprj/topics/baz')
             | WriteStringsToPubSub('projects/fakeprj/topics/a_topic')
             | beam.Map(lambda x: x))

    # Apply the necessary PTransformOverrides.
    overrides = _get_transform_overrides(p.options)
    p.replace_all(overrides)

    # Note that the direct output of ReadStringsFromPubSub will be replaced
    # by a PTransformOverride, so we use a no-op Map.
    write_transform = pcoll.producer.inputs[0].producer.transform

    # Ensure that the properties passed through correctly
    self.assertEqual('a_topic', write_transform.dofn.topic_name)


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
        DisplayDataItemMatcher('id_label', 'a_label')]

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
        DisplayDataItemMatcher('id_label', 'a_label')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_display_data_no_subscription(self):
    source = _PubSubSource('projects/fakeprj/topics/a_topic')
    dd = DisplayData.create_from(source)
    expected_items = [
        DisplayDataItemMatcher('topic', 'projects/fakeprj/topics/a_topic')]

    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestPubSubSink(unittest.TestCase):
  def test_display_data(self):
    sink = _PubSubPayloadSink('projects/fakeprj/topics/a_topic')
    dd = DisplayData.create_from(sink)
    expected_items = [
        DisplayDataItemMatcher('topic', 'projects/fakeprj/topics/a_topic')]

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
    messages = self.sub.client.messages
    self.ack_id_to_msg = dict(zip(range(len(messages)), messages))
    return self.ack_id_to_msg

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass


class FakePubsubClient(object):

  def __init__(self, messages, project=None, **unused_kwargs):
    self.messages = messages
    self.project = project

  def topic(self, name):
    return FakePubsubTopic(name, self)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
class TestReadFromPubSub(unittest.TestCase):

  @mock.patch('google.cloud.pubsub')
  def test_read_messages_success(self, mock_pubsub):
    payload = 'payload'
    message_id = 'message_id'
    attributes = {'attribute': 'value'}
    data = [pubsub.message.Message(payload, message_id, attributes)]
    expected_data = [PubsubMessage(payload, message_id, attributes, None)]

    mock_pubsub.Client = functools.partial(FakePubsubClient, data)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadMessagesFromPubSub('projects/fakeprj/topics/a_topic',
                                      None, 'a_label'))
    assert_that(pcoll, equal_to(expected_data))
    p.run()

  @mock.patch('google.cloud.pubsub')
  def test_read_strings_success(self, mock_pubsub):
    payload = u'🤷 ¯\\_(ツ)_/¯'
    payload_encoded = payload.encode('utf-8')
    data = [pubsub.message.Message(payload_encoded, None, None)]
    expected_data = [payload]

    mock_pubsub.Client = functools.partial(FakePubsubClient, data)
    mock_pubsub.subscription.AutoAck = FakeAutoAck

    p = TestPipeline()
    p.options.view_as(StandardOptions).streaming = True
    pcoll = (p
             | ReadStringsFromPubSub('projects/fakeprj/topics/a_topic',
                                     None, 'a_label'))
    assert_that(pcoll, equal_to(expected_data))
    p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
