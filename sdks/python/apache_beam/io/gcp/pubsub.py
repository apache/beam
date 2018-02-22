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
"""Google Cloud PubSub sources and sinks.

Cloud Pub/Sub sources and sinks are currently supported only in streaming
pipelines, during remote execution.

This API is currently under development and is subject to change.
"""

from __future__ import absolute_import

import re

import six

from apache_beam import coders
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import Map
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.transforms.display import DisplayDataItem

__all__ = ['ReadStringsFromPubSub', 'WriteStringsToPubSub']


class ReadStringsFromPubSub(PTransform):
  """A ``PTransform`` for reading utf-8 string payloads from Cloud Pub/Sub."""

  def __init__(self, topic=None, subscription=None, id_label=None):
    """Initializes ``ReadStringsFromPubSub``.

    Attributes:
      topic: Cloud Pub/Sub topic in the form "projects/<project>/topics/
        <topic>". If provided, subscription must be None.
      subscription: Existing Cloud Pub/Sub subscription to use in the
        form "projects/<project>/subscriptions/<subscription>". If not
        specified, a temporary subscription will be created from the specified
        topic. If provided, topic must be None.
      id_label: The attribute on incoming Pub/Sub messages to use as a unique
        record identifier.  When specified, the value of this attribute (which
        can be any string that uniquely identifies the record) will be used for
        deduplication of messages.  If not provided, we cannot guarantee
        that no duplicate data will be delivered on the Pub/Sub stream. In this
        case, deduplication of the stream will be strictly best effort.
    """
    super(ReadStringsFromPubSub, self).__init__()
    self._source = _PubSubPayloadSource(
        topic,
        subscription=subscription,
        id_label=id_label)

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pvalue):
    pcoll = pvalue.pipeline | Read(self._source)
    pcoll.element_type = bytes
    pcoll = pcoll | 'DecodeString' >> Map(lambda b: b.decode('utf-8'))
    pcoll.element_type = six.text_type
    return pcoll


class WriteStringsToPubSub(PTransform):
  """A ``PTransform`` for writing utf-8 string payloads to Cloud Pub/Sub."""

  def __init__(self, topic):
    """Initializes ``WriteStringsToPubSub``.

    Attributes:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    """
    super(WriteStringsToPubSub, self).__init__()
    self._sink = _PubSubPayloadSink(topic)

  def expand(self, pcoll):
    pcoll = pcoll | 'EncodeString' >> Map(lambda s: s.encode('utf-8'))
    pcoll.element_type = bytes
    return pcoll | Write(self._sink)


PROJECT_ID_REGEXP = '[a-z][-a-z0-9:.]{4,61}[a-z0-9]'
SUBSCRIPTION_REGEXP = 'projects/([^/]+)/subscriptions/(.+)'
TOPIC_REGEXP = 'projects/([^/]+)/topics/(.+)'


def parse_topic(full_topic):
  match = re.match(TOPIC_REGEXP, full_topic)
  if not match:
    raise ValueError(
        'PubSub topic must be in the form "projects/<project>/topics'
        '/<topic>" (got %r).' % full_topic)
  project, topic_name = match.group(1), match.group(2)
  if not re.match(PROJECT_ID_REGEXP, project):
    raise ValueError('Invalid PubSub project name: %r.' % project)
  return project, topic_name


def parse_subscription(full_subscription):
  match = re.match(SUBSCRIPTION_REGEXP, full_subscription)
  if not match:
    raise ValueError(
        'PubSub subscription must be in the form "projects/<project>'
        '/subscriptions/<subscription>" (got %r).' % full_subscription)
  project, subscription_name = match.group(1), match.group(2)
  if not re.match(PROJECT_ID_REGEXP, project):
    raise ValueError('Invalid PubSub project name: %r.' % project)
  return project, subscription_name


class _PubSubPayloadSource(dataflow_io.NativeSource):
  """Source for the payload of a message as bytes from a Cloud Pub/Sub topic.

  Attributes:
    topic: Cloud Pub/Sub topic in the form "projects/<project>/topics/<topic>".
      If provided, subscription must be None.
    subscription: Existing Cloud Pub/Sub subscription to use in the
      form "projects/<project>/subscriptions/<subscription>". If not specified,
      a temporary subscription will be created from the specified topic. If
      provided, topic must be None.
    id_label: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier.  When specified, the value of this attribute (which can
      be any string that uniquely identifies the record) will be used for
      deduplication of messages.  If not provided, Dataflow cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
  """

  def __init__(self, topic=None, subscription=None, id_label=None):
    # We are using this coder explicitly for portability reasons of PubsubIO
    # across implementations in languages.
    self.coder = coders.BytesCoder()
    self.full_topic = topic
    self.full_subscription = subscription
    self.topic_name = None
    self.subscription_name = None
    self.id_label = id_label

    # Perform some validation on the topic and subscription.
    if not (topic or subscription):
      raise ValueError('Either a topic or subscription must be provided.')
    if topic and subscription:
      raise ValueError('Only one of topic or subscription should be provided.')

    if topic:
      self.project, self.topic_name = parse_topic(topic)
    if subscription:
      self.project, self.subscription_name = parse_subscription(subscription)

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'id_label':
            DisplayDataItem(self.id_label,
                            label='ID Label Attribute').drop_if_none(),
            'topic':
            DisplayDataItem(self.full_topic,
                            label='Pubsub Topic').drop_if_none(),
            'subscription':
            DisplayDataItem(self.full_subscription,
                            label='Pubsub Subscription').drop_if_none()}

  def reader(self):
    raise NotImplementedError(
        'PubSubPayloadSource is not supported in local execution.')

  def is_bounded(self):
    return False


class _PubSubPayloadSink(dataflow_io.NativeSink):
  """Sink for the payload of a message as bytes to a Cloud Pub/Sub topic."""

  def __init__(self, topic):
    # we are using this coder explicitly for portability reasons of PubsubIO
    # across implementations in languages.
    self.coder = coders.BytesCoder()
    self.full_topic = topic

    self.project, self.topic_name = parse_topic(topic)

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'topic': DisplayDataItem(self.full_topic, label='Pubsub Topic')}

  def writer(self):
    raise NotImplementedError(
        'PubSubPayloadSink is not supported in local execution.')
