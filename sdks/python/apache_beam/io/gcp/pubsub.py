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

Description of common arguments used in this module:
  topic: Cloud Pub/Sub topic in the form "projects/<project>/topics/<topic>".
    If provided, subscription must be None.
  subscription: Existing Cloud Pub/Sub subscription to use in the
    form "projects/<project>/subscriptions/<subscription>". If not specified,
    a temporary subscription will be created from the specified topic. If
    provided, topic must be None.
  id_label: The attribute on incoming Pub/Sub messages to use as a unique
    record identifier. When specified, the value of this attribute (which
    can be any string that uniquely identifies the record) will be used for
    deduplication of messages. If not provided, we cannot guarantee
    that no duplicate data will be delivered on the Pub/Sub stream. In this
    case, deduplication of the stream will be strictly best effort.
  timestamp_attribute: Message value to use as element timestamp. If None,
    uses message publishing time as the timestamp.

    Timestamp values should be in one of two formats:
    - A numerical value representing the number of milliseconds since the Unix
      epoch.
    - A string in RFC 3339 format. For example,
      {@code 2015-10-29T23:41:41.123Z}. The sub-second component of the
      timestamp is optional, and digits beyond the first three (i.e., time units
      smaller than milliseconds) will be ignored.
"""

from __future__ import absolute_import

import re

from six import text_type

from apache_beam import coders
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import Map
from apache_beam.transforms import PTransform
from apache_beam.transforms import core
from apache_beam.transforms import window
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.proto.pubsub.v1 import pubsub_pb2
except ImportError:
  pubsub_pb2 = None


__all__ = ['PubsubMessage', 'ReadMessagesFromPubSub', 'ReadPayloadsFromPubSub',
           'ReadStringsFromPubSub', 'WriteStringsToPubSub']


class PubsubMessage(object):
  """Represents a message from Cloud Pub/Sub.

  This interface is experimental. No backwards compatibility guarantees.
  """

  def __init__(self, payload, attributes):
    """Constructs a message.

    This interface is experimental. No backwards compatibility guarantees.

    See also:
      https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage

    Attributes:
      payload: (str) Message payload, as a byte string.
      attributes: (dict) Map of string to string.
    """
    self.payload = payload
    self.attributes = attributes

  def __eq__(self, other):
    return isinstance(other, PubsubMessage) and (
        self.payload == other.payload and
        self.attributes == other.attributes)

  def __repr__(self):
    return 'PubsubMessage(%s, %s)' % (self.payload, self.attributes)

  @staticmethod
  def _from_proto(proto_msg):
    """Construct from serialized form of ``PubsubMessage``.

    https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage
    """
    msg = pubsub_pb2.PubsubMessage()
    msg.ParseFromString(proto_msg)
    # Convert ScalarMapContainer to dict.
    attributes = dict((key, msg.attributes[key]) for key in msg.attributes)
    return PubsubMessage(msg.data, attributes)

  @staticmethod
  def _from_message(msg):
    """Construct from ``google.cloud.pubsub.message.Message``.

    https://google-cloud-python.readthedocs.io/en/latest/pubsub/subscriber/api/message.html
    """
    # Convert ScalarMapContainer to dict.
    attributes = dict((key, msg.attributes[key]) for key in msg.attributes)
    return PubsubMessage(msg.data, attributes)


class ReadMessagesFromPubSub(PTransform):
  """A ``PTransform`` for reading from Cloud Pub/Sub.

  This interface is experimental. No backwards compatibility guarantees.

  Outputs elements of type :class:`~PubsubMessage`.
  """

  def __init__(self, topic=None, subscription=None, id_label=None,
               timestamp_attribute=None):
    """Initializes ``ReadMessagesFromPubSub``.

    Args:
    """
    super(ReadMessagesFromPubSub, self).__init__()
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label
    self.timestamp_attribute = timestamp_attribute

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    p = (pcoll.pipeline
         | _ReadFromPubSub(self.topic, self.subscription, self.id_label,
                           with_attributes=True,
                           timestamp_attribute=self.timestamp_attribute))
    return p


class ReadPayloadsFromPubSub(PTransform):
  """A ``PTransform`` for reading raw payloads from Cloud Pub/Sub.

  Outputs elements of type ``str``.
  """

  def __init__(self, topic=None, subscription=None, id_label=None,
               timestamp_attribute=None):
    super(ReadPayloadsFromPubSub, self).__init__()
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label
    self.timestamp_attribute = timestamp_attribute

  def get_windowing(self, unused_inputs):
    return core.Windowing(window.GlobalWindows())

  def expand(self, pcoll):
    p = (pcoll.pipeline
         | _ReadFromPubSub(self.topic, self.subscription, self.id_label,
                           with_attributes=False,
                           timestamp_attribute=self.timestamp_attribute))
    return p


class ReadStringsFromPubSub(ReadPayloadsFromPubSub):
  """A ``PTransform`` for reading utf-8 string payloads from Cloud Pub/Sub.

  Outputs elements of type ``unicode``, decoded from UTF-8.

  This class is deprecated.
  """

  def expand(self, pcoll):
    p = (super(ReadStringsFromPubSub, self).expand(pcoll)
         | 'DecodeString' >> Map(lambda b: b.decode('utf-8')))
    p.element_type = text_type
    return p


class _ReadFromPubSub(PTransform):
  """A ``PTransform`` for reading from Cloud Pub/Sub.

  This ``PTransform`` is overridden by Directrunner.
  """

  def __init__(self, topic, subscription, id_label, with_attributes,
               timestamp_attribute):
    """Initializes ``_ReadFromPubSub``.

    Args:
      with_attributes: False - output elements will be raw payload bytes.
        True - output will be :class:`~PubsubMessage` objects.
    """
    super(_ReadFromPubSub, self).__init__()
    self.with_attributes = with_attributes
    self._source = _PubSubSource(
        topic,
        subscription=subscription,
        id_label=id_label,
        with_attributes=with_attributes,
        timestamp_attribute=timestamp_attribute)

  def expand(self, pvalue):
    pcoll = pvalue.pipeline | Read(self._source)
    if self.with_attributes:
      pcoll = pcoll | Map(PubsubMessage._from_proto)
      pcoll.element_type = PubsubMessage
    else:
      pcoll.element_type = bytes
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


class _PubSubSource(dataflow_io.NativeSource):
  """Source for the payload of a message as bytes from a Cloud Pub/Sub topic.

  This ``NativeSource`` is overridden by a native Pubsub implementation.

  Attributes:
    with_attributes: If False, will fetch just message payload. Otherwise,
      fetches ``PubsubMessage`` protobufs.
  """

  def __init__(self, topic=None, subscription=None, id_label=None,
               with_attributes=False, timestamp_attribute=None):
    # We are using this coder explicitly for portability reasons of PubsubIO
    # across implementations in languages.
    self.coder = coders.BytesCoder()
    self.full_topic = topic
    self.full_subscription = subscription
    self.topic_name = None
    self.subscription_name = None
    self.id_label = id_label
    self.with_attributes = with_attributes
    self.timestamp_attribute = timestamp_attribute

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
    raise NotImplementedError

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
