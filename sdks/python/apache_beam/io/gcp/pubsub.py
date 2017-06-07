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

from apache_beam import coders
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import Write
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms import PTransform
from apache_beam.transforms import ParDo
from apache_beam.transforms.display import DisplayDataItem


__all__ = ['ReadStringsFromPubSub', 'WriteStringsToPubSub',
           'PubSubSource', 'PubSubSink']


class ReadStringsFromPubSub(PTransform):
  """A ``PTransform`` for reading utf-8 string payloads from Cloud Pub/Sub."""

  def __init__(self, topic=None, subscription=None, id_label=None):
    """Initializes ``ReadStringsFromPubSub``.

    Attributes:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>". If
        provided then subscription must be None.
      subscription: Existing Cloud Pub/Sub subscription to use in the
        form "projects/<project>/subscriptions/<subscription>". If provided then
        topic must be None.
      id_label: The attribute on incoming Pub/Sub messages to use as a unique
        record identifier.  When specified, the value of this attribute (which
        can be any string that uniquely identifies the record) will be used for
        deduplication of messages.  If not provided, we cannot guarantee
        that no duplicate data will be delivered on the Pub/Sub stream. In this
        case, deduplication of the stream will be strictly best effort.
    """
    super(ReadStringsFromPubSub, self).__init__()
    if topic and subscription:
      raise ValueError("Only one of topic or subscription should be provided.")

    if not (topic or subscription):
      raise ValueError("Either a topic or subscription must be provided.")

    self._source = _PubSubPayloadSource(
        topic,
        subscription=subscription,
        id_label=id_label)

  def expand(self, pvalue):
    pcoll = pvalue.pipeline | Read(self._source)
    pcoll.element_type = bytes
    pcoll = pcoll | 'decode string' >> ParDo(_decodeUtf8String)
    pcoll.element_type = unicode
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
    pcoll = pcoll | 'encode string' >> ParDo(_encodeUtf8String)
    pcoll.element_type = bytes
    return pcoll | Write(self._sink)


class _PubSubPayloadSource(dataflow_io.NativeSource):
  """Source for the payload of a message as bytes from a Cloud Pub/Sub topic.

  Attributes:
    topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>". If
      provided then topic must be None.
    subscription: Existing Cloud Pub/Sub subscription to use in the
      form "projects/<project>/subscriptions/<subscription>". If provided then
      subscription must be None.
    id_label: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier.  When specified, the value of this attribute (which can
      be any string that uniquely identifies the record) will be used for
      deduplication of messages.  If not provided, Dataflow cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
  """

  def __init__(self, topic=None, subscription=None, id_label=None):
    # we are using this coder explicitly for portability reasons of PubsubIO
    # across implementations in languages.
    self.coder = coders.BytesCoder()
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'id_label':
            DisplayDataItem(self.id_label,
                            label='ID Label Attribute').drop_if_none(),
            'topic':
            DisplayDataItem(self.topic,
                            label='Pubsub Topic'),
            'subscription':
            DisplayDataItem(self.subscription,
                            label='Pubsub Subscription').drop_if_none()}

  def reader(self):
    raise NotImplementedError(
        'PubSubPayloadSource is not supported in local execution.')


class _PubSubPayloadSink(dataflow_io.NativeSink):
  """Sink for the payload of a message as bytes to a Cloud Pub/Sub topic."""

  def __init__(self, topic):
    # we are using this coder explicitly for portability reasons of PubsubIO
    # across implementations in languages.
    self.coder = coders.BytesCoder()
    self.topic = topic

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'topic': DisplayDataItem(self.topic, label='Pubsub Topic')}

  def writer(self):
    raise NotImplementedError(
        'PubSubPayloadSink is not supported in local execution.')


def _decodeUtf8String(encoded_value):
  """Decodes a string in utf-8 format from bytes"""
  return encoded_value.decode('utf-8')


def _encodeUtf8String(value):
  """Encodes a string in utf-8 format to bytes"""
  return value.encode('utf-8')


class PubSubSource(dataflow_io.NativeSource):
  """Deprecated: do not use.

  Source for reading from a given Cloud Pub/Sub topic.

  Attributes:
    topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    subscription: Optional existing Cloud Pub/Sub subscription to use in the
      form "projects/<project>/subscriptions/<subscription>".
    id_label: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier.  When specified, the value of this attribute (which can
      be any string that uniquely identifies the record) will be used for
      deduplication of messages.  If not provided, Dataflow cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
    coder: The Coder to use for decoding incoming Pub/Sub messages.
  """

  def __init__(self, topic, subscription=None, id_label=None,
               coder=coders.StrUtf8Coder()):
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label
    self.coder = coder

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'id_label':
            DisplayDataItem(self.id_label,
                            label='ID Label Attribute').drop_if_none(),
            'topic':
            DisplayDataItem(self.topic,
                            label='Pubsub Topic'),
            'subscription':
            DisplayDataItem(self.subscription,
                            label='Pubsub Subscription').drop_if_none()}

  def reader(self):
    raise NotImplementedError(
        'PubSubSource is not supported in local execution.')


class PubSubSink(dataflow_io.NativeSink):
  """Deprecated: do not use.

  Sink for writing to a given Cloud Pub/Sub topic."""

  def __init__(self, topic, coder=coders.StrUtf8Coder()):
    self.topic = topic
    self.coder = coder

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'pubsub'

  def display_data(self):
    return {'topic': DisplayDataItem(self.topic, label='Pubsub Topic')}

  def writer(self):
    raise NotImplementedError(
        'PubSubSink is not supported in local execution.')
