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

Cloud Pub/Sub sources are currently supported only in streaming pipelines,
during remote execution. Cloud Pub/Sub sinks (WriteToPubSub) support both
streaming and batch pipelines.

This API is currently under development and is subject to change.

**Updates to the I/O connector code**

For any significant updates to this I/O connector, please consider involving
corresponding code reviewers mentioned in
https://github.com/apache/beam/blob/master/sdks/python/OWNERS
"""

# pytype: skip-file

import re
from typing import Any
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import Union

from apache_beam import coders
from apache_beam.io import iobase
from apache_beam.io.iobase import Read
from apache_beam.metrics.metric import Lineage
from apache_beam.transforms import DoFn
from apache_beam.transforms import Flatten
from apache_beam.transforms import Map
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.utils.annotations import deprecated

try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None

__all__ = [
    'MultipleReadFromPubSub',
    'PubsubMessage',
    'PubSubSourceDescriptor',
    'ReadFromPubSub',
    'ReadStringsFromPubSub',
    'WriteStringsToPubSub',
    'WriteToPubSub'
]


class PubsubMessage(object):
  """Represents a Cloud Pub/Sub message.

  Message payload includes the data and attributes fields. For the payload to be
  valid, at least one of its fields must be non-empty.

  Attributes:
    data: (bytes) Message data. May be None.
    attributes: (dict) Key-value map of str to str, containing both user-defined
      and service generated attributes (such as id_label and
      timestamp_attribute). May be None.
    message_id: (str) ID of the message, assigned by the pubsub service when the
      message is published. Guaranteed to be unique within the topic. Will be
      reset to None if the message is being written to pubsub.
    publish_time: (datetime) Time at which the message was published. Will be
      reset to None if the Message is being written to pubsub.
    ordering_key: (str) If non-empty, identifies related messages for which
      publish order is respected by the PubSub subscription.
  """
  def __init__(
      self,
      data,
      attributes,
      message_id=None,
      publish_time=None,
      ordering_key=""):
    if data is None and not attributes:
      raise ValueError(
          'Either data (%r) or attributes (%r) must be set.', data, attributes)
    self.data = data
    self.attributes = attributes
    self.message_id = message_id
    self.publish_time = publish_time
    self.ordering_key = ordering_key

  def __hash__(self):
    return hash((self.data, frozenset(self.attributes.items())))

  def __eq__(self, other):
    return isinstance(other, PubsubMessage) and (
        self.data == other.data and self.attributes == other.attributes)

  def __repr__(self):
    return 'PubsubMessage(%s, %s)' % (self.data, self.attributes)

  @staticmethod
  def _from_proto_str(proto_msg: bytes) -> 'PubsubMessage':
    """Construct from serialized form of ``PubsubMessage``.

    Args:
      proto_msg: String containing a serialized protobuf of type
      https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage

    Returns:
      A new PubsubMessage object.
    """
    msg = pubsub.types.PubsubMessage.deserialize(proto_msg)
    # Convert ScalarMapContainer to dict.
    attributes = dict(msg.attributes)
    return PubsubMessage(
        msg.data,
        attributes,
        msg.message_id,
        msg.publish_time,
        msg.ordering_key)

  def _to_proto_str(self, for_publish=False):
    """Get serialized form of ``PubsubMessage``.

    The serialized message is validated against pubsub message limits specified
    at https://cloud.google.com/pubsub/quotas#resource_limits

    Args:
      proto_msg: str containing a serialized protobuf.
      for_publish: bool, if True strip out message fields which cannot be
        published (currently message_id and publish_time) per
        https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#pubsubmessage

    Returns:
      A str containing a serialized protobuf of type
      https://cloud.google.com/pubsub/docs/reference/rpc/google.pubsub.v1#google.pubsub.v1.PubsubMessage
      containing the payload of this object.
    """
    if len(self.data) > (10_000_000):
      raise ValueError('A pubsub message data field must not exceed 10MB')

    if self.attributes:
      if len(self.attributes) > 100:
        raise ValueError(
            'A pubsub message must not have more than 100 attributes.')
      for key, value in self.attributes.items():
        if len(key) > 256:
          raise ValueError(
              'A pubsub message attribute key must not exceed 256 bytes.')
        if len(value) > 1024:
          raise ValueError(
              'A pubsub message attribute value must not exceed 1024 bytes')

    message_id = None
    publish_time = None
    if not for_publish:
      if self.message_id:
        message_id = self.message_id
        if self.publish_time:
          publish_time = self.publish_time

    if len(self.ordering_key) > 1024:
      raise ValueError(
          'A pubsub message ordering key must not exceed 1024 bytes.')

    msg = pubsub.types.PubsubMessage(
        data=self.data,
        attributes=self.attributes,
        message_id=message_id,
        publish_time=publish_time,
        ordering_key=self.ordering_key)
    serialized = pubsub.types.PubsubMessage.serialize(msg)
    if len(serialized) > (10_000_000):
      raise ValueError(
          'Serialized pubsub message exceeds the publish request limit of 10MB')
    return serialized

  @staticmethod
  def _from_message(msg: Any) -> 'PubsubMessage':
    """Construct from ``google.cloud.pubsub_v1.subscriber.message.Message``.

    https://googleapis.github.io/google-cloud-python/latest/pubsub/subscriber/api/message.html
    """
    # Convert ScalarMapContainer to dict.
    attributes = dict(msg.attributes)
    pubsubmessage = PubsubMessage(msg.data, attributes)
    if msg.message_id:
      pubsubmessage.message_id = msg.message_id
    if msg.publish_time:
      pubsubmessage.publish_time = msg.publish_time
    if msg.ordering_key:
      pubsubmessage.ordering_key = msg.ordering_key
    return pubsubmessage


class ReadFromPubSub(PTransform):
  """A ``PTransform`` for reading from Cloud Pub/Sub."""

  # Implementation note: This ``PTransform`` is overridden by Directrunner.

  def __init__(
      self,
      topic: Optional[str] = None,
      subscription: Optional[str] = None,
      id_label: Optional[str] = None,
      with_attributes: bool = False,
      timestamp_attribute: Optional[str] = None) -> None:
    """Initializes ``ReadFromPubSub``.

    Args:
      topic: Cloud Pub/Sub topic in the form
        "projects/<project>/topics/<topic>". If provided, subscription must be
        None.
      subscription: Existing Cloud Pub/Sub subscription to use in the
        form "projects/<project>/subscriptions/<subscription>". If not
        specified, a temporary subscription will be created from the specified
        topic. If provided, topic must be None.
      id_label: The attribute on incoming Pub/Sub messages to use as a unique
        record identifier. When specified, the value of this attribute (which
        can be any string that uniquely identifies the record) will be used for
        deduplication of messages. If not provided, we cannot guarantee
        that no duplicate data will be delivered on the Pub/Sub stream. In this
        case, deduplication of the stream will be strictly best effort.
      with_attributes:
        True - output elements will be :class:`~PubsubMessage` objects.
        False - output elements will be of type ``bytes`` (message
        data only).
      timestamp_attribute: Message value to use as element timestamp. If None,
        uses message publishing time as the timestamp.

        Timestamp values should be in one of two formats:

        - A numerical value representing the number of milliseconds since the
          Unix epoch.
        - A string in RFC 3339 format, UTC timezone. Example:
          ``2015-10-29T23:41:41.123Z``. The sub-second component of the
          timestamp is optional, and digits beyond the first three (i.e., time
          units smaller than milliseconds) may be ignored.
    """
    super().__init__()
    self.with_attributes = with_attributes
    self._source = _PubSubSource(
        topic=topic,
        subscription=subscription,
        id_label=id_label,
        with_attributes=self.with_attributes,
        timestamp_attribute=timestamp_attribute)

  def expand(self, pvalue):
    # TODO(BEAM-27443): Apply a proper transform rather than Read.
    pcoll = pvalue.pipeline | Read(self._source)
    # explicit element_type required after native read, otherwise coder error
    pcoll.element_type = bytes
    return self.expand_continued(pcoll)

  def expand_continued(self, pcoll):
    pcoll = pcoll | ParDo(
        _AddMetricsPassThrough(
            project=self._source.project,
            topic=self._source.topic_name,
            sub=self._source.subscription_name)).with_output_types(bytes)
    if self.with_attributes:
      pcoll = pcoll | Map(PubsubMessage._from_proto_str)
      pcoll.element_type = PubsubMessage
    return pcoll

  def to_runner_api_parameter(self, context):
    # Required as this is identified by type in PTransformOverrides.
    # TODO(https://github.com/apache/beam/issues/18713): Use an actual URN here.
    return self.to_runner_api_pickled(context)


class _AddMetricsPassThrough(DoFn):
  def __init__(self, project, topic=None, sub=None):
    self.project = project
    self.topic = topic
    self.sub = sub
    self.reported_lineage = False

  def setup(self):
    self.reported_lineage = False

  def process(self, element: bytes):
    self.report_lineage_once()
    yield element

  def report_lineage_once(self):
    if not self.reported_lineage:
      self.reported_lineage = True
      if self.topic is not None:
        Lineage.sources().add(
            'pubsub', self.project, self.topic, subtype='topic')
      elif self.sub is not None:
        Lineage.sources().add(
            'pubsub', self.project, self.sub, subtype='subscription')


@deprecated(since='2.7.0', extra_message='Use ReadFromPubSub instead.')
def ReadStringsFromPubSub(topic=None, subscription=None, id_label=None):
  return _ReadStringsFromPubSub(topic, subscription, id_label)


class _ReadStringsFromPubSub(PTransform):
  """This class is deprecated. Use ``ReadFromPubSub`` instead."""
  def __init__(self, topic=None, subscription=None, id_label=None):
    super().__init__()
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label

  def expand(self, pvalue):
    p = (
        pvalue.pipeline
        | ReadFromPubSub(
            self.topic, self.subscription, self.id_label, with_attributes=False)
        | 'DecodeString' >> Map(lambda b: b.decode('utf-8')))
    p.element_type = str
    return p


@deprecated(since='2.7.0', extra_message='Use WriteToPubSub instead.')
def WriteStringsToPubSub(topic):
  return _WriteStringsToPubSub(topic)


class _WriteStringsToPubSub(PTransform):
  """This class is deprecated. Use ``WriteToPubSub`` instead."""
  def __init__(self, topic):
    """Initializes ``_WriteStringsToPubSub``.

    Attributes:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    """
    super().__init__()
    self.topic = topic

  def expand(self, pcoll):
    pcoll = pcoll | 'EncodeString' >> Map(lambda s: s.encode('utf-8'))
    pcoll.element_type = bytes
    return pcoll | WriteToPubSub(self.topic)


class _AddMetricsAndMap(DoFn):
  def __init__(self, fn, project, topic=None):
    self.project = project
    self.topic = topic
    self.fn = fn
    self.reported_lineage = False

  def setup(self):
    self.reported_lineage = False

  def process(self, element):
    self.report_lineage_once()
    yield self.fn(element)

  def report_lineage_once(self):
    if not self.reported_lineage:
      self.reported_lineage = True
      Lineage.sinks().add('pubsub', self.project, self.topic, subtype='topic')


class WriteToPubSub(PTransform):
  """A ``PTransform`` for writing messages to Cloud Pub/Sub.
  
  This transform supports both streaming and batch pipelines. In streaming mode,
  messages are written continuously as they arrive. In batch mode, all messages
  are written when the pipeline completes.
  """

  # Implementation note: This ``PTransform`` is overridden by Directrunner.

  def __init__(
      self,
      topic: str,
      with_attributes: bool = False,
      id_label: Optional[str] = None,
      timestamp_attribute: Optional[str] = None) -> None:
    """Initializes ``WriteToPubSub``.

    Args:
      topic:
          Cloud Pub/Sub topic in the form
          "projects/<project>/topics/<topic>".
      with_attributes:
        True - input elements will be :class:`~PubsubMessage` objects.
        False - input elements will be of type ``bytes`` (message
        data only).
      id_label: If set, will set an attribute for each Cloud Pub/Sub message
        with the given name and a unique value. This attribute can then be used
        in a ReadFromPubSub PTransform to deduplicate messages.
      timestamp_attribute: If set, will set an attribute for each Cloud Pub/Sub
        message with the given name and the message's publish time as the value.
    """
    super().__init__()
    self.with_attributes = with_attributes
    self.id_label = id_label
    self.timestamp_attribute = timestamp_attribute
    self.project, self.topic_name = parse_topic(topic)
    self.full_topic = topic
    self._sink = _PubSubSink(topic, id_label, timestamp_attribute)
    self.pipeline_options = None  # Will be set during expand()

  @staticmethod
  def message_to_proto_str(element: PubsubMessage) -> bytes:
    if not isinstance(element, PubsubMessage):
      raise TypeError(
          'Unexpected element. Type: %s (expected: PubsubMessage), '
          'value: %r' % (type(element), element))
    return element._to_proto_str(for_publish=True)

  @staticmethod
  def bytes_to_proto_str(element: Union[bytes, str]) -> bytes:
    msg = PubsubMessage(element, {})
    return msg._to_proto_str(for_publish=True)

  def expand(self, pcoll):
    # Store pipeline options for use in DoFn
    self.pipeline_options = pcoll.pipeline.options if pcoll.pipeline else None

    if self.with_attributes:
      pcoll = pcoll | 'ToProtobufX' >> ParDo(
          _AddMetricsAndMap(
              self.message_to_proto_str, self.project,
              self.topic_name)).with_input_types(PubsubMessage)
    else:
      pcoll = pcoll | 'ToProtobufY' >> ParDo(
          _AddMetricsAndMap(
              self.bytes_to_proto_str, self.project,
              self.topic_name)).with_input_types(Union[bytes, str])
    pcoll.element_type = bytes
    return pcoll | ParDo(_PubSubWriteDoFn(self))

  def to_runner_api_parameter(self, context):
    # Required as this is identified by type in PTransformOverrides.
    # TODO(https://github.com/apache/beam/issues/18713): Use an actual URN here.
    return self.to_runner_api_pickled(context)

  def display_data(self):
    return {
        'topic': DisplayDataItem(self.full_topic, label='Pubsub Topic'),
        'id_label': DisplayDataItem(self.id_label, label='ID Label Attribute'),
        'with_attributes': DisplayDataItem(
            True, label='With Attributes').drop_if_none(),
        'timestamp_attribute': DisplayDataItem(
            self.timestamp_attribute, label='Timestamp Attribute'),
    }


PROJECT_ID_REGEXP = '[a-z][-a-z0-9:.]{4,61}[a-z0-9]'
SUBSCRIPTION_REGEXP = 'projects/([^/]+)/subscriptions/(.+)'
TOPIC_REGEXP = 'projects/([^/]+)/topics/(.+)'


def parse_topic(full_topic: str) -> Tuple[str, str]:
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


# TODO(BEAM-27443): Remove (or repurpose as a proper PTransform).
class _PubSubSource(iobase.SourceBase):
  """Source for a Cloud Pub/Sub topic or subscription.

  This ``NativeSource`` is overridden by a native Pubsub implementation.

  Attributes:
    with_attributes: If False, will fetch just message data. Otherwise,
      fetches ``PubsubMessage`` protobufs.
  """
  def __init__(
      self,
      topic: Optional[str] = None,
      subscription: Optional[str] = None,
      id_label: Optional[str] = None,
      with_attributes: bool = False,
      timestamp_attribute: Optional[str] = None):
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

  def display_data(self):
    return {
        'id_label': DisplayDataItem(self.id_label,
                                    label='ID Label Attribute').drop_if_none(),
        'topic': DisplayDataItem(self.full_topic,
                                 label='Pubsub Topic').drop_if_none(),
        'subscription': DisplayDataItem(
            self.full_subscription, label='Pubsub Subscription').drop_if_none(),
        'with_attributes': DisplayDataItem(
            self.with_attributes, label='With Attributes').drop_if_none(),
        'timestamp_attribute': DisplayDataItem(
            self.timestamp_attribute,
            label='Timestamp Attribute').drop_if_none(),
    }

  def default_output_coder(self):
    return self.coder

  def is_bounded(self):
    return False


class _PubSubWriteDoFn(DoFn):
  """DoFn for writing messages to Cloud Pub/Sub.
  
  This DoFn handles both streaming and batch modes by buffering messages
  and publishing them in batches to optimize performance.
  """
  BUFFER_SIZE_ELEMENTS = 100
  FLUSH_TIMEOUT_SECS = 5 * 60  # 5 minutes

  def __init__(self, transform):
    self.project = transform.project
    self.short_topic_name = transform.topic_name
    self.id_label = transform.id_label
    self.timestamp_attribute = transform.timestamp_attribute
    self.with_attributes = transform.with_attributes

    # TODO(https://github.com/apache/beam/issues/18939): Add support for
    # id_label and timestamp_attribute.
    # Only raise errors for DirectRunner or batch pipelines
    pipeline_options = transform.pipeline_options
    output_labels_supported = True

    if pipeline_options:
      from apache_beam.options.pipeline_options import StandardOptions

      # Check if using DirectRunner
      try:
        # Get runner from pipeline options
        all_options = pipeline_options.get_all_options()
        runner_name = all_options.get('runner', StandardOptions.DEFAULT_RUNNER)

        # Check if it's a DirectRunner variant
        if (runner_name is None or
            (runner_name in StandardOptions.LOCAL_RUNNERS or 'DirectRunner'
             in str(runner_name) or 'TestDirectRunner' in str(runner_name))):
          output_labels_supported = False
      except Exception:
        # If we can't determine runner, assume DirectRunner for safety
        output_labels_supported = False

      # Check if in batch mode (not streaming)
      standard_options = pipeline_options.view_as(StandardOptions)
      if not standard_options.streaming:
        output_labels_supported = False
    else:
      # If no pipeline options available, fall back to original behavior
      output_labels_supported = False

    # Log debug information for troubleshooting
    import logging
    runner_info = getattr(
        pipeline_options, 'runner',
        'None') if pipeline_options else 'No options'
    streaming_info = 'Unknown'
    if pipeline_options:
      try:
        standard_options = pipeline_options.view_as(StandardOptions)
        streaming_info = 'streaming=%s' % standard_options.streaming
      except Exception:
        streaming_info = 'streaming=unknown'

    logging.debug(
        'PubSub unsupported feature check: runner=%s, %s',
        runner_info,
        streaming_info)

    if not output_labels_supported:

      if transform.id_label:
        raise NotImplementedError(
            f'id_label is not supported for PubSub writes with DirectRunner '
            f'or in batch mode (runner={runner_info}, {streaming_info})')
      if transform.timestamp_attribute:
        raise NotImplementedError(
            f'timestamp_attribute is not supported for PubSub writes with '
            f'DirectRunner or in batch mode '
            f'(runner={runner_info}, {streaming_info})')

  def setup(self):
    from google.cloud import pubsub
    self._pub_client = pubsub.PublisherClient()
    self._topic = self._pub_client.topic_path(
        self.project, self.short_topic_name)

  def start_bundle(self):
    self._buffer = []

  def process(self, elem):
    self._buffer.append(elem)
    if len(self._buffer) >= self.BUFFER_SIZE_ELEMENTS:
      self._flush()

  def finish_bundle(self):
    self._flush()

  def _flush(self):
    if not self._buffer:
      return

    import time

    # The elements in buffer are serialized protobuf bytes from the previous
    # transforms. We need to deserialize them to extract data and attributes.
    futures = []
    for elem in self._buffer:
      # Deserialize the protobuf to get the original PubsubMessage
      pubsub_msg = PubsubMessage._from_proto_str(elem)

      # Publish with the correct data and attributes
      if self.with_attributes and pubsub_msg.attributes:
        future = self._pub_client.publish(
            self._topic, pubsub_msg.data, **pubsub_msg.attributes)
      else:
        future = self._pub_client.publish(self._topic, pubsub_msg.data)

      futures.append(future)

    timer_start = time.time()
    for future in futures:
      remaining = self.FLUSH_TIMEOUT_SECS - (time.time() - timer_start)
      if remaining <= 0:
        raise TimeoutError(
            f"PubSub publish timeout exceeded {self.FLUSH_TIMEOUT_SECS} seconds"
        )
      future.result(remaining)
    self._buffer = []


class _PubSubSink(object):
  """Sink for a Cloud Pub/Sub topic.

  This sink works for both streaming and batch pipelines by using a DoFn
  that buffers and batches messages for efficient publishing.
  """
  def __init__(
      self,
      topic: str,
      id_label: Optional[str],
      timestamp_attribute: Optional[str],
  ):
    self.coder = coders.BytesCoder()
    self.full_topic = topic
    self.id_label = id_label
    self.timestamp_attribute = timestamp_attribute

    self.project, self.topic_name = parse_topic(topic)


class PubSubSourceDescriptor(NamedTuple):
  """A PubSub source descriptor for ``MultipleReadFromPubSub```

  Attributes:
    source: Existing Cloud Pub/Sub topic or subscription to use in the
      form "projects/<project>/topics/<topic>" or
      "projects/<project>/subscriptions/<subscription>"
    id_label: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier. When specified, the value of this attribute (which
      can be any string that uniquely identifies the record) will be used for
      deduplication of messages. If not provided, we cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
    timestamp_attribute: Message value to use as element timestamp. If None,
      uses message publishing time as the timestamp.

      Timestamp values should be in one of two formats:

      - A numerical value representing the number of milliseconds since the
        Unix epoch.
      - A string in RFC 3339 format, UTC timezone. Example:
        ``2015-10-29T23:41:41.123Z``. The sub-second component of the
        timestamp is optional, and digits beyond the first three (i.e., time
        units smaller than milliseconds) may be ignored.
  """
  source: str
  id_label: str = None
  timestamp_attribute: str = None


PUBSUB_DESCRIPTOR_REGEXP = 'projects/([^/]+)/(topics|subscriptions)/(.+)'


class MultipleReadFromPubSub(PTransform):
  """A ``PTransform`` that expands ``ReadFromPubSub`` to read from multiple
  ``PubSubSourceDescriptor``.

  The `MultipleReadFromPubSub` transform allows you to read multiple topics
  and/or subscriptions using just one transform. It is the recommended transform
  to read multiple Pub/Sub sources when the output `PCollection` are going to be
  flattened. The transform takes a list of `PubSubSourceDescriptor` and organize
  them by type (topic / subscription) and project:::

    topic_1 = PubSubSourceDescriptor('projects/myproject/topics/a_topic')
    topic_2 = PubSubSourceDescriptor(
                'projects/myproject2/topics/b_topic',
                'my_label',
                'my_timestamp_attribute')
    subscription_1 = PubSubSourceDescriptor(
                'projects/myproject/subscriptions/a_subscription')

    results = pipeline | MultipleReadFromPubSub(
                [topic_1, topic_2, subscription_1])
  """
  def __init__(
      self,
      pubsub_source_descriptors: List[PubSubSourceDescriptor],
      with_attributes: bool = False,
  ):
    """Initializes ``PubSubMultipleReader``.

    Args:
      pubsub_source_descriptors: List of Cloud Pub/Sub topics or subscriptions
        of type `~PubSubSourceDescriptor`.
      with_attributes:
        True - input elements will be :class:`~PubsubMessage` objects.
        False - input elements will be of type ``bytes`` (message data only).
    """
    self.pubsub_source_descriptors = pubsub_source_descriptors
    self.with_attributes = with_attributes

    for descriptor in self.pubsub_source_descriptors:
      match_descriptor = re.match(PUBSUB_DESCRIPTOR_REGEXP, descriptor.source)

      if not match_descriptor:
        raise ValueError(
            'PubSub source descriptor must be in the form "projects/<project>'
            '/topics/<topic>" or "projects/<project>/subscription'
            '/<subscription>" (got %r).' % descriptor.source)

  def expand(self, pcol):
    sources_pcol = []
    for descriptor in self.pubsub_source_descriptors:
      source_match = re.match(PUBSUB_DESCRIPTOR_REGEXP, descriptor.source)
      source_project = source_match.group(1)
      source_type = source_match.group(2)
      source_name = source_match.group(3)

      read_step_name = 'PubSub %s/project:%s/Read %s' % (
          source_type, source_project, source_name)

      if source_type == 'topics':
        current_source = pcol | read_step_name >> ReadFromPubSub(
            topic=descriptor.source,
            id_label=descriptor.id_label,
            with_attributes=self.with_attributes,
            timestamp_attribute=descriptor.timestamp_attribute)
      else:
        current_source = pcol | read_step_name >> ReadFromPubSub(
            subscription=descriptor.source,
            id_label=descriptor.id_label,
            with_attributes=self.with_attributes,
            timestamp_attribute=descriptor.timestamp_attribute)

      sources_pcol.append(current_source)

    return tuple(sources_pcol) | Flatten()
