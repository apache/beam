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

from apache_beam import ExternalTransform
from apache_beam import pvalue
from apache_beam.coders import BytesCoder
from apache_beam.coders import VarIntCoder
from apache_beam.coders.coders import LengthPrefixCoder
from apache_beam.io.gcp import pubsub
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.transforms import Map
from apache_beam.transforms import ptransform


class ReadFromPubSub(ptransform.PTransform):
  """An external ``PTransform`` for reading from Cloud Pub/Sub."""

  _urn = 'beam:external:java:pubsub:read:v1'

  def __init__(self, topic=None, subscription=None, id_label=None,
               with_attributes=False, timestamp_attribute=None,
               expansion_service='localhost:8097'):
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
    super(ReadFromPubSub, self).__init__()
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label
    self.with_attributes = with_attributes
    self.timestamp_attribute = timestamp_attribute
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromPubSub must be a root transform")

    args = {
      'with_attributes': _encode_bool(self.with_attributes),
    }

    if self.topic is not None:
      args['topic'] = _encode_str(self.topic)

    if self.subscription is not None:
      args['subscription'] = _encode_str(self.subscription)

    if self.id_label is not None:
      args['id_label'] = _encode_str(self.id_label)

    if self.timestamp_attribute is not None:
      args['timestamp_attribute'] = _encode_str(self.timestamp_attribute)

    payload = ExternalConfigurationPayload(configuration=args)
    pcoll = pbegin.apply(
        ExternalTransform(
            self._urn,
            payload.SerializeToString(),
            self.expansion_service))
    if self.with_attributes:
      pcoll = pcoll | Map(pubsub.PubsubMessage._from_proto_str)
      pcoll.element_type = pubsub.PubsubMessage
    else:
      pcoll.element_type = bytes
    return pcoll


class WriteToPubSub(ptransform.PTransform):
  """An external ``PTransform`` for writing messages to Cloud Pub/Sub."""

  _urn = 'beam:external:java:pubsub:write:v1'

  def __init__(self, topic, with_attributes=False, id_label=None,
               timestamp_attribute=None, expansion_service='localhost:8097'):
    """Initializes ``WriteToPubSub``.

    Args:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
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
    super(WriteToPubSub, self).__init__()
    self.topic = topic
    self.with_attributes = with_attributes
    self.id_label = id_label
    self.timestamp_attribute = timestamp_attribute
    self.expansion_service = expansion_service

  def expand(self, pvalue):

    if self.with_attributes:
      pcoll = pvalue | 'ToProtobuf' >> Map(pubsub.WriteToPubSub.to_proto_str)
    else:
      pcoll = pvalue | 'ToProtobuf' >> Map(
          lambda x: pubsub.PubsubMessage(x, {})._to_proto_str())
    pcoll.element_type = bytes

    args = {
      'topic': _encode_str(self.topic),
    }

    if self.id_label is not None:
      args['id_label'] = _encode_str(self.id_label)

    if self.timestamp_attribute is not None:
      args['timestamp_attribute'] = _encode_str(self.timestamp_attribute)

    payload = ExternalConfigurationPayload(configuration=args)
    return pcoll.apply(
        ExternalTransform(
            self._urn,
            payload.SerializeToString(),
            self.expansion_service))


def _encode_str(str_obj):
  encoded_str = str_obj.encode('utf-8')
  coder = LengthPrefixCoder(BytesCoder())
  coder_urns = ['beam:coder:bytes:v1']
  return ConfigValue(
      coder_urn=coder_urns,
      payload=coder.encode(encoded_str))


def _encode_bool(bool_obj):
  coder = VarIntCoder()
  coder_urns = ['beam:coder:varint:v1']
  return ConfigValue(
      coder_urn=coder_urns,
      payload=coder.encode(int(bool_obj)))
