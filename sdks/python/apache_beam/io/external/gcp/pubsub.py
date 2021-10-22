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

# pytype: skip-file

import typing

import apache_beam as beam
from apache_beam.io.gcp import pubsub
from apache_beam.transforms import Map
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

ReadFromPubsubSchema = typing.NamedTuple(
    'ReadFromPubsubSchema',
    [
        ('topic', typing.Optional[str]),
        ('subscription', typing.Optional[str]),
        ('id_label', typing.Optional[str]),
        ('with_attributes', bool),
        ('timestamp_attribute', typing.Optional[str]),
    ])


class ReadFromPubSub(beam.PTransform):
  """An external ``PTransform`` for reading from Cloud Pub/Sub.

  Experimental; no backwards compatibility guarantees.  It requires special
  preparation of the Java SDK.  See BEAM-7870.
  """

  URN = 'beam:transform:org.apache.beam:pubsub_read:v1'

  def __init__(
      self,
      topic=None,
      subscription=None,
      id_label=None,
      with_attributes=False,
      timestamp_attribute=None,
      expansion_service=None):
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
        True - output elements will be
        :class:`~apache_beam.io.gcp.pubsub.PubsubMessage` objects.
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
    self.params = ReadFromPubsubSchema(
        topic=topic,
        subscription=subscription,
        id_label=id_label,
        with_attributes=with_attributes,
        timestamp_attribute=timestamp_attribute)
    self.expansion_service = expansion_service

  def expand(self, pbegin):
    pcoll = pbegin.apply(
        ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service))

    if self.params.with_attributes:
      pcoll = pcoll | 'FromProto' >> Map(pubsub.PubsubMessage._from_proto_str)
      pcoll.element_type = pubsub.PubsubMessage
    else:
      pcoll.element_type = bytes
    return pcoll


WriteToPubsubSchema = typing.NamedTuple(
    'WriteToPubsubSchema',
    [
        ('topic', str),
        ('id_label', typing.Optional[str]),
        # this is not implemented yet on the Java side:
        # ('with_attributes', bool),
        ('timestamp_attribute', typing.Optional[str]),
    ])


class WriteToPubSub(beam.PTransform):
  """An external ``PTransform`` for writing messages to Cloud Pub/Sub.

  Experimental; no backwards compatibility guarantees.  It requires special
  preparation of the Java SDK.  See BEAM-7870.
  """

  URN = 'beam:transform:org.apache.beam:pubsub_write:v1'

  def __init__(
      self,
      topic,
      with_attributes=False,
      id_label=None,
      timestamp_attribute=None,
      expansion_service=None):
    """Initializes ``WriteToPubSub``.

    Args:
      topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
      with_attributes:
        True - input elements will be
        :class:`~apache_beam.io.gcp.pubsub.PubsubMessage` objects.
        False - input elements will be of type ``bytes`` (message
        data only).
      id_label: If set, will set an attribute for each Cloud Pub/Sub message
        with the given name and a unique value. This attribute can then be used
        in a ReadFromPubSub PTransform to deduplicate messages.
      timestamp_attribute: If set, will set an attribute for each Cloud Pub/Sub
        message with the given name and the message's publish time as the value.
    """
    self.params = WriteToPubsubSchema(
        topic=topic,
        id_label=id_label,
        # with_attributes=with_attributes,
        timestamp_attribute=timestamp_attribute)
    self.expansion_service = expansion_service
    self.with_attributes = with_attributes

  def expand(self, pvalue):
    if self.with_attributes:
      pcoll = pvalue | 'ToProto' >> Map(pubsub.WriteToPubSub.to_proto_str)
    else:
      pcoll = pvalue | 'ToProto' >> Map(
          lambda x: pubsub.PubsubMessage(x, {})._to_proto_str())
    pcoll.element_type = bytes

    return pcoll.apply(
        ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service))
