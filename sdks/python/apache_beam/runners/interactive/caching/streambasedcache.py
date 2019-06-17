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
import contextlib
import time
import uuid

from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.runners.direct.direct_runner import _DirectWriteToPubSubFn
from apache_beam.runners.interactive.caching import PCollectionCache

try:
  from google.cloud import pubsub
  from google.api_core.exceptions import AlreadyExists
except ImportError:
  pubsub = None

__all__ = [
    "StreamBasedCache",
    "PubSubBasedCache",
]


class StreamBasedCache(PCollectionCache):
  pass


class PubSubBasedCache(StreamBasedCache):

  _reader_class = ReadFromPubSub
  _writer_class = WriteToPubSub
  _reader_passthrough_arguments = {
      "id_label", "with_attributes", "timestamp_attribute"
  }

  def __init__(self, location, if_exists="error", **writer_kwargs):
    self.location = location
    assert self.location.startswith("projects/")
    assert "/topics/" in self.location
    self._writer_kwargs = writer_kwargs

    pub_client = pubsub.PublisherClient()
    try:
      _ = pub_client.create_topic(self.location)
    except AlreadyExists:
      if if_exists == "error":
        raise
      elif if_error == "overwrite":
        self.clear()
        _ = publisher.create_topic(self.location)
      else:
        raise ValueError(
            '`if_exists` must be set to either "error" or "overwrite".')

    sub_client = pubsub.SubscriberClient()
    self.subscription_path = (
        self.location.replace("/topics/", "/subscriptions/") + "-subscription")
    assert self.subscription_path.startswith("projects/")
    assert "/subscriptions/" in self.subscription_path
    _ = sub_client.create_subscription(self.subscription_path, self.location)

    self.snapshot_path = (
        self.subscription_path.replace("/subscriptions/", "/snapshots/") +
        "-snapshot")
    assert self.snapshot_path.startswith("projects/")
    assert "/snapshots/" in self.snapshot_path
    _ = sub_client.create_snapshot(self.snapshot_path, self.subscription_path)

  def reader(self, **reader_kwargs):
    kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    kwargs.update(reader_kwargs)
    if "subscription" not in kwargs:
      kwargs["subscription"] = create_new_subscription(self.location,
                                                       self.snapshot_path)
    return self._reader_class(**kwargs)

  def writer(self):
    writer = self._writer_class(self.location, **self._writer_kwargs)
    return writer

  def read(self, **reader_kwargs):
    # TODO(ostrokach): Eventually, it would be nice to have a shared codebase
    # with `direct.transform_evaluator._PubSubReadEvaluator`.
    kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    kwargs.update(reader_kwargs)

    timestamp_attribute = kwargs.get("timestamp_attribute")

    def _get_element(message):
      parsed_message = PubsubMessage._from_message(message)
      if (timestamp_attribute and
          timestamp_attribute in parsed_message.attributes):
        rfc3339_or_milli = parsed_message.attributes[timestamp_attribute]
        try:
          timestamp = Timestamp.from_rfc3339(rfc3339_or_milli)
        except ValueError:
          try:
            timestamp = Timestamp(micros=int(rfc3339_or_milli) * 1000)
          except ValueError as e:
            raise ValueError('Bad timestamp value: %s' % e)
      else:
        timestamp = Timestamp(message.publish_time.seconds,
                              message.publish_time.nanos // 1000)

      return timestamp, parsed_message

    subscription_path = create_new_subscription(self.location,
                                                self.snapshot_path)
    with pubsub_subscriber_client() as sub_client:
      while True:
        response = sub_client.pull(subscription_path,
                                   max_messages=100,
                                   return_immediately=True)
        result = [_get_element(rm.message) for rm in response.received_messages]
        if not result:
          break
        ack_ids = [rm.ack_id for rm in response.received_messages]
        if ack_ids:
          sub_client.acknowledge(self._sub_name, ack_ids)
        for element in result:
          yield element

  def write(self, elements):
    writer = self.writer()
    do_fn = _DirectWriteToPubSubFn(writer._sink)
    do_fn.start_bundle()
    try:
      for element in elements:
        do_fn.process(element)
    finally:
      do_fn.finish_bundle()

  def clear(self):
    remove_topic_and_subscriptions(self.location)


def create_new_subscription(topic_path, snapshot_path=None, suffix=None):
  if suffix is None:
    suffix = "-{}".format(uuid.uuid1().hex)
  sub_client = pubsub.SubscriberClient()
  subscription_path = (topic_path.replace("/topics/", "/subscriptions/") +
                       suffix)
  _ = sub_client.create_subscription(subscription_path, topic_path)
  if snapshot_path is not None:
    sub_client.seek(subscription_path, snapshot=snapshot_path)
  return subscription_path


def remove_topic_and_subscriptions(location):
  project_path = "/".join(location.split('/')[:2])
  pub_client = pubsub.PublisherClient()
  sub_client = pubsub.SubscriberClient()
  for snapshot in list(sub_client.list_snapshots(project_path)):
    if snapshot.topic == location:
      sub_client.delete_snapshot(snapshot.name)
  for subscription in pub_client.list_topic_subscriptions(location):
    sub_client.delete_subscription(subscription)
  pub_client.delete_topic(location)


@contextlib.contextmanager
def pubsub_subscriber_client():
  # TODO(ostrokach): This probably should be pushed somewhere upstream.
  # https://github.com/googleapis/google-cloud-python/issues/5523
  sub_client = pubsub.SubscriberClient()
  try:
    yield sub_client
  finally:
    sub_client.api.transport.channel.close()
