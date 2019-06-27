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
import re
import time
import uuid

from apache_beam import coders
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.runners.direct.direct_runner import _DirectWriteToPubSubFn
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.runners.interactive.caching.datatype_inference import \
    infer_element_type
from apache_beam.transforms import Map
from apache_beam.transforms import PTransform
from apache_beam.utils.timestamp import Timestamp
from google.cloud import environment_vars as gcloud_environment_vars

try:
  from google.cloud import pubsub
  from google.api_core.exceptions import AlreadyExists, NotFound
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
      "id_label",
      "with_attributes",
      "timestamp_attribute",
      "coder",
  }

  def __init__(self, location, mode="error", coder=None, **writer_kwargs):
    if re.search("projects/.+/topics/.+", location) is None:
      raise ValueError(
          "'location' must be the path to a pubsub subscription in the form: "
          "'projects/{project}/topics/{topic}'.")

    self.location = location
    self._writer_kwargs = writer_kwargs
    self._timestamp = 0
    self._coder_was_provided = "coder" in writer_kwargs

    if mode not in ['error', 'append', 'overwrite']:
      raise ValueError(
          "'mode' must be set to one of: ['error', 'append', 'overwrite'].")
    if mode == "error" and self._topic_exists:
      raise IOError("The following topic already exists: {}".format(
          self.location))
    if mode == "overwrite":
      self.clear()

    pub_client = pubsub.PublisherClient()
    try:
      _ = pub_client.create_topic(self.location)
    except AlreadyExists:
      if mode != "append":
        raise

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
    if gcloud_environment_vars.PUBSUB_EMULATOR is None:
      _ = sub_client.create_snapshot(self.snapshot_path, self.subscription_path)

  @property
  def timestamp(self):
    return self._timestamp

  def reader(self, **reader_kwargs):
    if not self._topic_exists:
      raise IOError("Pubsub topic '{}' does not exist.".format(self.location))

    kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    kwargs.update(reader_kwargs)

    if "subscription" not in kwargs:
      kwargs["subscription"] = create_new_subscription(self.location,
                                                       self.snapshot_path)
    return PatchedReader(self._reader_class, (), kwargs)

  def writer(self):
    self._timestamp = time.time()
    writer = PatchedWriter(self._writer_class, (self.location,),
                           self._writer_kwargs)
    return writer

  def read(self, return_timestamp=False, **reader_kwargs):
    if not self._topic_exists:
      raise IOError("Pubsub topic '{}' does not exist.".format(self.location))

    kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    kwargs.update(reader_kwargs)

    if "subscription" not in kwargs:
      kwargs["subscription"] = create_new_subscription(self.location,
                                                       self.snapshot_path)

    for element in self._read_from_subscription(
        kwargs["subscription"], kwargs["coder"], return_timestamp,
        kwargs.get("with_attributes"), kwargs.get("timestamp_attribute")):
      yield element

  def _read_from_subscription(self, subscription_path, coder, return_timestamp,
                              with_attributes, timestamp_attribute):

    # TODO(ostrokach): Eventually, it would be nice to have a shared codebase
    # with `direct.transform_evaluator._PubSubReadEvaluator`.
    def get_element(message):
      parsed_message = PubsubMessage._from_message(message)
      parsed_message.data = coder.decode(parsed_message.data)
      timestamp = Timestamp(message.publish_time.seconds,
                            message.publish_time.nanos // 1000)

      if (with_attributes and timestamp_attribute and
          timestamp_attribute in parsed_message.attributes):
        rfc3339_or_milli = parsed_message.attributes[timestamp_attribute]
        try:
          timestamp = Timestamp.from_rfc3339(rfc3339_or_milli)
        except ValueError:
          try:
            timestamp = Timestamp(micros=int(rfc3339_or_milli) * 1000)
          except ValueError as e:
            raise ValueError('Bad timestamp value: %s' % e)

      return timestamp, parsed_message

    with pubsub_subscriber_client() as sub_client:
      while True:
        response = sub_client.pull(subscription_path,
                                   max_messages=100,
                                   return_immediately=True)
        result = [get_element(rm.message) for rm in response.received_messages]
        if not result:
          break
        ack_ids = [rm.ack_id for rm in response.received_messages]
        if ack_ids:
          sub_client.acknowledge(subscription_path, ack_ids)
        for timestamp, parsed_message in result:
          if not with_attributes:
            parsed_message = parsed_message.data
          if return_timestamp:
            yield timestamp, parsed_message
          else:
            yield parsed_message

  def write(self, elements):
    if self._infer_coder:
      # TODO(ostrokach): We might want to infer the element type from the first
      # N elements, rather than reading the entire iterator.
      elements = list(elements)
      element_type = infer_element_type(elements)
      coder = coders.registry.get_coder(element_type)
      self._writer_kwargs["coder"] = coder

    writer_kwargs = self._writer_kwargs.copy()
    coder = writer_kwargs.pop("coder")
    with_attributes = writer_kwargs.get("with_attributes")
    writer = self._writer_class(self.location, **writer_kwargs)

    do_fn = _DirectWriteToPubSubFn(writer._sink)
    do_fn.start_bundle()
    try:
      for element in elements:
        element = coder.encode(element)
        if with_attributes:
          element = PubsubMessage(data=element)
        do_fn.process(element)
    finally:
      do_fn.finish_bundle()

  def clear(self):
    remove_topic_and_subscriptions(self.location)
    if not self._coder_was_provided and "coder" in self._writer_kwargs:
      del self._writer_kwargs["coder"]

  @property
  def _infer_coder(self):
    return (not self._writer_kwargs.get("coder") and
            "coder" in self._reader_passthrough_arguments)

  @property
  def _topic_exists(self):
    pub_client = pubsub.PublisherClient()
    try:
      _ = pub_client.get_topic(self.location)
      return True
    except NotFound:
      return False


class PatchedReader(PTransform):

  def __init__(self, reader_class, reader_args, reader_kwargs):
    self._reader_class = reader_class
    self._reader_args = reader_args
    self._reader_kwargs = reader_kwargs

  def expand(self, pbegin):
    reader_kwargs = self._reader_kwargs.copy()
    coder = reader_kwargs.pop("coder")
    with_attributes = reader_kwargs.get("with_attributes")

    def decode_element(element):
      if with_attributes:
        element.data = coder.decode(element.data)
      else:
        element = coder.decode(element)
      return element

    reader = self._reader_class(*self._reader_args, **reader_kwargs)
    return pbegin | reader | Map(decode_element)


class PatchedWriter(PTransform):
  """

  .. note::
    This function updates the 'writer_kwargs' dictionary by assigning to
    the 'coder' key an instance of the inferred coder.
  """

  def __init__(self, writer_class, writer_args, writer_kwargs):
    self._writer_class = writer_class
    self._writer_args = writer_args
    self._writer_kwargs = writer_kwargs

  def expand(self, pcoll):
    if "coder" not in self._writer_kwargs:
      coder = coders.registry.get_coder(pcoll.element_type)
      self._writer_kwargs["coder"] = coder

    writer_kwargs = self._writer_kwargs.copy()
    coder = writer_kwargs.pop("coder")
    with_attributes = writer_kwargs.get("with_attributes")

    def encode_element(element):
      if with_attributes:
        element.data = coder.encode(element)
      else:
        element = coder.encode(element)
      return element

    writer = self._writer_class(*self._writer_args, **writer_kwargs)
    return pcoll | Map(encode_element) | writer


def create_new_subscription(topic_path, snapshot_path=None, suffix=None):
  if suffix is None:
    suffix = "-{}".format(uuid.uuid4().hex)
  sub_client = pubsub.SubscriberClient()
  subscription_path = (topic_path.replace("/topics/", "/subscriptions/") +
                       suffix)
  _ = sub_client.create_subscription(subscription_path, topic_path)
  if snapshot_path is not None:
    sub_client.seek(subscription_path, snapshot=snapshot_path)
  return subscription_path


def remove_topic_and_subscriptions(location):
  from google.api_core.exceptions import NotFound
  project_path = "/".join(location.split('/')[:2])
  pub_client = pubsub.PublisherClient()
  try:
    _ = pub_client.get_topic(location)
  except NotFound:
    return
  sub_client = pubsub.SubscriberClient()
  if gcloud_environment_vars.PUBSUB_EMULATOR is None:
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
