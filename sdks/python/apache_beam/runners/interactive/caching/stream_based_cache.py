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

import contextlib
import functools
import time
import uuid
from datetime import datetime

import pytz
from future.moves import queue

import apache_beam as beam
import apache_beam.io.gcp.pubsub as beam_pubsub
from apache_beam import coders
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.runners.direct.direct_runner import _DirectWriteToPubSubFn
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.testing import datatype_inference
from apache_beam.transforms import PTransform
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize

try:
  from google import api_core
  from google.cloud import pubsub
  from google.api_core import exceptions as gexc
except ImportError:
  api_core = None
  pubsub = None
  gexc = None

__all__ = [
    "PubSubBasedCache",
]


class PubSubBasedCache(PCollectionCache):
  """A ``PCollectionCache`` object which uses Google Cloud PubSub to store
  the underlying stream of data.

  ``PubSubBasedCache`` allows the storage of an unbounded PCollection of objects
  in a stream backed by Google Cloud Pubsub. By default, elements are converted
  to bytes using a compatible coder and are written as `PubsubMessage` objects
  with an attribute ("ts" by default) containing the timestamp of the original
  message. If 'timestamp_attribute_fn' is provided, instead of using the
  pipeline timestmap, we use this function to map each element to an appropriate
  timestamp. If 'with_attributes' is set to ``True``, the encoding of elements
  to and from ``PubsubMessage`` objects is relegated to the user.

  If the underlying system supports snapshots (e.g. Google Cloud Pubsub), a
  snapshot of the root subscription is created when the cache object is
  instantiated. Every time that elements are read from the cache (with
  'seek_to_start' set to ``True``), a new subscription is created and set to
  the snapshot of the root subscription. This allows us to repeatedly read all
  elements from the time that the subscription was created.

  If the underlying system does *not* support snapshots (e.g. Google PubSub
  Emulator), we can read all elements that have ever been written to the cache
  only once. However, we can perform multiple reads with 'seek_to_start' set to
  ``False``, in which case we will obtain only those elements that have been
  written since the start of the reading process.
  """

  _reader_passthrough_arguments = {
      "id_label",
      "with_attributes",
      "timestamp_attribute",
  }
  _default_timestamp_attribute = "ts"

  def __init__(self,
               location,
               mode="error",
               persist=False,
               timestamp_attribute_fn=None,
               **writer_kwargs):
    """Initialize a PCollectionCache object.

    Args:
      location (str): A string containing the path of the topic to which cached
          messages will be published. It should have the format:
          'projects/{project_name}/topics/{topic_name}'.
      mode (str): Controls the behavior when the topic specified by location
          already exit. If "error", an IOError will be raised. If "overwrite",
          the existing topic will be removed and a new topic with the same
          name will be created. If "append", messages will be published to the
          existing topic.
      persist (bool): A flag indicating whether the underlying data should be
          destroyed when the cache instance goes out of scope.
      timestamp_attribute_fn (Callable[[Any], int): A function which takes
          as input an element of a PCollection and returns the timestamp,
          *in milliseconds*, associated with that element. This timestamp will
          be assigned to the timestamp attribute of each message published to
          Cloud PubSub.
      writer_kwargs (Dict[str, Any]): A dictionary of key-value pairs
          to be passed to the underlying writer objects.

    Raises:
      ~exceptions.ValueError: If the specified mode is not supported.
      ~exceptions.IOError: If a topic matching `location` already exist and
          mode == "error".
    """
    self.location = location
    self.timestamp_attribute_fn = timestamp_attribute_fn
    self.element_type = None
    self.default_coder = writer_kwargs.pop("coder", None)
    if "timestamp_attribute" not in writer_kwargs:
      writer_kwargs["timestamp_attribute"] = self._default_timestamp_attribute
    self._writer_kwargs = writer_kwargs
    self._persist = persist
    self._timestamp = 0
    self._finalizers = []
    self._child_subscriptions = []
    self._primary_sub_exhausted = False

    if mode not in ["error", "append", "overwrite"]:
      raise ValueError("mode must be set to 'error', 'append', or 'overwrite'.")

    # Initialize PubSub resources. Note that we cannot save pub_client and
    # sub_client as class attributes because they fail to serialize using
    # pickle.
    pub_client = pubsub.PublisherClient()
    sub_client = pubsub.SubscriberClient()

    self.project, self.topic_name = beam_pubsub.parse_topic(self.location)

    topic_path = pub_client.topic_path(self.project, self.topic_name)
    try:
      self.topic = pub_client.create_topic(topic_path)
    except gexc.AlreadyExists:
      if mode == "error":
        raise IOError("Topic '{}' already exists.".format(topic_path))
      elif mode == "overwrite":
        pub_client.delete_topic(topic_path)
        self.topic = pub_client.create_topic(topic_path)
      else:
        self.topic = pub_client.get_topic(topic_path)

    subscription_path = sub_client.subscription_path(self.project,
                                                     self.topic_name)
    try:
      self.subscription = sub_client.create_subscription(
          subscription_path, self.topic.name)
    except gexc.AlreadyExists:
      if mode == "error":
        raise IOError(
            "Subscription '{}' already exists.".format(subscription_path))
      elif mode == "overwrite":
        sub_client.delete_subscription(subscription_path)
        self.subscription = sub_client.create_subscription(
            subscription_path, self.topic.name)
      else:
        self.subscription = sub_client.get_subscription(subscription_path)

    snapshot_path = sub_client.snapshot_path(self.project, self.topic_name)
    try:
      self.snapshot = sub_client.create_snapshot(snapshot_path,
                                                 self.subscription.name)
    except gexc.AlreadyExists:
      if mode == "error":
        raise IOError("Snapshot '{}' already exists.".format(snapshot_path))
      elif mode == "overwrite":
        sub_client.delete_snapshot(snapshot_path)
        self.snapshot = sub_client.create_snapshot(snapshot_path,
                                                   self.subscription.name)
      else:
        # See: https://github.com/googleapis/google-cloud-python/issues/8554
        self.snapshot = pubsub.types.Snapshot()
        self.snapshot.name = snapshot_path
    except gexc.MethodNotImplemented:
      self.snapshot = None

    self._child_subscriptions.append(self.subscription)
    self._finalizers = self._configure_finalizers(persist)

  @property
  def timestamp(self):
    return self._timestamp

  @property
  def persist(self):
    return self._persist

  @persist.setter
  def persist(self, persist):
    if self._persist == persist:
      return
    self._persist = persist
    for finalizer in self._finalizers:
      finalizer.detach()
    self._finalizers = self._configure_finalizers(persist)

  def reader(self, seek_to_start=True, **kwargs):
    """Returns a reader ``PTransform`` to be used for reading the contents of
    the cache into a Beam Pipeline.

    Args:
      seek_to_start (bool): If ``True``, read all data that was written to the
          cache since it was created. If ``False``, only read data that is
          written to the cache *after* this method is called.
      kwargs (Dict[str, Any]): Arguments to be passed to the underlying reader
          class.
    """
    self._assert_topic_exists()

    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)

    if "subscription" not in reader_kwargs:
      reader_kwargs["subscription"] = self._create_child_subscription(
          seek_to_start=seek_to_start).name

    reader = PatchedPubSubReader(self, **reader_kwargs)
    return reader

  def writer(self):
    """Returns a writer ``PTransform`` to be used for writing the contents of a
    PCollection from a Beam Pipeline into the cache.
    """
    self._timestamp = time.time()
    writer = PatchedPubSubWriter(self, self.location, **self._writer_kwargs)
    return writer

  @contextlib.contextmanager
  def read_to_queue(self, seek_to_start=True, **kwargs):
    """Starts a worker thread which reads the contents of the cache into a
    queue.

    Args:
      seek_to_start (bool): If ``True``, read all data that was written to the
          cache since it was created. If ``False``, only read data that is
          written to the cache *after* this method is called.
      delay (float): Number of seconds to wait before returning collected
          messages. A longer delay may result in messages being more accurately
          ordered according to their timestamp.
      timeout (float): If no new messags arrive within the specified number of
          seconds, the reading process will be terminated.
      kwargs (Dict[str, Any]): Arguments to be passed to the underlying reader
          class.

    Returns:
      A priority queue that is being populated with the contents of the cache.
    """
    self._assert_topic_exists()

    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)

    if "subscription" in reader_kwargs:
      created_subsciption = None
    else:
      created_subsciption = self._create_child_subscription(
          seek_to_start=seek_to_start)
      reader_kwargs["subscription"] = created_subsciption.name
      assert created_subsciption in self._child_subscriptions

    @functools.total_ordering
    class PrioritizedTimestampedValue(TimestampedValue):

      def __lt__(self, other):
        return self.timestamp < other.timestamp

    # Set arbitrary queue size limit to prevent OOM errors.
    parsed_message_queue = queue.PriorityQueue(1000)
    coder = (
        self.default_coder if self.default_coder is not None else
        coders.registry.get_coder(self.element_type))
    if reader_kwargs.get("with_attributes"):
      decoder = beam.Map(lambda e: e)
    else:
      decoder = DecodeFromPubSub(
          coder, timestamp_attribute=reader_kwargs["timestamp_attribute"])

    def callback(msg):
      msg.ack()
      message = PubsubMessage._from_message(msg)
      timestamped_value = next(decoder.process(message))
      ordered_timestamped_value = PrioritizedTimestampedValue(
          timestamped_value.value, timestamped_value.timestamp)
      parsed_message_queue.put(ordered_timestamped_value)

    sub_client = pubsub.SubscriberClient()
    future = sub_client.subscribe(
        reader_kwargs["subscription"], callback=callback)
    try:
      yield parsed_message_queue
    finally:
      future.cancel()
      if created_subsciption is not None:
        sub_client.delete_subscription(created_subsciption.name)
        self._child_subscriptions.remove(created_subsciption)

  def read(self, seek_to_start=True, delay=0, timeout=5, **kwargs):
    """Returns an iterator over the contents of the cache.

    Args:
      seek_to_start (bool): If ``True``, read all data that was written to the
          cache since it was created. If ``False``, only read data that is
          written to the cache *after* this method is called.
      delay (float): Number of seconds to wait before returning collected
          messages. A longer delay may result in messages being more accurately
          ordered according to their timestamp.
      timeout (float): If no new messags arrive within the specified number of
          seconds, the reading process will be terminated.
      kwargs (Dict[str, Any]): Arguments to be passed to the underlying reader
          class.

    Returns:
      An iterator over the elements in the cache.
    """
    with self.read_to_queue(
        seek_to_start=seek_to_start, **kwargs) as message_queue:
      time.sleep(delay)
      while True:
        try:
          element = message_queue.get(timeout=timeout)
          yield element
        except queue.Empty:
          return

  def write(self, elements):
    """Writes a collection of elements into the cache.

    Args:
      elements (Iterable[Any]): A collection of elements to be written to cache.
    """
    if self.element_type is None:
      # TODO(ostrokach): We might want to infer the element type from the first
      # N elements, rather than reading the entire iterator.
      elements = list(elements)
      self.element_type = datatype_inference.infer_element_type(elements)

    coder = (
        self.default_coder if self.default_coder is not None else
        coders.registry.get_coder(self.element_type))
    writer_kwargs = self._writer_kwargs.copy()
    # DirectRunner does not support timestamp_attribute
    timestamp_attribute = writer_kwargs.pop("timestamp_attribute")

    if writer_kwargs.get("with_attributes"):
      encoder = None
      if self.timestamp_attribute_fn:
        raise ValueError(
            "Only one of 'with_attributes' and 'timestamp_attribute_fn' "
            "can be provided.")
    else:
      writer_kwargs["with_attributes"] = True
      encoder = EncodeToPubSub(coder, timestamp_attribute,
                               self.timestamp_attribute_fn)

    writer = WriteToPubSub(self.location, **writer_kwargs)

    do_fn = _DirectWriteToPubSubFn(writer._sink)
    do_fn.start_bundle()
    try:
      for message in elements:
        if encoder is not None:
          current_timestamp = Timestamp.from_utc_datetime(
              datetime.utcnow().replace(tzinfo=pytz.UTC))
          message = next(encoder.process(message, timestamp=current_timestamp))
        do_fn.process(message)
    finally:
      do_fn.finish_bundle()

  def truncate(self):
    """Removes all contents from the cache."""
    self.element_type = None
    self._primary_sub_exhausted = False
    sub_client = pubsub.SubscriberClient()
    try:
      sub_client.delete_subscription(self.subscription.name)
    except gexc.NotFound:
      pass
    _ = sub_client.create_subscription(self.subscription.name, self.location)
    if self.snapshot is not None:
      try:
        sub_client.delete_snapshot(self.snapshot.name)
      except gexc.NotFound:
        pass
      _ = sub_client.create_snapshot(self.snapshot.name, self.subscription.name)

  def remove(self):
    """Deletes the cache, including all underlying data.

    The cache should not be used after this method is called.
    """
    for finalizer in self._finalizers:
      finalizer()

  @property
  def removed(self):
    return not any(finalizer.alive for finalizer in self._finalizers)

  def __del__(self):
    self.remove()

  @property
  def _reader_kwargs(self):
    reader_kwargs = {
        k: v for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    return reader_kwargs

  def _configure_finalizers(self, persist):
    if persist:
      return []

    pub_client = pubsub.PublisherClient()
    sub_client = pubsub.SubscriberClient()

    def delete_subscriptions(subscriptions):
      for sub in subscriptions:
        sub_client.delete_subscription(sub.name)

    finalizers = []
    if self.snapshot is not None:
      finalizers += [
          finalize(self, sub_client.delete_snapshot, self.snapshot.name)
      ]
    finalizers += [
        finalize(self, delete_subscriptions, self._child_subscriptions),
        finalize(self, pub_client.delete_topic, self.topic.name),
    ]
    return finalizers

  def _assert_topic_exists(self):
    pub_client = pubsub.PublisherClient()
    try:
      _ = pub_client.get_topic(self.topic.name)
    except gexc.NotFound:
      raise IOError("Pubsub topic '{}' does not exist.".format(self.topic.name))

  def _create_child_subscription(self, seek_to_start=True):
    sub_client = pubsub.SubscriberClient()
    sub_path = None
    existing_sub_paths = [
        sub.name for sub in ([self.subscription] + self._child_subscriptions)
    ]
    while sub_path is None or sub_path in existing_sub_paths:
      sub_path = sub_client.subscription_path(
          self.project, self.topic_name + '-' + uuid.uuid4().hex)
    if not seek_to_start or self.snapshot is not None:
      subscription = sub_client.create_subscription(sub_path, self.location)
      self._child_subscriptions.append(subscription)
      if seek_to_start:
        sub_client.seek(
            subscription.name,
            snapshot=self.snapshot.name,
            retry=api_core.retry.Retry())
    elif not self._primary_sub_exhausted:
      subscription = self.subscription
      self._primary_sub_exhausted = True
    else:
      raise ValueError(
          "Cannot seek_to_start more than once in an environment where "
          "snapshots are not supported.")
    return subscription


class PatchedPubSubReader(PTransform):
  """A wrapper over :class:`~apache_beam.io.gcp.ReadFromPubSub`.

  If 'with_attributes' is ``False``, ``PatchedPubSubReader`` decodes the
  messages using the same coder that was used to encode them and assigns to the
  resulting elements a timestamp from the 'timestamp_attribute' of the message.

  If 'with_attributes' is ``True``, the behavior is identical to that of
  :class:`~apache_beam.io.gcp.ReadFromPubSub`.
  """

  def __init__(self, cache, *reader_args, **reader_kwargs):
    self._cache = cache
    self._reader_args = reader_args
    self._reader_kwargs = reader_kwargs

    if "timestamp_attribute" not in reader_kwargs:
      raise ValueError(
          "timestamp_attribute must be specified when reading from cache.")

  def expand(self, pbegin):
    reader_kwargs = self._reader_kwargs.copy()

    reader = ReadFromPubSub(*self._reader_args, **reader_kwargs)
    if reader_kwargs.get("with_attributes"):
      return pbegin | reader
    else:
      coder = (
          self._cache.default_coder if self._cache.default_coder is not None
          else coders.registry.get_coder(self._cache.element_type))
      decoder = DecodeFromPubSub(coder, reader_kwargs["timestamp_attribute"])
      return pbegin | reader | beam.ParDo(decoder)


class DecodeFromPubSub(beam.DoFn):
  """``DecodeFromPubSub`` decodes ``PubsubMessage`` objects using the provided
  coder and wraps the resulting elements inside ``TimestampedValue`` objects,
  with the timestamp of those objects defined by 'timestamp_attribute' of the
  incoming messages.
  """

  def __init__(self, coder, timestamp_attribute):
    super(DecodeFromPubSub, self).__init__()
    self.coder = coder
    self.timestamp_attribute = timestamp_attribute

  def process(self, message):
    rfc3339_or_milli = message.attributes[self.timestamp_attribute]
    try:
      timestamp = Timestamp(micros=int(rfc3339_or_milli) * 1000.0)
    except ValueError:
      timestamp = Timestamp.from_rfc3339(rfc3339_or_milli)
    element = self.coder.decode(message.data)
    yield TimestampedValue(element, timestamp)


class PatchedPubSubWriter(PTransform):
  """A wrapper over :class:`~apache_beam.io.gcp.WriteToPubSub`.

  When ``PatchedPubSubWriter`` is expanded onto a Beam Pipeline, it assigns
  to the cache the element_type of the parent PCollection.

  If 'with_attributes' is ``False``, ``PatchedPubSubWriter`` encodes the
  elements of the parent PCollection using an appropriate coder and converts
  those elements into ``PubsubMessage`` objects with a 'timestamp_attribute'
  that either is obtained using 'timestamp_attribute_fn' or is inferred from
  the timestamp of the element.

  If 'with_attributes' is ``True``, the behavior of ``PatchedPubSubWriter`` is
  identical to that of :class:`~apache_beam.io.gcp.WriteToPubSub`.
  """

  def __init__(self, cache, *writer_args, **writer_kwargs):
    self._cache = cache
    self._writer_args = writer_args
    self._writer_kwargs = writer_kwargs

  def expand(self, pcoll):
    if self._cache.element_type is None:
      self._cache.element_type = pcoll.element_type

    writer_kwargs = self._writer_kwargs.copy()
    # DirectRunner does not support timestamp_attribute
    timestamp_attribute = writer_kwargs.pop("timestamp_attribute")

    if writer_kwargs.get("with_attributes"):
      writer = WriteToPubSub(*self._writer_args, **writer_kwargs)
      return pcoll | writer
    else:
      # Encode the element as a PubsubMessage ourselves
      writer_kwargs["with_attributes"] = True
      coder = (
          self._cache.default_coder if self._cache.default_coder is not None
          else coders.registry.get_coder(self._cache.element_type))
      encoder = EncodeToPubSub(coder, timestamp_attribute,
                               self._cache.timestamp_attribute_fn)
      writer = WriteToPubSub(*self._writer_args, **writer_kwargs)
      return pcoll | beam.ParDo(encoder) | writer


class EncodeToPubSub(beam.DoFn):
  """``EncodeToPubSub`` encodes elements using the provided coder and converts
  those elements into ``PubsubMessage`` objects, with the 'timestamp_attribute'
  attribute of those objects either obtained using 'timestamp_attribute_fn'
  or inferred from the timestamp of the incoming elements.
  """

  def __init__(self, coder, timestamp_attribute, timestamp_attribute_fn=None):
    self.coder = coder
    self.timestamp_attribute = timestamp_attribute
    self.timestamp_attribute_fn = timestamp_attribute_fn

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
    if self.timestamp_attribute_fn is None:
      timestamp = int(timestamp.micros / 1000.0)
    else:
      timestamp = self.timestamp_attribute_fn(element)
    attributes = {self.timestamp_attribute: str(timestamp)}
    element_bytes = self.coder.encode(element)
    message = PubsubMessage(element_bytes, attributes)
    yield message
