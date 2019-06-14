import contextlib

from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import WriteToPubSub
from apache_beam.runners.interactive.caching import PCollectionCache

try:
  from google.cloud import pubsub_v1
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

  def __init__(self, location, **writer_kwargs):
    self.topic_path = location
    publisher = pubsub_v1.PublisherClient()
    _ = publisher.create_topic(self.topic_path)

    def _create_subscription(topic_path):
      with pubsub_subscriber_client() as sub_client:
        for _ in range(12):
          subscription_path = (
              topic_path.replace("/topics/", "/subscriptions/") +
              uuid.uuid1().hex)
          try:
            _ = sub_client.create_subscription(subscription_path, topic_path)
            return subscription_path
          except AlreadyExists:
            pass
        raise Exception("Could not create a subscription for topic '{}'. "
                        "Last subscription path that we tried was '{}'.".format(
                            topic_path, subscription_path))

    self.subscription_path = _create_subscription(self.topic_path)

    if "coder" in writer_kwargs:
      writer_kwargs.remove("coder")
    self._writer_kwargs = writer_kwargs
    self._reader_kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    self._reader_kwargs["subscription"] = self.subscription_path

  def source(self, **reader_kwargs):
    kwargs = self._reader_kwargs.copy()
    kwargs.update(reader_kwargs)
    return self._reader_class(self.file_pattern, **kwargs)._source

  def sink(self):
    return self._writer_class(self.file_path_prefix,
                              **self._writer_kwargs)._sink

  def read(self, limit=None, **reader_kwargs):
    kwargs = self._reader_kwargs.copy()
    kwargs.update(reader_kwargs)
    timestamp_attribute = kwargs.get("timestamp_attribute")

    def _get_element(message):
      # Copied from direct.transform_evaluator._PubSubReadEvaluator._get_element
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

    with pubsub_subscriber_client() as sub_client:
      response = sub_client.pull(self.subscription_path,
                                 max_messages=limit,
                                 return_immediately=True)
      result = [_get_element(rm.message) for rm in response.received_messages]
      ack_ids = [rm.ack_id for rm in response.received_messages]
      if ack_ids:
        sub_client.acknowledge(self._sub_name, ack_ids)

    return result

  def clear(self):
    with pubsub_subscriber_client() as sub_client:
      for subscription in sub_client.list_topic_subscriptions(self.topic_path):
        sub_client.delete_subscription(subscription)
    publisher = pubsub_v1.PublisherClient()
    publisher.delete_topic(self.topic_path)


@contextlib.contextmanager
def pubsub_subscriber_client():
  # TODO(ostrokach): This probably should be pushed somewhere upstream.
  # https://github.com/googleapis/google-cloud-python/issues/5523
  sub_client = pubsub.SubscriberClient()
  try:
    yield sub_client
  finally:
    sub_client.api.transport.channel.close()
