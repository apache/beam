from apache_beam.transforms import Map, PTransform
from apache_beam.io.gcp.pubsublite.external import ReadExternal, WriteExternal

try:
  from google.cloud import pubsublite
except ImportError:
  pubsublite = None


class ReadFromPubSubLite(PTransform):
  """A ``PTransform`` for reading from Pub/Sub Lite."""

  def __init__(
      self,
      subscription_path,
      min_bundle_timeout=None,
      deduplicate=None
  ):
    """Initializes ``ReadFromPubSubLite``.

    Args:
      subscription_path: Pub/Sub Lite Subscription in the form
          "projects/<project>/locations/<location>/subscriptions/<subscription>".
      min_bundle_timeout: The minimum wall time to pass before allowing
          bundle closure. Setting this to too small of a value will result in
          increased compute costs and lower throughput per byte. Immediate
          timeouts (0) may be useful for testing.
      deduplicate: Whether to deduplicate messages based on the value of
          the 'x-goog-pubsublite-dataflow-uuid' attribute. Defaults to False.
    """
    super().__init__()
    self._source = ReadExternal(
      subscription_path=subscription_path,
      min_bundle_timeout=min_bundle_timeout,
      deduplicate=deduplicate
    )

  def expand(self, pvalue):
    pcoll = pvalue.pipeline | self._source
    pcoll.element_type = bytes
    pcoll = pcoll | Map(pubsublite.SequencedMessage.deserialize)
    pcoll.element_type = pubsublite.SequencedMessage
    return pcoll


class WriteToPubSubLite(PTransform):
  """A ``PTransform`` for writing to Pub/Sub Lite."""

  def __init__(
      self,
      topic_path,
      add_uuids=None
  ):
    """Initializes ``WriteToPubSubLite``.

    Args:
      topic_path: A Pub/Sub Lite Topic path.
      add_uuids: Whether to add uuids to the 'x-goog-pubsublite-dataflow-uuid'
          uuid attribute. Defaults to False.
    """
    super().__init__()
    self._source = WriteExternal(
      topic_path=topic_path,
      add_uuids=add_uuids
    )

  @staticmethod
  def _message_to_proto_str(element: pubsublite.PubSubMessage):
    if not isinstance(element, pubsublite.PubSubMessage):
      raise TypeError(
        'Unexpected element. Type: %s (expected: PubSubMessage), '
        'value: %r' % (type(element), element))
    return pubsublite.PubSubMessage.serialize(element)

  def expand(self, pcoll):
    pcoll = pcoll | Map(WriteToPubSubLite._message_to_proto_str)
    pcoll.element_type = bytes
    pcoll = pcoll | self._source
    return pcoll
