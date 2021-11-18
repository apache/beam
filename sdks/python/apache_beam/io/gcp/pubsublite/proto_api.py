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

from apache_beam.io.gcp.pubsublite.external import _ReadExternal
from apache_beam.io.gcp.pubsublite.external import _WriteExternal
from apache_beam.transforms import Map
from apache_beam.transforms import PTransform

try:
  from google.cloud import pubsublite
except ImportError:
  pubsublite = None


class ReadFromPubSubLite(PTransform):
  """
  A ``PTransform`` for reading from Pub/Sub Lite.

  Produces a PCollection of google.cloud.pubsublite.SequencedMessage

  Experimental; no backwards-compatibility guarantees.
  """
  def __init__(
      self,
      subscription_path,
      min_bundle_timeout=None,
      deduplicate=None,
      expansion_service=None,
  ):
    """Initializes ``ReadFromPubSubLite``.

    Args:
      subscription_path: Pub/Sub Lite Subscription in the form
          projects/<project>/locations/<location>/subscriptions/<subscription>
      min_bundle_timeout: The minimum wall time to pass before allowing
          bundle closure. Setting this to too small of a value will result in
          increased compute costs and lower throughput per byte. Immediate
          timeouts (0) may be useful for testing.
      deduplicate: Whether to deduplicate messages based on the value of
          the 'x-goog-pubsublite-dataflow-uuid' attribute. Defaults to False.
    """
    super().__init__()
    self._source = _ReadExternal(
        subscription_path=subscription_path,
        min_bundle_timeout=min_bundle_timeout,
        deduplicate=deduplicate,
        expansion_service=expansion_service,
    )

  def expand(self, pvalue):
    pcoll = pvalue.pipeline | self._source
    pcoll.element_type = bytes
    pcoll = pcoll | Map(pubsublite.SequencedMessage.deserialize)
    pcoll.element_type = pubsublite.SequencedMessage
    return pcoll


class WriteToPubSubLite(PTransform):
  """
  A ``PTransform`` for writing to Pub/Sub Lite.

  Consumes a PCollection of google.cloud.pubsublite.PubSubMessage

  Experimental; no backwards-compatibility guarantees.
  """
  def __init__(
      self,
      topic_path,
      add_uuids=None,
      expansion_service=None,
  ):
    """Initializes ``WriteToPubSubLite``.

    Args:
      topic_path: A Pub/Sub Lite Topic path.
      add_uuids: Whether to add uuids to the 'x-goog-pubsublite-dataflow-uuid'
          uuid attribute. Defaults to False.
    """
    super().__init__()
    self._source = _WriteExternal(
        topic_path=topic_path,
        add_uuids=add_uuids,
        expansion_service=expansion_service,
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
