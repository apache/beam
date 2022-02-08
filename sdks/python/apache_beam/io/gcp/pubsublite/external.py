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

"""Google Pub/Sub Lite sources and sinks.

This API is currently under development and is subject to change.
"""

# pytype: skip-file

import typing

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

_ReadSchema = typing.NamedTuple(
    '_ReadSchema',
    [('subscription_path', str), ('min_bundle_timeout', int),
     ('deduplicate', bool)])


def _default_io_expansion_service():
  return BeamJarExpansionService(
      'sdks:java:io:google-cloud-platform:expansion-service:shadowJar')


class _ReadExternal(ExternalTransform):
  """
    An external PTransform which reads from Pub/Sub Lite and returns a
    SequencedMessage as serialized bytes.

    This transform is not part of the public API.

    Experimental; no backwards-compatibility guarantees.
  """
  def __init__(
      self,
      subscription_path,
      min_bundle_timeout=None,
      deduplicate=None,
      expansion_service=None,
  ):
    """
    Initializes a read operation from Pub/Sub Lite, returning the serialized
    bytes of SequencedMessage protos.

    Args:
      subscription_path: A Pub/Sub Lite Subscription path.
      min_bundle_timeout: The minimum wall time to pass before allowing
          bundle closure. Setting this to too small of a value will result in
          increased compute costs and lower throughput per byte. Immediate
          timeouts (0) may be useful for testing.
      deduplicate: Whether to deduplicate messages based on the value of
          the 'x-goog-pubsublite-dataflow-uuid' attribute.
    """
    if min_bundle_timeout is None:
      min_bundle_timeout = 60 * 1000
    if deduplicate is None:
      deduplicate = False
    if expansion_service is None:
      expansion_service = _default_io_expansion_service()
    super().__init__(
        'beam:transform:org.apache.beam:pubsublite_read:v1',
        NamedTupleBasedPayloadBuilder(
            _ReadSchema(
                subscription_path=subscription_path,
                min_bundle_timeout=min_bundle_timeout,
                deduplicate=deduplicate)),
        expansion_service)


_WriteSchema = typing.NamedTuple(
    '_WriteSchema', [('topic_path', str), ('add_uuids', bool)])


class _WriteExternal(ExternalTransform):
  """
    An external PTransform which writes serialized PubSubMessage protos to
    Pub/Sub Lite.

    This transform is not part of the public API.

    Experimental; no backwards-compatibility guarantees.
  """
  def __init__(
      self,
      topic_path,
      add_uuids=None,
      expansion_service=None,
  ):
    """
    Initializes a write operation to Pub/Sub Lite, writing the serialized bytes
    of PubSubMessage protos.

    Args:
      topic_path: A Pub/Sub Lite Topic path.
      add_uuids: Whether to add uuids to the 'x-goog-pubsublite-dataflow-uuid'
          uuid attribute.
    """
    if add_uuids is None:
      add_uuids = False
    if expansion_service is None:
      expansion_service = _default_io_expansion_service()
    super().__init__(
        'beam:transform:org.apache.beam:pubsublite_write:v1',
        NamedTupleBasedPayloadBuilder(
            _WriteSchema(
                topic_path=topic_path,
                add_uuids=add_uuids,
            )),
        expansion_service)
