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

Cloud Pub/Sub sources and sinks are currently supported only in streaming
pipelines, during remote execution.
"""

from __future__ import absolute_import

from apache_beam import coders
from apache_beam.io import iobase


class PubSubSource(iobase.NativeSource):
  """Source for reading from a given Cloud Pub/Sub topic.

  Attributes:
    topic: Cloud Pub/Sub topic in the form "/topics/<project>/<topic>".
    subscription: Optional existing Cloud Pub/Sub subscription to use in the
      form "projects/<project>/subscriptions/<subscription>".
    id_label: The attribute on incoming Pub/Sub messages to use as a unique
      record identifier.  When specified, the value of this attribute (which can
      be any string that uniquely identifies the record) will be used for
      deduplication of messages.  If not provided, Dataflow cannot guarantee
      that no duplicate data will be delivered on the Pub/Sub stream. In this
      case, deduplication of the stream will be strictly best effort.
    coder: The Coder to use for decoding incoming Pub/Sub messages.
  """

  def __init__(self, topic, subscription=None, id_label=None,
               coder=coders.StrUtf8Coder()):
    self.topic = topic
    self.subscription = subscription
    self.id_label = id_label
    self.coder = coder

  @property
  def format(self):
    """Source format name required for remote execution."""
    return 'pubsub'

  def reader(self):
    raise NotImplementedError(
        'PubSubSource is not supported in local execution.')


class PubSubSink(iobase.NativeSink):
  """Sink for writing to a given Cloud Pub/Sub topic."""

  def __init__(self, topic, coder=coders.StrUtf8Coder()):
    self.topic = topic
    self.coder = coder

  @property
  def format(self):
    """Sink format name required for remote execution."""
    return 'pubsub'

  def writer(self):
    raise NotImplementedError(
        'PubSubSink is not supported in local execution.')
