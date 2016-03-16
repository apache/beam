# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Google Cloud PubSub sources and sinks.

Cloud Pub/Sub sources and sinks are currently supported only in streaming
pipelines, during remote execution.
"""

from __future__ import absolute_import

from google.cloud.dataflow import coders
from google.cloud.dataflow.io import iobase


class PubSubSource(iobase.Source):
  """Source for reading from a given Cloud Pub/Sub topic."""

  def __init__(self, topic, subscription=None, coder=coders.StrUtf8Coder()):
    self.topic = topic
    self.subscription = subscription
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
