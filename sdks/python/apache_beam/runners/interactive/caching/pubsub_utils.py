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

import uuid
import warnings
from datetime import datetime

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize

try:
  from google.api_core import exceptions as gexc
  from google.cloud import pubsub
except ImportError:
  gexc = None
  pubsub = None

__all__ = [
    "TemporaryPubsubTopic", "TemporaryPubsubSubscription",
    "TemporaryPubsubSnapshot"
]


def generate_unique_name(prefix):
  unique_name = "{}-{}-{}".format(prefix,
                                  datetime.now().strftime("%Y-%m-%d-%H%M%S"),
                                  uuid.uuid4().hex[:8])
  return unique_name


class TemporaryPubsubTopic(object):
  """Create a temporary PubSub topic.

  When the resulting object goes out of scope, the underlying topic is
  deleted. The result can also be used as a context manager, in which case
  the topic is deleted once the context is closed.

  Attributes:
    name (str): The name of the temporary topic.
  """

  def __init__(self, project, prefix="temp"):
    self.pub_client = pubsub.PublisherClient()
    self.project = project

    num_tires = 5
    for _ in range(num_tires):
      try:
        self.topic = self.pub_client.create_topic(
            self.pub_client.topic_path(self.project,
                                       generate_unique_name(prefix)))
        break
      except gexc.AlreadyExists:
        pass
    else:
      raise ValueError(
          "Could not create a unique topic after {} tries.".format(num_tires))

    self._finalizer = finalize(
        self,
        self._cleanup,
        self.pub_client,
        self.topic.name,
        warn_message="Implicitly cleaning up {!r}".format(self))

  @property
  def name(self):
    return self.topic.name

  @classmethod
  def _cleanup(cls, pub_client, name, warn_message):
    pub_client.delete_topic(name)
    warnings.warn(warn_message, ResourceWarning)

  def cleanup(self):
    if self._finalizer.detach():
      self.pub_client.delete_topic(self.topic.name)

  def __enter__(self):
    return self.name

  def __exit__(self, exc_type, exc_value, traceback):
    self.cleanup()


class TemporaryPubsubSubscription(object):
  """Create a temporary PubSub subscription.

  When the resulting object goes out of scope, the underlying subscription is
  deleted. The result can also be used as a context manager, in which case
  the subscription is deleted once the context is closed.

  Attributes:
    name (str): The name of the temporary subscription.
  """

  def __init__(self, project, topic_path, prefix="temp"):
    self.sub_client = pubsub.SubscriberClient()
    self.project = project
    self.topic_path = topic_path

    num_tires = 5
    for _ in range(num_tires):
      try:
        self.subscription = self.sub_client.create_subscription(
            self.sub_client.subscription_path(self.project,
                                              generate_unique_name(prefix)),
            self.topic_path)
        break
      except gexc.AlreadyExists:
        pass
    else:
      raise ValueError(
          "Could not create a unique subscription after {} tries.".format(
              num_tires))

    self._finalizer = finalize(
        self,
        self._cleanup,
        self.sub_client,
        self.subscription.name,
        warn_message="Implicitly cleaning up {!r}".format(self))

  @property
  def name(self):
    return self.subscription.name

  @classmethod
  def _cleanup(cls, sub_client, name, warn_message):
    sub_client.delete_subscription(name)
    warnings.warn(warn_message, ResourceWarning)

  def cleanup(self):
    if self._finalizer.detach():
      self.sub_client.delete_subscription(self.subscription.name)

  def __enter__(self):
    return self.name

  def __exit__(self, exc_type, exc_value, traceback):
    self.cleanup()


class TemporaryPubsubSnapshot(object):
  """Create a temporary PubSub snapshot.

  When the resulting object goes out of scope, the underlying snapshot is
  deleted. The result can also be used as a context manager, in which case
  the snapshot is deleted once the context is closed.

  Attributes:
    name (str): The name of the temporary snapshot.
  """

  def __init__(self, project, subscription_path, prefix="temp"):
    self.sub_client = pubsub.SubscriberClient()
    self.project = project
    self.subscription_path = subscription_path

    num_tires = 5
    for _ in range(num_tires):
      try:
        self.snapshot = self.sub_client.create_snapshot(
            self.sub_client.snapshot_path(self.project,
                                          generate_unique_name(prefix)),
            self.subscription_path)
        break
      except gexc.AlreadyExists:
        pass
    else:
      raise ValueError(
          "Could not create a unique snapshot after {} tries.".format(
              num_tires))

    self._finalizer = finalize(
        self,
        self._cleanup,
        self.sub_client,
        self.snapshot.name,
        warn_message="Implicitly cleaning up {!r}".format(self))

  @property
  def name(self):
    return self.snapshot.name

  @classmethod
  def _cleanup(cls, sub_client, name, warn_message):
    sub_client.delete_snapshot(name)
    warnings.warn(warn_message, ResourceWarning)

  def cleanup(self):
    if self._finalizer.detach():
      self.sub_client.delete_snapshot(self.snapshot.name)

  def __enter__(self):
    return self.name

  def __exit__(self, exc_type, exc_value, traceback):
    self.cleanup()
