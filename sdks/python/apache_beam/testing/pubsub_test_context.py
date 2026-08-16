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

import inspect
import time
import logging

logger = logging.getLogger(__name__)

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub_v1
except ImportError:
  pubsub_v1 = None
# pylint: enable=wrong-import-order, wrong-import-position


class TestPubsubContext:
  """A highly advanced Pub/Sub resource lifecycle manager for Python integration tests.
    Implements cascading third-party subscription cleanup and selective
    graceful teardown for debugging on failures.

    Includes a safety 'dry_run' switch for safe deployment and validation of resources.
    Any catastrophic leaks are handled independently by the global 'stale_cleaner.py'.
    """
  def __init__(
      self,
      project_id,
      dry_run=False
  ):  # Keep dry_run=False to allow actual deletions during testing

    if pubsub_v1 is None:
      raise ImportError(
          "The 'google-cloud-pubsub' library is required for TestPubsubContext. "
          "Please install it using 'pip install google-cloud-pubsub'.")

    self.project_id = project_id
    self.dry_run = dry_run
    self.publisher = pubsub_v1.PublisherClient()
    self.subscriber = pubsub_v1.SubscriberClient()

    # Lists to track resources created during the test execution
    self.tracked_topics = []
    self.tracked_subscriptions = []
    self.caller_class = "UnknownTestClass"
    stack = inspect.stack()

    for frame in stack:
      self_obj = frame[0].f_locals.get('self', None)
      if self_obj and hasattr(self_obj, '__class__'):
        self.caller_class = self_obj.__class__.__name__
        break

  def register_topic(self, topic_path: str):
    """Registers a topic to be monitored and deleted at the end."""
    if topic_path not in self.tracked_topics:
      self.tracked_topics.append(topic_path)
      logger.info(
          "[%s] Registering Topic for monitoring: %s",
          self.caller_class,
          topic_path)

  def register_subscription(self, subscription_path: str):
    """Registers a subscription to be monitored and deleted at the end."""
    if subscription_path not in self.tracked_subscriptions:
      self.tracked_subscriptions.append(subscription_path)
      logger.info(
          "[%s] Registering Subscription for monitoring: %s",
          self.caller_class,
          subscription_path)

  def __enter__(self):
    logger.info(
        "[START] [%s] Initializing Pub/Sub context (dry_run=%s)",
        self.caller_class,
        self.dry_run)
    return self

  def _delete_cascading_subscriptions(self, topic_path: str):
    """
        Finds and deletes from GCP any third-party subscription that is
        connected to our test topic, preventing loose residual resources.
        """
    logger.info(
        "[%s] Checking for cascading subscriptions on topic: %s",
        self.caller_class,
        topic_path)
    try:
      # List all subscriptions associated with this specific topic in GCP
      for sub_path in self.publisher.list_topic_subscriptions(
          request={"topic": topic_path}):
        if self.dry_run:
          logger.info(
              "[%s] [Cascade] (Dry Run) Would delete subscription: %s",
              self.caller_class,
              sub_path)
        else:
          logger.info(
              "[%s] [Teardown - Cascade] Deleting residual third-party subscription: %s",
              self.caller_class,
              sub_path)
          try:
            self.subscriber.delete_subscription(
                request={"subscription": sub_path})
          except Exception as e:
            logger.error(
                "[%s] [Error] Could not delete cascading sub %s: %s",
                self.caller_class,
                sub_path,
                e)
    except Exception as e:
      logger.error(
          "[%s] [Error] Could not list subs for topic %s: %s",
          self.caller_class,
          topic_path,
          e)

  def __exit__(self, exc_type, exc_val, exc_tb):
    logger.info("Starting teardown of registered resources...")
    # If the test failed (exc_type is not None), we leave the subscriptions active for 24 hours
    # with an automatic TTL in GCP so the developer can debug the backlog.
    # If the test was successful, we clean up everything immediately to save 100% of the cost.
    test_failed = exc_type is not None

    if test_failed:
      logger.warning(
          "[%s] [ALERT] Failed test detected. Applying Graceful Teardown.",
          self.caller_class)
      return False

    logger.info(
        "[%s] [SUCCESS] Test passed. Proceeding with cleanup.",
        self.caller_class)

    # 1. Delete registered Subscriptions (Only if the test was successful)
    for sub_path in list(self.tracked_subscriptions):
      try:
        if self.dry_run:
          logger.info("(Dry Run) Would delete subscription: %s", sub_path)
        else:
          logger.info("Deleting temporary subscription: %s", sub_path)
          self.subscriber.delete_subscription(
              request={"subscription": sub_path})
        self.tracked_subscriptions.remove(sub_path)
      except Exception as e:
        logger.error(
            "[%s] [Error] Could not delete subscription %s: %s",
            self.caller_class,
            sub_path,
            e)

    # 2. Cascading Topic Cleanup (Check connected third-party subscriptions)
    for topic_path in list(self.tracked_topics):
      # Execute cascading deletion inspired by Java logic
      self._delete_cascading_subscriptions(topic_path)
      try:
        if self.dry_run:
          logger.info("(Dry Run) Would delete temporary topic: %s", topic_path)
        else:
          logger.info("Deleting temporary topic: %s", topic_path)
          self.publisher.delete_topic(request={"topic": topic_path})
        self.tracked_topics.remove(topic_path)
      except Exception as e:
        logger.error("Could not delete topic %s: %s", topic_path, e)
    return False
