# -*- coding: utf-8 -*-
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
from __future__ import print_function

import functools
import itertools
import logging
import pickle
import sys
import unittest
import uuid

import dill
import numpy as np
from nose.plugins.attrib import attr
from parameterized import parameterized

import apache_beam as beam
from apache_beam import coders
from apache_beam import transforms
from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.io.gcp.tests import utils as gcp_test_utils
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive.caching import file_based_cache_it_test
from apache_beam.runners.interactive.caching import file_based_cache_test
from apache_beam.runners.interactive.caching import stream_based_cache
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin
from apache_beam.testing.test_pipeline import TestPipeline

# Protect against environments where the PubSub library is not available.
try:
  from google.cloud import pubsub_v1
except ImportError:
  pubsub_v1 = None


def read_directly(cache, limit=None, timeout=None, return_timestamp=False):
  """Read elements from cache using the cache API."""
  for element in itertools.islice(cache.read(timeout=timeout), limit):
    if return_timestamp:
      yield element
    else:
      yield element.value


def write_directly(cache, data_in):
  """Write elements to cache directly."""
  cache.write(data_in)


def write_through_pipeline(cache, data_in):
  """Write elements to cache using a Beam pipeline."""
  options = PipelineOptions(streaming=True)
  # Create is an "impulse source", so it terminates the streaming pipeline
  with beam.Pipeline(options=options) as p:
    _ = p | transforms.Create(data_in) | cache.writer()


class Validators(unittest.TestCase, ExtraAssertionsMixin):

  def validate_directly(self, cache, expected):
    """Validate the contents of a cache by reading it directly."""
    actual = list(read_directly(cache, limit=len(expected), timeout=10))
    self.assertUnhashableCountEqual(actual, expected)

  def validate_through_pipeline(self, cache, expected):
    """Validate the contents of cache by writing it out into a Beam pipeline."""
    project, topic_name = beam_pubsub.parse_topic(cache.location)

    pub_client = pubsub_v1.PublisherClient()
    introspect_topic = pub_client.create_topic(
        pub_client.topic_path(project, topic_name + "-introspec"))

    sub_client = pubsub_v1.SubscriberClient()
    introspect_sub = sub_client.create_subscription(
        sub_client.subscription_path(project, topic_name + "-introspec"),
        introspect_topic.name,
        ack_deadline_seconds=60,
    )

    options = PipelineOptions(runner="DirectRunner", streaming=True)
    p = beam.Pipeline(options=options)
    pr = None
    try:
      _ = (
          p
          | "Read" >> cache.reader(with_attributes=True)
          | "Write" >> beam.io.WriteToPubSub(
              topic=introspect_topic.name, with_attributes=True))
      pr = p.run()
      output_messages = gcp_test_utils.read_from_pubsub(
          sub_client,
          introspect_sub.name,
          number_of_elements=len(expected),
          timeout=3 * 60,
          with_attributes=True)
      coder = (
          cache.default_coder if cache.default_coder is not None else
          coders.registry.get_coder(cache.element_type))
      actual = [coder.decode(message.data) for message in output_messages]
      self.assertUnhashableCountEqual(actual, expected)
    finally:
      if pr is not None:
        pr.cancel()
      sub_client.delete_subscription(introspect_sub.name)
      pub_client.delete_topic(introspect_topic.name)

  if sys.version_info < (3,):

    def runTest(self):
      pass


def retry_flakes(fn):

  @functools.wraps(fn)
  def resilient_fn(self, *args, **kwargs):
    max_tries = 3
    error = None
    for _ in range(max_tries):
      try:
        return fn(self, *args, **kwargs)
      except AssertionError as e:
        error = e
        self.tearDown()
        self.setUp()
    raise error  # pylint: disable=raising-bad-type

  return resilient_fn


class TestCase(ExtraAssertionsMixin, unittest.TestCase):
  pass


@unittest.skipIf(pubsub_v1 is None, 'GCP dependencies are not installed.')
class PubSubBasedCacheSerializationTest(
    file_based_cache_it_test.SerializationTestBase, TestCase):

  cache_class = stream_based_cache.PubSubBasedCache

  def setUp(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    project = test_pipeline.get_option('project')
    topic_name = "{}-{}".format(self.cache_class.__name__, uuid.uuid4().hex)
    self.location = pubsub_v1.PublisherClient.topic_path(project, topic_name)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  @attr('IT')
  @retry_flakes
  def test_serialize_deserialize_empty(self, _, serializer):
    self.check_serialize_deserialize_empty(write_directly, read_directly,
                                           serializer)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  @attr('IT')
  @retry_flakes
  def test_serialize_deserialize_filled(self, _, serializer):
    self.check_serialize_deserialize_filled(write_directly, read_directly,
                                            serializer)


@unittest.skipIf(pubsub_v1 is None, 'GCP dependencies are not installed.')
class PubSubBasedCacheRoundtripTest(file_based_cache_it_test.RoundtripTestBase,
                                    TestCase):

  cache_class = stream_based_cache.PubSubBasedCache
  dataset = file_based_cache_test.GENERIC_TEST_DATA

  def setUp(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    project = test_pipeline.get_option('project')
    topic_name = "{}-{}".format(self.cache_class.__name__, uuid.uuid4().hex)
    self.location = pubsub_v1.PublisherClient.topic_path(project, topic_name)

  @parameterized.expand([
      ("{}-{}".format(
          write_fn.__name__,
          validate_fn.__name__,
      ), write_fn, validate_fn)
      for write_fn in [write_directly, write_through_pipeline]
      for validate_fn in
      [Validators().validate_directly,
       Validators().validate_through_pipeline]
  ])
  @attr('IT')
  @retry_flakes
  def test_roundtrip(self, _, write_fn, validate_fn):
    # Ideally, we would run this test with batches of different data types and
    # different coders. However, the test takes several seconds, so running it
    # for all possible writers, validators, and data types is not feasible.
    data = [
        None,
        100,
        1.23,
        "abc",
        b"def",
        u"±♠Ωℑ",
        ["d", 1],
        ("a", "b", "c"),
        (1, 2, 3, "b"),
        {"a": 1, 123: 1.234},
        np.zeros((3, 6)),
    ]
    self.check_roundtrip(write_fn, validate_fn, dataset=[data])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
