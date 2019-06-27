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
import os
import shutil
import tempfile
import time
import unittest
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.runners import PipelineState
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.runners.interactive.caching.filebasedcache import \
    TFRecordBasedCache
from apache_beam.runners.interactive.caching.filebasedcache_it_test import \
    GENERIC_TEST_DATA
from apache_beam.runners.interactive.caching.filebasedcache_it_test import \
    FunctionalTestBase
from apache_beam.runners.interactive.caching.streambasedcache import \
    PubSubBasedCache
from apache_beam.runners.interactive.caching.streambasedcache import \
    remove_topic_and_subscriptions
from apache_beam.transforms import Create
from apache_beam.transforms import WindowInto
from apache_beam.transforms import Map
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from parameterized import parameterized

# Protect against environments where the PubSub library is not available.
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None


def read_through_pipeline(cache, timeout=None, result_size=None):
  """Read elements from cache using a Beam pipeline."""
  temp_location = "projects/{}/topics/test-{}".format(os.environ["PROJECT_ID"],
                                                      uuid.uuid4().hex)
  temp_cache = PubSubBasedCache(temp_location)
  try:
    p = Pipeline(runner=BundleBasedDirectRunner(),
                 options=PipelineOptions(streaming=True))
    # yapf: disable
    _ = (
        p
        | "Read" >> cache.reader()
        | "Window" >> WindowInto(
            window.GlobalWindows(),
            # beam.window.FixedWindows(1),
            trigger=trigger.Repeatedly(trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | "Echo" >> Map(lambda e: print(e) or e)
        | "Write" >> temp_cache.writer()
    )
    # yapf: enable
    pr = p.run()
    start_time = time.time()
    results = []
    while True:
      try:
        results += list(
            temp_cache.read(subscription=temp_cache.subscription_path))
      except IOError:
        pass
      if result_size is not None and len(results) == result_size:
        print("results={}".format(results))
        break
      if timeout is not None and (time.time() - start_time) > timeout:
        print("Timeout (results={}, result_size={}).".format(
            results, result_size))
        break
      if pr.state in (PipelineState.DONE, PipelineState.CANCELLED,
                      PipelineState.FAILED):
        print("Invalid state: {}".format(pr.state))
        break
      time.sleep(1)
    pr.cancel()
    return results
  finally:
    temp_cache.clear()


def write_through_pipeline(cache, data_in, timeout=None, result_size=None):
  """Write elements to cache using a Beam pipeline."""
  p = Pipeline(runner=BundleBasedDirectRunner(),
               options=PipelineOptions(streaming=True))
  # yapf: disable
  _ = (
      p
      | "Read" >> Create(data_in)
      | "Window" >> WindowInto(
          window.GlobalWindows(),
          # beam.window.FixedWindows(1),
          trigger=trigger.Repeatedly(trigger.AfterCount(1)),
          accumulation_mode=trigger.AccumulationMode.DISCARDING)
      | "Echo" >> Map(lambda e: print(e) or e)
      | "Write" >> cache.writer()
  )
  # yapf: enable
  pr = p.run()
  start_time = time.time()
  results = []
  while True:
    try:
      results += list(cache.read(subscription=cache.subscription_path))
    except IOError:
      pass
    if result_size is not None and len(results) == result_size:
      break
    if timeout is not None and (time.time() - start_time) > timeout:
      break
    if pr.state in (PipelineState.DONE, PipelineState.CANCELLED,
                    PipelineState.FAILED):
      break
    time.sleep(1)
  pr.cancel()


def read_directly(cache, timeout=None, result_size=None):
  """Read elements from cache using the cache API."""
  return list(cache.read(subscription=cache.subscription_path))


def write_directly(cache, data_in, timeout=None, result_size=None):
  """Write elements to cache using the cache API."""
  cache.write(data_in)


@unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
@unittest.skipIf("PROJECT_ID" not in os.environ,
                 'Need to set PROJECT_ID environment variable to run this test')
class PubSubBasedCacheRoundtripTest(FunctionalTestBase, unittest.TestCase):

  _cache_class = PubSubBasedCache

  def setUp(self):
    self.location = "projects/{}/topics/test-{}".format(
        os.environ["PROJECT_ID"],
        uuid.uuid4().hex)

  def tearDown(self):
    remove_topic_and_subscriptions(self.location)

  @parameterized.expand([
      ("{}-{}-{}".format(
          data_name,
          write_fn.__name__,
          read_fn.__name__,
      ), write_fn, read_fn, data) for data_name, data in GENERIC_TEST_DATA
      for write_fn in [write_directly, write_through_pipeline]
      for read_fn in [read_directly, read_through_pipeline]
  ])
  def test_roundtrip(self, _, write_fn, read_fn, data):
    return self.check_roundtrip(write_fn, read_fn, data)

  def check_roundtrip(self, write_fn, read_fn, data):
    write_fn = functools.partial(write_fn, timeout=10)
    read_fn = functools.partial(read_fn, timeout=10)
    cache = self._cache_class(self.location, **self._get_writer_kwargs(data))
    write_fn(cache, data, result_size=len(data))
    data_out = read_fn(cache, result_size=len(data))
    self.assertElementsEqual(data_out, data)
    write_fn(cache, data, result_size=len(data))
    data_out = read_fn(cache, result_size=len(data))
    self.assertElementsEqual(data_out, data)
    # write_fn(cache, data, result_size=len(data) * 2)
    # data_out = read_fn(cache, result_size=len(data) * 2)
    # self.assertElementsEqual(data_out, data * 2)
    cache.clear()
    with self.assertRaises(IOError):
      data_out = read_fn(cache, result_size=0)
