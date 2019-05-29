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

"""
Cloud Datastore client and test functions.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import logging
import time
import uuid
from builtins import range

from google.api_core import exceptions
from google.cloud.datastore import client

from apache_beam.io.gcp.datastore.v1new import types
from apache_beam.utils import retry
from cachetools.func import ttl_cache

# https://cloud.google.com/datastore/docs/concepts/errors#error_codes
_RETRYABLE_DATASTORE_ERRORS = (
    exceptions.Aborted,
    exceptions.DeadlineExceeded,
    exceptions.InternalServerError,
    exceptions.ServiceUnavailable)


@ttl_cache(maxsize=128, ttl=3600)
def get_client(project, namespace):
  """Returns a Cloud Datastore client."""
  _client = client.Client(project=project, namespace=namespace)
  _client.base_url = 'https://batch-datastore.googleapis.com'  # BEAM-1387
  return _client


def retry_on_rpc_error(exception):
  """A retry filter for Cloud Datastore RPCErrors."""
  return isinstance(exception, _RETRYABLE_DATASTORE_ERRORS)


@retry.with_exponential_backoff(num_retries=5, retry_filter=retry_on_rpc_error)
def write_mutations(batch, throttler, rpc_stats_callback, throttle_delay=1):
  """A helper function to write a batch of mutations to Cloud Datastore.

  If a commit fails, it will be retried up to 5 times. All mutations in the
  batch will be committed again, even if the commit was partially successful.
  If the retry limit is exceeded, the last exception from Cloud Datastore will
  be raised.

  Assumes that the Datastore client library does not perform any retries on
  commits. It has not been determined how such retries would interact with the
  retries and throttler used here.
  See ``google.cloud.datastore_v1.gapic.datastore_client_config`` for
  retry config.

  Args:
    batch: (:class:`~google.cloud.datastore.batch.Batch`) An instance of an
      in-progress batch.
    rpc_stats_callback: a function to call with arguments `successes` and
        `failures` and `throttled_secs`; this is called to record successful
        and failed RPCs to Datastore and time spent waiting for throttling.
    throttler: (``apache_beam.io.gcp.datastore.v1.adaptive_throttler.
      AdaptiveThrottler``)
      Throttler instance used to select requests to be throttled.
    throttle_delay: (:class:`float`) time in seconds to sleep when throttled.

  Returns:
    (int) The latency of the successful RPC in milliseconds.
  """
  # Client-side throttling.
  while throttler.throttle_request(time.time() * 1000):
    logging.info(
        "Delaying request for %ds due to previous failures", throttle_delay)
    time.sleep(throttle_delay)
    rpc_stats_callback(throttled_secs=throttle_delay)

  try:
    start_time = time.time()
    batch.commit()
    end_time = time.time()

    rpc_stats_callback(successes=1)
    throttler.successful_request(start_time * 1000)
    commit_time_ms = int((end_time - start_time) * 1000)
    return commit_time_ms
  except Exception:
    rpc_stats_callback(errors=1)
    raise


def create_entities(count, id_or_name=False):
  """Creates a list of entities with random keys."""
  if id_or_name:
    ids_or_names = [uuid.uuid4().int & ((1 << 63) - 1) for _ in range(count)]
  else:
    ids_or_names = [str(uuid.uuid4()) for _ in range(count)]

  keys = [types.Key(['EntityKind', x], project='project') for x in ids_or_names]
  return [types.Entity(key) for key in keys]


def create_client_entities(count, id_or_name=False):
  """Creates a list of client-style entities with random keys."""
  return [
      entity.to_client_entity()
      for entity in create_entities(count, id_or_name=id_or_name)
  ]
