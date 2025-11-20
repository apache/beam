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

"""Pytest configuration and custom hooks."""

import gc
import os
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline

MAX_SUPPORTED_PYTHON_VERSION = (3, 13)


def pytest_addoption(parser):
  parser.addoption(
      '--test-pipeline-options',
      help='Options to use in test pipelines. NOTE: Tests may '
      'ignore some or all of these options.')
  parser.addoption(
      '--enable-test-cleanup',
      action='store_true',
      default=False,
      help='Enable expensive cleanup operations. Auto-enabled in CI by default. '
           'Use this flag to explicitly enable cleanup in local development.')


# See pytest.ini for main collection rules.
collect_ignore_glob = [
    '*_py3%d.py' % minor for minor in range(
        sys.version_info.minor + 1, MAX_SUPPORTED_PYTHON_VERSION[1] + 1)
]


@pytest.fixture(scope="session", autouse=True)
def configure_beam_rpc_timeouts():
  """
  Configure gRPC and RPC timeouts for Beam tests
  to prevent DEADLINE_EXCEEDED errors.
  """
  print("\n--- Applying Beam RPC timeout configuration ---")

  # Set gRPC keepalive and timeout settings
  timeout_env_vars = {
      'GRPC_ARG_KEEPALIVE_TIME_MS': '30000',
      'GRPC_ARG_KEEPALIVE_TIMEOUT_MS': '5000',
      'GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA': '0',
      'GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS': '1',
      'GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS': '300000',
      'GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS': '10000',

      # Additional stability settings for DinD environment
      'GRPC_ARG_MAX_RECONNECT_BACKOFF_MS': '120000',
      'GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS': '1000',
      'GRPC_ARG_MAX_CONNECTION_IDLE_MS': '300000',
      'GRPC_ARG_MAX_CONNECTION_AGE_MS': '1800000',

      # Beam-specific retry and timeout settings
      'BEAM_RETRY_MAX_ATTEMPTS': '5',
      'BEAM_RETRY_INITIAL_DELAY_MS': '1000',
      'BEAM_RETRY_MAX_DELAY_MS': '60000',
      'BEAM_RUNNER_BUNDLE_TIMEOUT_MS': '300000',

      # Force deterministic execution in DinD environment
      'BEAM_TESTING_FORCE_SINGLE_BUNDLE': 'true',
      'BEAM_TESTING_DETERMINISTIC_ORDER': 'true',
      'BEAM_SDK_WORKER_PARALLELISM': '1',
      'BEAM_WORKER_POOL_SIZE': '1',
      'BEAM_FN_API_CONTROL_PORT': '0',
      'BEAM_FN_API_DATA_PORT': '0',

  # Container-specific stability settings
      'PYTHONHASHSEED': '0',
      'OMP_NUM_THREADS': '1',
      'OPENBLAS_NUM_THREADS': '1',

      # Force sequential pytest execution (CRITICAL for DinD stability)
      'PYTEST_XDIST_WORKER_COUNT': '1',
      'PYTEST_CURRENT_TEST_TIMEOUT': '300',

      # Mock and test isolation improvements
      'PYTEST_MOCK_TIMEOUT': '60',
      'BEAM_TEST_ISOLATION_MODE': 'strict',
  }

  for key, value in timeout_env_vars.items():
    os.environ[key] = value
    print(f"Set {key}={value}")

  print("Successfully configured Beam RPC timeouts")


def _running_in_ci():
  """Returns True if running in a CI environment."""
  return (
      os.getenv('GITHUB_ACTIONS') == 'true' or
      os.getenv('CI') == 'true' or
      os.getenv('CONTINUOUS_INTEGRATION') == 'true'
  )


def _should_enable_test_cleanup(config):
  """Returns True if expensive cleanup operations should run.

  Result is cached on config object to avoid re-computation per test.
  """
  if hasattr(config, '_should_enable_test_cleanup_result'):
    return config._should_enable_test_cleanup_result

  if config.getoption('--enable-test-cleanup'):
    result = True
    reason = "enabled via --enable-test-cleanup"
  else:
    if _running_in_ci():
      result = True
      reason = "CI detected"
    else:
      result = False
      reason = "local development"

  # Log once per session
  if not hasattr(config, '_cleanup_decision_logged'):
    print(f"\n[Test Cleanup] Enabled: {result} ({reason})")
    config._cleanup_decision_logged = True

  config._should_enable_test_cleanup_result = result
  return result


@pytest.fixture(autouse=True)
def ensure_clean_state(request):
  """
  Ensures clean state between tests to prevent contamination.

  Expensive operations (sleeps, extra GC) only run in CI or when
  explicitly enabled to keep local tests fast.
  """
  enable_cleanup = _should_enable_test_cleanup(request.config)

  if enable_cleanup:
    gc.collect()

  thread_count = threading.active_count()
  if thread_count > 50:
    print(f"Warning: {thread_count} active threads detected before test")
    if enable_cleanup:
      time.sleep(0.5)
      gc.collect()

  yield

  try:
    if enable_cleanup:
      gc.collect()
      time.sleep(0.1)
      gc.collect()
  except Exception as e:
    print(f"Warning: Cleanup error: {e}")


@pytest.fixture(autouse=True)
def enhance_mock_stability(request):
  """Improves mock stability in DinD environment."""
  enable_cleanup = _should_enable_test_cleanup(request.config)

  if enable_cleanup:
    time.sleep(0.05)

  yield

  if enable_cleanup:
    time.sleep(0.05)


def pytest_configure(config):
  """Saves options added in pytest_addoption for later use.
  This is necessary since pytest-xdist workers do not have the
  same sys.argv as the main pytest invocation.
  xdist does seem to pickle TestPipeline
  """
  # for the entire test session.
  print("\n--- Applying global testcontainers timeout configuration ---")
  try:
    from testcontainers.core import waiting_utils
    waiting_utils.config = SimpleNamespace(
        timeout=int(os.getenv("TC_TIMEOUT", "120")),
        max_tries=int(os.getenv("TC_MAX_TRIES", "120")),
        sleep_time=float(os.getenv("TC_SLEEP_TIME", "1")),
    )
    print("Successfully set waiting utils config")
  except ModuleNotFoundError:
    print("The testcontainers library is not installed.")

  TestPipeline.pytest_test_pipeline_options = config.getoption(
      'test_pipeline_options', default='')
  # Enable optional type checks on all tests.
  pipeline_options.enable_all_additional_type_checks()
