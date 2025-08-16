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

import os
import sys
import pytest

from types import SimpleNamespace

from testcontainers.core import waiting_utils

from apache_beam.options import pipeline_options
from apache_beam.testing.test_pipeline import TestPipeline


MAX_SUPPORTED_PYTHON_VERSION = (3, 13)


def pytest_addoption(parser):
  parser.addoption(
      '--test-pipeline-options',
      help='Options to use in test pipelines. NOTE: Tests may '
      'ignore some or all of these options.')


# See pytest.ini for main collection rules.
collect_ignore_glob = [
    '*_py3%d.py' % minor for minor in range(
        sys.version_info.minor + 1, MAX_SUPPORTED_PYTHON_VERSION[1] + 1)
]


@pytest.fixture(scope="session", autouse=True)
def configure_beam_rpc_timeouts():
  """Configure gRPC and RPC timeouts for Beam tests to prevent DEADLINE_EXCEEDED errors."""
  print("\n--- Applying Beam RPC timeout configuration ---")
  
  # Set gRPC keepalive and timeout settings
  timeout_env_vars = {
      'GRPC_ARG_KEEPALIVE_TIME_MS': '30000',           # 30 seconds keepalive
      'GRPC_ARG_KEEPALIVE_TIMEOUT_MS': '5000',         # 5 seconds timeout
      'GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA': '0',    # Unlimited pings
      'GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS': '1',  # Allow keepalive without calls
      'GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS': '300000',  # 5 minutes
      'GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS': '10000',   # 10 seconds
      
      # Additional stability settings for DinD environment
      'GRPC_ARG_MAX_RECONNECT_BACKOFF_MS': '120000',   # 2 minutes max backoff
      'GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS': '1000', # 1 second initial backoff
      'GRPC_ARG_MAX_CONNECTION_IDLE_MS': '300000',     # 5 minutes idle timeout
      'GRPC_ARG_MAX_CONNECTION_AGE_MS': '1800000',     # 30 minutes max connection age
      
      # Beam-specific retry and timeout settings
      'BEAM_RETRY_MAX_ATTEMPTS': '5',                  # Increase retry attempts
      'BEAM_RETRY_INITIAL_DELAY_MS': '1000',          # 1 second initial delay
      'BEAM_RETRY_MAX_DELAY_MS': '60000',             # 60 seconds max delay
      'BEAM_RUNNER_BUNDLE_TIMEOUT_MS': '300000',      # 5 minutes bundle timeout
      
      # Force deterministic execution in DinD environment
      'BEAM_TESTING_FORCE_SINGLE_BUNDLE': 'true',     # Force single bundle processing
      'BEAM_TESTING_DETERMINISTIC_ORDER': 'true',     # Force deterministic element ordering
      'BEAM_SDK_WORKER_PARALLELISM': '1',             # Single-threaded workers
      'BEAM_WORKER_POOL_SIZE': '1',                   # Single worker pool
      'BEAM_FN_API_CONTROL_PORT': '0',                # Let system assign ports
      'BEAM_FN_API_DATA_PORT': '0',                   # Let system assign ports
      
      # Container-specific stability settings
      'PYTHONHASHSEED': '0',                          # Deterministic hash seed
      'OMP_NUM_THREADS': '1',                         # Single-threaded OpenMP
      'OPENBLAS_NUM_THREADS': '1',                    # Single-threaded BLAS operations
      
      # Force sequential pytest execution (CRITICAL for DinD stability)
      'PYTEST_XDIST_WORKER_COUNT': '1',               # Force single worker
      'PYTEST_CURRENT_TEST_TIMEOUT': '300',           # 5 minutes per test timeout
      
      # Mock and test isolation improvements
      'PYTEST_MOCK_TIMEOUT': '60',                    # 1 minute timeout for mock operations
      'BEAM_TEST_ISOLATION_MODE': 'strict',           # Strict test isolation
  }
  
  for key, value in timeout_env_vars.items():
    os.environ[key] = value
    print(f"Set {key}={value}")
  
  print("Successfully configured Beam RPC timeouts and deterministic execution")


@pytest.fixture(autouse=True)
def ensure_clean_state():
  """Ensure clean state before each test to prevent cross-test contamination."""
  import gc
  import threading
  import time
  
  # Force garbage collection to clean up any lingering resources
  gc.collect()
  
  # Log active thread count for debugging
  thread_count = threading.active_count()
  if thread_count > 50:  # Increased threshold since we see 104 threads
    print(f"Warning: {thread_count} active threads detected before test")
    
    # Force a brief pause to let threads settle
    time.sleep(0.5)
    gc.collect()
  
  yield
  
  # Enhanced cleanup after test
  try:
    # Force more aggressive cleanup
    gc.collect()
    
    # Brief pause to let any async operations complete
    time.sleep(0.1)
    
    # Additional garbage collection
    gc.collect()
  except Exception as e:
    print(f"Warning: Cleanup error: {e}")


@pytest.fixture(autouse=True)
def enhance_mock_stability():
  """Enhance mock stability in DinD environment."""
  import time
  
  # Brief pause before test to ensure clean mock state
  time.sleep(0.05)
  
  yield
  
  # Brief pause after test to let mocks clean up
  time.sleep(0.05)


def pytest_configure(config):
  """Saves options added in pytest_addoption for later use.
  This is necessary since pytest-xdist workers do not have the same sys.argv as
  the main pytest invocation. xdist does seem to pickle TestPipeline
  """
  # for the entire test session.
  print("\n--- Applying global testcontainers timeout configuration ---")
  waiting_utils.config = SimpleNamespace(
      timeout=int(os.getenv("TC_TIMEOUT", "120")),
      max_tries=int(os.getenv("TC_MAX_TRIES", "120")),
      sleep_time=float(os.getenv("TC_SLEEP_TIME", "1")),
  )
  print("Successfully set waiting utils config")

  TestPipeline.pytest_test_pipeline_options = config.getoption(
      'test_pipeline_options', default='')
  # Enable optional type checks on all tests.
  pipeline_options.enable_all_additional_type_checks()