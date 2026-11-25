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

import multiprocessing

import pytest


@pytest.fixture(autouse=True)
def cleanup_spawned_children():
  """Terminate spawned MultiProcessShared child processes after each ML test.

  RunInference spawns dedicated server processes when
  share_model_across_processes() is enabled (for example, large_model=True or
  VLLMModelHandler). Terminating newly spawned child processes after each test
  prevents leaked ML model server processes from accumulating across
  pytest-xdist workers (triggering OOM kills or leaking into later
  multiprocessing.active_children() checks).
  """
  before = set(multiprocessing.active_children())
  yield
  for proc in multiprocessing.active_children():
    if proc not in before and proc.is_alive():
      try:
        proc.terminate()
        proc.join(timeout=2)
        if proc.is_alive():
          proc.kill()
          proc.join(timeout=2)
      except OSError:
        pass
