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

"""Evaluate typed questions with a model connection owned by each DoFn instance."""

import time
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics

from apache_beam.examples.inference.decision_models.model import DecisionResponse


@dataclass(frozen=True)
class DecisionResult:
  state: Any
  response: DecisionResponse
  latency_ms: float


class EvaluateDecisions(beam.DoFn):
  """Reuse a model client across elements and bundles on this DoFn instance.

  The adapter controls network timeouts and retries. Keeping the call on the
  processing thread avoids a per-element executor and preserves backpressure.
  """
  def __init__(self, model, questions):
    self.model = model
    self.questions = questions
    self.requests = Metrics.counter(self.__class__, 'requests')
    self.failures = Metrics.counter(self.__class__, 'failures')
    self.latency = Metrics.distribution(self.__class__, 'request_latency_ms')

  def setup(self):
    self.model.__enter__()

  def process(self, state):
    self.requests.inc()
    started = time.perf_counter()
    try:
      response = self.model.evaluate(state, self.questions)
    except Exception:
      self.failures.inc()
      raise
    elapsed_ms = (time.perf_counter() - started) * 1000
    self.latency.update(int(elapsed_ms))
    yield DecisionResult(state, response, elapsed_ms)

  def teardown(self):
    self.model.__exit__(None, None, None)
