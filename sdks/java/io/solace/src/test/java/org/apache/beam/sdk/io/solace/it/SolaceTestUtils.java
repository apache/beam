/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.solace.it;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SolaceTestUtils {
  private static final Duration POLL_INTERVAL = Duration.standardSeconds(1);

  /**
   * Waits until the counter reaches {@code expectedCount} or {@code timeout} passes, then cancels
   * the pipeline and returns the last value of the counter.
   *
   * <p>The Read connector keeps polling the queue forever, so the pipeline never finishes on its
   * own. Waiting for the expected count instead of a fixed amount of time keeps the tests from
   * stopping too early on a slow machine.
   */
  static long waitForCounterAndCancel(
      PipelineResult pipelineResult,
      String namespace,
      String counterName,
      long expectedCount,
      Duration timeout)
      throws IOException {
    MetricsReader metricsReader = new MetricsReader(pipelineResult, namespace);
    Instant deadline = Instant.now().plus(timeout);
    long count = metricsReader.getCounterMetric(counterName);
    while (count < expectedCount && Instant.now().isBefore(deadline)) {
      PipelineResult.State state = pipelineResult.waitUntilFinish(POLL_INTERVAL);
      count = metricsReader.getCounterMetric(counterName);
      if (state != null && state.isTerminal()) {
        break;
      }
    }
    pipelineResult.cancel();
    return count;
  }
}
