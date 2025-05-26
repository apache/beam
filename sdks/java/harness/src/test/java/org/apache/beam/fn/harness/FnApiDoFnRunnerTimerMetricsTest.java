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
package org.apache.beam.fn.harness;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FnApiDoFnRunnerTimerMetricsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMetricsInTimerCallback() {
    List<KV<String, Long>> inputElements = new ArrayList<>();
    inputElements.add(KV.of("key1", 1L));
    inputElements.add(KV.of("key2", 2L));
    inputElements.add(KV.of("key3", 3L));

    pipeline
        .apply(
            Create.of(inputElements).withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
        .apply(ParDo.of(new TimerMetricsTestDoFn()));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish(); // Ensure pipeline processing is complete

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(TimerMetricsTestDoFn.class, "timersFired"))
                    .build());

    long timersFiredCount = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      if (counter.getName().getName().equals("timersFired")) {
        // In tests, attempted value is usually what we want for counters.
        timersFiredCount = counter.getAttempted();
        break;
      }
    }
    assertEquals(
        "Counter 'timersFired' should reflect all timers that fired.",
        inputElements.size(),
        timersFiredCount);
  }
}
