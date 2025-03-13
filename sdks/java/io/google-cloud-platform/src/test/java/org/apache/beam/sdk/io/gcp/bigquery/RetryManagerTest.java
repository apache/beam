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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;

import com.google.api.core.ApiFutures;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.RetryType;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RetryManager}. */
@RunWith(JUnit4.class)
public class RetryManagerTest {
  static class Context extends Operation.Context<String> {
    int numStarted = 0;
    int numSucceeded = 0;
    int numFailed = 0;
  }

  @Test
  public void testNoFailures() throws Exception {
    List<Context> contexts = Lists.newArrayList();
    MetricName metricName = MetricName.named("TEST_NAMESPACE", "THROTTLED_COUNTER");
    CounterCell throttleTimeCounter = new CounterCell(metricName);
    RetryManager<String, RetryManagerTest.Context> retryManager =
        new RetryManager<>(Duration.millis(1), Duration.millis(1), 5, throttleTimeCounter);
    for (int i = 0; i < 5; ++i) {
      Context context = new Context();
      contexts.add(context);
      retryManager.addOperation(
          c -> {
            ++c.numStarted;
            return ApiFutures.immediateFuture("yes");
          },
          cs -> {
            cs.forEach(c -> ++c.numFailed);
            return RetryType.DONT_RETRY;
          },
          c -> ++c.numSucceeded,
          context);
    }
    contexts.forEach(
        c -> {
          assertEquals(0, c.numStarted);
          assertEquals(0, c.numSucceeded);
          assertEquals(0, c.numFailed);
        });
    retryManager.run(true);
    contexts.forEach(
        c -> {
          assertEquals(1, c.numStarted);
          assertEquals(1, c.numSucceeded);
          assertEquals(0, c.numFailed);
        });

    assertEquals(0L, (long) throttleTimeCounter.getCumulative());
  }

  @Test
  public void testRetryInOrder() throws Exception {
    Map<String, Context> contexts = Maps.newHashMap();
    Map<String, Integer> expectedStarts = Maps.newHashMap();
    Map<String, Integer> expectedFailures = Maps.newHashMap();

    MetricName metricName = MetricName.named("TEST_NAMESPACE", "THROTTLED_COUNTER");
    CounterCell throttleTimeCounter = new CounterCell(metricName);

    RetryManager<String, RetryManagerTest.Context> retryManager =
        new RetryManager<>(Duration.millis(1), Duration.millis(1), 50, throttleTimeCounter);
    for (int i = 0; i < 5; ++i) {
      final int index = i;
      String value = "yes " + i;
      Context context = new Context();
      contexts.put(value, context);
      expectedStarts.put(value, i + 2);
      expectedFailures.put(value, i + 1);
      retryManager.addOperation(
          c -> {
            // Make sure that each operation fails on its own. Failing a previous operation
            // automatically
            // fails all subsequent operations.
            if (c.numStarted <= index) {
              ++c.numStarted;
              RuntimeException e = new RuntimeException("foo");
              return ApiFutures.immediateFailedFuture(e);
            } else {
              ++c.numStarted;
              return ApiFutures.immediateFuture(value);
            }
          },
          cs -> {
            cs.forEach(c -> ++c.numFailed);
            return RetryType.RETRY_ALL_OPERATIONS;
          },
          c -> ++c.numSucceeded,
          context);
    }
    contexts
        .values()
        .forEach(
            c -> {
              assertEquals(0, c.numStarted);
              assertEquals(0, c.numSucceeded);
              assertEquals(0, c.numFailed);
            });
    retryManager.run(true);
    contexts
        .entrySet()
        .forEach(
            e -> {
              assertEquals((int) expectedStarts.get(e.getKey()), e.getValue().numStarted);
              assertEquals(1, e.getValue().numSucceeded);
              assertEquals((int) expectedFailures.get(e.getKey()), e.getValue().numFailed);
            });
    // Each operation backsoff once and each backoff is 1ms.
    assertEquals(5L, (long) throttleTimeCounter.getCumulative());
  }

  @Test
  public void testDontRetry() throws Exception {
    List<Context> contexts = Lists.newArrayList();
    MetricName metricName = MetricName.named("TEST_NAMESPACE", "THROTTLED_COUNTER");
    CounterCell throttleTimeCounter = new CounterCell(metricName);

    RetryManager<String, RetryManagerTest.Context> retryManager =
        new RetryManager<>(Duration.millis(1), Duration.millis(1), 50, throttleTimeCounter);
    for (int i = 0; i < 5; ++i) {
      Context context = new Context();
      contexts.add(context);
      String value = "yes " + i;
      retryManager.addOperation(
          c -> {
            if (c.numStarted == 0) {
              ++c.numStarted;
              RuntimeException e = new RuntimeException("foo");
              return ApiFutures.immediateFailedFuture(e);
            } else {
              ++c.numStarted;
              return ApiFutures.immediateFuture(value);
            }
          },
          cs -> {
            cs.forEach(c -> ++c.numFailed);
            return RetryType.DONT_RETRY;
          },
          c -> ++c.numSucceeded,
          context);
    }
    contexts.forEach(
        c -> {
          assertEquals(0, c.numStarted);
          assertEquals(0, c.numSucceeded);
          assertEquals(0, c.numFailed);
        });
    retryManager.run(true);
    contexts.forEach(
        c -> {
          assertEquals(1, c.numStarted);
          assertEquals(0, c.numSucceeded);
          assertEquals(1, c.numFailed);
        });

    assertEquals(0L, (long) throttleTimeCounter.getCumulative());
  }

  @Test
  public void testHasSucceeded() throws Exception {
    List<Context> contexts = Lists.newArrayList();
    RetryManager<String, RetryManagerTest.Context> retryManager =
        new RetryManager<>(Duration.millis(1), Duration.millis(1), 5);
    for (int i = 0; i < 5; ++i) {
      Context context = new Context();
      contexts.add(context);
      retryManager.addOperation(
          c -> {
            ++c.numStarted;
            return ApiFutures.immediateFuture("yes");
          },
          cs -> {
            cs.forEach(c -> ++c.numFailed);
            return RetryType.DONT_RETRY;
          },
          c -> ++c.numSucceeded,
          c -> false,
          context);
    }
    contexts.forEach(
        c -> {
          assertEquals(0, c.numStarted);
          assertEquals(0, c.numSucceeded);
          assertEquals(0, c.numFailed);
        });
    retryManager.run(true);
    contexts.forEach(
        c -> {
          assertEquals(1, c.numStarted);
          assertEquals(0, c.numSucceeded);
          assertEquals(1, c.numFailed);
        });
  }
}
