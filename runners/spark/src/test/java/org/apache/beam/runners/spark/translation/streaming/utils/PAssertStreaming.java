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
package org.apache.beam.runners.spark.translation.streaming.utils;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Since PAssert doesn't propagate assert exceptions, use Aggregators to assert streaming
 * success/failure counters.
 */
public final class PAssertStreaming implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PAssertStreaming.class);

  private PAssertStreaming() {
  }

  /**
   * Adds a pipeline run-time assertion that the contents of {@code actual} are {@code expected}.
   * Note that it is oblivious to windowing, so the assertion will apply indiscriminately to all
   * windows.
   */
  public static <T> SparkPipelineResult runAndAssertContents(
      Pipeline p,
      PCollection<T> actual,
      T[] expected,
      Duration timeout,
      boolean stopGracefully) {
    // Because PAssert does not support non-global windowing, but all our data is in one window,
    // we set up the assertion directly.
    actual
        .apply(WithKeys.<String, T>of("dummy"))
        .apply(GroupByKey.<String, T>create())
        .apply(Values.<Iterable<T>>create())
        .apply(ParDo.of(new AssertDoFn<>(expected)));

    // run the pipeline.
    SparkPipelineResult res = (SparkPipelineResult) p.run();
    res.waitUntilFinish(timeout);
    // validate assertion succeeded (at least once).
    int success = res.getAggregatorValue(PAssert.SUCCESS_COUNTER, Integer.class);
    Assert.assertThat("Success aggregator should be greater than zero.", success, not(0));
    // validate assertion didn't fail.
    int failure = res.getAggregatorValue(PAssert.FAILURE_COUNTER, Integer.class);
    Assert.assertThat("Failure aggregator should be zero.", failure, is(0));

    LOG.info("PAssertStreaming had {} successful assertion and {} failed.", success, failure);
    return res;
  }

  /**
   * Default to stop gracefully so that tests will finish processing even if slower for reasons
   * such as a slow runtime environment.
   */
  public static <T> SparkPipelineResult runAndAssertContents(
      Pipeline p,
      PCollection<T> actual,
      T[] expected,
      Duration timeout) {
    return runAndAssertContents(p, actual, expected, timeout, true);
  }

  private static class AssertDoFn<T> extends DoFn<Iterable<T>, Void> {
    private final Aggregator<Integer, Integer> success =
        createAggregator(PAssert.SUCCESS_COUNTER, Sum.ofIntegers());
    private final Aggregator<Integer, Integer> failure =
        createAggregator(PAssert.FAILURE_COUNTER, Sum.ofIntegers());
    private final T[] expected;

    AssertDoFn(T[] expected) {
      this.expected = expected;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      try {
        assertThat(c.element(), containsInAnyOrder(expected));
        success.addValue(1);
      } catch (Throwable t) {
        failure.addValue(1);
        LOG.error("PAssert failed expectations.", t);
        // don't throw t because it will fail this bundle and the failure count will be lost.
      }
    }
  }
}
