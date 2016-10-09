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
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;



/**
 * Since PAssert doesn't propagate assert exceptions, use Aggregators to assert streaming
 * success/failure counters.
 */
public final class PAssertStreaming implements Serializable {

  private static final String ASSERT_MARKER = "PAssertExecuted";

  private PAssertStreaming() {
  }

  /**
   * Adds a pipeline run-time assertion that the contents of {@code actual} are {@code expected}.
   * Note that it is oblivious to windowing, so the assertion will apply indiscriminately to all
   * windows.
   */
  public static <T> EvaluationResult runAndAssertContents(Pipeline p,
                                                          PCollection<T> actual,
                                                          T[] expected) {
    // Because PAssert does not support non-global windowing, but all our data is in one window,
    // we set up the assertion directly.
    actual
        .apply(WithKeys.<String, T>of("dummy"))
        .apply(GroupByKey.<String, T>create())
        .apply(Values.<Iterable<T>>create())
        .apply(ParDo.of(new AssertDoFn<>(expected)));

    // run the pipeline.
    EvaluationResult res = (EvaluationResult) p.run();
    res.close();
    // validate assertion happened at least once.
    int marker = res.getAggregatorValue(ASSERT_MARKER, Integer.class);
    Assert.assertThat("Marker should be greater than zero.", marker, not(0));
    return res;
  }

  private static class AssertDoFn<T> extends OldDoFn<Iterable<T>, Void> {
    private final Aggregator<Integer, Integer> aggregator =
        createAggregator(ASSERT_MARKER, new Sum.SumIntegerFn());
    private final T[] expected;

    AssertDoFn(T[] expected) {
      this.expected = expected;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      assertThat(c.element(), containsInAnyOrder(expected));
      aggregator.addValue(1);
    }
  }
}
