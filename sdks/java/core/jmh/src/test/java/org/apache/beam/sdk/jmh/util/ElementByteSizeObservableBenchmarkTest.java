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
package org.apache.beam.sdk.jmh.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collection;
import org.apache.beam.sdk.jmh.util.ElementByteSizeObservableBenchmark.ObservableState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/** Smoke tests for {@link ElementByteSizeObservableBenchmark}. */
@RunWith(JUnit4.class)
public class ElementByteSizeObservableBenchmarkTest {

  @Test
  public void testDirectBenchmarksAccumulateExactExpectedValues() throws Exception {
    ElementByteSizeObservableBenchmark benchmark = new ElementByteSizeObservableBenchmark();
    ObservableState state = new ObservableState();
    state.setUpTrial();

    long expectedTotal = 0L;
    long expectedScaledTotal = 0L;
    long expectedPolyTotal = 0L;
    for (int i = 0; i < state.sizes.length; i++) {
      long sz = state.sizes[i];
      expectedTotal += sz;
      expectedScaledTotal += (long) (1.5 * sz);
      int bucket = i % 5;
      if (bucket == 0) {
        expectedPolyTotal += sz + 1L; // IteratorObserver (non-Collection) adds +1 per element
      } else if (bucket < 4) {
        expectedPolyTotal += sz;
      }
    }

    state.resetIteration();
    assertThat(benchmark.notifyValueReturned_noObservers(state), equalTo(0L));

    state.resetIteration();
    assertThat(
        benchmark.notifyValueReturned_singleElementByteSizeObserver(state), equalTo(expectedTotal));

    state.resetIteration();
    assertThat(
        benchmark.notifyValueReturned_singleElementByteSizeObserverScaled(state),
        equalTo(expectedScaledTotal));

    state.resetIteration();
    assertThat(
        benchmark.notifyValueReturned_twoElementByteSizeObservers(state),
        equalTo(2L * expectedTotal));

    // Non-Collection IterableCoder adds +1 byte per element on top of sizes (5L header was cleared
    // in resetIteration).
    state.resetIteration();
    assertThat(
        benchmark.notifyValueReturned_iterableLikeCoderIteratorObserver(state),
        equalTo(expectedTotal + ElementByteSizeObservableBenchmark.BATCH_SIZE));

    state.resetIteration();
    assertThat(
        benchmark.notifyValueReturned_polymorphicObservers(state), equalTo(expectedPolyTotal));
  }

  @Test
  public void testBlackholeBenchmarksExecuteCleanlyViaJmhRunner() throws Exception {
    String prevIgnoreLock = System.getProperty("jmh.ignoreLock");
    System.setProperty("jmh.ignoreLock", "true");
    try {
      Options opts =
          new OptionsBuilder()
              .include(
                  ElementByteSizeObservableBenchmark.class.getName()
                      + "\\.(iterableCreateAndIterate_.*|notifyValueReturned_mixedMultiObserver"
                      + "|notifyValueReturned_singleGenericObserver"
                      + "|concurrentIterableLikeCoder_createAndIterate)")
              .forks(0)
              .warmupIterations(0)
              .measurementIterations(1)
              .mode(Mode.SingleShotTime)
              .shouldFailOnError(true)
              .build();
      Collection<RunResult> results = new Runner(opts).run();
      assertThat(results.size(), equalTo(7));
    } finally {
      if (prevIgnoreLock == null) {
        System.clearProperty("jmh.ignoreLock");
      } else {
        System.setProperty("jmh.ignoreLock", prevIgnoreLock);
      }
    }
  }
}
