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
package org.apache.beam.runners.core;

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link OutputAndTimeBoundedSplittableProcessElementInvoker}. */
public class OutputAndTimeBoundedSplittableProcessElementInvokerTest {
  private static class SomeFn extends DoFn<Integer, String> {
    private final int numOutputsPerProcessCall;
    private final Duration sleepBeforeEachOutput;

    private SomeFn(int numOutputsPerProcessCall, Duration sleepBeforeEachOutput) {
      this.numOutputsPerProcessCall = numOutputsPerProcessCall;
      this.sleepBeforeEachOutput = sleepBeforeEachOutput;
    }

    @ProcessElement
    public ProcessContinuation process(ProcessContext context, OffsetRangeTracker tracker)
        throws Exception {
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 1;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        Thread.sleep(sleepBeforeEachOutput.getMillis());
        context.output("" + i);
        if (numIterations == numOutputsPerProcessCall) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(Integer element) {
      throw new UnsupportedOperationException("Should not be called in this test");
    }
  }

  private SplittableProcessElementInvoker<Integer, String, OffsetRange, OffsetRangeTracker>.Result
      runTest(int totalNumOutputs, int numOutputsPerProcessCall, Duration sleepPerElement) {
    SomeFn fn = new SomeFn(numOutputsPerProcessCall, sleepPerElement);
    SplittableProcessElementInvoker<Integer, String, OffsetRange, OffsetRangeTracker> invoker =
        new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            fn,
            PipelineOptionsFactory.create(),
            new OutputWindowedValue<String>() {
              @Override
              public void outputWindowedValue(
                  String output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {}

              @Override
              public <AdditionalOutputT> void outputWindowedValue(
                  TupleTag<AdditionalOutputT> tag,
                  AdditionalOutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {}
            },
            NullSideInputReader.empty(),
            Executors.newSingleThreadScheduledExecutor(),
            1000,
            Duration.standardSeconds(3));

    return invoker.invokeProcessElement(
        DoFnInvokers.invokerFor(fn),
        WindowedValue.of(totalNumOutputs, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING),
        new OffsetRangeTracker(new OffsetRange(0, totalNumOutputs)));
  }

  @Test
  public void testInvokeProcessElementOutputBounded() throws Exception {
    SplittableProcessElementInvoker<Integer, String, OffsetRange, OffsetRangeTracker>.Result res =
        runTest(10000, Integer.MAX_VALUE, Duration.ZERO);
    assertFalse(res.getContinuation().shouldResume());
    OffsetRange residualRange = res.getResidualRestriction();
    // Should process the first 100 elements.
    assertEquals(1000, residualRange.getFrom());
    assertEquals(10000, residualRange.getTo());
  }

  @Test
  public void testInvokeProcessElementTimeBounded() throws Exception {
    SplittableProcessElementInvoker<Integer, String, OffsetRange, OffsetRangeTracker>.Result res =
        runTest(10000, Integer.MAX_VALUE, Duration.millis(100));
    assertFalse(res.getContinuation().shouldResume());
    OffsetRange residualRange = res.getResidualRestriction();
    // Should process ideally around 30 elements - but due to timing flakiness, we can't enforce
    // that precisely. Just test that it's not egregiously off.
    assertThat(residualRange.getFrom(), greaterThan(10L));
    assertThat(residualRange.getFrom(), lessThan(100L));
    assertEquals(10000, residualRange.getTo());
  }

  @Test
  public void testInvokeProcessElementVoluntaryReturnStop() throws Exception {
    SplittableProcessElementInvoker<Integer, String, OffsetRange, OffsetRangeTracker>.Result res =
        runTest(5, Integer.MAX_VALUE, Duration.millis(100));
    assertFalse(res.getContinuation().shouldResume());
    assertNull(res.getResidualRestriction());
  }

  @Test
  public void testInvokeProcessElementVoluntaryReturnResume() throws Exception {
    SplittableProcessElementInvoker<Integer, String, OffsetRange, OffsetRangeTracker>.Result res =
        runTest(10, 5, Duration.millis(100));
    assertTrue(res.getContinuation().shouldResume());
    assertEquals(new OffsetRange(5, 10), res.getResidualRestriction());
  }
}
