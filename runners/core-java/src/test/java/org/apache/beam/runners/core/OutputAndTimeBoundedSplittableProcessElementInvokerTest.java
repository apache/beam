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
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link OutputAndTimeBoundedSplittableProcessElementInvoker}. */
public class OutputAndTimeBoundedSplittableProcessElementInvokerTest {
  @Rule public transient ExpectedException e = ExpectedException.none();

  private static class SomeFn extends DoFn<Void, String> {
    private final Duration sleepBeforeFirstClaim;
    private final int numOutputsPerProcessCall;
    private final Duration sleepBeforeEachOutput;

    private SomeFn(
        Duration sleepBeforeFirstClaim,
        int numOutputsPerProcessCall,
        Duration sleepBeforeEachOutput) {
      this.sleepBeforeFirstClaim = sleepBeforeFirstClaim;
      this.numOutputsPerProcessCall = numOutputsPerProcessCall;
      this.sleepBeforeEachOutput = sleepBeforeEachOutput;
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker) {
      Uninterruptibles.sleepUninterruptibly(
          sleepBeforeFirstClaim.getMillis(), TimeUnit.MILLISECONDS);
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 1;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        Uninterruptibles.sleepUninterruptibly(
            sleepBeforeEachOutput.getMillis(), TimeUnit.MILLISECONDS);
        context.output("" + i);
        if (numIterations == numOutputsPerProcessCall) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element Void element) {
      throw new UnsupportedOperationException("Should not be called in this test");
    }
  }

  private SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result runTest(
      int totalNumOutputs,
      Duration sleepBeforeFirstClaim,
      int numOutputsPerProcessCall,
      Duration sleepBeforeEachOutput)
      throws Exception {
    SomeFn fn = new SomeFn(sleepBeforeFirstClaim, numOutputsPerProcessCall, sleepBeforeEachOutput);
    OffsetRange initialRestriction = new OffsetRange(0, totalNumOutputs);
    return runTest(fn, initialRestriction);
  }

  private SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result runTest(
      DoFn<Void, String> fn, OffsetRange initialRestriction) throws Exception {
    SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void> invoker =
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
            Duration.standardSeconds(3),
            () -> {
              throw new UnsupportedOperationException("BundleFinalizer not configured for test.");
            });

    SplittableProcessElementInvoker.Result rval =
        invoker.invokeProcessElement(
            DoFnInvokers.invokerFor(fn),
            WindowedValue.of(null, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING),
            new OffsetRangeTracker(initialRestriction),
            new WatermarkEstimator<Void>() {
              @Override
              public Instant currentWatermark() {
                return GlobalWindow.TIMESTAMP_MIN_VALUE;
              }

              @Override
              public Void getState() {
                return null;
              }
            });
    return rval;
  }

  @Test
  public void testInvokeProcessElementOutputBounded() throws Exception {
    SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result res =
        runTest(10000, Duration.ZERO, Integer.MAX_VALUE, Duration.ZERO);
    assertFalse(res.getContinuation().shouldResume());
    OffsetRange residualRange = res.getResidualRestriction();
    // Should process the first 100 elements.
    assertEquals(1000, residualRange.getFrom());
    assertEquals(10000, residualRange.getTo());
  }

  @Test
  public void testInvokeProcessElementTimeBounded() throws Exception {
    SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result res =
        runTest(10000, Duration.ZERO, Integer.MAX_VALUE, Duration.millis(100));
    assertFalse(res.getContinuation().shouldResume());
    OffsetRange residualRange = res.getResidualRestriction();
    // Should process ideally around 30 elements - but due to timing flakiness, we can't enforce
    // that precisely. Just test that it's not egregiously off.
    assertThat(residualRange.getFrom(), greaterThan(10L));
    assertThat(residualRange.getFrom(), lessThan(100L));
    assertEquals(10000, residualRange.getTo());
  }

  @Test
  public void testInvokeProcessElementTimeBoundedWithStartupDelay() throws Exception {
    SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result res =
        runTest(10000, Duration.standardSeconds(3), Integer.MAX_VALUE, Duration.millis(100));
    assertFalse(res.getContinuation().shouldResume());
    OffsetRange residualRange = res.getResidualRestriction();
    // Same as above, but this time it counts from the time of the first tryClaim() call
    assertThat(residualRange.getFrom(), greaterThan(10L));
    assertThat(residualRange.getFrom(), lessThan(100L));
    assertEquals(10000, residualRange.getTo());
  }

  @Test
  public void testInvokeProcessElementVoluntaryReturnStop() throws Exception {
    SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result res =
        runTest(5, Duration.ZERO, Integer.MAX_VALUE, Duration.millis(100));
    assertFalse(res.getContinuation().shouldResume());
    assertNull(res.getResidualRestriction());
  }

  @Test
  public void testInvokeProcessElementVoluntaryReturnResume() throws Exception {
    SplittableProcessElementInvoker<Void, String, OffsetRange, Long, Void>.Result res =
        runTest(10, Duration.ZERO, 5, Duration.millis(100));
    assertTrue(res.getContinuation().shouldResume());
    assertEquals(new OffsetRange(5, 10), res.getResidualRestriction());
  }

  @Test
  public void testInvokeProcessElementOutputDisallowedBeforeTryClaim() throws Exception {
    DoFn<Void, String> brokenFn =
        new DoFn<Void, String>() {
          @ProcessElement
          public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
            c.output("foo");
          }

          @GetInitialRestriction
          public OffsetRange getInitialRestriction(@Element Void element) {
            throw new UnsupportedOperationException("Should not be called in this test");
          }
        };
    e.expectMessage("Output is not allowed before tryClaim()");
    runTest(brokenFn, new OffsetRange(0, 5));
  }

  @Test
  public void testInvokeProcessElementOutputDisallowedAfterFailedTryClaim() throws Exception {
    DoFn<Void, String> brokenFn =
        new DoFn<Void, String>() {
          @ProcessElement
          public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
            assertFalse(tracker.tryClaim(6L));
            c.output("foo");
          }

          @GetInitialRestriction
          public OffsetRange getInitialRestriction(@Element Void element) {
            throw new UnsupportedOperationException("Should not be called in this test");
          }
        };
    e.expectMessage("Output is not allowed after a failed tryClaim()");
    runTest(brokenFn, new OffsetRange(0, 5));
  }
}
