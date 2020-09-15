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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.worker.fn.control.BeamFnMapTaskExecutor.Interpolator;
import org.apache.beam.runners.dataflow.worker.fn.control.BeamFnMapTaskExecutor.SingularProcessBundleProgressTracker;
import org.apache.beam.runners.dataflow.worker.fn.data.RemoteGrpcPortWriteOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.Progress;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link BeamFnMapTaskExecutor.SingularProcessBundleProgressTracker}. */
@RunWith(JUnit4.class)
public class SingularProcessBundleProgressTrackerTest {

  private static class TestProgress implements Progress {
    private final String name;

    public TestProgress(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      return o instanceof TestProgress && this.name.equals(((TestProgress) o).name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public String toString() {
      return "TestProgress[" + name + "]";
    }
  }

  @Test
  public void testProgressInterpolation() throws Exception {
    ReadOperation read = Mockito.mock(ReadOperation.class);
    RemoteGrpcPortWriteOperation grpcWrite = Mockito.mock(RemoteGrpcPortWriteOperation.class);
    RegisterAndProcessBundleOperation process =
        Mockito.mock(RegisterAndProcessBundleOperation.class);

    when(grpcWrite.processedElementsConsumer()).thenReturn(elementsConsumed -> {});

    SingularProcessBundleProgressTracker tracker =
        new SingularProcessBundleProgressTracker(read, grpcWrite, process);

    when(read.getProgress())
        .thenReturn(new TestProgress("A"), new TestProgress("B"), new TestProgress("C"));
    when(grpcWrite.getElementsSent()).thenReturn(1, 10, 20, 30);

    // This test ignores them, directly working on mocked getInputElementsConsumed
    when(process.getProcessBundleProgress())
        .thenReturn(
            CompletableFuture.completedFuture(
                BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance()));

    when(process.getInputElementsConsumed(any(Iterable.class)))
        .thenReturn(1L, 4L, 10L)
        .thenThrow(new RuntimeException());

    // Initially no progress is known.
    assertEquals(null, tracker.getWorkerProgress());

    // After reading, and writing, and processing one element, the progress is aligned at A.
    tracker.updateProgress();
    assertEquals(new TestProgress("A"), tracker.getWorkerProgress());

    // We've read up to B (10 elements) but only consumed 4.  Progress remains at A.
    tracker.updateProgress();
    assertEquals(new TestProgress("A"), tracker.getWorkerProgress());

    // Once 10 elements have been consumed, advance to B.
    tracker.updateProgress();
    assertEquals(new TestProgress("B"), tracker.getWorkerProgress());

    // An exception is thrown, default to latest read progress.
    tracker.updateProgress();
    assertEquals(new TestProgress("C"), tracker.getWorkerProgress());
  }

  @Test
  public void testInterpolator() {
    Interpolator<Double> interpolator =
        new Interpolator<Double>(10) {
          @Override
          protected Double interpolate(Double prev, Double next, double fraction) {
            // Simple linear interpolation.
            return prev * (1 - fraction) + next * fraction;
          }
        };
    // As long as no more than 10 values need to be kept in memory, no data is lost.
    // The comparisons below are *exact.*
    for (int i = 0; i < 100; i++) {
      interpolator.addPoint(i, Math.sqrt(i));
      assertEquals(Math.sqrt(i), interpolator.interpolate(i), 0);
      if (i > 5) {
        assertEquals(Math.sqrt(i - 1), interpolator.interpolate(i - 1), 0);
        assertEquals(Math.sqrt(i - 5), interpolator.interpolate(i - 5), 0);
      }
      if (i % 3 == 0) {
        interpolator.purgeUpTo(i - 5);
      }
    }
    // Now we stop purging old values, forcing purging by interpolation.
    // The comparisons are no longer exact, but still reasonably close.
    for (int i = 100; i < 200; i++) {
      interpolator.addPoint(i, Math.sqrt(i));
      assertEquals(Math.sqrt(i), interpolator.interpolate(i), 0);
      if (i > 110) {
        assertNotEquals(10.0, interpolator.interpolate(100), 0);
        assertEquals(10.0, interpolator.interpolate(100), .02);
      }
    }
  }
}
