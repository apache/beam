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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

/**
 * Implements a group of linked {@link ProgressTracker ProgressTrackers} that collectively track how
 * far a processing loop has gotten through the elements it's processing. Individual {@code
 * ProgressTracker} instances may be copied, capturing an independent view of the progress of the
 * system; this turns out to be useful for some non-trivial processing loops. The furthest point
 * reached by any {@code ProgressTracker} is the one reported.
 *
 * <p>This class is abstract. Its single extension point is {@link #report}, which should be
 * overridden to provide a function that handles the reporting of the supplied element, as
 * appropriate.
 *
 * @param <T> the type of elements being tracked
 */
public abstract class ProgressTrackerGroup<T> {
  // TODO: Instead of an abstract class, strongly consider adding an
  // interface like Receiver to the SDK, so that this class can be final and all
  // that good stuff.
  private long nextIndexToReport = 0;

  public ProgressTrackerGroup() {}

  public final ProgressTracker<T> start() {
    return new Tracker(0);
  }

  /** Reports the indicated element. */
  protected abstract void report(T element);

  private final class Tracker implements ProgressTracker<T> {
    private long nextElementIndex;

    private Tracker(long nextElementIndex) {
      this.nextElementIndex = nextElementIndex;
    }

    @Override
    public ProgressTracker<T> copy() {
      return new Tracker(nextElementIndex);
    }

    @Override
    public void saw(T element) {
      long thisElementIndex = nextElementIndex;
      nextElementIndex++;
      if (thisElementIndex == nextIndexToReport) {
        nextIndexToReport = nextElementIndex;
        report(element);
      }
    }
  }
}
