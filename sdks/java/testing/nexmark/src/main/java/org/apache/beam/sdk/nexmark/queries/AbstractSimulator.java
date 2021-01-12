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
package org.apache.beam.sdk.nexmark.queries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Abstract base class for simulator of a query.
 *
 * @param <InputT> Type of input elements.
 * @param <OutputT> Type of output elements.
 */
public abstract class AbstractSimulator<InputT, OutputT extends KnownSize> {
  /** Window size for action bucket sampling. */
  private static final Duration WINDOW_SIZE = Duration.standardMinutes(1);

  /** Input event stream we should draw from. */
  private final Iterator<TimestampedValue<InputT>> input;

  /** Set to true when no more results. */
  private boolean isDone;

  /** Results which have not yet been returned by the {@link #results} iterator. */
  private final List<TimestampedValue<OutputT>> pendingResults;

  /** Current window timestamp (ms since epoch). */
  private long currentWindow;

  /** Number of (possibly intermediate) results for the current window. */
  private long currentCount;

  /**
   * Result counts per window which have not yet been returned by the {@link #resultsPerWindow}
   * iterator.
   */
  private final List<Long> pendingCounts;

  public AbstractSimulator(Iterator<TimestampedValue<InputT>> input) {
    this.input = input;
    isDone = false;
    pendingResults = new ArrayList<>();
    currentWindow = BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis();
    currentCount = 0;
    pendingCounts = new ArrayList<>();
  }

  /** Called by implementors of {@link #run}: Fetch the next input element. */
  @Nullable
  TimestampedValue<InputT> nextInput() {
    if (!input.hasNext()) {
      return null;
    }
    TimestampedValue<InputT> timestampedInput = input.next();
    NexmarkUtils.info("input: %s", timestampedInput);
    return timestampedInput;
  }

  /**
   * Called by implementors of {@link #run}: Capture an intermediate result, for the purpose of
   * recording the expected activity of the query over time.
   */
  void addIntermediateResult(TimestampedValue<OutputT> result) {
    NexmarkUtils.info("intermediate result: %s", result);
    updateCounts(result.getTimestamp());
  }

  /**
   * Called by implementors of {@link #run}: Capture a final result, for the purpose of checking
   * semantic correctness.
   */
  void addResult(TimestampedValue<OutputT> result) {
    NexmarkUtils.info("result: %s", result);
    pendingResults.add(result);
    updateCounts(result.getTimestamp());
  }

  /** Update window and counts. */
  private void updateCounts(Instant timestamp) {
    long window = timestamp.getMillis() - timestamp.getMillis() % WINDOW_SIZE.getMillis();
    if (window > currentWindow) {
      if (currentWindow > BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()) {
        pendingCounts.add(currentCount);
      }
      currentCount = 0;
      currentWindow = window;
    }
    currentCount++;
  }

  /** Called by implementors of {@link #run}: Record that no more results will be emitted. */
  void allDone() {
    isDone = true;
  }

  /**
   * Overridden by derived classes to do the next increment of work. Each call should call one or
   * more of {@link #nextInput}, {@link #addIntermediateResult}, {@link #addResult} or {@link
   * #allDone}. It is ok for a single call to emit more than one result via {@link #addResult}. It
   * is ok for a single call to run the entire simulation, though this will prevent the {@link
   * #results} and {@link #resultsPerWindow} iterators to stall.
   */
  protected abstract void run();

  /**
   * Return iterator over all expected timestamped results. The underlying simulator state is
   * changed. Only one of {@link #results} or {@link #resultsPerWindow} can be called.
   */
  public Iterator<TimestampedValue<OutputT>> results() {
    return new Iterator<TimestampedValue<OutputT>>() {
      @Override
      public boolean hasNext() {
        while (true) {
          if (!pendingResults.isEmpty()) {
            return true;
          }
          if (isDone) {
            return false;
          }
          run();
        }
      }

      @Override
      public TimestampedValue<OutputT> next() {
        TimestampedValue<OutputT> result = pendingResults.get(0);
        pendingResults.remove(0);
        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Return an iterator over the number of results per {@link #WINDOW_SIZE} period. The underlying
   * simulator state is changed. Only one of {@link #results} or {@link #resultsPerWindow} can be
   * called.
   */
  public Iterator<Long> resultsPerWindow() {
    return new Iterator<Long>() {
      @Override
      public boolean hasNext() {
        while (true) {
          if (!pendingCounts.isEmpty()) {
            return true;
          }
          if (isDone) {
            if (currentCount > 0) {
              pendingCounts.add(currentCount);
              currentCount = 0;
              currentWindow = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
              return true;
            } else {
              return false;
            }
          }
          run();
        }
      }

      @Override
      public Long next() {
        Long result = pendingCounts.get(0);
        pendingCounts.remove(0);
        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
