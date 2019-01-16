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
package org.apache.beam.runners.samza.adapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.ElementEvent;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.Event;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.ExceptionEvent;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.LatchEvent;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.SourceBuilder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/** A bounded source that can be used for test purposes. */
public class TestBoundedSource<T> extends BoundedSource<T> {
  // each list of events is a split
  private final List<List<Event<T>>> events;

  public static <T> Builder<T> createBuilder() {
    return new Builder<>();
  }

  public static <T> SplittableBuilder<T> createSplits(int numSplits) {
    return new SplittableBuilder<>(numSplits);
  }

  private TestBoundedSource(List<List<Event<T>>> events) {
    this.events = Collections.unmodifiableList(new ArrayList<>(events));
  }

  @Override
  public List<? extends BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    return events.stream()
        .map(ev -> new TestBoundedSource<>(Collections.singletonList(ev)))
        .collect(Collectors.toList());
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return events.size();
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    assert events.size() == 1;

    return new Reader(events.get(0));
  }

  @Override
  public void validate() {}

  /** A builder used to populate the events emitted by {@link TestBoundedSource}. */
  public static class Builder<T> extends SourceBuilder<T, TestBoundedSource<T>> {
    @Override
    public TestBoundedSource<T> build() {
      return new TestBoundedSource<>(Collections.singletonList(getEvents()));
    }
  }

  /**
   * A SplittableBuilder supports multiple splits and each split {@link TestUnboundedSource} can be
   * built separately from the above Builder.
   */
  public static class SplittableBuilder<T> extends SourceBuilder<T, TestBoundedSource<T>> {
    private final List<Builder<T>> builders = new ArrayList<>();

    private SplittableBuilder(int splits) {
      while (splits != 0) {
        builders.add(new Builder<T>());
        --splits;
      }
    }

    @Override
    public TestBoundedSource<T> build() {
      final List<List<Event<T>>> events = new ArrayList<>();
      builders.forEach(builder -> events.add(builder.getEvents()));
      return new TestBoundedSource<>(events);
    }

    public Builder<T> forSplit(int split) {
      return builders.get(split);
    }
  }

  private class Reader extends BoundedReader<T> {
    private final List<Event<T>> events;
    private boolean started;
    private boolean finished;
    private int index = -1;

    private Reader(List<Event<T>> events) {
      this.events = events;
    }

    @Override
    public boolean start() throws IOException {
      if (started) {
        throw new IllegalStateException("Start called when reader was already started");
      }
      started = true;
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (!started) {
        throw new IllegalStateException("Advance called when reader was not started");
      }

      if (finished) {
        return false;
      }

      for (++index; index < events.size(); ++index) {
        final Event<T> event = events.get(index);
        if (event instanceof ExceptionEvent) {
          throw ((ExceptionEvent<T>) event).exception;
        } else if (event instanceof LatchEvent) {
          try {
            ((LatchEvent) event).latch.await();
          } catch (InterruptedException e) {
            // Propagate interrupt
            Thread.currentThread().interrupt();
          }
        } else {
          return true;
        }
      }

      finished = true;
      return false;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (!started || finished) {
        throw new NoSuchElementException();
      }

      final Event<T> event = events.get(index);
      assert event instanceof ElementEvent;
      return ((ElementEvent<T>) event).element;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (!started || finished) {
        throw new NoSuchElementException();
      }

      final Event<T> event = events.get(index);
      assert event instanceof ElementEvent;
      return ((ElementEvent<T>) event).timestamp;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public BoundedSource<T> getCurrentSource() {
      return TestBoundedSource.this;
    }
  }
}
