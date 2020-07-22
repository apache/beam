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
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.NoElementEvent;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.SourceBuilder;
import org.apache.beam.runners.samza.adapter.TestSourceHelpers.WatermarkEvent;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A unbounded source that can be used for test purposes.
 *
 * @param <T> element type
 */
public class TestUnboundedSource<T> extends UnboundedSource<T, TestCheckpointMark> {
  // each list of events is a split
  private final List<List<Event<T>>> events;

  public static <T> Builder<T> createBuilder() {
    return new Builder<>();
  }

  public static <T> SplittableBuilder<T> createSplits(int numSplits) {
    return new SplittableBuilder<>(numSplits);
  }

  private TestUnboundedSource(List<List<Event<T>>> events) {
    this.events = Collections.unmodifiableList(new ArrayList<>(events));
  }

  @Override
  public List<? extends UnboundedSource<T, TestCheckpointMark>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    return events.stream()
        .map(ev -> new TestUnboundedSource<>(Collections.singletonList(ev)))
        .collect(Collectors.toList());
  }

  @Override
  public UnboundedReader<T> createReader(
      PipelineOptions options, @Nullable TestCheckpointMark checkpointMark) throws IOException {
    assert events.size() == 1;

    return new Reader(events.get(0), checkpointMark);
  }

  @Override
  public Coder<TestCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(TestCheckpointMark.class);
  }

  @Override
  public void validate() {}

  /** A builder used to populate the events emitted by {@link TestUnboundedSource}. */
  public static class Builder<T> extends SourceBuilder<T, TestUnboundedSource<T>> {

    @Override
    public TestUnboundedSource<T> build() {
      return new TestUnboundedSource<>(Collections.singletonList(getEvents()));
    }
  }

  /**
   * A SplittableBuilder supports multiple splits and each split {@link TestUnboundedSource} can be
   * built separately from the above Builder.
   */
  public static class SplittableBuilder<T> extends SourceBuilder<T, TestUnboundedSource<T>> {
    private final List<Builder<T>> builders = new ArrayList<>();

    private SplittableBuilder(int splits) {
      while (splits != 0) {
        builders.add(new Builder<T>());
        --splits;
      }
    }

    @Override
    public TestUnboundedSource<T> build() {
      List<List<Event<T>>> events = new ArrayList<>();
      builders.forEach(builder -> events.add(builder.getEvents()));
      return new TestUnboundedSource<>(events);
    }

    public Builder<T> forSplit(int split) {
      return builders.get(split);
    }
  }

  private class Reader extends UnboundedReader<T> {
    private final List<Event<T>> events;
    private Instant curTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
    private Instant watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    private boolean started;
    private int index = -1;
    private int offset;

    private Reader(List<Event<T>> events, TestCheckpointMark checkpointMark) {
      this.events = events;
      this.offset = checkpointMark == null ? -1 : checkpointMark.checkpoint;
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
        } else if (event instanceof WatermarkEvent) {
          watermark = ((WatermarkEvent) event).watermark;
        } else if (event instanceof NoElementEvent) {
          return false;
        } else {
          curTime = ((ElementEvent<T>) event).timestamp;
          ++offset;
          return true;
        }
      }

      return false;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (!started) {
        throw new NoSuchElementException();
      }

      final Event<T> event = events.get(index);
      assert event instanceof ElementEvent;
      return ((ElementEvent<T>) event).element;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return curTime;
    }

    @Override
    public Instant getWatermark() {
      return watermark;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return TestCheckpointMark.of(offset);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public UnboundedSource<T, ?> getCurrentSource() {
      return TestUnboundedSource.this;
    }
  }
}
