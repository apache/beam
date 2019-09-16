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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.joda.time.Instant;

/** Helper classes and functions to build source for testing. */
public class TestSourceHelpers {

  private TestSourceHelpers() {}

  interface Event<T> {}

  static class ElementEvent<T> implements Event<T> {
    final T element;
    final Instant timestamp;

    private ElementEvent(T element, Instant timestamp) {
      this.element = element;
      this.timestamp = timestamp;
    }
  }

  static class WatermarkEvent<T> implements Event<T> {
    final Instant watermark;

    private WatermarkEvent(Instant watermark) {
      this.watermark = watermark;
    }
  }

  static class ExceptionEvent<T> implements Event<T> {
    final IOException exception;

    private ExceptionEvent(IOException exception) {
      this.exception = exception;
    }
  }

  static class LatchEvent<T> implements Event<T> {
    final CountDownLatch latch;

    private LatchEvent(CountDownLatch latch) {
      this.latch = latch;
    }
  }

  static class NoElementEvent<T> implements Event<T> {}

  /** A builder used to populate the events emitted by {@link TestBoundedSource}. */
  abstract static class SourceBuilder<T, W extends Source<T>> {
    private final List<Event<T>> events = new ArrayList<>();
    private Instant currentTimestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;

    @SafeVarargs
    public final SourceBuilder<T, W> addElements(T... elements) {
      for (T element : elements) {
        events.add(new ElementEvent<>(element, currentTimestamp));
      }
      return this;
    }

    public SourceBuilder<T, W> addException(IOException exception) {
      events.add(new ExceptionEvent<>(exception));
      return this;
    }

    public SourceBuilder<T, W> addLatch(CountDownLatch latch) {
      events.add(new LatchEvent<>(latch));
      return this;
    }

    public SourceBuilder<T, W> setTimestamp(Instant timestamp) {
      assertTrue(
          "Expected " + timestamp + " to be greater than or equal to " + currentTimestamp,
          timestamp.isEqual(currentTimestamp) || timestamp.isAfter(currentTimestamp));
      currentTimestamp = timestamp;
      return this;
    }

    public SourceBuilder<T, W> advanceWatermarkTo(Instant watermark) {
      events.add(new WatermarkEvent<>(watermark));
      return this;
    }

    public SourceBuilder<T, W> noElements() {
      events.add(new NoElementEvent<T>());
      return this;
    }

    protected List<Event<T>> getEvents() {
      return this.events;
    }

    public abstract W build();
  }

  static IncomingMessageEnvelope createElementMessage(
      SystemStreamPartition ssp, String offset, String element, Instant timestamp) {
    return new IncomingMessageEnvelope(
        ssp,
        offset,
        null,
        OpMessage.ofElement(WindowedValue.timestampedValueInGlobalWindow(element, timestamp)));
  }

  static IncomingMessageEnvelope createWatermarkMessage(
      SystemStreamPartition ssp, Instant watermark) {
    return IncomingMessageEnvelope.buildWatermarkEnvelope(ssp, watermark.getMillis());
  }

  static IncomingMessageEnvelope createEndOfStreamMessage(SystemStreamPartition ssp) {
    return IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp);
  }

  static <T> void expectWrappedException(Exception expectedException, Callable<T> callable)
      throws Exception {
    try {
      callable.call();
      fail("Expected exception (" + expectedException + "), but no exception was thrown");
    } catch (Exception e) {
      Throwable currentException = e;
      while (currentException != null) {
        if (currentException.equals(expectedException)) {
          return;
        }
        currentException = currentException.getCause();
      }
      assertEquals(expectedException, e);
    }
  }
}
