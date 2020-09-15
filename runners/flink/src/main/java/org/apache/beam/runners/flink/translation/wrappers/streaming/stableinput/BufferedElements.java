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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Elements which can be buffered as part of a checkpoint for @RequiresStableInput. */
class BufferedElements {

  static final class Element implements BufferedElement {
    private final WindowedValue element;

    Element(WindowedValue element) {
      this.element = element;
    }

    @Override
    public void processWith(DoFnRunner doFnRunner) {
      doFnRunner.processElement(element);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Element element1 = (Element) o;
      return element.equals(element1.element);
    }

    @Override
    public int hashCode() {
      return Objects.hash(element);
    }
  }

  static final class Timer<KeyT> implements BufferedElement {

    private final String timerId;
    private final String timerFamilyId;
    private final BoundedWindow window;
    private final Instant timestamp;
    private final Instant outputTimestamp;
    private final TimeDomain timeDomain;
    private final KeyT key;

    Timer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      this.timerId = timerId;
      this.window = window;
      this.timestamp = timestamp;
      this.key = key;
      this.timeDomain = timeDomain;
      this.outputTimestamp = outputTimestamp;
      this.timerFamilyId = timerFamilyId;
    }

    @Override
    public void processWith(DoFnRunner doFnRunner) {
      doFnRunner.onTimer(
          timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Timer timer = (Timer) o;
      return timerId.equals(timer.timerId)
          && window.equals(timer.window)
          && timestamp.equals(timer.timestamp)
          && timeDomain == timer.timeDomain;
    }

    @Override
    public int hashCode() {
      return Objects.hash(timerId, window, timestamp, timeDomain);
    }
  }

  static class Coder extends org.apache.beam.sdk.coders.Coder<BufferedElement> {

    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();
    private static final int ELEMENT_MAGIC_BYTE = 0;
    private static final int TIMER_MAGIC_BYTE = 1;

    private final org.apache.beam.sdk.coders.Coder<WindowedValue> elementCoder;
    private final org.apache.beam.sdk.coders.Coder<BoundedWindow> windowCoder;
    private final Object key;

    public Coder(
        org.apache.beam.sdk.coders.Coder<WindowedValue> elementCoder,
        org.apache.beam.sdk.coders.Coder<BoundedWindow> windowCoder,
        Object key) {
      this.elementCoder = elementCoder;
      this.windowCoder = windowCoder;
      this.key = key;
    }

    @Override
    public void encode(BufferedElement value, OutputStream outStream) throws IOException {
      if (value instanceof Element) {
        outStream.write(ELEMENT_MAGIC_BYTE);
        elementCoder.encode(((Element) value).element, outStream);
      } else if (value instanceof Timer) {
        outStream.write(TIMER_MAGIC_BYTE);
        Timer timer = (Timer) value;
        STRING_CODER.encode(timer.timerId, outStream);
        STRING_CODER.encode(timer.timerFamilyId, outStream);
        windowCoder.encode(timer.window, outStream);
        INSTANT_CODER.encode(timer.timestamp, outStream);
        INSTANT_CODER.encode(timer.outputTimestamp, outStream);
        outStream.write(timer.timeDomain.ordinal());
      } else {
        throw new IllegalStateException("Unexpected element " + value);
      }
    }

    @Override
    public BufferedElement decode(InputStream inStream) throws IOException {
      int firstByte = inStream.read();
      switch (firstByte) {
        case ELEMENT_MAGIC_BYTE:
          return new Element(elementCoder.decode(inStream));
        case TIMER_MAGIC_BYTE:
          return new Timer<>(
              STRING_CODER.decode(inStream),
              STRING_CODER.decode(inStream),
              key,
              windowCoder.decode(inStream),
              INSTANT_CODER.decode(inStream),
              INSTANT_CODER.decode(inStream),
              TimeDomain.values()[inStream.read()]);
        default:
          throw new IllegalStateException(
              "Unexpected byte while reading BufferedElement: " + firstByte);
      }
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Arrays.asList(elementCoder, windowCoder);
    }

    @Override
    public void verifyDeterministic() {}
  }
}
