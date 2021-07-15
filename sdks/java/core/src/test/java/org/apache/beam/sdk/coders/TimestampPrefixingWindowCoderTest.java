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
package org.apache.beam.sdk.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.CoderProperties.TestElementByteSizeObserver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;

public class TimestampPrefixingWindowCoderTest {

  private static class CustomWindow extends IntervalWindow {
    private boolean isBig;

    CustomWindow(Instant start, Instant end, boolean isBig) {
      super(start, end);
      this.isBig = isBig;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CustomWindow that = (CustomWindow) o;
      return super.equals(o) && this.isBig == that.isBig;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isBig);
    }
  }

  private static class CustomWindowCoder extends CustomCoder<CustomWindow> {

    private static final CustomWindowCoder INSTANCE = new CustomWindowCoder();
    private static final Coder<IntervalWindow> INTERVAL_WINDOW_CODER = IntervalWindow.getCoder();

    public static CustomWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CustomWindow window, OutputStream outStream) throws IOException {
      INTERVAL_WINDOW_CODER.encode(window, outStream);
      BooleanCoder.of().encode(window.isBig, outStream);
    }

    @Override
    public CustomWindow decode(InputStream inStream) throws IOException {
      IntervalWindow superWindow = INTERVAL_WINDOW_CODER.decode(inStream);
      boolean isBig = BooleanCoder.of().decode(inStream);
      return new CustomWindow(superWindow.start(), superWindow.end(), isBig);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      INTERVAL_WINDOW_CODER.verifyDeterministic();
      BooleanCoder.of().verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
      return INTERVAL_WINDOW_CODER.consistentWithEquals()
          && BooleanCoder.of().consistentWithEquals();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(CustomWindow value) {
      return INTERVAL_WINDOW_CODER.isRegisterByteSizeObserverCheap(value)
          && BooleanCoder.of().isRegisterByteSizeObserverCheap(true);
    }

    @Override
    public void registerByteSizeObserver(CustomWindow value, ElementByteSizeObserver observer)
        throws Exception {
      INTERVAL_WINDOW_CODER.registerByteSizeObserver(value, observer);
      BooleanCoder.of().registerByteSizeObserver(true, observer);
    }
  }

  private static final List<CustomWindow> CUSTOM_WINDOW_LIST =
      Lists.newArrayList(
          new CustomWindow(new Instant(0L), new Instant(1L), true),
          new CustomWindow(new Instant(100L), new Instant(200L), false),
          new CustomWindow(new Instant(0L), BoundedWindow.TIMESTAMP_MAX_VALUE, true));

  private static final TimestampPrefixingWindowCoder<CustomWindow> TEST_CODER =
      TimestampPrefixingWindowCoder.of(CustomWindowCoder.of());

  @Test
  public void testEncodeAndDecode() throws Exception {
    List<IntervalWindow> intervalWindowsToTest =
        Lists.newArrayList(
            new IntervalWindow(new Instant(0L), new Instant(1L)),
            new IntervalWindow(new Instant(100L), new Instant(200L)),
            new IntervalWindow(new Instant(0L), BoundedWindow.TIMESTAMP_MAX_VALUE));
    TimestampPrefixingWindowCoder<IntervalWindow> coder1 =
        TimestampPrefixingWindowCoder.of(IntervalWindow.getCoder());
    for (IntervalWindow window : intervalWindowsToTest) {
      CoderProperties.coderDecodeEncodeEqual(coder1, window);
    }

    GlobalWindow globalWindow = GlobalWindow.INSTANCE;
    TimestampPrefixingWindowCoder<GlobalWindow> coder2 =
        TimestampPrefixingWindowCoder.of(GlobalWindow.Coder.INSTANCE);
    CoderProperties.coderDecodeEncodeEqual(coder2, globalWindow);

    for (CustomWindow window : CUSTOM_WINDOW_LIST) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, window);
    }
  }

  @Test
  public void testConsistentWithEquals() {
    assertThat(TEST_CODER.consistentWithEquals(), equalTo(true));
  }

  @Test
  public void testIsRegisterByteSizeObserverCheap() {
    assertThat(
        TEST_CODER.isRegisterByteSizeObserverCheap(CUSTOM_WINDOW_LIST.get(0)), equalTo(true));
  }

  @Test
  public void testGetEncodedElementByteSize() throws Exception {
    TestElementByteSizeObserver observer = new TestElementByteSizeObserver();
    for (CustomWindow value : CUSTOM_WINDOW_LIST) {
      TEST_CODER.registerByteSizeObserver(value, observer);
      observer.advance();
      assertThat(
          observer.getSumAndReset(),
          equalTo(
              (long) CoderUtils.encodeToByteArray(TEST_CODER, value, Coder.Context.NESTED).length));
    }
  }
}
