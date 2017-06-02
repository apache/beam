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
package org.apache.beam.sdk.transforms.windowing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests merging of custom windows.
 */
@RunWith(JUnit4.class)
public class MergingCustomWindowsTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testMergingCustomWindows() {
    Instant startInstant = new Instant(0L);
    List<TimestampedValue<String>> input = new ArrayList<>();
    input.add(TimestampedValue.of("big", startInstant.plus(Duration.standardSeconds(10))));
    input.add(TimestampedValue.of("small", startInstant.plus(Duration.standardSeconds(20))));
    input.add(TimestampedValue.of("small", startInstant.plus(Duration.standardSeconds(30))));
    PCollection<String> inputCollection = pipeline.apply(Create.timestamped(input));
    PCollection<String> windowedCollection = inputCollection
        .apply(Window.into(new CustomWindowFn()));
    PCollection<Long> count = windowedCollection
        .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults());
    PAssert.thatSingleton("Wrong number of elements in output collection", count).isEqualTo(3L);
    pipeline.run();
  }

  private static class CustomWindow extends IntervalWindow {

    private boolean isBig;

    private CustomWindow(Instant start, Instant end, boolean isBig) {
      super(start, end);
      this.isBig = isBig;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      CustomWindow that = (CustomWindow) o;
      return isBig == that.isBig;
    }

    @Override public int hashCode() {
      return Objects.hash(super.hashCode(), isBig);
    }
  }

  private static class CustomWindowCoder extends CustomCoder<CustomWindow> {

    private static final CustomWindowCoder INSTANCE = new CustomWindowCoder();
    private static final Coder<IntervalWindow> INTERVAL_WINDOW_CODER = IntervalWindow.getCoder();
    private static final VarIntCoder VAR_INT_CODER = VarIntCoder.of();

    public static CustomWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CustomWindow window, OutputStream outStream)
        throws IOException {
      INTERVAL_WINDOW_CODER.encode(window, outStream);
      VAR_INT_CODER.encode(window.isBig ? 1 : 0, outStream);
    }

    @Override
    public CustomWindow decode(InputStream inStream) throws IOException {
      IntervalWindow superWindow = INTERVAL_WINDOW_CODER.decode(inStream);
      boolean isBig = VAR_INT_CODER.decode(inStream) != 0;
      return new CustomWindow(superWindow.start(), superWindow.end(), isBig);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      INTERVAL_WINDOW_CODER.verifyDeterministic();
      VAR_INT_CODER.verifyDeterministic();
    }
  }

  private static class CustomWindowFn extends WindowFn<String, CustomWindow> {

    @Override
    public Collection<CustomWindow> assignWindows(AssignContext c) throws Exception {
      String element = c.element();
      CustomWindow customWindow;
      // put big elements in windows of 30s and small ones in windows of 5s
      if ("big".equals(element)) {
        customWindow =
            new CustomWindow(c.timestamp(), c.timestamp().plus(Duration.standardSeconds(30)), true);
      } else {
        customWindow =
            new CustomWindow(c.timestamp(), c.timestamp().plus(Duration.standardSeconds(5)), false);
      }
      return Collections.singletonList(customWindow);
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
      List<CustomWindow> toBeMerged = new ArrayList<>();
      CustomWindow bigWindow = null;
      for (CustomWindow customWindow : c.windows()) {
        toBeMerged.add(customWindow);
        if (customWindow.isBig) {
          bigWindow = customWindow;
        }
      }
      // merge small windows into big windows
      c.merge(toBeMerged, bigWindow);
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof CustomWindowFn;
    }

    @Override
    public Coder<CustomWindow> windowCoder() {
      return CustomWindowCoder.of();
    }

    @Override
    public WindowMappingFn<CustomWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException("side inputs not supported");
    }
  }
}
