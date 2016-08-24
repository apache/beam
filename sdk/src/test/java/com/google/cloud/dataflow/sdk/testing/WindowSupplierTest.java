/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.testing;

import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;

/**
 * Tests for {@link WindowSupplier}.
 */
@RunWith(JUnit4.class)
public class WindowSupplierTest {
  private final IntervalWindow window = new IntervalWindow(new Instant(0L), new Instant(100L));
  private final IntervalWindow otherWindow =
      new IntervalWindow(new Instant(-100L), new Instant(100L));
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void getReturnsProvidedWindows() {
    assertThat(
        WindowSupplier.of(IntervalWindow.getCoder(), ImmutableList.of(window, otherWindow)).get(),
        Matchers.<BoundedWindow>containsInAnyOrder(otherWindow, window));
  }

  @Test
  public void getAfterSerialization() {
    WindowSupplier supplier =
        WindowSupplier.of(IntervalWindow.getCoder(), ImmutableList.of(window, otherWindow));
    assertThat(
        SerializableUtils.clone(supplier).get(),
        Matchers.<BoundedWindow>containsInAnyOrder(otherWindow, window));
  }

  @Test
  public void unencodableWindowFails() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Could not encode");
    WindowSupplier.of(
        new FailingCoder(),
        Collections.<BoundedWindow>singleton(window));
  }

  private static class FailingCoder extends AtomicCoder<BoundedWindow> {
    @Override
    public void encode(
        BoundedWindow value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      throw new CoderException("Test Enccode Exception");
    }

    @Override
    public BoundedWindow decode(
        InputStream inStream, Context context) throws CoderException, IOException {
      throw new CoderException("Test Decode Exception");
    }
  }
}
