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
package org.apache.beam.sdk.testing;

import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.SerializableUtils;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

  private static class FailingCoder extends CustomCoder<BoundedWindow>  {
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
