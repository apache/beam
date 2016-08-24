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
package org.apache.beam.sdk.util.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link StateNamespaces}.
 */
@RunWith(JUnit4.class)
public class StateNamespacesTest {

  private final Coder<IntervalWindow> intervalCoder = IntervalWindow.getCoder();

  private IntervalWindow intervalWindow(long start, long end) {
    return new IntervalWindow(new Instant(start), new Instant(end));
  }

  /**
   * This test should not be changed. It verifies that the stringKey matches certain expectations.
   * If this changes, the ability to reload any pipeline that has persisted these namespaces will
   * be impacted.
   */
  @Test
  public void testStability() {
    StateNamespace global = StateNamespaces.global();
    StateNamespace intervalWindow =
        StateNamespaces.window(intervalCoder, intervalWindow(1000, 87392));
    StateNamespace intervalWindowAndTrigger =
        StateNamespaces.windowAndTrigger(intervalCoder, intervalWindow(1000, 87392), 57);
    StateNamespace globalWindow = StateNamespaces.window(
        GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    StateNamespace globalWindowAndTrigger = StateNamespaces.windowAndTrigger(
        GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE, 12);

    assertEquals("/", global.stringKey());
    assertEquals("/gAAAAAABVWD4ogU/", intervalWindow.stringKey());
    assertEquals("/gAAAAAABVWD4ogU/1L/", intervalWindowAndTrigger.stringKey());
    assertEquals("//", globalWindow.stringKey());
    assertEquals("//C/", globalWindowAndTrigger.stringKey());
  }

  /**
   * Test that WindowAndTrigger namespaces are prefixed by the related Window namespace.
   */
  @Test
  public void testIntervalWindowPrefixing() {
    StateNamespace window =
        StateNamespaces.window(intervalCoder, intervalWindow(1000, 87392));
    StateNamespace windowAndTrigger = StateNamespaces.windowAndTrigger(
        intervalCoder, intervalWindow(1000, 87392), 57);
    assertThat(windowAndTrigger.stringKey(), Matchers.startsWith(window.stringKey()));
    assertThat(StateNamespaces.global().stringKey(),
        Matchers.not(Matchers.startsWith(window.stringKey())));
  }

  /**
   * Test that WindowAndTrigger namespaces are prefixed by the related Window namespace.
   */
  @Test
  public void testGlobalWindowPrefixing() {
    StateNamespace window =
        StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    StateNamespace windowAndTrigger = StateNamespaces.windowAndTrigger(
        GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE, 57);
    assertThat(windowAndTrigger.stringKey(), Matchers.startsWith(window.stringKey()));
    assertThat(StateNamespaces.global().stringKey(),
        Matchers.not(Matchers.startsWith(window.stringKey())));
  }

  @Test
  public void testFromStringGlobal() {
    assertStringKeyRoundTrips(intervalCoder, StateNamespaces.global());
  }

  @Test
  public void testFromStringIntervalWindow() {
    assertStringKeyRoundTrips(
        intervalCoder, StateNamespaces.window(intervalCoder, intervalWindow(1000, 8000)));
    assertStringKeyRoundTrips(
        intervalCoder, StateNamespaces.window(intervalCoder, intervalWindow(1000, 8000)));

    assertStringKeyRoundTrips(intervalCoder,
        StateNamespaces.windowAndTrigger(intervalCoder, intervalWindow(1000, 8000), 18));
    assertStringKeyRoundTrips(intervalCoder,
        StateNamespaces.windowAndTrigger(intervalCoder, intervalWindow(1000, 8000), 19));
    assertStringKeyRoundTrips(intervalCoder,
        StateNamespaces.windowAndTrigger(intervalCoder, intervalWindow(2000, 8000), 19));
  }

  @Test
  public void testFromStringGlobalWindow() {
    assertStringKeyRoundTrips(GlobalWindow.Coder.INSTANCE, StateNamespaces.global());
    assertStringKeyRoundTrips(GlobalWindow.Coder.INSTANCE,
        StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE));
    assertStringKeyRoundTrips(GlobalWindow.Coder.INSTANCE,
        StateNamespaces.windowAndTrigger(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE, 18));
  }

  private void assertStringKeyRoundTrips(
      Coder<? extends BoundedWindow> coder, StateNamespace namespace) {
    assertEquals(namespace, StateNamespaces.fromString(namespace.stringKey(), coder));
  }
}
