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
package org.apache.beam.runners.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InMemoryMultimapSideInputView}. */
@RunWith(JUnit4.class)
public class InMemoryMultimapSideInputViewTest {
  @Test
  public void testStructuralKeyEquality() {
    MultimapView<byte[], Integer> view =
        InMemoryMultimapSideInputView.fromIterable(
            ByteArrayCoder.of(),
            ImmutableList.of(KV.of(new byte[] {0x00}, 0), KV.of(new byte[] {0x01}, 1)));
    assertEquals(view.get(new byte[] {0x00}), ImmutableList.of(0));
    assertEquals(view.get(new byte[] {0x01}), ImmutableList.of(1));
    assertEquals(view.get(new byte[] {0x02}), ImmutableList.of());
    assertThat(
        view.get(),
        Matchers.containsInAnyOrder(
            new ByteArrayMatcher(new byte[] {0x00}), new ByteArrayMatcher(new byte[] {0x01})));
  }

  /** A matcher for byte[]s since primitive arrays only support reference equality by default. */
  private static class ByteArrayMatcher extends TypeSafeMatcher<byte[]> {
    private final byte[] expected;

    public ByteArrayMatcher(byte[] expected) {
      this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(byte[] item) {
      return Arrays.equals(expected, item);
    }

    @Override
    public void describeTo(Description description) {
      description.appendValue(expected);
    }
  }

  @Test
  public void testValueGrouping() {
    MultimapView<String, String> view =
        InMemoryMultimapSideInputView.fromIterable(
            StringUtf8Coder.of(),
            ImmutableList.of(KV.of("A", "a1"), KV.of("A", "a2"), KV.of("B", "b1")));
    assertEquals(view.get("A"), ImmutableList.of("a1", "a2"));
    assertEquals(view.get("B"), ImmutableList.of("b1"));
    assertEquals(view.get("C"), ImmutableList.of());
    assertThat(view.get(), containsInAnyOrder("A", "B"));
  }
}
