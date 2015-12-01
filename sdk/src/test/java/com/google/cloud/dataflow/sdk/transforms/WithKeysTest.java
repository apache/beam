/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for ExtractKeys transform.
 */
@RunWith(JUnit4.class)
public class WithKeysTest {
  static final String[] COLLECTION = new String[] {
    "a",
    "aa",
    "b",
    "bb",
    "bbb"
  };

  static final List<KV<Integer, String>> WITH_KEYS = Arrays.asList(
    KV.of(1, "a"),
    KV.of(2, "aa"),
    KV.of(1, "b"),
    KV.of(2, "bb"),
    KV.of(3, "bbb")
  );

  static final List<KV<Integer, String>> WITH_CONST_KEYS = Arrays.asList(
    KV.of(100, "a"),
    KV.of(100, "aa"),
    KV.of(100, "b"),
    KV.of(100, "bb"),
    KV.of(100, "bbb")
  );

  @Test
  public void testExtractKeys() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(
            StringUtf8Coder.of()));

    PCollection<KV<Integer, String>> output = input.apply(WithKeys.of(
        new LengthAsKey()));
    DataflowAssert.that(output)
        .containsInAnyOrder(WITH_KEYS);

    p.run();
  }

  @Test
  public void testConstantKeys() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(
            StringUtf8Coder.of()));

    PCollection<KV<Integer, String>> output =
        input.apply(WithKeys.<Integer, String>of(100));
    DataflowAssert.that(output)
        .containsInAnyOrder(WITH_CONST_KEYS);

    p.run();
  }

  @Test
  public void testWithKeysGetName() {
    assertEquals("WithKeys", WithKeys.<Integer, String>of(100).getName());
  }

  @Test
  public void testWithKeysWithUnneededWithKeyTypeSucceeds() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(
            StringUtf8Coder.of()));

    PCollection<KV<Integer, String>> output =
        input.apply(WithKeys.of(new LengthAsKey()).withKeyType(TypeDescriptor.of(Integer.class)));
    DataflowAssert.that(output).containsInAnyOrder(WITH_KEYS);

    p.run();
  }

  /**
   * Key a value by its length.
   */
  public static class LengthAsKey
      implements SerializableFunction<String, Integer> {
    @Override
    public Integer apply(String value) {
      return value.length();
    }
  }
}
