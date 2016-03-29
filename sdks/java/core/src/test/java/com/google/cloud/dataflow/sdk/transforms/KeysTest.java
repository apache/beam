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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/**
 * Tests for Keys transform.
 */
@RunWith(JUnit4.class)
public class KeysTest {
  @SuppressWarnings({"rawtypes", "unchecked"})
  static final KV<String, Integer>[] TABLE = new KV[] {
    KV.of("one", 1),
    KV.of("two", 2),
    KV.of("three", 3),
    KV.of("dup", 4),
    KV.of("dup", 5)
  };

  @SuppressWarnings({"rawtypes", "unchecked"})
  static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {
  };

  @Test
  @Category(RunnableOnService.class)
  public void testKeys() {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(Arrays.asList(TABLE)).withCoder(
            KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<String> output = input.apply(Keys.<String>create());
    DataflowAssert.that(output)
        .containsInAnyOrder("one", "two", "three", "dup", "dup");

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testKeysEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(Arrays.asList(EMPTY_TABLE)).withCoder(
            KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<String> output = input.apply(Keys.<String>create());
    DataflowAssert.that(output).empty();
    p.run();
  }
}
