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
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.PAssert;
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
 * Tests for KvSwap transform.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class KvSwapTest {
  static final KV<String, Integer>[] TABLE = new KV[] {
    KV.of("one", 1),
    KV.of("two", 2),
    KV.of("three", 3),
    KV.of("four", 4),
    KV.of("dup", 4),
    KV.of("dup", 5)
  };

  static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {
  };

  @Test
  @Category(RunnableOnService.class)
  public void testKvSwap() {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(Arrays.asList(TABLE)).withCoder(
            KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<Integer, String>> output = input.apply(
        KvSwap.<String, Integer>create());

    PAssert.that(output).containsInAnyOrder(
        KV.of(1, "one"),
        KV.of(2, "two"),
        KV.of(3, "three"),
        KV.of(4, "four"),
        KV.of(4, "dup"),
        KV.of(5, "dup"));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testKvSwapEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(Arrays.asList(EMPTY_TABLE)).withCoder(
            KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<Integer, String>> output = input.apply(
        KvSwap.<String, Integer>create());

    PAssert.that(output).empty();
    p.run();
  }
}
