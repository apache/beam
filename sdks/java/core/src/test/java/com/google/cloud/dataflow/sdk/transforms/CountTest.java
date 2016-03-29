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

import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES;
import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.Pipeline;
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
import java.util.List;

/**
 * Tests for Count.
 */
@RunWith(JUnit4.class)
public class CountTest {
  static final String[] WORDS_ARRAY = new String[] {
    "hi", "there", "hi", "hi", "sue", "bob",
    "hi", "sue", "", "", "ZOW", "bob", "" };

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  @Test
  @Category(RunnableOnService.class)
  @SuppressWarnings("unchecked")
  public void testCountPerElementBasic() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(WORDS));

    PCollection<KV<String, Long>> output =
        input.apply(Count.<String>perElement());

    DataflowAssert.that(output)
        .containsInAnyOrder(
            KV.of("hi", 4L),
            KV.of("there", 1L),
            KV.of("sue", 2L),
            KV.of("bob", 2L),
            KV.of("", 3L),
            KV.of("ZOW", 1L));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  @SuppressWarnings("unchecked")
  public void testCountPerElementEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(NO_LINES).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Long>> output =
        input.apply(Count.<String>perElement());

    DataflowAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCountGloballyBasic() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(WORDS));

    PCollection<Long> output =
        input.apply(Count.<String>globally());

    DataflowAssert.that(output)
        .containsInAnyOrder(13L);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testCountGloballyEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(NO_LINES).withCoder(StringUtf8Coder.of()));

    PCollection<Long> output =
        input.apply(Count.<String>globally());

    DataflowAssert.that(output)
        .containsInAnyOrder(0L);
    p.run();
  }

  @Test
  public void testCountGetName() {
    assertEquals("Count.PerElement", Count.perElement().getName());
    assertEquals("Count.Globally", Count.globally().getName());
  }
}
