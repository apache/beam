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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.TestUtils.NO_LINES;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Count. */
@RunWith(JUnit4.class)
public class CountTest {
  static final String[] WORDS_ARRAY =
      new String[] {"hi", "there", "hi", "hi", "sue", "bob", "hi", "sue", "", "", "ZOW", "bob", ""};

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  @Rule public TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  @SuppressWarnings("unchecked")
  public void testCountPerElementBasic() {
    PCollection<String> input = p.apply(Create.of(WORDS));

    PCollection<KV<String, Long>> output = input.apply(Count.perElement());

    PAssert.that(output)
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
  @Category(NeedsRunner.class)
  @SuppressWarnings("unchecked")
  public void testCountPerElementEmpty() {
    PCollection<String> input = p.apply(Create.of(NO_LINES).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Long>> output = input.apply(Count.perElement());

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCountGloballyBasic() {
    PCollection<String> input = p.apply(Create.of(WORDS));

    PCollection<Long> output = input.apply(Count.globally());

    PAssert.that(output).containsInAnyOrder(13L);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCountGloballyEmpty() {
    PCollection<String> input = p.apply(Create.of(NO_LINES).withCoder(StringUtf8Coder.of()));

    PCollection<Long> output = input.apply(Count.globally());

    PAssert.that(output).containsInAnyOrder(0L);
    p.run();
  }

  @Test
  public void testCountGetName() {
    assertEquals("Count.PerElement", Count.perElement().getName());
    assertEquals("Combine.globally(Count)", Count.globally().getName());
  }

  @Test
  public void testGlobalWindowErrorMessageShows() {
    PCollection<String> input = p.apply(Create.of(NO_LINES).withCoder(StringUtf8Coder.of()));
    PCollection<String> windowed =
        input.apply(Window.into(FixedWindows.of(Duration.standardDays(1))));

    String expected = Count.combineFn().getIncompatibleGlobalWindowErrorMessage();
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage(expected);
    windowed.apply(Count.globally());
  }
}
