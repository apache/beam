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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MapValues} transform. */
@RunWith(JUnit4.class)
public class MapValuesTest {

  private static final List<KV<String, Integer>> TABLE =
      ImmutableList.of(KV.of("one", 1), KV.of("two", 2), KV.of("dup", 2));
  private static final List<KV<String, String>> WORDS_TABLE =
      ImmutableList.of(
          KV.of("Length = 3", "one"), KV.of("Length = 4", "three"), KV.of("Length = 0", ""));

  private static final List<KV<String, Integer>> EMPTY_TABLE = new ArrayList<>();
  public static final String EXPECTED_FAILURE_MESSAGE = "/ by zero";

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testMapValuesInto() {

    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(TABLE)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Double>> output =
        input
            .apply(
                MapValues.into(TypeDescriptors.doubles())
                    .via((SerializableFunction<Integer, Double>) input1 -> input1 * 2d))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(KV.of("one", 2.0d), KV.of("two", 4.0d), KV.of("dup", 4.0d)));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapValuesWithFailures() {

    PCollection<KV<String, String>> input =
        p.apply(
            Create.of(WORDS_TABLE)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    WithFailures.Result<PCollection<KV<String, Integer>>, String> result =
        input.apply(
            MapValues.into(TypeDescriptors.integers())
                .<String, String>via(word -> 1 / word.length())
                .exceptionsInto(TypeDescriptors.strings())
                .exceptionsVia(ee -> ee.exception().getMessage()));
    result.output().setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PAssert.that(result.output())
        .containsInAnyOrder(ImmutableList.of(KV.of("Length = 3", 0), KV.of("Length = 4", 0)));
    PAssert.that(result.failures()).containsInAnyOrder(EXPECTED_FAILURE_MESSAGE);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapValuesEmpty() {

    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(EMPTY_TABLE)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<KV<String, Double>> output =
        input
            .apply(MapValues.into(TypeDescriptors.doubles()).via(Integer::doubleValue))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

    PAssert.that(output).empty();

    p.run();
  }
}
