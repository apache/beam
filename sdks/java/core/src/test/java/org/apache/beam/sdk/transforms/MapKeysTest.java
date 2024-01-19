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

/** Tests for {@link MapKeys} transform. */
@RunWith(JUnit4.class)
public class MapKeysTest {

  private static final List<KV<Integer, String>> TABLE =
      ImmutableList.of(KV.of(1, "one"), KV.of(2, "two"), KV.of(3, "none"));
  private static final List<KV<String, String>> WORDS_TABLE =
      ImmutableList.of(
          KV.of("one", "Length = 3"), KV.of("three", "Length = 4"), KV.of("", "Length = 0"));

  private static final List<KV<Integer, String>> EMPTY_TABLE = new ArrayList<>();
  public static final String EXPECTED_FAILURE_MESSAGE = "/ by zero";

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testMapKeysInto() {

    PCollection<KV<Integer, String>> input =
        p.apply(
            Create.of(TABLE)
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PCollection<KV<Double, String>> output =
        input
            .apply(
                MapKeys.into(TypeDescriptors.doubles())
                    .via((SerializableFunction<Integer, Double>) input1 -> input1 * 2d))
            .setCoder(KvCoder.of(DoubleCoder.of(), StringUtf8Coder.of()));

    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(KV.of(2.0d, "one"), KV.of(4.0d, "two"), KV.of(6.0d, "none")));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapKeysWithFailures() {

    PCollection<KV<String, String>> input =
        p.apply(
            Create.of(WORDS_TABLE)
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

    WithFailures.Result<PCollection<KV<Integer, String>>, String> result =
        input.apply(
            MapKeys.into(TypeDescriptors.integers())
                .<String, String>via(word -> 1 / word.length())
                .exceptionsInto(TypeDescriptors.strings())
                .exceptionsVia(ee -> ee.exception().getMessage()));
    result.output().setCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()));

    PAssert.that(result.output())
        .containsInAnyOrder(ImmutableList.of(KV.of(0, "Length = 3"), KV.of(0, "Length = 4")));
    PAssert.that(result.failures()).containsInAnyOrder(EXPECTED_FAILURE_MESSAGE);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapKeysEmpty() {

    PCollection<KV<Integer, String>> input =
        p.apply(
            Create.of(EMPTY_TABLE)
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PCollection<KV<Double, String>> output =
        input
            .apply(MapKeys.into(TypeDescriptors.doubles()).via(Integer::doubleValue))
            .setCoder(KvCoder.of(DoubleCoder.of(), StringUtf8Coder.of()));

    PAssert.that(output).empty();

    p.run();
  }
}
