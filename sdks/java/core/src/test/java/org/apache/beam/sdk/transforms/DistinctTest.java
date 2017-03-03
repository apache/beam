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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Distinct}.
 */
@RunWith(JUnit4.class)
public class DistinctTest {

  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testDistinct() {
    List<String> strings = Arrays.asList(
        "k1",
        "k5",
        "k5",
        "k2",
        "k1",
        "k2",
        "k3");

    PCollection<String> input =
        p.apply(Create.of(strings)
            .withCoder(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(Distinct.<String>create());

    PAssert.that(output)
        .containsInAnyOrder("k1", "k5", "k2", "k3");
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testDistinctEmpty() {
    List<String> strings = Arrays.asList();

    PCollection<String> input =
        p.apply(Create.of(strings)
            .withCoder(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(Distinct.<String>create());

    PAssert.that(output).empty();
    p.run();
  }

  private static class Keys implements SerializableFunction<KV<String, String>, String> {
    @Override
    public String apply(KV<String, String> input) {
      return input.getKey();
    }
  }

  private static class Checker implements SerializableFunction<Iterable<KV<String, String>>, Void> {
    @Override
    public Void apply(Iterable<KV<String, String>> input) {
      Map<String, String> values = new HashMap<>();
      for (KV<String, String> kv : input) {
        values.put(kv.getKey(), kv.getValue());
      }
      assertEquals(2, values.size());
      assertTrue(values.get("k1").equals("v1") || values.get("k1").equals("v2"));
      assertEquals("v1", values.get("k2"));
      return null;
    }
  }


  @Test
  @Category(ValidatesRunner.class)
  public void testDistinctWithRepresentativeValue() {
    List<KV<String, String>> strings = Arrays.asList(
        KV.of("k1", "v1"),
        KV.of("k1", "v2"),
        KV.of("k2", "v1"));

    PCollection<KV<String, String>> input = p.apply(Create.of(strings));

    PCollection<KV<String, String>> output =
        input.apply(Distinct.withRepresentativeValueFn(new Keys()));


    PAssert.that(output).satisfies(new Checker());

    p.run();
  }
}
