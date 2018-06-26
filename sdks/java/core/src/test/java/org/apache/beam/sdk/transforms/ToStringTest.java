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
import java.util.Arrays;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ToString} transform.
 */
@RunWith(JUnit4.class)
public class ToStringTest {
  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testToStringOf() {
    Integer[] ints = {1, 2, 3, 4, 5};
    String[] strings = {"1", "2", "3", "4", "5"};
    PCollection<Integer> input = p.apply(Create.of(Arrays.asList(ints)));
    PCollection<String> output = input.apply(ToString.elements());
    PAssert.that(output).containsInAnyOrder(strings);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testToStringKV() {
    ArrayList<KV<String, Integer>> kvs = new ArrayList<>();
    kvs.add(KV.of("one", 1));
    kvs.add(KV.of("two", 2));

    ArrayList<String> expected = new ArrayList<>();
    expected.add("one,1");
    expected.add("two,2");

    PCollection<KV<String, Integer>> input = p.apply(Create.of(kvs));
    PCollection<String> output = input.apply(ToString.kvs());
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testToStringKVWithDelimiter() {
    ArrayList<KV<String, Integer>> kvs = new ArrayList<>();
    kvs.add(KV.of("one", 1));
    kvs.add(KV.of("two", 2));

    ArrayList<String> expected = new ArrayList<>();
    expected.add("one\t1");
    expected.add("two\t2");

    PCollection<KV<String, Integer>> input = p.apply(Create.of(kvs));
    PCollection<String> output = input.apply(ToString.kvs("\t"));
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testToStringIterable() {
    ArrayList<Iterable<String>> iterables = new ArrayList<>();
    iterables.add(Arrays.asList(new String[]{"one", "two", "three"}));
    iterables.add(Arrays.asList(new String[]{"four", "five", "six"}));

    ArrayList<String> expected = new ArrayList<>();
    expected.add("one,two,three");
    expected.add("four,five,six");

    PCollection<Iterable<String>> input = p.apply(Create.of(iterables)
            .withCoder(IterableCoder.of(StringUtf8Coder.of())));
    PCollection<String> output = input.apply(ToString.iterables());
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testToStringIterableWithDelimiter() {
    ArrayList<Iterable<String>> iterables = new ArrayList<>();
    iterables.add(Arrays.asList(new String[]{"one", "two", "three"}));
    iterables.add(Arrays.asList(new String[]{"four", "five", "six"}));

    ArrayList<String> expected = new ArrayList<>();
    expected.add("one\ttwo\tthree");
    expected.add("four\tfive\tsix");

    PCollection<Iterable<String>> input = p.apply(Create.of(iterables)
            .withCoder(IterableCoder.of(StringUtf8Coder.of())));
    PCollection<String> output = input.apply(ToString.iterables("\t"));
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }
}
