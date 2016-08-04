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
package org.apache.beam.examples.complete;

import org.apache.beam.examples.complete.AutoComplete.CompletionCandidate;
import org.apache.beam.examples.complete.AutoComplete.ComputeTopCompletions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests of AutoComplete.
 */
@RunWith(Parameterized.class)
public class AutoCompleteTest implements Serializable {
  private boolean recursive;

  public AutoCompleteTest(Boolean recursive) {
    this.recursive = recursive;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> testRecursive() {
    return Arrays.asList(new Object[][] {
        { true },
        { false }
      });
  }

  @Test
  public void testAutoComplete() {
    List<String> words = Arrays.asList(
        "apple",
        "apple",
        "apricot",
        "banana",
        "blackberry",
        "blackberry",
        "blackberry",
        "blueberry",
        "blueberry",
        "cherry");

    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(words));

    PCollection<KV<String, List<CompletionCandidate>>> output =
      input.apply(new ComputeTopCompletions(2, recursive))
           .apply(Filter.by(
               new SerializableFunction<KV<String, List<CompletionCandidate>>, Boolean>() {
                 @Override
                 public Boolean apply(KV<String, List<CompletionCandidate>> element) {
                   return element.getKey().length() <= 2;
                 }
               }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", parseList("apple:2", "apricot:1")),
        KV.of("ap", parseList("apple:2", "apricot:1")),
        KV.of("b", parseList("blackberry:3", "blueberry:2")),
        KV.of("ba", parseList("banana:1")),
        KV.of("bl", parseList("blackberry:3", "blueberry:2")),
        KV.of("c", parseList("cherry:1")),
        KV.of("ch", parseList("cherry:1")));
    p.run();
  }

  @Test
  public void testTinyAutoComplete() {
    List<String> words = Arrays.asList("x", "x", "x", "xy", "xy", "xyz");

    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(words));

    PCollection<KV<String, List<CompletionCandidate>>> output =
      input.apply(new ComputeTopCompletions(2, recursive));

    PAssert.that(output).containsInAnyOrder(
        KV.of("x", parseList("x:3", "xy:2")),
        KV.of("xy", parseList("xy:2", "xyz:1")),
        KV.of("xyz", parseList("xyz:1")));
    p.run();
  }

  @Test
  public void testWindowedAutoComplete() {
    List<TimestampedValue<String>> words = Arrays.asList(
        TimestampedValue.of("xA", new Instant(1)),
        TimestampedValue.of("xA", new Instant(1)),
        TimestampedValue.of("xB", new Instant(1)),
        TimestampedValue.of("xB", new Instant(2)),
        TimestampedValue.of("xB", new Instant(2)));

    Pipeline p = TestPipeline.create();

    PCollection<String> input = p
      .apply(Create.of(words))
      .apply(new ReifyTimestamps<String>());

    PCollection<KV<String, List<CompletionCandidate>>> output =
      input.apply(Window.<String>into(SlidingWindows.of(new Duration(2))))
           .apply(new ComputeTopCompletions(2, recursive));

    PAssert.that(output).containsInAnyOrder(
        // Window [0, 2)
        KV.of("x", parseList("xA:2", "xB:1")),
        KV.of("xA", parseList("xA:2")),
        KV.of("xB", parseList("xB:1")),

        // Window [1, 3)
        KV.of("x", parseList("xB:3", "xA:2")),
        KV.of("xA", parseList("xA:2")),
        KV.of("xB", parseList("xB:3")),

        // Window [2, 3)
        KV.of("x", parseList("xB:2")),
        KV.of("xB", parseList("xB:2")));
    p.run();
  }

  private static List<CompletionCandidate> parseList(String... entries) {
    List<CompletionCandidate> all = new ArrayList<>();
    for (String s : entries) {
      String[] countValue = s.split(":");
      all.add(new CompletionCandidate(countValue[0], Integer.valueOf(countValue[1])));
    }
    return all;
  }

  private static class ReifyTimestamps<T>
      extends PTransform<PCollection<TimestampedValue<T>>, PCollection<T>> {
    @Override
    public PCollection<T> apply(PCollection<TimestampedValue<T>> input) {
      return input.apply(ParDo.of(new OldDoFn<TimestampedValue<T>, T>() {
        @Override
        public void processElement(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), c.element().getTimestamp());
        }
      }));
    }
  }
}
