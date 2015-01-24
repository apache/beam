/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;

/** Unit tests for bucketing. */
@RunWith(JUnit4.class)
@SuppressWarnings({"serial", "unchecked"})
public class WindowingTest implements Serializable {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static class WindowedCount extends PTransform<PCollection<String>, PCollection<String>> {
    private WindowFn<Object, ?> windowFn;
    public WindowedCount(WindowFn<? super String, ?> windowFn) {
      this.windowFn = (WindowFn) windowFn;
    }
    @Override
    public PCollection<String> apply(PCollection<String> in) {
      return in
          .apply(Window.named("Window").<String>into(windowFn))
          .apply(Count.<String>perElement())
          .apply(ParDo
              .named("FormatCounts")
              .of(new DoFn<KV<String, Long>, String>() {
                    @Override
                        public void processElement(ProcessContext c) {
                      c.output(c.element().getKey() + ":" + c.element().getValue()
                          + ":" + c.timestamp().getMillis() + ":" + c.windows());
                    }
                  }))
          .setCoder(StringUtf8Coder.of());
    }
  }

  private String output(String value, int count, int timestamp, int windowStart, int windowEnd) {
    return value + ":" + count + ":" + timestamp
        + ":[[" + new Instant(windowStart) + ".." + new Instant(windowEnd) + ")]";
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testPartitioningWindowing() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("b", new Instant(2)),
                TimestampedValue.of("b", new Instant(3)),
                TimestampedValue.of("c", new Instant(11)),
                TimestampedValue.of("d", new Instant(11))));

    PCollection<String> output =
        input
        .apply(new WindowedCount(FixedWindows.of(new Duration(10))));

    DataflowAssert.that(output).containsInAnyOrder(
        output("a", 1, 9, 0, 10),
        output("b", 2, 9, 0, 10),
        output("c", 1, 19, 10, 20),
        output("d", 1, 19, 10, 20));

    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testNonPartitioningWindowing() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("a", new Instant(7)),
                TimestampedValue.of("b", new Instant(8))));

    PCollection<String> output =
        input
        .apply(new WindowedCount(
            SlidingWindows.<String>of(new Duration(10)).every(new Duration(5))));

    DataflowAssert.that(output).containsInAnyOrder(
        output("a", 1, 4, -5, 5),
        output("a", 2, 9, 0, 10),
        output("a", 1, 14, 5, 15),
        output("b", 1, 9, 0, 10),
        output("b", 1, 14, 5, 15));

    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testMergingWindowing() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("a", new Instant(5)),
                TimestampedValue.of("a", new Instant(20))));

    PCollection<String> output =
        input
        .apply(new WindowedCount(Sessions.<String>withGapDuration(new Duration(10))));

    DataflowAssert.that(output).containsInAnyOrder(
        output("a", 2, 14, 1, 15),
        output("a", 1, 29, 20, 30));

    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testWindowPreservation() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input1 = p.apply(
        Create.timestamped(
            TimestampedValue.of("a", new Instant(1)),
            TimestampedValue.of("b", new Instant(2))));

    PCollection<String> input2 = p.apply(
        Create.timestamped(
            TimestampedValue.of("a", new Instant(3)),
            TimestampedValue.of("b", new Instant(4))));

    PCollectionList<String> input = PCollectionList.of(input1).and(input2);

    PCollection<String> output =
        input
        .apply(Flatten.<String>create())
        .apply(new WindowedCount(FixedWindows.<String>of(new Duration(5))));

    DataflowAssert.that(output).containsInAnyOrder(
        output("a", 2, 4, 0, 5),
        output("b", 2, 4, 0, 5));

    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testElementsSortedByTimestamp() {
    // The Windowing API does not guarantee that elements will be sorted by
    // timestamp, but the implementation currently relies on this, so it
    // needs to be tested.

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, String>> a = p
        .apply(Create.timestamped(
            TimestampedValue.of(KV.of("k", "a"), new Instant(1)),
            TimestampedValue.of(KV.of("k", "b"), new Instant(4)),
            TimestampedValue.of(KV.of("k", "c"), new Instant(3)),
            TimestampedValue.of(KV.of("k", "d"), new Instant(5)),
            TimestampedValue.of(KV.of("k", "e"), new Instant(2)),
            TimestampedValue.of(KV.of("k", "f"), new Instant(-5)),
            TimestampedValue.of(KV.of("k", "g"), new Instant(-6)),
            TimestampedValue.of(KV.of("k", "h"), new Instant(-255)),
            TimestampedValue.of(KV.of("k", "i"), new Instant(-256)),
            TimestampedValue.of(KV.of("k", "j"), new Instant(255))));

    PCollection<KV<String, String>> b = a
        .apply(Window.<KV<String, String>>into(
            FixedWindows.of(new Duration(1000)).withOffset(new Duration(500))));

    PCollection<KV<String, Iterable<String>>> output = b
        .apply(GroupByKey.<String, String>create());

    DataflowAssert.that(output).containsInAnyOrder(
        KV.of("k",
            (Iterable<String>) Arrays.asList("i", "h", "g", "f", "a", "e", "c", "b", "d", "j")));

    p.run();
  }

  @Test
  public void testEmptyInput() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(Create.<String>timestamped())
        .setCoder(StringUtf8Coder.of());

    PCollection<String> output =
        input
        .apply(new WindowedCount(FixedWindows.<String>of(new Duration(10))));

    DataflowAssert.that(output).containsInAnyOrder();

    p.run();
  }

  @Test
  public void testTextIoInput() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    String filename = tmpFile.getPath();

    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      writer.println("a 1");
      writer.println("b 2");
      writer.println("b 3");
      writer.println("c 11");
      writer.println("d 11");
    }

    Pipeline p = TestPipeline.create();
    PCollection<String> output = p.begin()
        .apply(TextIO.Read.named("ReadLines").from(filename))
        .apply(ParDo.of(new ExtractWordsWithTimestampsFn()))
        .apply(new WindowedCount(FixedWindows.<String>of(Duration.millis(10))));

    DataflowAssert.that(output).containsInAnyOrder(
        output("a", 1, 9, 0, 10),
        output("b", 2, 9, 0, 10),
        output("c", 1, 19, 10, 20),
        output("d", 1, 19, 10, 20));

    p.run();
  }

  /** A DoFn that tokenizes lines of text into individual words. */
  static class ExtractWordsWithTimestampsFn extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      String[] words = c.element().split("[^a-zA-Z0-9']+");
      if (words.length == 2) {
        c.outputWithTimestamp(words[0], new Instant(Long.parseLong(words[1])));
      }
    }
  }
}
