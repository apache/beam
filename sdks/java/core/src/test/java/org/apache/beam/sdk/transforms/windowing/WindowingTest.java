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
package org.apache.beam.sdk.transforms.windowing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.DataflowRunnerV2Incompatible;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for bucketing. */
@RunWith(JUnit4.class)
public class WindowingTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  private static class WindowedCount extends PTransform<PCollection<String>, PCollection<String>> {

    private static final class FormatCountsDoFn extends DoFn<KV<String, Long>, String> {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) {
        c.output(
            c.element().getKey()
                + ":"
                + c.element().getValue()
                + ":"
                + c.timestamp().getMillis()
                + ":"
                + window);
      }
    }

    private WindowFn<? super String, ?> windowFn;

    public WindowedCount(WindowFn<? super String, ?> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
              "Window",
              Window.<String>into(windowFn).withTimestampCombiner(TimestampCombiner.EARLIEST))
          .apply(Count.perElement())
          .apply("FormatCounts", ParDo.of(new FormatCountsDoFn()))
          .setCoder(StringUtf8Coder.of());
    }
  }

  private String output(String value, int count, int timestamp, int windowStart, int windowEnd) {
    return value
        + ":"
        + count
        + ":"
        + timestamp
        + ":["
        + new Instant(windowStart)
        + ".."
        + new Instant(windowEnd)
        + ")";
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPartitioningWindowing() {
    PCollection<String> input =
        p.apply(
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("b", new Instant(2)),
                TimestampedValue.of("b", new Instant(3)),
                TimestampedValue.of("c", new Instant(11)),
                TimestampedValue.of("d", new Instant(11))));

    PCollection<String> output = input.apply(new WindowedCount(FixedWindows.of(new Duration(10))));

    PAssert.that(output)
        .containsInAnyOrder(
            output("a", 1, 1, 0, 10),
            output("b", 2, 2, 0, 10),
            output("c", 1, 11, 10, 20),
            output("d", 1, 11, 10, 20));

    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, DataflowRunnerV2Incompatible.class})
  public void testNonPartitioningWindowing() {
    PCollection<String> input =
        p.apply(
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("a", new Instant(7)),
                TimestampedValue.of("b", new Instant(8))));

    PCollection<String> output =
        input.apply(new WindowedCount(SlidingWindows.of(new Duration(10)).every(new Duration(5))));

    PAssert.that(output)
        .containsInAnyOrder(
            output("a", 1, 1, -5, 5),
            output("a", 2, 5, 0, 10),
            output("a", 1, 10, 5, 15),
            output("b", 1, 8, 0, 10),
            output("b", 1, 10, 5, 15));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMergingWindowing() {
    PCollection<String> input =
        p.apply(
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("a", new Instant(5)),
                TimestampedValue.of("a", new Instant(20))));

    PCollection<String> output =
        input.apply(new WindowedCount(Sessions.withGapDuration(new Duration(10))));

    PAssert.that(output).containsInAnyOrder(output("a", 2, 1, 1, 15), output("a", 1, 20, 20, 30));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowPreservation() {
    PCollection<String> input1 =
        p.apply(
            "Create12",
            Create.timestamped(
                TimestampedValue.of("a", new Instant(1)),
                TimestampedValue.of("b", new Instant(2))));

    PCollection<String> input2 =
        p.apply(
            "Create34",
            Create.timestamped(
                TimestampedValue.of("a", new Instant(3)),
                TimestampedValue.of("b", new Instant(4))));

    PCollectionList<String> input = PCollectionList.of(input1).and(input2);

    PCollection<String> output =
        input
            .apply(Flatten.pCollections())
            .apply(new WindowedCount(FixedWindows.of(new Duration(5))));

    PAssert.that(output).containsInAnyOrder(output("a", 2, 1, 0, 5), output("b", 2, 2, 0, 5));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEmptyInput() {
    PCollection<String> input = p.apply(Create.empty(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(new WindowedCount(FixedWindows.of(new Duration(10))));

    PAssert.that(output).empty();

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
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

    PCollection<String> output =
        p.begin()
            .apply("ReadLines", TextIO.read().from(filename))
            .apply(ParDo.of(new ExtractWordsWithTimestampsFn()))
            .apply(new WindowedCount(FixedWindows.of(Duration.millis(10))));

    PAssert.that(output)
        .containsInAnyOrder(
            output("a", 1, 1, 0, 10),
            output("b", 2, 2, 0, 10),
            output("c", 1, 11, 10, 20),
            output("d", 1, 11, 10, 20));

    p.run();
  }

  /** A DoFn that tokenizes lines of text into individual words. */
  static class ExtractWordsWithTimestampsFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      List<String> words = Splitter.onPattern("[^a-zA-Z0-9']+").splitToList(c.element());
      if (words.size() == 2) {
        c.outputWithTimestamp(words.get(0), new Instant(Long.parseLong(words.get(1))));
      }
    }
  }
}
