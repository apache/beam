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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.SimpleSink.SimpleWriter;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactoryTest.TestPipelineOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the WriteFiles PTransform.
 */
@RunWith(JUnit4.class)
public class WriteFilesTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule
  public final TestPipeline p = TestPipeline.create();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @SuppressWarnings("unchecked") // covariant cast
  private static final PTransform<PCollection<String>, PCollection<String>> IDENTITY_MAP =
      (PTransform)
          MapElements.via(
              new SimpleFunction<String, String>() {
                @Override
                public String apply(String input) {
                  return input;
                }
              });

  private static final PTransform<PCollection<String>, PCollectionView<Integer>>
      SHARDING_TRANSFORM =
      new PTransform<PCollection<String>, PCollectionView<Integer>>() {
        @Override
        public PCollectionView<Integer> expand(PCollection<String> input) {
          return null;
        }
      };

  private static class WindowAndReshuffle<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final Window<T> window;

    public WindowAndReshuffle(Window<T> window) {
      this.window = window;
    }

    private static class AddArbitraryKey<T> extends DoFn<T, KV<Integer, T>> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(KV.of(ThreadLocalRandom.current().nextInt(), c.element()));
      }
    }

    private static class RemoveArbitraryKey<T> extends DoFn<KV<Integer, Iterable<T>>, T> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        for (T s : c.element().getValue()) {
          c.output(s);
        }
      }
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          .apply(window)
          .apply(ParDo.of(new AddArbitraryKey<T>()))
          .apply(GroupByKey.<Integer, T>create())
          .apply(ParDo.of(new RemoveArbitraryKey<T>()));
    }
  }

  private String appendToTempFolder(String filename) {
    return Paths.get(tmpFolder.getRoot().getPath(), filename).toString();
  }

  private String getBaseOutputFilename() {
    return getBaseOutputDirectory()
        .resolve("file", StandardResolveOptions.RESOLVE_FILE).toString();
  }

  /**
   * Test a WriteFiles transform with a PCollection of elements.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws IOException {
    List<String> inputs = Arrays.asList("Critical canary", "Apprehensive eagle",
        "Intimidating pigeon", "Pedantic gull", "Frisky finch");
    runWrite(inputs, IDENTITY_MAP, getBaseOutputFilename());
  }

  /**
   * Test that WriteFiles with an empty input still produces one shard.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testEmptyWrite() throws IOException {
    runWrite(Collections.<String>emptyList(), IDENTITY_MAP, getBaseOutputFilename());
    checkFileContents(getBaseOutputFilename(), Collections.<String>emptyList(),
        Optional.of(1));
  }

  /**
   * Test that WriteFiles with a configured number of shards produces the desired number of shards
   * even when there are many elements.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testShardedWrite() throws IOException {
    runShardedWrite(
        Arrays.asList("one", "two", "three", "four", "five", "six"),
        IDENTITY_MAP,
        getBaseOutputFilename(),
        Optional.of(1));
  }

  private ResourceId getBaseOutputDirectory() {
    return LocalResources.fromFile(tmpFolder.getRoot(), true)
        .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY);

  }
  private SimpleSink makeSimpleSink() {
    return new SimpleSink(getBaseOutputDirectory(), "file", "-SS-of-NN", "simple");
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCustomShardedWrite() throws IOException {
    // Flag to validate that the pipeline options are passed to the Sink
    WriteOptions options = TestPipeline.testingPipelineOptions().as(WriteOptions.class);
    options.setTestFlag("test_value");
    Pipeline p = TestPipeline.create(options);

    List<String> inputs = new ArrayList<>();
    // Prepare timestamps for the elements.
    List<Long> timestamps = new ArrayList<>();
    for (long i = 0; i < 1000; i++) {
      inputs.add(Integer.toString(3));
      timestamps.add(i + 1);
    }

    SimpleSink sink = makeSimpleSink();
    WriteFiles<String> write = WriteFiles.to(sink).withSharding(new LargestInt());
    p.apply(Create.timestamped(inputs, timestamps).withCoder(StringUtf8Coder.of()))
        .apply(IDENTITY_MAP)
        .apply(write);

    p.run();

    checkFileContents(getBaseOutputFilename(), inputs, Optional.of(3));
  }

  /**
   * Test that WriteFiles with a configured number of shards produces the desired number of shard
   * even when there are too few elements.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testExpandShardedWrite() throws IOException {
    runShardedWrite(
        Arrays.asList("one", "two", "three", "four", "five", "six"),
        IDENTITY_MAP,
        getBaseOutputFilename(),
        Optional.of(20));
  }

  /**
   * Test a WriteFiles transform with an empty PCollection.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithEmptyPCollection() throws IOException {
    List<String> inputs = new ArrayList<>();
    runWrite(inputs, IDENTITY_MAP, getBaseOutputFilename());
  }

  /**
   * Test a WriteFiles with a windowed PCollection.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testWriteWindowed() throws IOException {
    List<String> inputs = Arrays.asList("Critical canary", "Apprehensive eagle",
        "Intimidating pigeon", "Pedantic gull", "Frisky finch");
    runWrite(
        inputs, new WindowAndReshuffle<>(Window.<String>into(FixedWindows.of(Duration.millis(2)))),
        getBaseOutputFilename());
  }

  /**
   * Test a WriteFiles with sessions.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithSessions() throws IOException {
    List<String> inputs = Arrays.asList("Critical canary", "Apprehensive eagle",
        "Intimidating pigeon", "Pedantic gull", "Frisky finch");

    runWrite(
        inputs,
        new WindowAndReshuffle<>(
            Window.<String>into(Sessions.withGapDuration(Duration.millis(1)))),
        getBaseOutputFilename());
  }

  @Test
  public void testBuildWrite() {
    SimpleSink sink = makeSimpleSink();
    WriteFiles<String> write = WriteFiles.to(sink).withNumShards(3);
    assertThat((SimpleSink) write.getSink(), is(sink));
    PTransform<PCollection<String>, PCollectionView<Integer>> originalSharding =
        write.getSharding();

    assertThat(write.getSharding(), is(nullValue()));
    assertThat(write.getNumShards(), instanceOf(StaticValueProvider.class));
    assertThat(write.getNumShards().get(), equalTo(3));
    assertThat(write.getSharding(), equalTo(originalSharding));

    WriteFiles<String> write2 = write.withSharding(SHARDING_TRANSFORM);
    assertThat((SimpleSink) write2.getSink(), is(sink));
    assertThat(write2.getSharding(), equalTo(SHARDING_TRANSFORM));
    // original unchanged

    WriteFiles<String> writeUnsharded = write2.withRunnerDeterminedSharding();
    assertThat(writeUnsharded.getSharding(), nullValue());
    assertThat(write.getSharding(), equalTo(originalSharding));
  }

  @Test
  public void testDisplayData() {
    SimpleSink sink = new SimpleSink(getBaseOutputDirectory(), "file", "-SS-of-NN", "") {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };
    WriteFiles<String> write = WriteFiles.to(sink);
    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("sink", sink.getClass()));
    assertThat(displayData, includesDisplayDataFor("sink", sink));
  }

  @Test
  public void testShardedDisplayData() {
    SimpleSink sink = new SimpleSink(getBaseOutputDirectory(), "file", "-SS-of-NN", "") {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };
    WriteFiles<String> write = WriteFiles.to(sink).withNumShards(1);
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("sink", sink.getClass()));
    assertThat(displayData, includesDisplayDataFor("sink", sink));
    assertThat(displayData, hasDisplayItem("numShards", "1"));
  }

  @Test
  public void testCustomShardStrategyDisplayData() {
    SimpleSink sink = new SimpleSink(getBaseOutputDirectory(), "file", "-SS-of-NN", "") {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };
    WriteFiles<String> write =
        WriteFiles.to(sink)
            .withSharding(
                new PTransform<PCollection<String>, PCollectionView<Integer>>() {
                  @Override
                  public PCollectionView<Integer> expand(PCollection<String> input) {
                    return null;
                  }

                  @Override
                  public void populateDisplayData(DisplayData.Builder builder) {
                    builder.add(DisplayData.item("spam", "ham"));
                  }
                });
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("sink", sink.getClass()));
    assertThat(displayData, includesDisplayDataFor("sink", sink));
    assertThat(displayData, hasDisplayItem("spam", "ham"));
  }

  /**
   * Performs a WriteFiles transform and verifies the WriteFiles transform calls the appropriate
   * methods on a test sink in the correct order, as well as verifies that the elements of a
   * PCollection are written to the sink.
   */
  private void runWrite(
      List<String> inputs, PTransform<PCollection<String>, PCollection<String>> transform,
      String baseName) throws IOException {
    runShardedWrite(inputs, transform, baseName, Optional.<Integer>absent());
  }

  /**
   * Performs a WriteFiles transform with the desired number of shards. Verifies the WriteFiles
   * transform calls the appropriate methods on a test sink in the correct order, as well as
   * verifies that the elements of a PCollection are written to the sink. If numConfiguredShards
   * is not null, also verifies that the output number of shards is correct.
   */
  private void runShardedWrite(
      List<String> inputs,
      PTransform<PCollection<String>, PCollection<String>> transform,
      String baseName,
      Optional<Integer> numConfiguredShards) throws IOException {
    // Flag to validate that the pipeline options are passed to the Sink
    WriteOptions options = TestPipeline.testingPipelineOptions().as(WriteOptions.class);
    options.setTestFlag("test_value");
    Pipeline p = TestPipeline.create(options);

    // Prepare timestamps for the elements.
    List<Long> timestamps = new ArrayList<>();
    for (long i = 0; i < inputs.size(); i++) {
      timestamps.add(i + 1);
    }

    SimpleSink sink = makeSimpleSink();
    WriteFiles<String> write = WriteFiles.to(sink);
    if (numConfiguredShards.isPresent()) {
      write = write.withNumShards(numConfiguredShards.get());
    }
    p.apply(Create.timestamped(inputs, timestamps).withCoder(StringUtf8Coder.of()))
        .apply(transform)
        .apply(write);
    p.run();

    checkFileContents(baseName, inputs, numConfiguredShards);
  }

  static void checkFileContents(String baseName, List<String> inputs,
                                Optional<Integer> numExpectedShards) throws IOException {
    List<File> outputFiles = Lists.newArrayList();
    final String pattern = baseName + "*";
    List<Metadata> metadata =
        FileSystems.match(Collections.singletonList(pattern)).get(0).metadata();
    for (Metadata meta : metadata) {
      outputFiles.add(new File(meta.resourceId().toString()));
    }
    if (numExpectedShards.isPresent()) {
      assertEquals(numExpectedShards.get().intValue(), outputFiles.size());
    }

    List<String> actual = Lists.newArrayList();
    for (File outputFile : outputFiles) {
      try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
        for (;;) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          if (!line.equals(SimpleWriter.HEADER) && !line.equals(SimpleWriter.FOOTER)) {
            actual.add(line);
          }
        }
      }
    }
    assertThat(actual, containsInAnyOrder(inputs.toArray()));
  }

  /**
   * Options for test, exposed for PipelineOptionsFactory.
   */
  public interface WriteOptions extends TestPipelineOptions {
    @Description("Test flag and value")
    String getTestFlag();
    void setTestFlag(String value);
  }

  /**
   * Outputs the largest integer in a {@link PCollection} into a {@link PCollectionView}. The input
   * {@link PCollection} must be convertible to integers via {@link Integer#valueOf(String)}
   */
  private static class LargestInt
      extends PTransform<PCollection<String>, PCollectionView<Integer>> {
    @Override
    public PCollectionView<Integer> expand(PCollection<String> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<String, Integer>() {
                    @ProcessElement
                    public void toInteger(ProcessContext ctxt) {
                      ctxt.output(Integer.valueOf(ctxt.element()));
                    }
                  }))
          .apply(Top.<Integer>largest(1))
          .apply(Flatten.<Integer>iterables())
          .apply(View.<Integer>asSingleton());
    }
  }
}
