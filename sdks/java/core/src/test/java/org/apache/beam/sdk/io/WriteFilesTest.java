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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.SimpleSink.SimpleWriter;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactoryTest.TestPipelineOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.UsesTestStreamWithProcessingTime;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.commons.compress.utils.Sets;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the WriteFiles PTransform. */
@RunWith(JUnit4.class)
public class WriteFilesTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public final TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final int CUSTOM_FILE_TRIGGERING_RECORD_COUNT = 50000;
  private static final int CUSTOM_FILE_TRIGGERING_BYTE_COUNT = 32 * 1024 * 1024; // 32MiB
  private static final Duration CUSTOM_FILE_TRIGGERING_RECORD_BUFFERING_DURATION =
      Duration.standardSeconds(4);

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
          .apply(ParDo.of(new AddArbitraryKey<>()))
          .apply(GroupByKey.create())
          .apply(ParDo.of(new RemoveArbitraryKey<>()));
    }
  }

  private static class VerifyFilesExist<DestinationT>
      extends PTransform<PCollection<KV<DestinationT, String>>, PDone> {
    @Override
    public PDone expand(PCollection<KV<DestinationT, String>> input) {
      input
          .apply(Values.create())
          .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW));
      return PDone.in(input.getPipeline());
    }
  }

  private String getBaseOutputFilename() {
    return getBaseOutputDirectory().resolve("file", StandardResolveOptions.RESOLVE_FILE).toString();
  }

  /** Test a WriteFiles transform with a PCollection of elements. */
  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws IOException {
    List<String> inputs =
        Arrays.asList(
            "Critical canary",
            "Apprehensive eagle",
            "Intimidating pigeon",
            "Pedantic gull",
            "Frisky finch");
    runWrite(inputs, IDENTITY_MAP, getBaseOutputFilename(), WriteFiles.to(makeSimpleSink()));
  }

  /** Test that WriteFiles with an empty input still produces one shard. */
  @Test
  @Category(NeedsRunner.class)
  public void testEmptyWrite() throws IOException {
    runWrite(
        Collections.emptyList(),
        IDENTITY_MAP,
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink()));
    checkFileContents(
        getBaseOutputFilename(),
        Collections.emptyList(),
        Optional.of(1),
        true /* expectRemovedTempDirectory */);
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
        WriteFiles.to(makeSimpleSink()));
  }

  private ResourceId getBaseOutputDirectory() {
    return LocalResources.fromFile(tmpFolder.getRoot(), true)
        .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  private SimpleSink<Void> makeSimpleSink() {
    FilenamePolicy filenamePolicy =
        new PerWindowFiles(
            getBaseOutputDirectory().resolve("file", StandardResolveOptions.RESOLVE_FILE),
            "simple");
    return SimpleSink.makeSimpleSink(getBaseOutputDirectory(), filenamePolicy);
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

    SimpleSink<Void> sink = makeSimpleSink();
    WriteFiles<String, ?, String> write = WriteFiles.to(sink).withSharding(new LargestInt());
    p.apply(Create.timestamped(inputs, timestamps).withCoder(StringUtf8Coder.of()))
        .apply(IDENTITY_MAP)
        .apply(write)
        .getPerDestinationOutputFilenames()
        .apply(new VerifyFilesExist<>());

    p.run();

    checkFileContents(
        getBaseOutputFilename(), inputs, Optional.of(3), true /* expectRemovedTempDirectory */);
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
        WriteFiles.to(makeSimpleSink()).withNumShards(20));
  }

  /**
   * Test that WriteFiles with a configured sharding function distributes elements according to
   * assigned key. Test use simple sharding which co-locate same elements into same shard.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testCustomShardingFunctionWrite() throws IOException {
    runShardedWrite(
        Arrays.asList("1", "2", "3", "1", "2", "3"),
        IDENTITY_MAP,
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink())
            .withNumShards(4)
            .withShardingFunction(new TestShardingFunction()),
        new BiFunction<Integer, List<String>, Void>() {
          @Override
          public Void apply(Integer shardNumber, List<String> shardContent) {
            // Function creates only shards 1,2,3 but WriteFiles will ensure shard 0 with empty
            // content will be created as well
            if (shardNumber == 0) {
              assertTrue(shardContent.isEmpty());
            } else {
              assertEquals(
                  Lists.newArrayList(shardNumber.toString(), shardNumber.toString()), shardContent);
            }
            return null;
          }
        },
        false);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testWithRunnerDeterminedShardingUnbounded() throws IOException {
    runShardedWrite(
        Arrays.asList("one", "two", "three", "four", "five", "six"),
        Window.into(FixedWindows.of(Duration.standardSeconds(10))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink()).withWindowedWrites().withRunnerDeterminedSharding(),
        null,
        true);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testWithShardingUnbounded() throws IOException {
    runShardedWrite(
        Arrays.asList("one", "two", "three", "four", "five", "six"),
        Window.into(FixedWindows.of(Duration.standardSeconds(10))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink()).withWindowedWrites().withAutoSharding(),
        null,
        true);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testWriteUnboundedWithCustomBatchParameters() throws IOException {
    runShardedWrite(
        Arrays.asList("one", "two", "three", "four", "five", "six"),
        Window.into(FixedWindows.of(Duration.standardSeconds(10))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink())
            .withWindowedWrites()
            .withAutoSharding()
            .withBatchSize(CUSTOM_FILE_TRIGGERING_RECORD_COUNT)
            .withBatchSizeBytes(CUSTOM_FILE_TRIGGERING_BYTE_COUNT)
            .withBatchMaxBufferingDuration(CUSTOM_FILE_TRIGGERING_RECORD_BUFFERING_DURATION),
        null,
        true);
  }

  @Test
  @Category({
    NeedsRunner.class,
    UsesUnboundedPCollections.class,
    UsesTestStream.class,
    UsesTestStreamWithProcessingTime.class
  })
  public void testWithRunnerDeterminedShardingTestStream() throws IOException {
    List<String> elements = Lists.newArrayList();
    for (int i = 0; i < 30; ++i) {
      elements.add("number: " + i);
    }
    Instant startInstant = new Instant(0L);
    TestStream<String> testStream =
        TestStream.create(StringUtf8Coder.of())
            // Initialize watermark for timer to be triggered correctly.
            .advanceWatermarkTo(startInstant)
            // Add 10 elements in the first window.
            .addElements(elements.get(0), Iterables.toArray(elements.subList(1, 10), String.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(5)))
            // Add 10 more elements in the first window.
            .addElements(
                elements.get(10), Iterables.toArray(elements.subList(11, 20), String.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .advanceWatermarkTo(startInstant.plus(Duration.standardSeconds(10)))
            // Add the remaining relements in the second window.
            .addElements(
                elements.get(20), Iterables.toArray(elements.subList(21, 30), String.class))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .advanceWatermarkToInfinity();

    // Flag to validate that the pipeline options are passed to the Sink
    WriteOptions options = TestPipeline.testingPipelineOptions().as(WriteOptions.class);
    options.setTestFlag("test_value");
    Pipeline p = TestPipeline.create(options);
    WriteFiles<String, Void, String> write =
        WriteFiles.to(makeSimpleSink()).withWindowedWrites().withRunnerDeterminedSharding();
    p.apply(testStream)
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(write)
        .getPerDestinationOutputFilenames()
        .apply(new VerifyFilesExist<>());
    p.run();

    checkFileContents(
        getBaseOutputFilename(), elements, Optional.absent(), !write.getWindowedWrites(), null);
  }

  /** Test a WriteFiles transform with an empty PCollection. */
  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithEmptyPCollection() throws IOException {
    List<String> inputs = new ArrayList<>();
    runWrite(inputs, IDENTITY_MAP, getBaseOutputFilename(), WriteFiles.to(makeSimpleSink()));
  }

  /** Test a WriteFiles with a windowed PCollection. */
  @Test
  @Category(NeedsRunner.class)
  public void testWriteWindowed() throws IOException {
    List<String> inputs =
        Arrays.asList(
            "Critical canary",
            "Apprehensive eagle",
            "Intimidating pigeon",
            "Pedantic gull",
            "Frisky finch");
    runWrite(
        inputs,
        new WindowAndReshuffle<>(Window.into(FixedWindows.of(Duration.millis(2)))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink()));
  }

  /** Test a WriteFiles with sessions. */
  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithSessions() throws IOException {
    List<String> inputs =
        Arrays.asList(
            "Critical canary",
            "Apprehensive eagle",
            "Intimidating pigeon",
            "Pedantic gull",
            "Frisky finch");

    runWrite(
        inputs,
        new WindowAndReshuffle<>(Window.into(Sessions.withGapDuration(Duration.millis(1)))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteSpilling() throws IOException {
    List<String> inputs = Lists.newArrayList();
    for (int i = 0; i < 100; ++i) {
      inputs.add("mambo_number_" + i);
    }
    runWrite(
        inputs,
        Window.into(FixedWindows.of(Duration.millis(1))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink()).withMaxNumWritersPerBundle(2).withWindowedWrites());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteNoSpilling() throws IOException {
    List<String> inputs = Lists.newArrayList();
    for (int i = 0; i < 100; ++i) {
      inputs.add("mambo_number_" + i);
    }
    runWrite(
        inputs,
        Window.into(FixedWindows.of(Duration.millis(1))),
        getBaseOutputFilename(),
        WriteFiles.to(makeSimpleSink())
            .withMaxNumWritersPerBundle(2)
            .withWindowedWrites()
            .withNoSpilling());
  }

  @Test
  public void testBuildWrite() {
    SimpleSink<Void> sink = makeSimpleSink();
    WriteFiles<String, ?, String> write = WriteFiles.to(sink).withNumShards(3);
    assertThat((SimpleSink<Void>) write.getSink(), is(sink));
    PTransform<PCollection<String>, PCollectionView<Integer>> originalSharding =
        write.getComputeNumShards();

    assertThat(write.getComputeNumShards(), is(nullValue()));
    assertThat(write.getNumShardsProvider(), instanceOf(StaticValueProvider.class));
    assertThat(write.getNumShardsProvider().get(), equalTo(3));
    assertThat(write.getComputeNumShards(), equalTo(originalSharding));

    WriteFiles<String, ?, ?> write2 = write.withSharding(SHARDING_TRANSFORM);
    assertThat((SimpleSink<Void>) write2.getSink(), is(sink));
    assertThat(write2.getComputeNumShards(), equalTo(SHARDING_TRANSFORM));
    // original unchanged

    WriteFiles<String, ?, ?> writeUnsharded = write2.withRunnerDeterminedSharding();
    assertThat(writeUnsharded.getComputeNumShards(), nullValue());
    assertThat(write.getComputeNumShards(), equalTo(originalSharding));
  }

  @Test
  public void testDisplayData() {
    DynamicDestinations<String, Void, String> dynamicDestinations =
        DynamicFileDestinations.constant(
            DefaultFilenamePolicy.fromParams(
                new Params()
                    .withBaseFilename(
                        getBaseOutputDirectory()
                            .resolve("file", StandardResolveOptions.RESOLVE_FILE))
                    .withShardTemplate("-SS-of-NN")));
    SimpleSink<Void> sink =
        new SimpleSink<Void>(
            getBaseOutputDirectory(), dynamicDestinations, Compression.UNCOMPRESSED) {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };
    WriteFiles<String, ?, String> write = WriteFiles.to(sink);

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("sink", sink.getClass()));
    assertThat(displayData, includesDisplayDataFor("sink", sink));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnboundedNeedsWindowed() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Must use windowed writes when applying WriteFiles to an unbounded PCollection");

    SimpleSink<Void> sink = makeSimpleSink();
    p.apply(Create.of("foo")).setIsBoundedInternal(IsBounded.UNBOUNDED).apply(WriteFiles.to(sink));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnboundedWritesWithMergingWindowNeedSharding() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "When applying WriteFiles to an unbounded PCollection with merging windows, "
            + "must specify number of output shards explicitly");

    SimpleSink<Void> sink = makeSimpleSink();
    p.apply(Create.of("foo"))
        .setIsBoundedInternal(IsBounded.UNBOUNDED)
        .apply(Window.into(Sessions.withGapDuration(Duration.millis(100))))
        .apply(WriteFiles.to(sink).withWindowedWrites());
    p.run();
  }

  // Test DynamicDestinations class. Expects user values to be string-encoded integers.
  // Stores the integer mod 5 as the destination, and uses that in the file prefix.
  static class TestDestinations extends DynamicDestinations<String, Integer, String> {
    private ResourceId baseOutputDirectory;

    TestDestinations(ResourceId baseOutputDirectory) {
      this.baseOutputDirectory = baseOutputDirectory;
    }

    @Override
    public String formatRecord(String record) {
      return "record_" + record;
    }

    @Override
    public Integer getDestination(String element) {
      return Integer.valueOf(element) % 5;
    }

    @Override
    public Integer getDefaultDestination() {
      return 0;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Integer destination) {
      return new PerWindowFiles(
          baseOutputDirectory.resolve("file_" + destination, StandardResolveOptions.RESOLVE_FILE),
          "simple");
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsBounded() throws Exception {
    testDynamicDestinationsHelper(true, false);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testDynamicDestinationsUnbounded() throws Exception {
    testDynamicDestinationsHelper(false, false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsFillEmptyShards() throws Exception {
    testDynamicDestinationsHelper(true, true);
  }

  private void testDynamicDestinationsHelper(boolean bounded, boolean emptyShards)
      throws IOException {
    TestDestinations dynamicDestinations = new TestDestinations(getBaseOutputDirectory());
    SimpleSink<Integer> sink =
        new SimpleSink<>(getBaseOutputDirectory(), dynamicDestinations, Compression.UNCOMPRESSED);

    // Flag to validate that the pipeline options are passed to the Sink.
    WriteOptions options = TestPipeline.testingPipelineOptions().as(WriteOptions.class);
    options.setTestFlag("test_value");
    Pipeline p = TestPipeline.create(options);

    final int numInputs = 100;
    List<String> inputs = Lists.newArrayList();
    for (int i = 0; i < numInputs; ++i) {
      inputs.add(Integer.toString(i));
    }
    // Prepare timestamps for the elements.
    List<Long> timestamps = new ArrayList<>();
    for (long i = 0; i < inputs.size(); i++) {
      timestamps.add(i + 1);
    }
    // If emptyShards==true make numShards larger than the number of elements per destination.
    // This will force every destination to generate some empty shards.
    int numShards = emptyShards ? 2 * numInputs / 5 : 2;
    WriteFiles<String, Integer, String> writeFiles = WriteFiles.to(sink).withNumShards(numShards);

    PCollection<String> input = p.apply(Create.timestamped(inputs, timestamps));
    WriteFilesResult<Integer> res;
    if (!bounded) {
      input.setIsBoundedInternal(IsBounded.UNBOUNDED);
      input = input.apply(Window.into(FixedWindows.of(Duration.standardDays(1))));
      res = input.apply(writeFiles.withWindowedWrites());
    } else {
      res = input.apply(writeFiles);
    }
    res.getPerDestinationOutputFilenames().apply(new VerifyFilesExist<>());
    p.run();

    for (int i = 0; i < 5; ++i) {
      ResourceId base =
          getBaseOutputDirectory().resolve("file_" + i, StandardResolveOptions.RESOLVE_FILE);
      List<String> expected = Lists.newArrayList();
      for (int j = i; j < numInputs; j += 5) {
        expected.add("record_" + j);
      }
      checkFileContents(
          base.toString(),
          expected,
          Optional.of(numShards),
          bounded /* expectRemovedTempDirectory */);
    }
  }

  // Test FailingDynamicDestinations class. Expects user values to be string-encoded integers.
  // Throws exceptions when trying to format records or get destinations based on the mod
  // of the element
  static class FailingTestDestinations extends DynamicDestinations<String, Integer, String> {
    private ResourceId baseOutputDirectory;

    FailingTestDestinations(ResourceId baseOutputDirectory) {
      this.baseOutputDirectory = baseOutputDirectory;
    }

    @Override
    public String formatRecord(String record) {
      int value = Integer.valueOf(record);
      // deterministically fail to format 1/3rd of records
      if (value % 3 == 0) {
        throw new RuntimeException("Failed To Format Record");
      }
      return "record_" + record;
    }

    @Override
    public Integer getDestination(String element) {
      int value = Integer.valueOf(element);
      // deterministically fail to find the destination for 1/7th of records
      if (value % 7 == 0) {
        throw new RuntimeException("Failed To Get Destination");
      }
      return value % 5;
    }

    @Override
    public Integer getDefaultDestination() {
      return 0;
    }

    @Override
    public FilenamePolicy getFilenamePolicy(Integer destination) {
      return new PerWindowFiles(
          baseOutputDirectory.resolve("file_" + destination, StandardResolveOptions.RESOLVE_FILE),
          "simple");
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFailingDynamicDestinationsBounded() throws Exception {
    testFailingDynamicDestinationsHelper(true, false);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testFailingDynamicDestinationsUnbounded() throws Exception {
    testFailingDynamicDestinationsHelper(false, false);
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testFailingDynamicDestinationsAutosharding() throws Exception {
    testFailingDynamicDestinationsHelper(false, true);
  }

  private void testFailingDynamicDestinationsHelper(boolean bounded, boolean autosharding)
      throws IOException {
    FailingTestDestinations dynamicDestinations =
        new FailingTestDestinations(getBaseOutputDirectory());
    SimpleSink<Integer> sink =
        new SimpleSink<>(getBaseOutputDirectory(), dynamicDestinations, Compression.UNCOMPRESSED);

    // Flag to validate that the pipeline options are passed to the Sink.
    WriteOptions options = TestPipeline.testingPipelineOptions().as(WriteOptions.class);
    options.setTestFlag("test_value");
    Pipeline p = TestPipeline.create(options);

    final int numInputs = 100;
    long expectedFailures = 0;
    List<String> inputs = Lists.newArrayList();
    for (int i = 0; i < numInputs; ++i) {
      inputs.add(Integer.toString(i));
      if (i % 7 == 0 || i % 3 == 0) {
        expectedFailures++;
      }
    }
    // Prepare timestamps for the elements.
    List<Long> timestamps = new ArrayList<>();
    for (long i = 0; i < inputs.size(); i++) {
      timestamps.add(i + 1);
    }

    BadRecordErrorHandler<PCollection<Long>> errorHandler =
        p.registerBadRecordErrorHandler(new ErrorSinkTransform());
    int numShards = autosharding ? 0 : 2;
    WriteFiles<String, Integer, String> writeFiles =
        WriteFiles.to(sink).withNumShards(numShards).withBadRecordErrorHandler(errorHandler);

    PCollection<String> input = p.apply(Create.timestamped(inputs, timestamps));
    WriteFilesResult<Integer> res;
    if (!bounded) {
      input.setIsBoundedInternal(IsBounded.UNBOUNDED);
      input = input.apply(Window.into(FixedWindows.of(Duration.standardDays(1))));
      res = input.apply(writeFiles.withWindowedWrites());
    } else {
      res = input.apply(writeFiles);
    }

    errorHandler.close();

    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(expectedFailures);

    res.getPerDestinationOutputFilenames().apply(new VerifyFilesExist<>());
    p.run();

    for (int i = 0; i < 5; ++i) {
      ResourceId base =
          getBaseOutputDirectory().resolve("file_" + i, StandardResolveOptions.RESOLVE_FILE);
      List<String> expected = Lists.newArrayList();
      for (int j = i; j < numInputs; j += 5) {
        if (j % 3 != 0 && j % 7 != 0) {
          expected.add("record_" + j);
        }
      }
      checkFileContents(
          base.toString(),
          expected,
          Optional.fromNullable(autosharding ? null : numShards),
          bounded /* expectRemovedTempDirectory */);
    }
  }

  @Test
  public void testShardedDisplayData() {
    DynamicDestinations<String, Void, String> dynamicDestinations =
        DynamicFileDestinations.constant(
            DefaultFilenamePolicy.fromParams(
                new Params()
                    .withBaseFilename(
                        getBaseOutputDirectory()
                            .resolve("file", StandardResolveOptions.RESOLVE_FILE))
                    .withShardTemplate("-SS-of-NN")));
    SimpleSink<Void> sink =
        new SimpleSink<Void>(
            getBaseOutputDirectory(), dynamicDestinations, Compression.UNCOMPRESSED) {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };
    WriteFiles<String, ?, String> write = WriteFiles.to(sink).withNumShards(1);
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("sink", sink.getClass()));
    assertThat(displayData, includesDisplayDataFor("sink", sink));
    assertThat(displayData, hasDisplayItem("numShards", 1));
  }

  @Test
  public void testCustomShardStrategyDisplayData() {
    DynamicDestinations<String, Void, String> dynamicDestinations =
        DynamicFileDestinations.constant(
            DefaultFilenamePolicy.fromParams(
                new Params()
                    .withBaseFilename(
                        getBaseOutputDirectory()
                            .resolve("file", StandardResolveOptions.RESOLVE_FILE))
                    .withShardTemplate("-SS-of-NN")));
    SimpleSink<Void> sink =
        new SimpleSink<Void>(
            getBaseOutputDirectory(), dynamicDestinations, Compression.UNCOMPRESSED) {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };
    WriteFiles<String, ?, String> write =
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
      List<String> inputs,
      PTransform<PCollection<String>, PCollection<String>> transform,
      String baseName,
      WriteFiles<String, ?, String> write)
      throws IOException {
    runShardedWrite(inputs, transform, baseName, write);
  }

  private static class PerWindowFiles extends FilenamePolicy {
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinuteSecondMillis();
    private final ResourceId baseFilename;
    private final String suffix;

    public PerWindowFiles(ResourceId baseFilename, String suffix) {
      this.baseFilename = baseFilename;
      this.suffix = suffix;
    }

    public String filenamePrefixForWindow(IntervalWindow window) {
      String prefix =
          baseFilename.isDirectory() ? "" : firstNonNull(baseFilename.getFilename(), "");
      return String.format(
          "%s%s-%s", prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
    }

    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints) {
      DecimalFormat df = new DecimalFormat("0000");
      IntervalWindow intervalWindow = (IntervalWindow) window;
      String filename =
          String.format(
              "%s-%s-of-%s%s%s",
              filenamePrefixForWindow(intervalWindow),
              df.format(shardNumber),
              df.format(numShards),
              outputFileHints.getSuggestedFilenameSuffix(),
              suffix);
      return baseFilename
          .getCurrentDirectory()
          .resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints) {
      DecimalFormat df = new DecimalFormat("0000");
      String prefix =
          baseFilename.isDirectory() ? "" : firstNonNull(baseFilename.getFilename(), "");
      String filename =
          String.format(
              "%s-%s-of-%s%s%s",
              prefix,
              df.format(shardNumber),
              df.format(numShards),
              outputFileHints.getSuggestedFilenameSuffix(),
              suffix);
      return baseFilename
          .getCurrentDirectory()
          .resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }
  }

  /**
   * Same as {@link #runShardedWrite(List, PTransform, String, WriteFiles, BiFunction, boolean)} but
   * without shard content check. This means content will be checked only globally, that shards
   * together contains written input and not content per shard
   */
  private void runShardedWrite(
      List<String> inputs,
      PTransform<PCollection<String>, PCollection<String>> transform,
      String baseName,
      WriteFiles<String, ?, String> write)
      throws IOException {
    runShardedWrite(inputs, transform, baseName, write, null, false);
  }

  /**
   * Performs a WriteFiles transform with the desired number of shards. Verifies the WriteFiles
   * transform calls the appropriate methods on a test sink in the correct order, as well as
   * verifies that the elements of a PCollection are written to the sink. If numConfiguredShards is
   * not null, also verifies that the output number of shards is correct.
   */
  private void runShardedWrite(
      List<String> inputs,
      PTransform<PCollection<String>, PCollection<String>> transform,
      String baseName,
      WriteFiles<String, ?, String> write,
      BiFunction<Integer, List<String>, Void> shardContentChecker,
      boolean isUnbounded)
      throws IOException {
    // Flag to validate that the pipeline options are passed to the Sink
    WriteOptions options = TestPipeline.testingPipelineOptions().as(WriteOptions.class);
    options.setTestFlag("test_value");
    Pipeline p = TestPipeline.create(options);

    // Prepare timestamps for the elements.
    List<Long> timestamps = new ArrayList<>();
    for (long i = 0; i < inputs.size(); i++) {
      timestamps.add(i + 1);
    }
    p.apply(Create.timestamped(inputs, timestamps).withCoder(StringUtf8Coder.of()))
        .setIsBoundedInternal(isUnbounded ? IsBounded.UNBOUNDED : IsBounded.BOUNDED)
        .apply(transform)
        .apply(write)
        .getPerDestinationOutputFilenames()
        .apply(new VerifyFilesExist<>());
    p.run();

    Optional<Integer> numShards =
        (write.getNumShardsProvider() != null && !write.getWindowedWrites())
            ? Optional.of(write.getNumShardsProvider().get())
            : Optional.absent();
    checkFileContents(baseName, inputs, numShards, !write.getWindowedWrites(), shardContentChecker);
  }

  static void checkFileContents(
      String baseName,
      List<String> inputs,
      Optional<Integer> numExpectedShards,
      boolean expectRemovedTempDirectory)
      throws IOException {
    checkFileContents(baseName, inputs, numExpectedShards, expectRemovedTempDirectory, null);
  }

  static void checkFileContents(
      String baseName,
      List<String> inputs,
      Optional<Integer> numExpectedShards,
      boolean expectRemovedTempDirectory,
      BiFunction<Integer, List<String>, Void> shardContentChecker)
      throws IOException {
    List<File> outputFiles = Lists.newArrayList();
    final String pattern = baseName + "*";
    List<Metadata> metadata =
        FileSystems.match(Collections.singletonList(pattern)).get(0).metadata();
    for (Metadata meta : metadata) {
      outputFiles.add(new File(meta.resourceId().toString()));
    }
    assertFalse("Should have produced at least 1 output file", outputFiles.isEmpty());
    Pattern shardPattern = Pattern.compile("(\\d{4})-of-\\d{4}");
    if (numExpectedShards.isPresent()) {
      assertEquals(numExpectedShards.get().intValue(), outputFiles.size());

      Set<String> expectedShards = Sets.newHashSet();
      DecimalFormat df = new DecimalFormat("0000");
      for (int i = 0; i < numExpectedShards.get(); i++) {
        expectedShards.add(
            String.format("%s-of-%s", df.format(i), df.format(numExpectedShards.get())));
      }

      Set<String> outputShards = Sets.newHashSet();
      for (File file : outputFiles) {
        Matcher matcher = shardPattern.matcher(file.getName());
        assertTrue(matcher.find());
        assertTrue(outputShards.add(matcher.group()));
      }
      assertEquals(expectedShards, outputShards);
    }

    List<String> actual = Lists.newArrayList();
    for (File outputFile : outputFiles) {
      List<String> actualShard = Lists.newArrayList();
      try (BufferedReader reader =
          Files.newBufferedReader(outputFile.toPath(), StandardCharsets.UTF_8)) {
        for (; ; ) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          if (!line.equals(SimpleWriter.HEADER) && !line.equals(SimpleWriter.FOOTER)) {
            actualShard.add(line);
          }
        }
      }
      if (shardContentChecker != null) {
        Matcher matcher = shardPattern.matcher(outputFile.getName());
        matcher.find();
        int shardNumber = Integer.parseInt(matcher.group(1));
        shardContentChecker.apply(shardNumber, actualShard);
      }
      actual.addAll(actualShard);
    }
    assertThat(actual, containsInAnyOrder(inputs.toArray()));
    if (expectRemovedTempDirectory) {
      assertThat(
          Lists.newArrayList(new File(baseName).getParentFile().list()),
          Matchers.everyItem(not(containsString(FileBasedSink.TEMP_DIRECTORY_PREFIX))));
    }
  }

  /** Options for test, exposed for PipelineOptionsFactory. */
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
          .apply(Top.largest(1))
          .apply(Flatten.iterables())
          .apply(View.asSingleton());
    }
  }

  private static class TestShardingFunction implements ShardingFunction<String, Void> {
    @Override
    public ShardedKey<Integer> assignShardKey(Void destination, String element, int shardCount)
        throws Exception {
      int shard = Integer.parseInt(element);
      return ShardedKey.of(0, shard);
    }
  }
}
