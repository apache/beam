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
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects.firstNonNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
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
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
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
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.commons.compress.utils.Sets;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
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
  public void testUnboundedWritesNeedSharding() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "When applying WriteFiles to an unbounded PCollection, "
            + "must specify number of output shards explicitly");

    SimpleSink<Void> sink = makeSimpleSink();
    p.apply(Create.of("foo"))
        .setIsBoundedInternal(IsBounded.UNBOUNDED)
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
  @Category(NeedsRunner.class)
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
   * Performs a WriteFiles transform with the desired number of shards. Verifies the WriteFiles
   * transform calls the appropriate methods on a test sink in the correct order, as well as
   * verifies that the elements of a PCollection are written to the sink. If numConfiguredShards is
   * not null, also verifies that the output number of shards is correct.
   */
  private void runShardedWrite(
      List<String> inputs,
      PTransform<PCollection<String>, PCollection<String>> transform,
      String baseName,
      WriteFiles<String, ?, String> write)
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
        .apply(transform)
        .apply(write)
        .getPerDestinationOutputFilenames()
        .apply(new VerifyFilesExist<>());
    p.run();

    Optional<Integer> numShards =
        (write.getNumShardsProvider() != null && !write.getWindowedWrites())
            ? Optional.of(write.getNumShardsProvider().get())
            : Optional.absent();
    checkFileContents(baseName, inputs, numShards, !write.getWindowedWrites());
  }

  static void checkFileContents(
      String baseName,
      List<String> inputs,
      Optional<Integer> numExpectedShards,
      boolean expectRemovedTempDirectory)
      throws IOException {
    List<File> outputFiles = Lists.newArrayList();
    final String pattern = baseName + "*";
    List<Metadata> metadata =
        FileSystems.match(Collections.singletonList(pattern)).get(0).metadata();
    for (Metadata meta : metadata) {
      outputFiles.add(new File(meta.resourceId().toString()));
    }
    assertFalse("Should have produced at least 1 output file", outputFiles.isEmpty());
    if (numExpectedShards.isPresent()) {
      assertEquals(numExpectedShards.get().intValue(), outputFiles.size());
      Pattern shardPattern = Pattern.compile("\\d{4}-of-\\d{4}");

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
      try (BufferedReader reader = Files.newBufferedReader(outputFile.toPath(), Charsets.UTF_8)) {
        for (; ; ) {
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
}
