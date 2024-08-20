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

import static org.apache.beam.sdk.TestUtils.LINES2_ARRAY;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.apache.beam.sdk.io.fs.MatchResult.Status.NOT_FOUND;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.commons.lang3.SystemUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TextIO.Write}. */
@RunWith(JUnit4.class)
public class TextIOWriteTest {
  private static final String MY_HEADER = "myHeader";
  private static final String MY_FOOTER = "myFooter";
  private static final int CUSTOM_FILE_TRIGGERING_RECORD_COUNT = 50000;
  private static final int CUSTOM_FILE_TRIGGERING_BYTE_COUNT = 32 * 1024 * 1024; // 32MiB
  private static final Duration CUSTOM_FILE_TRIGGERING_RECORD_BUFFERING_DURATION =
      Duration.standardSeconds(4);

  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Rule public transient ExpectedException expectedException = ExpectedException.none();

  static class TestDynamicDestinations
      extends FileBasedSink.DynamicDestinations<String, String, String> {
    ResourceId baseDir;

    TestDynamicDestinations(ResourceId baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public String formatRecord(String record) {
      return record;
    }

    @Override
    public String getDestination(String element) {
      // Destination is based on first character of string.
      return element.substring(0, 1);
    }

    @Override
    public String getDefaultDestination() {
      return "";
    }

    @Override
    public @Nullable Coder<String> getDestinationCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(String destination) {
      return DefaultFilenamePolicy.fromStandardParameters(
          ValueProvider.StaticValueProvider.of(
              baseDir.resolve(
                  "file_" + destination + ".txt",
                  ResolveOptions.StandardResolveOptions.RESOLVE_FILE)),
          null,
          null,
          false);
    }
  }

  static class StartsWith implements Predicate<String> {
    String prefix;

    StartsWith(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public boolean apply(@Nullable String input) {
      return input.startsWith(prefix);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinations() throws Exception {
    testDynamicDestinations(false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsWithCustomType() throws Exception {
    testDynamicDestinations(true);
  }

  private void testDynamicDestinations(boolean customType) throws Exception {
    ResourceId baseDir =
        FileSystems.matchNewResource(
            Files.createTempDirectory(tempFolder.getRoot().toPath(), "testDynamicDestinations")
                .toString(),
            true);

    List<String> elements = Lists.newArrayList("aaaa", "aaab", "baaa", "baab", "caaa", "caab");
    PCollection<String> input = p.apply(Create.of(elements).withCoder(StringUtf8Coder.of()));
    if (customType) {
      input.apply(
          TextIO.<String>writeCustomType()
              .to(new TestDynamicDestinations(baseDir))
              .withTempDirectory(baseDir));
    } else {
      input.apply(
          TextIO.write().to(new TestDynamicDestinations(baseDir)).withTempDirectory(baseDir));
    }
    p.run();

    assertOutputFiles(
        Iterables.toArray(Iterables.filter(elements, new StartsWith("a")), String.class),
        null,
        null,
        0,
        baseDir.resolve("file_a.txt", ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
    assertOutputFiles(
        Iterables.toArray(Iterables.filter(elements, new StartsWith("b")), String.class),
        null,
        null,
        0,
        baseDir.resolve("file_b.txt", ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
    assertOutputFiles(
        Iterables.toArray(Iterables.filter(elements, new StartsWith("c")), String.class),
        null,
        null,
        0,
        baseDir.resolve("file_c.txt", ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
  }

  public static class UserWriteTypeCoder extends CustomCoder<UserWriteType> {

    @Override
    public void encode(UserWriteType value, OutputStream outStream)
        throws CoderException, IOException {
      DataOutputStream stream = new DataOutputStream(outStream);
      StringUtf8Coder.of().encode(value.destination, stream);
      StringUtf8Coder.of().encode(value.metadata, stream);
    }

    @Override
    public UserWriteType decode(InputStream inStream) throws CoderException, IOException {
      DataInputStream stream = new DataInputStream(inStream);
      String dest = StringUtf8Coder.of().decode(stream);
      String meta = StringUtf8Coder.of().decode(stream);
      return new UserWriteType(dest, meta);
    }
  }

  @DefaultCoder(UserWriteTypeCoder.class)
  private static class UserWriteType {
    String destination;
    String metadata;

    UserWriteType() {
      this.destination = "";
      this.metadata = "";
    }

    UserWriteType(String destination, String metadata) {
      this.destination = destination;
      this.metadata = metadata;
    }

    @Override
    public String toString() {
      return String.format("destination: %s metadata : %s", destination, metadata);
    }
  }

  private static class SerializeUserWrite implements SerializableFunction<UserWriteType, String> {
    @Override
    public String apply(UserWriteType input) {
      return input.toString();
    }
  }

  private static class UserWriteDestination
      implements SerializableFunction<UserWriteType, DefaultFilenamePolicy.Params> {
    private ResourceId baseDir;

    UserWriteDestination(ResourceId baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public DefaultFilenamePolicy.Params apply(UserWriteType input) {
      return new DefaultFilenamePolicy.Params()
          .withBaseFilename(
              baseDir.resolve(
                  "file_" + input.destination.substring(0, 1) + ".txt",
                  ResolveOptions.StandardResolveOptions.RESOLVE_FILE));
    }
  }

  private static class ExtractWriteDestination implements Function<UserWriteType, String> {
    @Override
    public String apply(@Nullable UserWriteType input) {
      return input.destination;
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDefaultFilenamePolicy() throws Exception {
    ResourceId baseDir =
        FileSystems.matchNewResource(
            Files.createTempDirectory(tempFolder.getRoot().toPath(), "testDynamicDestinations")
                .toString(),
            true);

    List<UserWriteType> elements =
        Lists.newArrayList(
            new UserWriteType("aaaa", "first"),
            new UserWriteType("aaab", "second"),
            new UserWriteType("baaa", "third"),
            new UserWriteType("baab", "fourth"),
            new UserWriteType("caaa", "fifth"),
            new UserWriteType("caab", "sixth"));

    p.getCoderRegistry().registerCoderForClass(UserWriteType.class, new UserWriteTypeCoder());
    PCollection<UserWriteType> input = p.apply(Create.of(elements));
    input.apply(
        TextIO.<UserWriteType>writeCustomType()
            .to(
                new UserWriteDestination(baseDir),
                new DefaultFilenamePolicy.Params()
                    .withBaseFilename(
                        baseDir.resolve(
                            "empty", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)))
            .withFormatFunction(new SerializeUserWrite())
            .withTempDirectory(FileSystems.matchNewResource(baseDir.toString(), true)));
    p.run();

    String[] aElements =
        Iterables.toArray(
            StreamSupport.stream(
                    elements.stream()
                        .filter(
                            Predicates.compose(new StartsWith("a"), new ExtractWriteDestination())
                                ::apply)
                        .collect(Collectors.toList())
                        .spliterator(),
                    false)
                .map(Functions.toStringFunction()::apply)
                .collect(Collectors.toList()),
            String.class);
    String[] bElements =
        Iterables.toArray(
            StreamSupport.stream(
                    elements.stream()
                        .filter(
                            Predicates.compose(new StartsWith("b"), new ExtractWriteDestination())
                                ::apply)
                        .collect(Collectors.toList())
                        .spliterator(),
                    false)
                .map(Functions.toStringFunction()::apply)
                .collect(Collectors.toList()),
            String.class);
    String[] cElements =
        Iterables.toArray(
            StreamSupport.stream(
                    elements.stream()
                        .filter(
                            Predicates.compose(new StartsWith("c"), new ExtractWriteDestination())
                                ::apply)
                        .collect(Collectors.toList())
                        .spliterator(),
                    false)
                .map(Functions.toStringFunction()::apply)
                .collect(Collectors.toList()),
            String.class);
    assertOutputFiles(
        aElements,
        null,
        null,
        0,
        baseDir.resolve("file_a.txt", ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
    assertOutputFiles(
        bElements,
        null,
        null,
        0,
        baseDir.resolve("file_b.txt", ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
    assertOutputFiles(
        cElements,
        null,
        null,
        0,
        baseDir.resolve("file_c.txt", ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
  }

  private void runTestWrite(String[] elems) throws Exception {
    runTestWrite(elems, null, null, 1);
  }

  private void runTestWrite(String[] elems, int numShards) throws Exception {
    runTestWrite(elems, null, null, numShards);
  }

  private void runTestWrite(String[] elems, String header, String footer) throws Exception {
    runTestWrite(elems, header, footer, 1);
  }

  private void runTestWrite(String[] elems, String header, String footer, int numShards)
      throws Exception {
    runTestWrite(elems, header, footer, numShards, false);
  }

  private void runTestWrite(
      String[] elems, String header, String footer, int numShards, boolean skipIfEmpty)
      throws Exception {
    String outputName = "file.txt";
    Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "testwrite");
    ResourceId baseFilename =
        FileBasedSink.convertToFileResourceIfPossible(baseDir.resolve(outputName).toString());

    PCollection<String> input =
        p.apply("CreateInput", Create.of(Arrays.asList(elems)).withCoder(StringUtf8Coder.of()));

    TextIO.TypedWrite<String, Void> write =
        TextIO.write().to(baseFilename).withHeader(header).withFooter(footer).withOutputFilenames();

    if (numShards == 1) {
      write = write.withoutSharding();
    } else if (numShards > 0) {
      write = write.withNumShards(numShards).withShardNameTemplate(ShardNameTemplate.INDEX_OF_MAX);
    }
    if (skipIfEmpty) {
      write = write.skipIfEmpty();
    }

    input.apply(write);

    p.run();

    assertOutputFiles(
        elems,
        header,
        footer,
        numShards,
        baseFilename,
        firstNonNull(
            write.getShardTemplate(), DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE),
        skipIfEmpty);
  }

  private static void assertOutputFiles(
      String[] elems,
      final String header,
      final String footer,
      int numShards,
      ResourceId outputPrefix,
      String shardNameTemplate)
      throws Exception {
    assertOutputFiles(elems, header, footer, numShards, outputPrefix, shardNameTemplate, false);
  }

  private static void assertOutputFiles(
      String[] elems,
      final String header,
      final String footer,
      int numShards,
      ResourceId outputPrefix,
      String shardNameTemplate,
      boolean skipIfEmpty)
      throws Exception {
    List<File> expectedFiles = new ArrayList<>();
    if (skipIfEmpty && elems.length == 0) {
      String pattern = outputPrefix.toString() + "*";
      MatchResult matches =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(pattern)));
      assertEquals(NOT_FOUND, matches.status());
    } else if (numShards == 0) {
      String pattern = outputPrefix.toString() + "*";
      List<MatchResult> matches = FileSystems.match(Collections.singletonList(pattern));
      for (Metadata expectedFile : Iterables.getOnlyElement(matches).metadata()) {
        expectedFiles.add(new File(expectedFile.resourceId().toString()));
      }
    } else {
      for (int i = 0; i < numShards; i++) {
        expectedFiles.add(
            new File(
                DefaultFilenamePolicy.constructName(
                        outputPrefix, shardNameTemplate, "", i, numShards, null, null)
                    .toString()));
      }
    }

    List<List<String>> actual = new ArrayList<>();

    for (File tmpFile : expectedFiles) {
      List<String> currentFile = readLinesFromFile(tmpFile);
      actual.add(currentFile);
    }

    List<String> expectedElements = new ArrayList<>(elems.length);
    for (String elem : elems) {
      byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
      String line = new String(encodedElem, StandardCharsets.UTF_8);
      expectedElements.add(line);
    }

    List<String> actualElements =
        Lists.newArrayList(
            Iterables.concat(
                FluentIterable.from(actual)
                    .transform(removeHeaderAndFooter(header, footer))
                    .toList()));

    assertThat(actualElements, containsInAnyOrder(expectedElements.toArray()));
    assertTrue(actual.stream().allMatch(haveProperHeaderAndFooter(header, footer)::apply));
  }

  private static List<String> readLinesFromFile(File f) throws IOException {
    List<String> currentFile = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        currentFile.add(line);
      }
    }
    return currentFile;
  }

  private static Function<List<String>, List<String>> removeHeaderAndFooter(
      final String header, final String footer) {
    return new Function<List<String>, List<String>>() {
      @Override
      public @Nullable List<String> apply(List<String> lines) {
        ArrayList<String> newLines = Lists.newArrayList(lines);
        if (header != null) {
          newLines.remove(0);
        }
        if (footer != null) {
          int last = newLines.size() - 1;
          newLines.remove(last);
        }
        return newLines;
      }
    };
  }

  private static Predicate<List<String>> haveProperHeaderAndFooter(
      final String header, final String footer) {
    return fileLines -> {
      int last = fileLines.size() - 1;
      return (header == null || fileLines.get(0).equals(header))
          && (footer == null || fileLines.get(last).equals(footer));
    };
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteStrings() throws Exception {
    runTestWrite(LINES_ARRAY);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStringsNoSharding() throws Exception {
    runTestWrite(NO_LINES_ARRAY, 0);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStrings() throws Exception {
    runTestWrite(NO_LINES_ARRAY);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStringsSkipIfEmpty() throws Exception {
    runTestWrite(NO_LINES_ARRAY, null, null, 0, true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testShardedWrite() throws Exception {
    runTestWrite(LINES_ARRAY, 5);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithHeader() throws Exception {
    runTestWrite(LINES_ARRAY, MY_HEADER, null);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithFooter() throws Exception {
    runTestWrite(LINES_ARRAY, null, MY_FOOTER);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithHeaderAndFooter() throws Exception {
    runTestWrite(LINES_ARRAY, MY_HEADER, MY_FOOTER);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithWritableByteChannelFactory() throws Exception {
    Coder<String> coder = StringUtf8Coder.of();
    String outputName = "file.txt";
    ResourceId baseDir =
        FileSystems.matchNewResource(
            Files.createTempDirectory(tempFolder.getRoot().toPath(), "testwrite").toString(), true);

    PCollection<String> input = p.apply(Create.of(Arrays.asList(LINES2_ARRAY)).withCoder(coder));

    final WritableByteChannelFactory writableByteChannelFactory =
        new DrunkWritableByteChannelFactory();
    TextIO.Write write =
        TextIO.write()
            .to(
                baseDir
                    .resolve(outputName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
                    .toString())
            .withoutSharding()
            .withWritableByteChannelFactory(writableByteChannelFactory);
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("writableByteChannelFactory", "DRUNK"));

    input.apply(write);

    p.run();

    final List<String> drunkElems = new ArrayList<>(LINES2_ARRAY.length * 2 + 2);
    for (String elem : LINES2_ARRAY) {
      drunkElems.add(elem);
      drunkElems.add(elem);
    }
    assertOutputFiles(
        drunkElems.toArray(new String[0]),
        null,
        null,
        1,
        baseDir.resolve(
            outputName + writableByteChannelFactory.getSuggestedFilenameSuffix(),
            ResolveOptions.StandardResolveOptions.RESOLVE_FILE),
        write.inner.getShardTemplate());
  }

  @Test
  public void testWriteDisplayData() {
    // TODO: Java core test failing on windows, https://github.com/apache/beam/issues/20467
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    TextIO.Write write =
        TextIO.write()
            .to("/foo")
            .withSuffix("bar")
            .withShardNameTemplate("-SS-of-NN-")
            .withNumShards(100)
            .withFooter("myFooter")
            .withHeader("myHeader");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "/foo"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(displayData, hasDisplayItem("fileHeader", "myHeader"));
    assertThat(displayData, hasDisplayItem("fileFooter", "myFooter"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("writableByteChannelFactory", "UNCOMPRESSED"));
  }

  @Test
  public void testWriteDisplayDataValidateThenHeader() {
    TextIO.Write write = TextIO.write().to("foo").withHeader("myHeader");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("fileHeader", "myHeader"));
  }

  @Test
  public void testWriteDisplayDataValidateThenFooter() {
    TextIO.Write write = TextIO.write().to("foo").withFooter("myFooter");

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("fileFooter", "myFooter"));
  }

  @Test
  public void testGetName() {
    assertEquals("TextIO.Write", TextIO.write().to("somefile").getName());
  }

  /** Options for testing. */
  public interface RuntimeTestOptions extends PipelineOptions {
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApply() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);

    p.apply(Create.of("")).apply(TextIO.write().to(options.getOutput()));
  }

  @Test
  @Category({NeedsRunner.class, UsesUnboundedPCollections.class})
  public void testWriteUnboundedWithCustomBatchParameters() throws Exception {
    Coder<String> coder = StringUtf8Coder.of();
    String outputName = "file.txt";
    Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "testwrite");
    ResourceId baseFilename =
        FileBasedSink.convertToFileResourceIfPossible(baseDir.resolve(outputName).toString());

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(LINES2_ARRAY)).withCoder(coder))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))));

    TextIO.Write write =
        TextIO.write()
            .to(baseFilename)
            .withWindowedWrites()
            .withShardNameTemplate(DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE)
            .withBatchSize(CUSTOM_FILE_TRIGGERING_RECORD_COUNT)
            .withBatchSizeBytes(CUSTOM_FILE_TRIGGERING_BYTE_COUNT)
            .withBatchMaxBufferingDuration(CUSTOM_FILE_TRIGGERING_RECORD_BUFFERING_DURATION);

    input.apply(write);
    p.run();

    assertOutputFiles(
        LINES2_ARRAY,
        null,
        null,
        3,
        baseFilename,
        DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE,
        false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWindowedWritesWithOnceTrigger() throws Throwable {
    p.enableAbandonedNodeEnforcement(false);
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unsafe trigger");

    // Tests for https://issues.apache.org/jira/browse/BEAM-3169
    PCollection<String> data =
        p.apply(Create.of("0", "1", "2"))
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                    // According to this trigger, all data should be written.
                    // However, the continuation of this trigger is elementCountAtLeast(1),
                    // so with a buggy implementation that used a GBK before renaming files,
                    // only 1 file would be renamed.
                    .triggering(AfterPane.elementCountAtLeast(3))
                    .withAllowedLateness(Duration.standardMinutes(1))
                    .discardingFiredPanes());
    data.apply(
            TextIO.write()
                .to(new File(tempFolder.getRoot(), "windowed-writes").getAbsolutePath())
                .withNumShards(2)
                .withWindowedWrites()
                .<Void>withOutputFilenames())
        .getPerDestinationOutputFilenames()
        .apply(Values.create());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteViaSink() throws Exception {
    List<String> data = ImmutableList.of("a", "b", "c", "d", "e", "f");

    PAssert.that(
            p.apply("Create Data ReadFiles", Create.of(data))
                .apply(
                    "Write ReadFiles",
                    FileIO.<String>write()
                        .to(tempFolder.getRoot().toString())
                        .withSuffix(".txt")
                        .via(TextIO.sink())
                        .withIgnoreWindowing())
                .getPerDestinationOutputFilenames()
                .apply("Extract Values ReadFiles", Values.create())
                .apply("Match All", FileIO.matchAll())
                .apply("Read Matches", FileIO.readMatches())
                .apply("Read Files", TextIO.readFiles()))
        .containsInAnyOrder(data);

    p.run();
  }

  @Test
  public void testSink() throws Exception {
    TextIO.Sink sink = TextIO.sink().withHeader("header").withFooter("footer");
    File f = new File(tempFolder.getRoot(), "file");
    try (WritableByteChannel chan = Channels.newChannel(new FileOutputStream(f))) {
      sink.open(chan);
      sink.write("a");
      sink.write("b");
      sink.write("c");
      sink.flush();
    }

    assertEquals(Arrays.asList("header", "a", "b", "c", "footer"), readLinesFromFile(f));
  }
}
