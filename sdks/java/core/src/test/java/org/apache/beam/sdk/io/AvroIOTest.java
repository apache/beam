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

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for AvroIO Read and Write transforms. */
@RunWith(JUnit4.class)
public class AvroIOTest {

  @Rule
  public TestPipeline writePipeline = TestPipeline.create();

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAvroIOGetName() {
    assertEquals("AvroIO.Read", AvroIO.read(String.class).from("/tmp/foo*/baz").getName());
    assertEquals("AvroIO.Write", AvroIO.write(String.class).to("/tmp/foo/baz").getName());
  }

  @DefaultCoder(AvroCoder.class)
  static class GenericClass {
    int intField;
    String stringField;

    public GenericClass() {}

    public GenericClass(int intValue, String stringValue) {
      this.intField = intValue;
      this.stringField = stringValue;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("intField", intField)
          .add("stringField", stringField)
          .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(intField, stringField);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof GenericClass)) {
        return false;
      }
      GenericClass o = (GenericClass) other;
      return Objects.equals(intField, o.intField) && Objects.equals(stringField, o.stringField);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAvroIOWriteAndReadASingleFile() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(AvroIO.write(GenericClass.class).to(outputFile.getAbsolutePath()).withoutSharding());
    writePipeline.run().waitUntilFinish();

    // Test the same data via read(), read().withHintMatchesManyFiles(), and readAll()
    PAssert.that(
            readPipeline.apply(
                "Read", AvroIO.read(GenericClass.class).from(outputFile.getAbsolutePath())))
        .containsInAnyOrder(values);
    PAssert.that(
            readPipeline.apply(
                "Read withHintMatchesManyFiles",
                AvroIO.read(GenericClass.class)
                    .from(outputFile.getAbsolutePath())
                    .withHintMatchesManyFiles()))
        .containsInAnyOrder(values);
    PAssert.that(
            "ReadAll",
            readPipeline
                .apply(Create.of(outputFile.getAbsolutePath()))
                .apply(AvroIO.readAll(GenericClass.class).withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(values);

    readPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAvroIOWriteAndReadMultipleFilepatterns() throws Throwable {
    List<GenericClass> firstValues = Lists.newArrayList();
    List<GenericClass> secondValues = Lists.newArrayList();
    for (int i = 0; i < 10; ++i) {
      firstValues.add(new GenericClass(i, "a" + i));
      secondValues.add(new GenericClass(i, "b" + i));
    }
    writePipeline
        .apply("Create first", Create.of(firstValues))
        .apply(
            "Write first",
            AvroIO.write(GenericClass.class)
                .to(tmpFolder.getRoot().getAbsolutePath() + "/first")
                .withNumShards(2));
    writePipeline
        .apply("Create second", Create.of(secondValues))
        .apply(
            "Write second",
            AvroIO.write(GenericClass.class)
                .to(tmpFolder.getRoot().getAbsolutePath() + "/second")
                .withNumShards(3));
    writePipeline.run().waitUntilFinish();

    // Test both read() and readAll()
    PAssert.that(
            readPipeline.apply(
                "Read first",
                AvroIO.read(GenericClass.class)
                    .from(tmpFolder.getRoot().getAbsolutePath() + "/first*")))
        .containsInAnyOrder(firstValues);
    PAssert.that(
            readPipeline.apply(
                "Read second",
                AvroIO.read(GenericClass.class)
                    .from(tmpFolder.getRoot().getAbsolutePath() + "/second*")))
        .containsInAnyOrder(secondValues);
    PAssert.that(
            readPipeline
                .apply(
                    "Create paths",
                    Create.of(
                        tmpFolder.getRoot().getAbsolutePath() + "/first*",
                        tmpFolder.getRoot().getAbsolutePath() + "/second*"))
                .apply(
                    "Read all", AvroIO.readAll(GenericClass.class).withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(Iterables.concat(firstValues, secondValues));

    readPipeline.run();
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testAvroIOCompressedWriteAndReadASingleFile() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(outputFile.getAbsolutePath())
                .withoutSharding()
                .withCodec(CodecFactory.deflateCodec(9)));
    writePipeline.run().waitUntilFinish();

    PCollection<GenericClass> input =
        readPipeline.apply(AvroIO.read(GenericClass.class).from(outputFile.getAbsolutePath()));

    PAssert.that(input).containsInAnyOrder(values);
    readPipeline.run();
    DataFileStream dataFileStream =
        new DataFileStream(new FileInputStream(outputFile), new GenericDatumReader());
    assertEquals("deflate", dataFileStream.getMetaString("avro.codec"));
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testAvroIONullCodecWriteAndReadASingleFile() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(outputFile.getAbsolutePath())
                .withoutSharding()
                .withCodec(CodecFactory.nullCodec()));
    writePipeline.run().waitUntilFinish();

    PCollection<GenericClass> input =
        readPipeline.apply(AvroIO.read(GenericClass.class).from(outputFile.getAbsolutePath()));

    PAssert.that(input).containsInAnyOrder(values);
    readPipeline.run();
    DataFileStream dataFileStream =
        new DataFileStream(new FileInputStream(outputFile), new GenericDatumReader());
    assertEquals("null", dataFileStream.getMetaString("avro.codec"));
  }

  @DefaultCoder(AvroCoder.class)
  static class GenericClassV2 {
    int intField;
    String stringField;
    @Nullable String nullableField;

    public GenericClassV2() {}

    public GenericClassV2(int intValue, String stringValue, String nullableValue) {
      this.intField = intValue;
      this.stringField = stringValue;
      this.nullableField = nullableValue;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("intField", intField)
          .add("stringField", stringField)
          .add("nullableField", nullableField)
          .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(intField, stringField, nullableField);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof GenericClassV2)) {
        return false;
      }
      GenericClassV2 o = (GenericClassV2) other;
      return Objects.equals(intField, o.intField)
          && Objects.equals(stringField, o.stringField)
          && Objects.equals(nullableField, o.nullableField);
    }
  }

  /**
   * Tests that {@code AvroIO} can read an upgraded version of an old class, as long as the schema
   * resolution process succeeds. This test covers the case when a new, {@code @Nullable} field has
   * been added.
   *
   * <p>For more information, see http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution
   */
  @Test
  @Category(NeedsRunner.class)
  public void testAvroIOWriteAndReadSchemaUpgrade() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(AvroIO.write(GenericClass.class).to(outputFile.getAbsolutePath()).withoutSharding());
    writePipeline.run().waitUntilFinish();

    List<GenericClassV2> expected =
        ImmutableList.of(new GenericClassV2(3, "hi", null), new GenericClassV2(5, "bar", null));

    PCollection<GenericClassV2> input =
        readPipeline.apply(AvroIO.read(GenericClassV2.class).from(outputFile.getAbsolutePath()));

    PAssert.that(input).containsInAnyOrder(expected);
    readPipeline.run();
  }

  private static class WindowedFilenamePolicy extends FilenamePolicy {
    final ResourceId outputFilePrefix;

    WindowedFilenamePolicy(ResourceId outputFilePrefix) {
      this.outputFilePrefix = outputFilePrefix;
    }

    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints) {
      String filenamePrefix =
          outputFilePrefix.isDirectory() ? "" : firstNonNull(outputFilePrefix.getFilename(), "");

      String filename =
          String.format(
              "%s-%s-%s-of-%s-pane-%s%s%s",
              filenamePrefix,
              window,
              shardNumber,
              numShards - 1,
              paneInfo.getIndex(),
              paneInfo.isLast() ? "-final" : "",
              outputFileHints.getSuggestedFilenameSuffix());
      return outputFilePrefix
          .getCurrentDirectory()
          .resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Expecting windowed outputs only");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(
          DisplayData.item("fileNamePrefix", outputFilePrefix.toString())
              .withLabel("File Name Prefix"));
    }
  }

  @Rule public TestPipeline windowedAvroWritePipeline = TestPipeline.create();

  @Test
  @Category({ValidatesRunner.class, UsesTestStream.class})
  public void testWindowedAvroIOWrite() throws Throwable {
    Path baseDir = Files.createTempDirectory(tmpFolder.getRoot().toPath(), "testwrite");
    String baseFilename = baseDir.resolve("prefix").toString();

    Instant base = new Instant(0);
    ArrayList<GenericClass> allElements = new ArrayList<>();
    ArrayList<TimestampedValue<GenericClass>> firstWindowElements = new ArrayList<>();
    ArrayList<Instant> firstWindowTimestamps =
        Lists.newArrayList(
            base.plus(Duration.standardSeconds(0)), base.plus(Duration.standardSeconds(10)),
            base.plus(Duration.standardSeconds(20)), base.plus(Duration.standardSeconds(30)));

    Random random = new Random();
    for (int i = 0; i < 100; ++i) {
      GenericClass item = new GenericClass(i, String.valueOf(i));
      allElements.add(item);
      firstWindowElements.add(
          TimestampedValue.of(
              item, firstWindowTimestamps.get(random.nextInt(firstWindowTimestamps.size()))));
    }

    ArrayList<TimestampedValue<GenericClass>> secondWindowElements = new ArrayList<>();
    ArrayList<Instant> secondWindowTimestamps =
        Lists.newArrayList(
            base.plus(Duration.standardSeconds(60)), base.plus(Duration.standardSeconds(70)),
            base.plus(Duration.standardSeconds(80)), base.plus(Duration.standardSeconds(90)));
    for (int i = 100; i < 200; ++i) {
      GenericClass item = new GenericClass(i, String.valueOf(i));
      allElements.add(new GenericClass(i, String.valueOf(i)));
      secondWindowElements.add(
          TimestampedValue.of(
              item, secondWindowTimestamps.get(random.nextInt(secondWindowTimestamps.size()))));
    }

    TimestampedValue<GenericClass>[] firstWindowArray =
        firstWindowElements.toArray(new TimestampedValue[100]);
    TimestampedValue<GenericClass>[] secondWindowArray =
        secondWindowElements.toArray(new TimestampedValue[100]);

    TestStream<GenericClass> values =
        TestStream.create(AvroCoder.of(GenericClass.class))
            .advanceWatermarkTo(new Instant(0))
            .addElements(
                firstWindowArray[0],
                Arrays.copyOfRange(firstWindowArray, 1, firstWindowArray.length))
            .advanceWatermarkTo(new Instant(0).plus(Duration.standardMinutes(1)))
            .addElements(
                secondWindowArray[0],
                Arrays.copyOfRange(secondWindowArray, 1, secondWindowArray.length))
            .advanceWatermarkToInfinity();

    FilenamePolicy policy =
        new WindowedFilenamePolicy(FileBasedSink.convertToFileResourceIfPossible(baseFilename));
    windowedAvroWritePipeline
        .apply(values)
        .apply(Window.<GenericClass>into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(policy)
                .withTempDirectory(
                    StaticValueProvider.of(FileSystems.matchNewResource(baseDir.toString(), true)))
                .withWindowedWrites()
                .withNumShards(2));
    windowedAvroWritePipeline.run();

    // Validate that the data written matches the expected elements in the expected order
    List<File> expectedFiles = new ArrayList<>();
    for (int shard = 0; shard < 2; shard++) {
      for (int window = 0; window < 2; window++) {
        Instant windowStart = new Instant(0).plus(Duration.standardMinutes(window));
        IntervalWindow intervalWindow =
            new IntervalWindow(windowStart, Duration.standardMinutes(1));
        expectedFiles.add(
            new File(
                baseFilename
                    + "-"
                    + intervalWindow.toString()
                    + "-"
                    + shard
                    + "-of-1"
                    + "-pane-0-final"));
      }
    }

    List<GenericClass> actualElements = new ArrayList<>();
    for (File outputFile : expectedFiles) {
      assertTrue("Expected output file " + outputFile.getAbsolutePath(), outputFile.exists());
      try (DataFileReader<GenericClass> reader =
          new DataFileReader<>(
              outputFile,
              new ReflectDatumReader<GenericClass>(
                  ReflectData.get().getSchema(GenericClass.class)))) {
        Iterators.addAll(actualElements, reader);
      }
      outputFile.delete();
    }
    assertThat(actualElements, containsInAnyOrder(allElements.toArray()));
  }

  @Test
  public void testWriteWithDefaultCodec() throws Exception {
    AvroIO.Write<String> write = AvroIO.write(String.class).to("/tmp/foo/baz");
    assertEquals(CodecFactory.deflateCodec(6).toString(), write.getCodec().toString());
  }

  @Test
  public void testWriteWithCustomCodec() throws Exception {
    AvroIO.Write<String> write =
        AvroIO.write(String.class).to("/tmp/foo/baz").withCodec(CodecFactory.snappyCodec());
    assertEquals(SNAPPY_CODEC, write.getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteWithSerDeCustomDeflateCodec() throws Exception {
    AvroIO.Write<String> write =
        AvroIO.write(String.class).to("/tmp/foo/baz").withCodec(CodecFactory.deflateCodec(9));

    assertEquals(
        CodecFactory.deflateCodec(9).toString(),
        SerializableUtils.clone(write.getCodec()).getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteWithSerDeCustomXZCodec() throws Exception {
    AvroIO.Write<String> write =
        AvroIO.write(String.class).to("/tmp/foo/baz").withCodec(CodecFactory.xzCodec(9));

    assertEquals(
        CodecFactory.xzCodec(9).toString(),
        SerializableUtils.clone(write.getCodec()).getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testMetadata() throws Exception {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(outputFile.getAbsolutePath())
                .withoutSharding()
                .withMetadata(
                    ImmutableMap.<String, Object>of(
                        "stringKey",
                        "stringValue",
                        "longKey",
                        100L,
                        "bytesKey",
                        "bytesValue".getBytes())));
    writePipeline.run();

    DataFileStream dataFileStream =
        new DataFileStream(new FileInputStream(outputFile), new GenericDatumReader());
    assertEquals("stringValue", dataFileStream.getMetaString("stringKey"));
    assertEquals(100L, dataFileStream.getMetaLong("longKey"));
    assertArrayEquals("bytesValue".getBytes(), dataFileStream.getMeta("bytesKey"));
  }

  @SuppressWarnings("deprecation") // using AvroCoder#createDatumReader for tests.
  private void runTestWrite(String[] expectedElements, int numShards) throws IOException {
    File baseOutputFile = new File(tmpFolder.getRoot(), "prefix");
    String outputFilePrefix = baseOutputFile.getAbsolutePath();

    AvroIO.Write<String> write = AvroIO.write(String.class).to(outputFilePrefix);
    if (numShards > 1) {
      System.out.println("NumShards " + numShards);
      write = write.withNumShards(numShards);
    } else {
      System.out.println("no sharding");
      write = write.withoutSharding();
    }
    writePipeline.apply(Create.of(ImmutableList.copyOf(expectedElements))).apply(write);
    writePipeline.run();

    String shardNameTemplate =
        firstNonNull(
            write.getShardTemplate(), DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);

    assertTestOutputs(expectedElements, numShards, outputFilePrefix, shardNameTemplate);
  }

  public static void assertTestOutputs(
      String[] expectedElements, int numShards, String outputFilePrefix, String shardNameTemplate)
      throws IOException {
    // Validate that the data written matches the expected elements in the expected order
    List<File> expectedFiles = new ArrayList<>();
    for (int i = 0; i < numShards; i++) {
      expectedFiles.add(
          new File(
              DefaultFilenamePolicy.constructName(
                      FileBasedSink.convertToFileResourceIfPossible(outputFilePrefix),
                      shardNameTemplate,
                      "" /* no suffix */,
                      i,
                      numShards,
                      null,
                      null)
                  .toString()));
    }

    List<String> actualElements = new ArrayList<>();
    for (File outputFile : expectedFiles) {
      assertTrue("Expected output file " + outputFile.getName(), outputFile.exists());
      try (DataFileReader<String> reader =
          new DataFileReader<>(
              outputFile, new ReflectDatumReader(ReflectData.get().getSchema(String.class)))) {
        Iterators.addAll(actualElements, reader);
      }
    }
    assertThat(actualElements, containsInAnyOrder(expectedElements));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAvroSinkWrite() throws Exception {
    String[] expectedElements = new String[] {"first", "second", "third"};

    runTestWrite(expectedElements, 1);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAvroSinkShardedWrite() throws Exception {
    String[] expectedElements = new String[] {"first", "second", "third", "fourth", "fifth"};

    runTestWrite(expectedElements, 4);
  }
  // TODO: for Write only, test withSuffix,
  // withShardNameTemplate and withoutSharding.

  @Test
  public void testReadDisplayData() {
    AvroIO.Read<String> read = AvroIO.read(String.class).from("/foo.*");

    DisplayData displayData = DisplayData.from(read);
    assertThat(displayData, hasDisplayItem("filePattern", "/foo.*"));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    AvroIO.Read<GenericRecord> read =
        AvroIO.readGenericRecords(Schema.create(Schema.Type.STRING)).from("/foo.*");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat(
        "AvroIO.Read should include the file pattern in its primitive transform",
        displayData,
        hasItem(hasDisplayItem("filePattern")));
  }

  @Test
  public void testWriteDisplayData() {
    AvroIO.Write<GenericClass> write =
        AvroIO.write(GenericClass.class)
            .to("/foo")
            .withShardNameTemplate("-SS-of-NN-")
            .withSuffix("bar")
            .withNumShards(100)
            .withCodec(CodecFactory.snappyCodec());

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "/foo"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(displayData, hasDisplayItem("schema", GenericClass.class));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("codec", CodecFactory.snappyCodec().toString()));
  }
}
