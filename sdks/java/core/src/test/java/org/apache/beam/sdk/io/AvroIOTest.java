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
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.sdk.transforms.Requirements.requiresSideInputs;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
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
public class AvroIOTest implements Serializable {

  @Rule
  public transient TestPipeline writePipeline = TestPipeline.create();

  @Rule
  public transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public transient TestPipeline windowedAvroWritePipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient ExpectedException expectedException = ExpectedException.none();

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

    public GenericClass(int intField, String stringField) {
      this.intField = intField;
      this.stringField = stringField;
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

  private static class ParseGenericClass
      implements SerializableFunction<GenericRecord, GenericClass> {
    @Override
    public GenericClass apply(GenericRecord input) {
      return new GenericClass(
          (int) input.get("intField"), input.get("stringField").toString());
    }
  }

  private enum Sharding {
    RUNNER_DETERMINED,
    WITHOUT_SHARDING,
    FIXED_3_SHARDS
  }

  private enum WriteMethod {
    AVROIO_WRITE,
    AVROIO_SINK
  }

  private static final String SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenReadJavaClass() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline
        .apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(writePipeline.newProvider(outputFile.getAbsolutePath()))
                .withoutSharding());
    writePipeline.run();

    PAssert.that(
            readPipeline.apply(
                "Read",
                AvroIO.read(GenericClass.class)
                    .from(readPipeline.newProvider(outputFile.getAbsolutePath()))))
        .containsInAnyOrder(values);

    readPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenReadCustomType() throws Throwable {
    List<Long> values = Arrays.asList(0L, 1L, 2L);
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline
        .apply(Create.of(values))
        .apply(
            AvroIO.<Long, GenericClass>writeCustomType()
                .to(writePipeline.newProvider(outputFile.getAbsolutePath()))
                .withFormatFunction(new CreateGenericClass())
                .withSchema(ReflectData.get().getSchema(GenericClass.class))
                .withoutSharding());
    writePipeline.run();

    PAssert.that(
            readPipeline
                .apply(
                    "Read",
                    AvroIO.read(GenericClass.class)
                        .from(readPipeline.newProvider(outputFile.getAbsolutePath())))
                .apply(
                    MapElements.via(
                        new SimpleFunction<GenericClass, Long>() {
                          @Override
                          public Long apply(GenericClass input) {
                            return (long) input.intField;
                          }
                        })))
        .containsInAnyOrder(values);

    readPipeline.run();
  }

  private <T extends GenericRecord> void testWriteThenReadGeneratedClass(
      AvroIO.Write<T> writeTransform,
      AvroIO.Read<T> readTransform
  ) throws Exception {
    File outputFile = tmpFolder.newFile("output.avro");

    List<T> values =
        ImmutableList.of(
            (T) new AvroGeneratedUser("Bob", 256, null),
            (T) new AvroGeneratedUser("Alice", 128, null),
            (T) new AvroGeneratedUser("Ted", null, "white"));

    writePipeline
        .apply(Create.of(values))
        .apply(
            writeTransform
                .to(writePipeline.newProvider(outputFile.getAbsolutePath()))
                .withoutSharding());
    writePipeline.run();

    PAssert.that(
        readPipeline.apply(
            "Read",
            readTransform
                .from(readPipeline.newProvider(outputFile.getAbsolutePath()))))
        .containsInAnyOrder(values);

    readPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenReadGeneratedClassWithClass() throws Throwable {
    testWriteThenReadGeneratedClass(
        AvroIO.write(AvroGeneratedUser.class), AvroIO.read(AvroGeneratedUser.class));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenReadGeneratedClassWithSchema() throws Throwable {
    testWriteThenReadGeneratedClass(
        AvroIO.writeGenericRecords(SCHEMA), AvroIO.readGenericRecords(SCHEMA));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenReadGeneratedClassWithSchemaString() throws Throwable {
    testWriteThenReadGeneratedClass(
        AvroIO.writeGenericRecords(SCHEMA.toString()),
        AvroIO.readGenericRecords(SCHEMA.toString()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteSingleFileThenReadUsingAllMethods() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(AvroIO.write(GenericClass.class).to(outputFile.getAbsolutePath()).withoutSharding());
    writePipeline.run();

    // Test the same data using all versions of read().
    PCollection<String> path =
        readPipeline.apply("Create path", Create.of(outputFile.getAbsolutePath()));
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
        path.apply(
            "ReadAll", AvroIO.readAll(GenericClass.class).withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(values);
    PAssert.that(
        readPipeline.apply(
            "Parse",
            AvroIO.parseGenericRecords(new ParseGenericClass())
                .from(outputFile.getAbsolutePath())
                .withCoder(AvroCoder.of(GenericClass.class))))
        .containsInAnyOrder(values);
    PAssert.that(
        readPipeline.apply(
            "Parse withHintMatchesManyFiles",
            AvroIO.parseGenericRecords(new ParseGenericClass())
                .from(outputFile.getAbsolutePath())
                .withCoder(AvroCoder.of(GenericClass.class))
                .withHintMatchesManyFiles()))
        .containsInAnyOrder(values);
    PAssert.that(
        path.apply(
            "ParseAll",
            AvroIO.parseAllGenericRecords(new ParseGenericClass())
                .withCoder(AvroCoder.of(GenericClass.class))
                .withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(values);

    readPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenReadMultipleFilepatterns() throws Throwable {
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
    writePipeline.run();

    // Test readAll() and parseAllGenericRecords().
    PCollection<String> paths =
        readPipeline.apply(
            "Create paths",
            Create.of(
                tmpFolder.getRoot().getAbsolutePath() + "/first*",
                tmpFolder.getRoot().getAbsolutePath() + "/second*"));
    PAssert.that(
            paths.apply(
                "Read all", AvroIO.readAll(GenericClass.class).withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(Iterables.concat(firstValues, secondValues));
    PAssert.that(
            paths.apply(
                "Parse all",
                AvroIO.parseAllGenericRecords(new ParseGenericClass())
                    .withCoder(AvroCoder.of(GenericClass.class))
                    .withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(Iterables.concat(firstValues, secondValues));

    readPipeline.run();
  }

  private static class CreateGenericClass extends SimpleFunction<Long, GenericClass> {
    @Override
    public GenericClass apply(Long i) {
      return new GenericClass(i.intValue(), "value" + i);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testContinuouslyWriteAndReadMultipleFilepatterns() throws Throwable {
    SimpleFunction<Long, GenericClass> mapFn = new CreateGenericClass();
    List<GenericClass> firstValues = Lists.newArrayList();
    List<GenericClass> secondValues = Lists.newArrayList();
    for (int i = 0; i < 7; ++i) {
      (i < 3 ? firstValues : secondValues).add(mapFn.apply((long) i));
    }
    // Configure windowing of the input so that it fires every time a new element is generated,
    // so that files are written continuously.
    Window<Long> window = Window.<Long>into(FixedWindows.of(Duration.millis(100)))
        .withAllowedLateness(Duration.ZERO)
        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
        .discardingFiredPanes();
    readPipeline.apply(
            "Sequence first",
            GenerateSequence.from(0).to(3).withRate(1, Duration.millis(300)))
        .apply("Window first", window)
        .apply("Map first", MapElements.via(mapFn))
        .apply(
            "Write first",
            AvroIO.write(GenericClass.class)
                .to(tmpFolder.getRoot().getAbsolutePath() + "/first")
                .withNumShards(2).withWindowedWrites());
    readPipeline.apply(
            "Sequence second",
            GenerateSequence.from(3).to(7).withRate(1, Duration.millis(300)))
        .apply("Window second", window)
        .apply("Map second", MapElements.via(mapFn))
        .apply(
            "Write second",
            AvroIO.write(GenericClass.class)
                .to(tmpFolder.getRoot().getAbsolutePath() + "/second")
                .withNumShards(3).withWindowedWrites());

    // Test read(), readAll(), parse(), and parseAllGenericRecords() with watchForNewFiles().
    PAssert.that(
            readPipeline.apply(
                "Read",
                AvroIO.read(GenericClass.class)
                    .from(tmpFolder.getRoot().getAbsolutePath() + "/first*")
                    .watchForNewFiles(
                        Duration.millis(100),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3)))))
        .containsInAnyOrder(firstValues);
    PAssert.that(
            readPipeline.apply(
                "Parse",
                AvroIO.parseGenericRecords(new ParseGenericClass())
                    .from(tmpFolder.getRoot().getAbsolutePath() + "/first*")
                    .watchForNewFiles(
                        Duration.millis(100),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3)))))
        .containsInAnyOrder(firstValues);

    PCollection<String> paths =
        readPipeline.apply(
            "Create paths",
            Create.of(
                tmpFolder.getRoot().getAbsolutePath() + "/first*",
                tmpFolder.getRoot().getAbsolutePath() + "/second*"));
    PAssert.that(
            paths.apply(
                "Read all",
                AvroIO.readAll(GenericClass.class)
                    .watchForNewFiles(
                        Duration.millis(100),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3)))
                    .withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(Iterables.concat(firstValues, secondValues));
    PAssert.that(
            paths.apply(
                "Parse all",
                AvroIO.parseAllGenericRecords(new ParseGenericClass())
                    .withCoder(AvroCoder.of(GenericClass.class))
                    .watchForNewFiles(
                        Duration.millis(100),
                        Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3)))
                    .withDesiredBundleSizeBytes(10)))
        .containsInAnyOrder(Iterables.concat(firstValues, secondValues));
    readPipeline.run();
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testCompressedWriteAndReadASingleFile() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(outputFile.getAbsolutePath())
                .withoutSharding()
                .withCodec(CodecFactory.deflateCodec(9)));
    writePipeline.run();

    PAssert.that(
            readPipeline.apply(AvroIO.read(GenericClass.class).from(outputFile.getAbsolutePath())))
        .containsInAnyOrder(values);
    readPipeline.run();

    try (DataFileStream dataFileStream =
        new DataFileStream(new FileInputStream(outputFile), new GenericDatumReader())) {
      assertEquals("deflate", dataFileStream.getMetaString("avro.codec"));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testWriteThenReadASingleFileWithNullCodec() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(outputFile.getAbsolutePath())
                .withoutSharding()
                .withCodec(CodecFactory.nullCodec()));
    writePipeline.run();

    PAssert.that(
            readPipeline.apply(AvroIO.read(GenericClass.class).from(outputFile.getAbsolutePath())))
        .containsInAnyOrder(values);
    readPipeline.run();

    try (DataFileStream dataFileStream =
        new DataFileStream(new FileInputStream(outputFile), new GenericDatumReader())) {
      assertEquals("null", dataFileStream.getMetaString("avro.codec"));
    }
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
  public void testWriteThenReadSchemaUpgrade() throws Throwable {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline.apply(Create.of(values))
        .apply(AvroIO.write(GenericClass.class).to(outputFile.getAbsolutePath()).withoutSharding());
    writePipeline.run();

    List<GenericClassV2> expected =
        ImmutableList.of(new GenericClassV2(3, "hi", null), new GenericClassV2(5, "bar", null));

    PAssert.that(
            readPipeline.apply(
                AvroIO.read(GenericClassV2.class).from(outputFile.getAbsolutePath())))
        .containsInAnyOrder(expected);
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

      IntervalWindow interval = (IntervalWindow) window;
      String windowStr =
          String.format("%s-%s", interval.start().toString(), interval.end().toString());
      String filename =
          String.format(
              "%s-%s-%s-of-%s-pane-%s%s%s.avro",
              filenamePrefix,
              windowStr,
              shardNumber,
              numShards,
              paneInfo.getIndex(),
              paneInfo.isLast() ? "-last" : "",
              outputFileHints.getSuggestedFilenameSuffix());
      return outputFilePrefix
          .getCurrentDirectory()
          .resolve(filename, RESOLVE_FILE);
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

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testWriteWindowed() throws Throwable {
    testWindowedAvroIOWriteUsingMethod(WriteMethod.AVROIO_WRITE);
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void testWindowedAvroIOWriteViaSink() throws Throwable {
    testWindowedAvroIOWriteUsingMethod(WriteMethod.AVROIO_SINK);
  }

  void testWindowedAvroIOWriteUsingMethod(WriteMethod method) throws IOException {
    Path baseDir = Files.createTempDirectory(tmpFolder.getRoot().toPath(), "testwrite");
    final String baseFilename = baseDir.resolve("prefix").toString();

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

    final PTransform<PCollection<GenericClass>, WriteFilesResult<Void>> write;
    switch (method) {
      case AVROIO_WRITE:
        {
          FilenamePolicy policy =
              new WindowedFilenamePolicy(
                  FileBasedSink.convertToFileResourceIfPossible(baseFilename));
          write =
              AvroIO.write(GenericClass.class)
                  .to(policy)
                  .withTempDirectory(
                      StaticValueProvider.of(
                          FileSystems.matchNewResource(baseDir.toString(), true)))
                  .withWindowedWrites()
                  .withNumShards(2)
                  .withOutputFilenames();
          break;
        }

      case AVROIO_SINK:
        {
          write =
              FileIO.<GenericClass>write()
                  .via(AvroIO.sink(GenericClass.class))
                  .to(baseDir.toString())
                  .withPrefix("prefix")
                  .withSuffix(".avro")
                  .withTempDirectory(baseDir.toString())
                  .withNumShards(2);
          break;
        }

      default:
        throw new UnsupportedOperationException();
    }
    windowedAvroWritePipeline
        .apply(values)
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(write);
    windowedAvroWritePipeline.run();

    // Validate that the data written matches the expected elements in the expected order
    List<File> expectedFiles = new ArrayList<>();
    for (int shard = 0; shard < 2; shard++) {
      for (int window = 0; window < 2; window++) {
        Instant windowStart = new Instant(0).plus(Duration.standardMinutes(window));
        IntervalWindow iw =
            new IntervalWindow(windowStart, Duration.standardMinutes(1));
        String baseAndWindow = baseFilename + "-" + iw.start() + "-" + iw.end();
        switch(method) {
          case AVROIO_WRITE:
            expectedFiles.add(new File(baseAndWindow + "-" + shard + "-of-2-pane-0-last.avro"));
            break;
          case AVROIO_SINK:
            expectedFiles.add(new File(baseAndWindow + "-0000" + shard + "-of-00002.avro"));
            break;
        }
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

  private static final String SCHEMA_TEMPLATE_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"TestTemplateSchema$$\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"$$full\", \"type\": \"string\"},\n"
          + "     {\"name\": \"$$suffix\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static String schemaFromPrefix(String prefix) {
    return SCHEMA_TEMPLATE_STRING.replace("$$", prefix);
  }

  private static GenericRecord createRecord(String record, String prefix, Schema schema) {
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put(prefix + "full", record);
    genericRecord.put(prefix + "suffix", record.substring(1));
    return genericRecord;
  }

  private static class TestDynamicDestinations
      extends DynamicAvroDestinations<String, String, GenericRecord> {
    ResourceId baseDir;
    PCollectionView<Map<String, String>> schemaView;

    TestDynamicDestinations(ResourceId baseDir, PCollectionView<Map<String, String>> schemaView) {
      this.baseDir = baseDir;
      this.schemaView = schemaView;
    }

    @Override
    public Schema getSchema(String destination) {
      // Return a per-destination schema.
      String schema = sideInput(schemaView).get(destination);
      return new Schema.Parser().parse(schema);
    }

    @Override
    public List<PCollectionView<?>> getSideInputs() {
      return ImmutableList.of(schemaView);
    }

    @Override
    public GenericRecord formatRecord(String record) {
      String prefix = record.substring(0, 1);
      return createRecord(record, prefix, getSchema(prefix));
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
    public FilenamePolicy getFilenamePolicy(String destination) {
      return DefaultFilenamePolicy.fromStandardParameters(
          StaticValueProvider.of(
              baseDir.resolve("file_" + destination, RESOLVE_FILE)),
          "-SSSSS-of-NNNNN",
          ".avro",
          false);
    }
  }

  private void testDynamicDestinationsUnwindowedWithSharding(
      WriteMethod writeMethod, Sharding sharding) throws Exception {
    final ResourceId baseDir =
        FileSystems.matchNewResource(
            Files.createTempDirectory(tmpFolder.getRoot().toPath(), "testDynamicDestinations")
                .toString(),
            true);

    List<String> elements = Lists.newArrayList("aaaa", "aaab", "baaa", "baab", "caaa", "caab");
    Multimap<String, GenericRecord> expectedElements = ArrayListMultimap.create();
    Map<String, String> schemaMap = Maps.newHashMap();
    for (String element : elements) {
      String prefix = element.substring(0, 1);
      String jsonSchema = schemaFromPrefix(prefix);
      schemaMap.put(prefix, jsonSchema);
      expectedElements.put(
          prefix, createRecord(element, prefix, new Schema.Parser().parse(jsonSchema)));
    }
    final PCollectionView<Map<String, String>> schemaView =
        writePipeline.apply("createSchemaView", Create.of(schemaMap)).apply(View.asMap());

    PCollection<String> input =
        writePipeline.apply("createInput", Create.of(elements).withCoder(StringUtf8Coder.of()));

    switch (writeMethod) {
      case AVROIO_WRITE: {
        AvroIO.TypedWrite<String, String, GenericRecord> write =
            AvroIO.<String>writeCustomTypeToGenericRecords()
                .to(new TestDynamicDestinations(baseDir, schemaView))
                .withTempDirectory(baseDir);

        switch (sharding) {
          case RUNNER_DETERMINED:
            break;
          case WITHOUT_SHARDING:
            write = write.withoutSharding();
            break;
          case FIXED_3_SHARDS:
            write = write.withNumShards(3);
            break;
          default:
            throw new IllegalArgumentException("Unknown sharding " + sharding);
        }

        input.apply(write);
        break;
      }

      case AVROIO_SINK:
        {
          final AvroIO.RecordFormatter<String> formatter =
              (element, schema) -> {
                String prefix = element.substring(0, 1);
                GenericRecord record = new GenericData.Record(schema);
                record.put(prefix + "full", element);
                record.put(prefix + "suffix", element.substring(1));
                return record;
              };
          FileIO.Write<String, String> write =
              FileIO.<String, String>writeDynamic()
                  .by(fn((element, c) -> {
                      c.sideInput(schemaView); // Ignore result
                      return element.substring(0, 1);
                    },
                    requiresSideInputs(schemaView)))
                  .via(fn((dest, c) -> {
                      Schema schema =
                          new Schema.Parser().parse(c.sideInput(schemaView).get(dest));
                      return AvroIO.sinkViaGenericRecords(schema, formatter);
                    },
                    requiresSideInputs(schemaView)))
                  .to(baseDir.toString())
                  .withNaming(fn((dest, c) -> {
                      c.sideInput(schemaView); // Ignore result
                      return FileIO.Write.defaultNaming("file_" + dest, ".avro");
                    },
                    requiresSideInputs(schemaView)))
                  .withTempDirectory(baseDir.toString())
                  .withDestinationCoder(StringUtf8Coder.of())
                  .withIgnoreWindowing();
          switch (sharding) {
            case RUNNER_DETERMINED:
              break;
            case WITHOUT_SHARDING:
              write = write.withNumShards(1);
              break;
            case FIXED_3_SHARDS:
              write = write.withNumShards(3);
              break;
            default:
              throw new IllegalArgumentException("Unknown sharding " + sharding);
          }

          input.apply(write);
          break;
        }
    }

    writePipeline.run();

    // Validate that the data written matches the expected elements in the expected order.

    for (String prefix : expectedElements.keySet()) {
      String shardPattern;
      switch (sharding) {
        case RUNNER_DETERMINED:
          shardPattern = "-*";
          break;
        case WITHOUT_SHARDING:
          shardPattern = "-00000-of-00001";
          break;
        case FIXED_3_SHARDS:
          shardPattern = "-*-of-00003";
          break;
        default:
          throw new IllegalArgumentException("Unknown sharding " + sharding);
      }
      String expectedFilepattern =
          baseDir
              .resolve("file_" + prefix + shardPattern + ".avro", RESOLVE_FILE)
              .toString();

      PCollection<GenericRecord> records =
          readPipeline.apply(
              "read_" + prefix,
              AvroIO.readGenericRecords(schemaFromPrefix(prefix)).from(expectedFilepattern));
      PAssert.that(records).containsInAnyOrder(expectedElements.get(prefix));
    }
    readPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsRunnerDeterminedSharding() throws Exception {
    testDynamicDestinationsUnwindowedWithSharding(
        WriteMethod.AVROIO_WRITE, Sharding.RUNNER_DETERMINED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsWithoutSharding() throws Exception {
    testDynamicDestinationsUnwindowedWithSharding(
        WriteMethod.AVROIO_WRITE, Sharding.WITHOUT_SHARDING);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsWithNumShards() throws Exception {
    testDynamicDestinationsUnwindowedWithSharding(
        WriteMethod.AVROIO_WRITE, Sharding.FIXED_3_SHARDS);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsViaSinkRunnerDeterminedSharding() throws Exception {
    testDynamicDestinationsUnwindowedWithSharding(
        WriteMethod.AVROIO_SINK, Sharding.RUNNER_DETERMINED);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsViaSinkWithoutSharding() throws Exception {
    testDynamicDestinationsUnwindowedWithSharding(
        WriteMethod.AVROIO_SINK, Sharding.WITHOUT_SHARDING);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDynamicDestinationsViaSinkWithNumShards() throws Exception {
    testDynamicDestinationsUnwindowedWithSharding(WriteMethod.AVROIO_SINK, Sharding.FIXED_3_SHARDS);
  }

  @Test
  public void testWriteWithDefaultCodec() throws Exception {
    AvroIO.Write<String> write = AvroIO.write(String.class).to("/tmp/foo/baz");
    assertEquals(CodecFactory.snappyCodec().toString(), write.inner.getCodec().toString());
  }

  @Test
  public void testWriteWithCustomCodec() throws Exception {
    AvroIO.Write<String> write =
        AvroIO.write(String.class).to("/tmp/foo/baz").withCodec(CodecFactory.snappyCodec());
    assertEquals(SNAPPY_CODEC, write.inner.getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteWithSerDeCustomDeflateCodec() throws Exception {
    AvroIO.Write<String> write =
        AvroIO.write(String.class).to("/tmp/foo/baz").withCodec(CodecFactory.deflateCodec(9));

    assertEquals(
        CodecFactory.deflateCodec(9).toString(),
        SerializableUtils.clone(write.inner.getCodec()).getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteWithSerDeCustomXZCodec() throws Exception {
    AvroIO.Write<String> write =
        AvroIO.write(String.class).to("/tmp/foo/baz").withCodec(CodecFactory.xzCodec(9));

    assertEquals(
        CodecFactory.xzCodec(9).toString(),
        SerializableUtils.clone(write.inner.getCodec()).getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testMetadata() throws Exception {
    List<GenericClass> values =
        ImmutableList.of(new GenericClass(3, "hi"), new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    writePipeline
        .apply(Create.of(values))
        .apply(
            AvroIO.write(GenericClass.class)
                .to(outputFile.getAbsolutePath())
                .withoutSharding()
                .withMetadata(
                    ImmutableMap.of(
                        "stringKey",
                        "stringValue",
                        "longKey",
                        100L,
                        "bytesKey",
                        "bytesValue".getBytes())));
    writePipeline.run();

    try (DataFileStream dataFileStream =
        new DataFileStream(new FileInputStream(outputFile), new GenericDatumReader())) {
      assertEquals("stringValue", dataFileStream.getMetaString("stringKey"));
      assertEquals(100L, dataFileStream.getMetaLong("longKey"));
      assertArrayEquals("bytesValue".getBytes(), dataFileStream.getMeta("bytesKey"));
    }
  }

  @SuppressWarnings("deprecation") // using AvroCoder#createDatumReader for tests.
  private void runTestWrite(String[] expectedElements, int numShards) throws IOException {
    File baseOutputFile = new File(tmpFolder.getRoot(), "prefix");
    String outputFilePrefix = baseOutputFile.getAbsolutePath();

    AvroIO.Write<String> write =
        AvroIO.write(String.class).to(outputFilePrefix).withSuffix(".avro");
    if (numShards > 1) {
      write = write.withNumShards(numShards);
    } else {
      write = write.withoutSharding();
    }
    writePipeline.apply(Create.of(ImmutableList.copyOf(expectedElements))).apply(write);
    writePipeline.run();

    String shardNameTemplate =
        firstNonNull(
            write.inner.getShardTemplate(),
            DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);

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
                      ".avro",
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
            .withCodec(CodecFactory.deflateCodec(6));

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "/foo"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(
        displayData,
        hasDisplayItem(
            "schema",
            "{\"type\":\"record\",\"name\":\"GenericClass\",\"namespace\":\"org.apache.beam.sdk.io"
                + ".AvroIOTest$\",\"fields\":[{\"name\":\"intField\",\"type\":\"int\"},"
                + "{\"name\":\"stringField\",\"type\":\"string\"}]}"));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("codec", CodecFactory.deflateCodec(6).toString()));
  }
}
