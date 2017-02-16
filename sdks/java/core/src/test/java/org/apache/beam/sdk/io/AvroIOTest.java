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

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO.Write.Bound;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for AvroIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class AvroIOTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupClass() {
    IOChannelUtils.registerIOFactoriesAllowOverride(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testReadWithoutValidationFlag() throws Exception {
    AvroIO.Read.Bound<GenericRecord> read = AvroIO.Read.from("gs://bucket/foo*/baz");
    assertTrue(read.needsValidation());
    assertFalse(read.withoutValidation().needsValidation());
  }

  @Test
  public void testWriteWithoutValidationFlag() throws Exception {
    AvroIO.Write.Bound<GenericRecord> write = AvroIO.Write.to("gs://bucket/foo/baz");
    assertTrue(write.needsValidation());
    assertFalse(write.withoutValidation().needsValidation());
  }

  @Test
  public void testAvroIOGetName() {
    assertEquals("AvroIO.Read", AvroIO.Read.from("gs://bucket/foo*/baz").getName());
    assertEquals("AvroIO.Write", AvroIO.Write.to("gs://bucket/foo/baz").getName());
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
    List<GenericClass> values = ImmutableList.of(new GenericClass(3, "hi"),
        new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    p.apply(Create.of(values))
      .apply(AvroIO.Write.to(outputFile.getAbsolutePath())
          .withoutSharding()
          .withSchema(GenericClass.class));
    p.run();

    PCollection<GenericClass> input = p
        .apply(AvroIO.Read.from(outputFile.getAbsolutePath()).withSchema(GenericClass.class));

    PAssert.that(input).containsInAnyOrder(values);
    p.run();
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testAvroIOCompressedWriteAndReadASingleFile() throws Throwable {
    List<GenericClass> values = ImmutableList.of(new GenericClass(3, "hi"),
        new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    p.apply(Create.of(values))
        .apply(AvroIO.Write.to(outputFile.getAbsolutePath())
            .withoutSharding()
            .withCodec(CodecFactory.deflateCodec(9))
            .withSchema(GenericClass.class));
    p.run();

    PCollection<GenericClass> input = p
        .apply(AvroIO.Read
            .from(outputFile.getAbsolutePath())
            .withSchema(GenericClass.class));

    PAssert.that(input).containsInAnyOrder(values);
    p.run();
    DataFileStream dataFileStream = new DataFileStream(new FileInputStream(outputFile),
        new GenericDatumReader());
    assertEquals("deflate", dataFileStream.getMetaString("avro.codec"));
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testAvroIONullCodecWriteAndReadASingleFile() throws Throwable {
    List<GenericClass> values = ImmutableList.of(new GenericClass(3, "hi"),
        new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    p.apply(Create.of(values))
        .apply(AvroIO.Write.to(outputFile.getAbsolutePath())
            .withoutSharding()
            .withSchema(GenericClass.class)
            .withCodec(CodecFactory.nullCodec()));
    p.run();

    PCollection<GenericClass> input = p
        .apply(AvroIO.Read
            .from(outputFile.getAbsolutePath())
            .withSchema(GenericClass.class));

    PAssert.that(input).containsInAnyOrder(values);
    p.run();
    DataFileStream dataFileStream = new DataFileStream(new FileInputStream(outputFile),
        new GenericDatumReader());
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
   * Tests that {@code AvroIO} can read an upgraded version of an old class, as long as the
   * schema resolution process succeeds. This test covers the case when a new, {@code @Nullable}
   * field has been added.
   *
   * <p>For more information, see http://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution
   */
  @Test
  @Category(NeedsRunner.class)
  public void testAvroIOWriteAndReadSchemaUpgrade() throws Throwable {
    List<GenericClass> values = ImmutableList.of(new GenericClass(3, "hi"),
        new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    p.apply(Create.of(values))
      .apply(AvroIO.Write.to(outputFile.getAbsolutePath())
          .withoutSharding()
          .withSchema(GenericClass.class));
    p.run();

    List<GenericClassV2> expected = ImmutableList.of(new GenericClassV2(3, "hi", null),
        new GenericClassV2(5, "bar", null));

    PCollection<GenericClassV2> input = p
        .apply(AvroIO.Read.from(outputFile.getAbsolutePath()).withSchema(GenericClassV2.class));

    PAssert.that(input).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testWriteWithDefaultCodec() throws Exception {
    AvroIO.Write.Bound<GenericRecord> write = AvroIO.Write
        .to("gs://bucket/foo/baz");
    assertEquals(CodecFactory.deflateCodec(6).toString(), write.getCodec().toString());
  }

  @Test
  public void testWriteWithCustomCodec() throws Exception {
    AvroIO.Write.Bound<GenericRecord> write = AvroIO.Write
        .to("gs://bucket/foo/baz")
        .withCodec(CodecFactory.snappyCodec());
    assertEquals(SNAPPY_CODEC, write.getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteWithSerDeCustomDeflateCodec() throws Exception {
    AvroIO.Write.Bound<GenericRecord> write = AvroIO.Write
        .to("gs://bucket/foo/baz")
        .withCodec(CodecFactory.deflateCodec(9));

    AvroIO.Write.Bound<GenericRecord> serdeWrite = SerializableUtils.clone(write);

    assertEquals(CodecFactory.deflateCodec(9).toString(), serdeWrite.getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteWithSerDeCustomXZCodec() throws Exception {
    AvroIO.Write.Bound<GenericRecord> write = AvroIO.Write
        .to("gs://bucket/foo/baz")
        .withCodec(CodecFactory.xzCodec(9));

    AvroIO.Write.Bound<GenericRecord> serdeWrite = SerializableUtils.clone(write);

    assertEquals(CodecFactory.xzCodec(9).toString(), serdeWrite.getCodec().toString());
  }

  @Test
  @SuppressWarnings("unchecked")
  @Category(NeedsRunner.class)
  public void testMetdata() throws Exception {
    List<GenericClass> values = ImmutableList.of(new GenericClass(3, "hi"),
        new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    p.apply(Create.of(values))
        .apply(AvroIO.Write.to(outputFile.getAbsolutePath())
            .withoutSharding()
            .withSchema(GenericClass.class)
            .withMetadata(ImmutableMap.<String, Object>of(
                "stringKey", "stringValue",
                "longKey", 100L,
                "bytesKey", "bytesValue".getBytes())));
    p.run();

    DataFileStream dataFileStream = new DataFileStream(new FileInputStream(outputFile),
        new GenericDatumReader());
    assertEquals("stringValue", dataFileStream.getMetaString("stringKey"));
    assertEquals(100L, dataFileStream.getMetaLong("longKey"));
    assertArrayEquals("bytesValue".getBytes(), dataFileStream.getMeta("bytesKey"));
  }


  @SuppressWarnings("deprecation") // using AvroCoder#createDatumReader for tests.
  private void runTestWrite(String[] expectedElements, int numShards) throws IOException {
    File baseOutputFile = new File(tmpFolder.getRoot(), "prefix");
    String outputFilePrefix = baseOutputFile.getAbsolutePath();

    Bound<String> write = AvroIO.Write.to(outputFilePrefix).withSchema(String.class);
    if (numShards > 1) {
      write = write.withNumShards(numShards);
    } else {
      write = write.withoutSharding();
    }
    p.apply(Create.of(ImmutableList.copyOf(expectedElements))).apply(write);
    p.run();

    String shardNameTemplate = write.getShardNameTemplate();

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
              IOChannelUtils.constructName(
                  outputFilePrefix, shardNameTemplate, "" /* no suffix */, i, numShards)));
    }

    List<String> actualElements = new ArrayList<>();
    for (File outputFile : expectedFiles) {
      assertTrue("Expected output file " + outputFile.getName(), outputFile.exists());
      try (DataFileReader<String> reader =
          new DataFileReader<>(outputFile, AvroCoder.of(String.class).createDatumReader())) {
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
    AvroIO.Read.Bound<?> read = AvroIO.Read.from("foo.*")
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);
    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    AvroIO.Read.Bound<?> read = AvroIO.Read.from("foo.*")
        .withSchema(Schema.create(Schema.Type.STRING))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("AvroIO.Read should include the file pattern in its primitive transform",
        displayData, hasItem(hasDisplayItem("filePattern")));
  }

  @Test
  public void testWriteDisplayData() {
    AvroIO.Write.Bound<?> write = AvroIO.Write
        .to("foo")
        .withShardNameTemplate("-SS-of-NN-")
        .withSuffix("bar")
        .withSchema(GenericClass.class)
        .withNumShards(100)
        .withoutValidation()
        .withCodec(CodecFactory.snappyCodec());

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "foo"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(displayData, hasDisplayItem("schema", GenericClass.class));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("validation", false));
    assertThat(displayData, hasDisplayItem("codec", CodecFactory.snappyCodec().toString()));
  }

  @Test
  @Category(RunnableOnService.class)
  @Ignore("[BEAM-436] DirectRunner RunnableOnService tempLocation configuration insufficient")
  public void testPrimitiveWriteDisplayData() throws IOException {
    PipelineOptions options = DisplayDataEvaluator.getDefaultOptions();
    String tempRoot = options.as(TestPipelineOptions.class).getTempRoot();
    String outputPath = IOChannelUtils.getFactory(tempRoot).resolve(tempRoot, "foo");

    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create(options);

    AvroIO.Write.Bound<?> write = AvroIO.Write
        .to(outputPath)
        .withSchema(Schema.create(Schema.Type.STRING))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("AvroIO.Write should include the file pattern in its primitive transform",
        displayData, hasItem(hasDisplayItem("fileNamePattern")));
  }
}
