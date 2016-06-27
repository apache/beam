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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO.Write.Bound;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tests for AvroIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class AvroIOTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

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
    TestPipeline p = TestPipeline.create();
    List<GenericClass> values = ImmutableList.of(new GenericClass(3, "hi"),
        new GenericClass(5, "bar"));
    File outputFile = tmpFolder.newFile("output.avro");

    p.apply(Create.of(values))
      .apply(AvroIO.Write.to(outputFile.getAbsolutePath())
          .withoutSharding()
          .withSchema(GenericClass.class));
    p.run();

    p = TestPipeline.create();
    PCollection<GenericClass> input = p
        .apply(AvroIO.Read.from(outputFile.getAbsolutePath()).withSchema(GenericClass.class));

    PAssert.that(input).containsInAnyOrder(values);
    p.run();
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
    TestPipeline p = TestPipeline.create();
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
    p = TestPipeline.create();
    PCollection<GenericClassV2> input = p
        .apply(AvroIO.Read.from(outputFile.getAbsolutePath()).withSchema(GenericClassV2.class));

    PAssert.that(input).containsInAnyOrder(expected);
    p.run();
  }

  @SuppressWarnings("deprecation") // using AvroCoder#createDatumReader for tests.
  private void runTestWrite(String[] expectedElements, int numShards) throws IOException {
    File baseOutputFile = new File(tmpFolder.getRoot(), "prefix");
    String outputFilePrefix = baseOutputFile.getAbsolutePath();
    TestPipeline p = TestPipeline.create();
    Bound<String> write = AvroIO.Write.to(outputFilePrefix).withSchema(String.class);
    if (numShards > 1) {
      write = write.withNumShards(numShards);
    } else {
      write = write.withoutSharding();
    }
    p.apply(Create.<String>of(expectedElements)).apply(write);
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
  public void testWriteDisplayData() {
    AvroIO.Write.Bound<?> write = AvroIO.Write
        .to("foo")
        .withShardNameTemplate("-SS-of-NN-")
        .withSuffix("bar")
        .withSchema(GenericClass.class)
        .withNumShards(100)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "foo"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(displayData, hasDisplayItem("schema", GenericClass.class));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("validation", false));
  }
}
