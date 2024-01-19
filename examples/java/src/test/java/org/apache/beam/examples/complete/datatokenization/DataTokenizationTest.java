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
package org.apache.beam.examples.complete.datatokenization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.beam.examples.complete.datatokenization.options.DataTokenizationOptions;
import org.apache.beam.examples.complete.datatokenization.transforms.io.TokenizationFileSystemIO;
import org.apache.beam.examples.complete.datatokenization.transforms.io.TokenizationFileSystemIO.FORMAT;
import org.apache.beam.examples.complete.datatokenization.utils.FailsafeElementCoder;
import org.apache.beam.examples.complete.datatokenization.utils.RowToCsv;
import org.apache.beam.examples.complete.datatokenization.utils.SchemasUtils;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link DataTokenization}. */
@RunWith(JUnit4.class)
public class DataTokenizationTest {

  private static final String testSchema =
      "{\"fields\":[{\"mode\":\"REQUIRED\",\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"mode\":\"REQUIRED\",\"name\":\"FieldName2\",\"type\":\"STRING\"}]}";
  String[] fields = {"TestValue1", "TestValue2"};

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "./";

  private static final String CSV_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.csv").getPath();

  private static final String JSON_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.txt").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "schema.txt").getPath();

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  @Test
  public void testGetBeamSchema() {
    Schema expectedSchema =
        Schema.builder()
            .addField("FieldName1", FieldType.STRING)
            .addField("FieldName2", FieldType.STRING)
            .build();
    SchemasUtils schemasUtils = new SchemasUtils(testSchema);
    Assert.assertEquals(expectedSchema, schemasUtils.getBeamSchema());
  }

  @Test
  public void testGetBigQuerySchema() {
    SchemasUtils schemasUtils = new SchemasUtils(testSchema);
    Assert.assertEquals(testSchema, schemasUtils.getBigQuerySchema().toString());
  }

  @Test
  public void testRowToCSV() {
    Schema beamSchema = new SchemasUtils(testSchema).getBeamSchema();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValues(new ArrayList<>(Arrays.asList(fields))).build();
    String csvResult = new RowToCsv(";").getCsvFromRow(row);
    Assert.assertEquals(String.join(";", fields), csvResult);
  }

  @Test
  public void testRowToCSVWithNull() {
    final String nullableTestSchema =
        "{\"fields\":[{\"mode\":\"REQUIRED\",\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"mode\":\"NULLABLE\",\"name\":\"FieldName2\",\"type\":\"STRING\"}]}";
    final String expectedCsv = "TestValueOne;null";

    List<Object> values = Lists.newArrayList("TestValueOne", null);

    Schema beamSchema = new SchemasUtils(nullableTestSchema).getBeamSchema();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValues(values).build();
    String csvResult = new RowToCsv(";").getCsvFromRow(row);
    Assert.assertEquals(expectedCsv, csvResult);
  }

  @Test
  public void testFileSystemIOReadCSV() throws IOException {
    PCollection<Row> jsons = fileSystemIORead(CSV_FILE_PATH, FORMAT.CSV);
    assertRows(jsons);
    testPipeline.run();
  }

  @Test
  public void testFileSystemIOReadJSON() throws IOException {
    PCollection<Row> jsons = fileSystemIORead(JSON_FILE_PATH, FORMAT.JSON);
    assertRows(jsons);
    testPipeline.run();
  }

  @Test
  public void testJsonToRow() throws IOException {
    PCollection<Row> rows = fileSystemIORead(JSON_FILE_PATH, FORMAT.JSON);
    PAssert.that(rows)
        .satisfies(
            x -> {
              LinkedList<Row> beamRows = Lists.newLinkedList(x);
              assertThat(beamRows, hasSize(3));
              beamRows.forEach(
                  row -> {
                    List<Object> fieldValues = row.getValues();
                    for (Object element : fieldValues) {
                      assertThat((String) element, startsWith("FieldValue"));
                    }
                  });
              return null;
            });
    testPipeline.run();
  }

  private PCollection<Row> fileSystemIORead(String inputGcsFilePattern, FORMAT inputGcsFileFormat)
      throws IOException {
    DataTokenizationOptions options =
        PipelineOptionsFactory.create().as(DataTokenizationOptions.class);
    options.setDataSchemaPath(SCHEMA_FILE_PATH);
    options.setInputFilePattern(inputGcsFilePattern);
    options.setInputFileFormat(inputGcsFileFormat);
    if (inputGcsFileFormat == FORMAT.CSV) {
      options.setCsvContainsHeaders(Boolean.FALSE);
    }

    SchemasUtils testSchemaUtils =
        new SchemasUtils(options.getDataSchemaPath(), StandardCharsets.UTF_8);

    CoderRegistry coderRegistry = testPipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        RowCoder.of(testSchemaUtils.getBeamSchema()).getEncodedTypeDescriptor(),
        RowCoder.of(testSchemaUtils.getBeamSchema()));
    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder =
        FailsafeElementCoder.of(
            RowCoder.of(testSchemaUtils.getBeamSchema()),
            RowCoder.of(testSchemaUtils.getBeamSchema()));
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    return new TokenizationFileSystemIO(options).read(testPipeline, testSchemaUtils);
  }

  private void assertRows(PCollection<Row> jsons) {
    PAssert.that(jsons)
        .satisfies(
            x -> {
              LinkedList<Row> rows = Lists.newLinkedList(x);
              assertThat(rows, hasSize(3));
              rows.forEach(
                  row -> {
                    assertNotNull(row.getSchema());
                    assertThat(row.getSchema().getFields(), hasSize(3));
                    assertThat(row.getSchema().getField(0).getName(), equalTo("Field1"));

                    assertThat(row.getValues(), hasSize(3));
                  });
              return null;
            });
  }
}
