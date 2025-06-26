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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.apache.beam.sdk.io.gcp.spanner.SpannerReadSchemaTransformProvider.SpannerReadSchemaTransformConfiguration;
import static org.apache.beam.sdk.io.gcp.spanner.SpannerWriteSchemaTransformProvider.SpannerWriteSchemaTransformConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link org.apache.beam.sdk.io.gcp.spanner.SpannerReadSchemaTransformProvider} and
 * {@link org.apache.beam.sdk.io.gcp.spanner.SpannerWriteSchemaTransformProvider}.
 */
@RunWith(JUnit4.class)
public class SpannerSchemaTransformProviderTest {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testSpannerWriteValidations() {

    // Missing instanceId should throw IllegalArgumentException
    assertThrows(
        IllegalStateException.class,
        () -> {
          SpannerWriteSchemaTransformConfiguration.builder()
              .setDatabaseId("validDatabase")
              .setTableId("validTable")
              .build()
              .validate();
        });

    // Missing databaseId should throw IllegalArgumentException
    assertThrows(
        IllegalStateException.class,
        () -> {
          SpannerWriteSchemaTransformConfiguration.builder()
              .setInstanceId("validInstance")
              .setTableId("validTable")
              .build()
              .validate();
        });

    // Missing tableId should throw IllegalArgumentException
    assertThrows(
        IllegalStateException.class,
        () -> {
          SpannerWriteSchemaTransformConfiguration.builder()
              .setInstanceId("validInstance")
              .setDatabaseId("validDatabase")
              .build()
              .validate();
        });

    // Valid config should NOT throw any exceptions
    SpannerWriteSchemaTransformConfiguration.builder()
        .setInstanceId("validInstance")
        .setDatabaseId("validDatabase")
        .setTableId("validTable")
        .build()
        .validate();
  }

  @Test
  public void testSpannerReadValidations() {

    // Missing instanceId should throw
    assertThrows(
        IllegalStateException.class,
        () -> {
          SpannerReadSchemaTransformConfiguration.builder()
              .setDatabaseId("db")
              .setTableId("table")
              .build()
              .validate();
        });

    // Missing databaseId should throw
    assertThrows(
        IllegalStateException.class,
        () -> {
          SpannerReadSchemaTransformConfiguration.builder()
              .setInstanceId("instance")
              .setTableId("table")
              .build()
              .validate();
        });

    // Missing both tableId and query should throw
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          SpannerReadSchemaTransformConfiguration.builder()
              .setInstanceId("instance")
              .setDatabaseId("db")
              .build()
              .validate();
        });

    // TableId without columns should throw
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          SpannerReadSchemaTransformConfiguration.builder()
              .setInstanceId("instance")
              .setDatabaseId("db")
              .setTableId("table")
              .build()
              .validate();
        });

    // Valid table-based read config
    SpannerReadSchemaTransformConfiguration.builder()
        .setInstanceId("instance")
        .setDatabaseId("db")
        .setTableId("table")
        .setColumns(Arrays.asList("col1"))
        .build()
        .validate();

    // Valid query-based config
    SpannerReadSchemaTransformConfiguration.builder()
        .setInstanceId("instance")
        .setDatabaseId("db")
        .setQuery("SELECT * FROM table")
        .build()
        .validate();

    // Both query and tableId provided – should throw if mutually exclusive
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          SpannerReadSchemaTransformConfiguration.builder()
              .setInstanceId("instance")
              .setDatabaseId("db")
              .setQuery("SELECT * FROM table")
              .setTableId("table")
              .build()
              .validate();
        });
  }

  @Test
  public void testReadBuildTransform() {
    SpannerReadSchemaTransformProvider provider = new SpannerReadSchemaTransformProvider();
    provider.from(
        SpannerReadSchemaTransformConfiguration.builder()
            .setProjectId("test-project")
            .setInstanceId("test-instance")
            .setDatabaseId("test-database")
            .setQuery("SELECT * FROM users")
            .build());
  }

  @Test
  public void testWriteBuildTransform() {
    SpannerWriteSchemaTransformProvider provider = new SpannerWriteSchemaTransformProvider();
    provider.from(
        SpannerWriteSchemaTransformConfiguration.builder()
            .setProjectId("test-project")
            .setInstanceId("test-instance")
            .setDatabaseId("test-database")
            .setTableId("test-table")
            .build());
  }

  @Test
  public void testReadFindTransformAndMakeItWork() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == SpannerReadSchemaTransformProvider.class)
            .collect(Collectors.toList());

    assertFalse("SpannerReadSchemaTransformProvider not found", providers.isEmpty());

    SchemaTransformProvider spannerProvider = providers.get(0);

    // Check expected output and input collection names (adjust if different)
    assertEquals(Lists.newArrayList("output", "errors"), spannerProvider.outputCollectionNames());
    assertEquals(Lists.newArrayList(), spannerProvider.inputCollectionNames());

    assertEquals(
        Sets.newHashSet(
            "project_id",
            "instance_id",
            "database_id",
            "table_id",
            "query",
            "columns",
            "index",
            "batching",
            "error_handling"),
        spannerProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testWriteFindTransformAndMakeItWork() {
    ServiceLoader<SchemaTransformProvider> serviceLoader =
        ServiceLoader.load(SchemaTransformProvider.class);

    List<SchemaTransformProvider> providers =
        StreamSupport.stream(serviceLoader.spliterator(), false)
            .filter(provider -> provider.getClass() == SpannerWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());

    assertFalse("SpannerWriteSchemaTransformProvider not found", providers.isEmpty());

    SchemaTransformProvider spannerWriteProvider = providers.get(0);

    // Typically write transforms output to 'output' and 'errors' collections (adjust if needed)
    assertEquals(
        Lists.newArrayList("post-write", "errors"), spannerWriteProvider.outputCollectionNames());

    assertEquals(
        Sets.newHashSet("instance_id", "database_id", "table_id", "project_id", "error_handling"),
        spannerWriteProvider.configurationSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testErrorFnCapturesStructFailureAsRow() {

    Struct badStruct =
        Struct.newBuilder().set("non_existing_field").to(Value.string("bad_value")).build();

    // Define a mismatched schema (does not match the struct)
    Schema expectedSchema = Schema.builder().addStringField("id").build(); // "id" not in struct

    TupleTag<Row> outputTag = SpannerReadSchemaTransformProvider.OUTPUT_TAG;
    TupleTag<Row> errorTag = SpannerReadSchemaTransformProvider.ERROR_TAG;

    PCollection<Struct> spannerRows = pipeline.apply("CreateBadStruct", Create.of(badStruct));

    PCollectionTuple result =
        spannerRows.apply(
            ParDo.of(
                    new SpannerReadSchemaTransformProvider.ErrorFn(
                        "test-counter",
                        SpannerReadSchemaTransformProvider.ERROR_SCHEMA,
                        expectedSchema,
                        true))
                .withOutputTags(outputTag, TupleTagList.of(errorTag)));

    result.get(outputTag).setRowSchema(expectedSchema);
    PCollection<Row> errorOutput =
        result.get(errorTag).setRowSchema(SpannerReadSchemaTransformProvider.ERROR_SCHEMA);

    PAssert.that(errorOutput)
        .satisfies(
            rows -> {
              if (!rows.iterator().hasNext()) {
                throw new AssertionError("Expected at least one error row but got none.");
              }

              Row errorRow = rows.iterator().next();
              String errorMsg = errorRow.getString("error");
              String rowString = errorRow.getString("row");

              assert errorMsg != null && errorMsg.contains("Field not found: id")
                  : "Missing expected error message. Got: " + errorMsg;
              assert rowString != null && rowString.contains("bad_value")
                  : "Row string does not contain expected content. Got: " + rowString;
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
