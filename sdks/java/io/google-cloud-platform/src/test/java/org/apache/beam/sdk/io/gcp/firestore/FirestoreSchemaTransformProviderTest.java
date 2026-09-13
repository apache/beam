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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Firestore SchemaTransform providers. */
@RunWith(JUnit4.class)
public class FirestoreSchemaTransformProviderTest {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.fromOptions(PipelineOptionsFactory.create())
          .enableAbandonedNodeEnforcement(false);

  @Test
  public void testReadFindTransform() {
    SchemaTransformProvider provider = loadReadProvider();

    assertEquals(Lists.newArrayList("output"), provider.outputCollectionNames());
    assertEquals(Lists.newArrayList(), provider.inputCollectionNames());
    assertEquals("beam:schematransform:org.apache.beam:firestore_read:v1", provider.identifier());
    assertNotNull(provider.description());

    assertEquals(
        Sets.newHashSet("project_id", "database_id", "collection_id", "schema", "error_handling"),
        provider.configurationSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testReadBuildTransform() {
    FirestoreReadSchemaTransformConfiguration readConfig =
        FirestoreReadSchemaTransformConfiguration.builder()
            .setProjectId("test-project")
            .setDatabaseId("(default)")
            .setCollectionId("users")
            .setSchema(
                "{"
                    + "\"type\":\"object\","
                    + "\"properties\":{"
                    + "\"document_id\":{\"type\":\"string\"},"
                    + "\"name\":{\"type\":\"string\"}"
                    + "},"
                    + "\"required\":[\"document_id\",\"name\"]"
                    + "}")
            .build();

    SchemaTransform transform = new FirestoreReadSchemaTransformProvider().from(readConfig);
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.empty(pipeline));

    assertEquals(1, output.getAll().size());
    assertTrue(output.has("output"));
    assertEquals(
        Schema.builder().addStringField("document_id").addStringField("name").build(),
        output.get("output").getSchema());
  }

  @Test
  public void testReadWithNonEmptyInputThrows() {
    FirestoreReadSchemaTransformConfiguration readConfig =
        FirestoreReadSchemaTransformConfiguration.builder()
            .setProjectId("test-project")
            .setCollectionId("users")
            .setSchema("{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}")
            .build();
    SchemaTransform transform = new FirestoreReadSchemaTransformProvider().from(readConfig);

    PCollection<Row> dummyInput =
        pipeline.apply(
            "CreateDummy", Create.empty(Schema.builder().addStringField("dummy").build()));
    assertThrows(
        IllegalStateException.class,
        () -> transform.expand(PCollectionRowTuple.of("input", dummyInput)));
  }

  @Test
  public void testWriteFindTransform() {
    SchemaTransformProvider provider = loadWriteProvider();

    assertEquals(Lists.newArrayList(), provider.outputCollectionNames());
    assertEquals(Lists.newArrayList("input"), provider.inputCollectionNames());
    assertEquals("beam:schematransform:org.apache.beam:firestore_write:v1", provider.identifier());
    assertNotNull(provider.description());

    assertEquals(
        Sets.newHashSet(
            "project_id", "database_id", "collection_id", "document_id_field", "error_handling"),
        provider.configurationSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testWriteBuildTransform() {
    FirestoreWriteSchemaTransformConfiguration writeConfig =
        FirestoreWriteSchemaTransformConfiguration.builder()
            .setProjectId("test-project")
            .setDatabaseId("(default)")
            .setCollectionId("users")
            .build();
    SchemaTransform transform = new FirestoreWriteSchemaTransformProvider().from(writeConfig);
    Schema schema = Schema.builder().addStringField("document_id").addStringField("name").build();
    PCollection<Row> inputRows = pipeline.apply("CreateRows", Create.empty(schema));
    PCollectionRowTuple output = transform.expand(PCollectionRowTuple.of("input", inputRows));
    assertEquals(1, output.getAll().size());
    assertTrue(output.has("errors"));
  }

  @Test
  public void testWriteMissingDocumentIdFieldThrows() {
    FirestoreWriteSchemaTransformConfiguration writeConfig =
        FirestoreWriteSchemaTransformConfiguration.builder()
            .setProjectId("test-project")
            .setCollectionId("users")
            .build();
    SchemaTransform transform = new FirestoreWriteSchemaTransformProvider().from(writeConfig);
    Schema schema = Schema.builder().addStringField("name").build();
    PCollection<Row> inputRows = pipeline.apply("CreateInvalidRows", Create.empty(schema));
    assertThrows(
        IllegalArgumentException.class,
        () -> transform.expand(PCollectionRowTuple.of("input", inputRows)));
  }

  private static SchemaTransformProvider loadReadProvider() {
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(ServiceLoader.load(SchemaTransformProvider.class).spliterator(), false)
            .filter(provider -> provider.getClass() == FirestoreReadSchemaTransformProvider.class)
            .collect(Collectors.toList());
    return providers.get(0);
  }

  private static SchemaTransformProvider loadWriteProvider() {
    List<SchemaTransformProvider> providers =
        StreamSupport.stream(ServiceLoader.load(SchemaTransformProvider.class).spliterator(), false)
            .filter(provider -> provider.getClass() == FirestoreWriteSchemaTransformProvider.class)
            .collect(Collectors.toList());
    return providers.get(0);
  }
}
