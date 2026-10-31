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
package org.apache.beam.sdk.extensions.sql.expansion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.ServiceLoader;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SqlTransformSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class SqlTransformSchemaTransformProviderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TemporaryFolder tmp = new TemporaryFolder();

  private final SqlTransformSchemaTransformProvider provider =
      new SqlTransformSchemaTransformProvider();

  private Row.FieldValueBuilder config(String query) {
    return Row.withSchema(provider.configurationSchema()).withFieldValue("query", query);
  }

  /** Python (and other SDKs) can only decode schemas made of non-Java-specific types. */
  @Test
  public void testConfigurationSchemaUsesOnlyPortableTypes() {
    Schema schema = provider.configurationSchema();
    assertEquals(
        Arrays.asList("query", "ddl", "dialect", "autoload", "tableproviders"),
        schema.getFieldNames());
    assertEquals(FieldType.STRING, schema.getField("query").getType());
    assertEquals(FieldType.STRING.withNullable(true), schema.getField("ddl").getType());
    assertEquals(FieldType.STRING.withNullable(true), schema.getField("dialect").getType());
    assertEquals(FieldType.BOOLEAN.withNullable(true), schema.getField("autoload").getType());
    assertEquals(
        FieldType.array(FieldType.STRING).withNullable(true),
        schema.getField("tableproviders").getType());
    for (Schema.Field field : schema.getFields()) {
      assertTrue(
          field.getName() + " must not be a logical type",
          !field.getType().getTypeName().equals(TypeName.LOGICAL_TYPE)
              && (field.getType().getCollectionElementType() == null
                  || !field
                      .getType()
                      .getCollectionElementType()
                      .getTypeName()
                      .equals(TypeName.LOGICAL_TYPE)));
    }
  }

  @Test
  public void testInvalidDialectIsRejected() {
    Row row = config("SELECT 1").withFieldValue("dialect", "not_a_dialect").build();
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> provider.from(row));
    assertTrue(e.getMessage(), e.getMessage().contains("not_a_dialect"));
    assertTrue(e.getMessage(), e.getMessage().contains("calcite"));
  }

  @Test
  public void testUnknownTableProviderIsRejected() {
    Row row =
        config("SELECT 1")
            .withFieldValue("tableproviders", Collections.singletonList("no_such_provider"))
            .build();
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> provider.from(row));
    assertTrue(e.getMessage(), e.getMessage().contains("no_such_provider"));
  }

  @Test
  public void testValidConfigurationIsAccepted() {
    String tableType = ServiceLoader.load(TableProvider.class).iterator().next().getTableType();
    provider.from(config("SELECT 1").build());
    provider.from(
        config("SELECT 1")
            .withFieldValue("dialect", "calcite")
            .withFieldValue("autoload", false)
            .withFieldValue("tableproviders", Collections.singletonList(tableType))
            .build());
  }

  @Test
  public void testExpandWithCalciteDialect() {
    Schema inputSchema = Schema.builder().addInt64Field("id").addStringField("name").build();
    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputSchema).addValues(1L, "a").build(),
                    Row.withSchema(inputSchema).addValues(2L, "b").build())
                .withRowSchema(inputSchema));

    PCollectionRowTuple result =
        PCollectionRowTuple.of("PCOLLECTION", input)
            .apply(
                provider.from(
                    config("SELECT name FROM PCOLLECTION WHERE id > 1")
                        .withFieldValue("dialect", "calcite")
                        .build()));

    Schema outputSchema = Schema.builder().addStringField("name").build();
    PAssert.that(result.get("output"))
        .containsInAnyOrder(Row.withSchema(outputSchema).addValues("b").build());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testExpandWithDdl() throws Exception {
    // DDL table types (here 'text') are resolved from the auto-loaded table providers.
    File data = tmp.newFile("people.csv");
    Files.write(data.toPath(), Arrays.asList("1,a", "2,b", "3,c"), StandardCharsets.UTF_8);

    String ddl =
        "CREATE EXTERNAL TABLE people (id BIGINT, name VARCHAR) TYPE 'text' LOCATION '"
            + data.getAbsolutePath()
            + "'";

    PCollectionRowTuple result =
        PCollectionRowTuple.empty(pipeline)
            .apply(
                provider.from(
                    config("SELECT name FROM people WHERE id > 1")
                        .withFieldValue("ddl", ddl)
                        .withFieldValue("autoload", true)
                        .build()));

    Schema outputSchema = Schema.builder().addStringField("name").build();
    PAssert.that(result.get("output"))
        .containsInAnyOrder(
            Row.withSchema(outputSchema).addValues("b").build(),
            Row.withSchema(outputSchema).addValues("c").build());
    pipeline.run().waitUntilFinish();
  }
}
