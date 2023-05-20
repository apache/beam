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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link TableSchemaUpdateUtils class}. */
@RunWith(JUnit4.class)
public class TableSchemaUpdateUtilsTest {
  @Test
  public void testSchemaUpdate() {
    TableSchema baseSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema schema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(baseSchema.getFieldsList()))
            .build();
    TableSchema topSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(schema.getFieldsList()))
            .build();

    TableSchema newBaseSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("d").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema newSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(newBaseSchema.getFieldsList()))
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("d").setType(TableFieldSchema.Type.STRING))
            .build();

    TableSchema newTopSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(newSchema.getFieldsList()))
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .build();

    TableSchema expectedSchemaBaseSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("d").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema expectedSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(expectedSchemaBaseSchema.getFieldsList()))
            .addFields(
                TableFieldSchema.newBuilder().setName("d").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema expectedTopSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(expectedSchema.getFieldsList()))
            .build();

    TableSchema updatedTopSchema =
        TableSchemaUpdateUtils.getUpdatedSchema(topSchema, newTopSchema).get();
    assertEquals(expectedTopSchema, updatedTopSchema);
  }

  @Test
  public void testEquivalentSchema() {
    TableSchema baseSchema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema schema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(baseSchema1.getFieldsList()))
            .build();

    TableSchema baseSchema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema schema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(baseSchema2.getFieldsList()))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .build();
    assertFalse(TableSchemaUpdateUtils.getUpdatedSchema(schema1, schema2).isPresent());
  }

  @Test
  public void testNonEquivalentSchema() {
    TableSchema baseSchema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema schema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(baseSchema1.getFieldsList()))
            .build();
    TableSchema baseSchema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.INT64))
            .build();
    TableSchema schema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("a").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("b").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder().setName("c").setType(TableFieldSchema.Type.STRING))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addAllFields(baseSchema2.getFieldsList()))
            .build();
    assertTrue(TableSchemaUpdateUtils.getUpdatedSchema(schema1, schema2).isPresent());
  }
}
