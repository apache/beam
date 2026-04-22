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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UpgradeTableSchemaTest {

  @Test
  public void testNewErrorCollector() throws Exception {
    TableRowToStorageApiProto.ErrorCollector collector = UpgradeTableSchema.newErrorCollector();

    // Test exceptions that should be collected
    TableRowToStorageApiProto.SchemaTooNarrowException tooNarrow =
        new TableRowToStorageApiProto.SchemaTooNarrowException("field", "error", false, false);
    TableRowToStorageApiProto.SchemaMissingRequiredFieldException missingRequired =
        new TableRowToStorageApiProto.SchemaMissingRequiredFieldException(Sets.newHashSet("field"));

    collector.collect(tooNarrow);
    collector.collect(missingRequired);

    assertEquals(2, collector.getExceptions().size());

    // Test exception that should not be collected
    TableRowToStorageApiProto.SchemaConversionException randomException =
        new TableRowToStorageApiProto.SchemaConversionException("generic error") {};
    assertThrows(
        TableRowToStorageApiProto.SchemaConversionException.class,
        () -> {
          collector.collect(randomException);
        });
  }

  @Test
  public void testGetIncrementalSchema_SchemaTooNarrow() throws Exception {
    TableRowToStorageApiProto.ErrorCollector collector = UpgradeTableSchema.newErrorCollector();
    TableRowToStorageApiProto.SchemaTooNarrowException tooNarrow =
        new TableRowToStorageApiProto.SchemaTooNarrowException("new_field", "error", false, false);
    collector.collect(tooNarrow);

    TableSchema oldSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("old_field")
                    .setType(TableFieldSchema.Type.STRING))
            .build();

    TableSchema incrementalSchema = UpgradeTableSchema.getIncrementalSchema(collector, oldSchema);
    assertEquals(1, incrementalSchema.getFieldsCount());
    assertEquals("new_field", incrementalSchema.getFields(0).getName());
    assertEquals(TableFieldSchema.Mode.NULLABLE, incrementalSchema.getFields(0).getMode());
    assertEquals(TableFieldSchema.Type.STRING, incrementalSchema.getFields(0).getType());
  }

  @Test
  public void testGetIncrementalSchema_SchemaMissingRequiredField() throws Exception {
    TableRowToStorageApiProto.ErrorCollector collector = UpgradeTableSchema.newErrorCollector();
    TableRowToStorageApiProto.SchemaMissingRequiredFieldException missingRequired =
        new TableRowToStorageApiProto.SchemaMissingRequiredFieldException(
            Sets.newHashSet("required_field"));
    collector.collect(missingRequired);

    TableSchema oldSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("required_field")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.REQUIRED))
            .build();

    TableSchema incrementalSchema = UpgradeTableSchema.getIncrementalSchema(collector, oldSchema);
    assertEquals(1, incrementalSchema.getFieldsCount());
    assertEquals("required_field", incrementalSchema.getFields(0).getName());
    assertEquals(TableFieldSchema.Type.STRING, incrementalSchema.getFields(0).getType());
    assertEquals(TableFieldSchema.Mode.NULLABLE, incrementalSchema.getFields(0).getMode());
  }

  @Test
  public void testGetIncrementalSchema_NestedFields() throws Exception {
    TableRowToStorageApiProto.ErrorCollector collector = UpgradeTableSchema.newErrorCollector();

    // 1. Add a nested field
    TableRowToStorageApiProto.SchemaTooNarrowException tooNarrow =
        new TableRowToStorageApiProto.SchemaTooNarrowException(
            "nested.new_field", "error", false, false);
    collector.collect(tooNarrow);

    // 2. Relax a nested field
    TableRowToStorageApiProto.SchemaMissingRequiredFieldException missingRequired =
        new TableRowToStorageApiProto.SchemaMissingRequiredFieldException(
            Sets.newHashSet("nested.required_field"));
    collector.collect(missingRequired);

    TableSchema oldSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("top")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.REQUIRED))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("required_field")
                            .setType(TableFieldSchema.Type.STRING)
                            .setMode(TableFieldSchema.Mode.REQUIRED)))
            .build();

    TableSchema incrementalSchema = UpgradeTableSchema.getIncrementalSchema(collector, oldSchema);

    TableSchema expected =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("required_field")
                            .setType(TableFieldSchema.Type.STRING)
                            .setMode(TableFieldSchema.Mode.NULLABLE))
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("new_field")
                            .setType(TableFieldSchema.Type.STRING)
                            .setMode(TableFieldSchema.Mode.NULLABLE)))
            .build();
    assertEquals(expected, incrementalSchema);
  }

  @Test
  public void testMergeSchemas() {
    TableSchema schema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f1")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.REQUIRED))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f3")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.NULLABLE))
            .build();
    TableSchema schema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f1")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.NULLABLE))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f2")
                    .setType(TableFieldSchema.Type.INT64)
                    .setMode(TableFieldSchema.Mode.REQUIRED))
            .build();

    TableSchema merged = UpgradeTableSchema.mergeSchemas(schema1, schema2);

    TableSchema expectedSchema =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f1")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.NULLABLE))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f3")
                    .setType(TableFieldSchema.Type.STRING)
                    .setMode(TableFieldSchema.Mode.NULLABLE))
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("f2")
                    .setType(TableFieldSchema.Type.INT64)
                    .setMode(TableFieldSchema.Mode.REQUIRED))
            .build();
    assertEquals(expectedSchema, merged);
    assertEquals(3, merged.getFieldsCount());
  }

  @Test
  public void testMergeSchemasConflict() {
    TableSchema schema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("f1").setType(TableFieldSchema.Type.STRING))
            .build();
    TableSchema schema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder().setName("f1").setType(TableFieldSchema.Type.INT64))
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          UpgradeTableSchema.mergeSchemas(schema1, schema2);
        });
  }

  @Test
  public void testMergeSchemasNested() {
    TableSchema schema1 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("n1")
                            .setType(TableFieldSchema.Type.STRING)
                            .setMode(TableFieldSchema.Mode.REQUIRED)))
            .build();

    TableSchema schema2 =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("n1")
                            .setType(TableFieldSchema.Type.STRING)
                            .setMode(TableFieldSchema.Mode.NULLABLE))
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("n2")
                            .setType(TableFieldSchema.Type.INT64)))
            .build();

    TableSchema merged = UpgradeTableSchema.mergeSchemas(schema1, schema2);

    TableSchema expected =
        TableSchema.newBuilder()
            .addFields(
                TableFieldSchema.newBuilder()
                    .setName("nested")
                    .setType(TableFieldSchema.Type.STRUCT)
                    .setMode(TableFieldSchema.Mode.NULLABLE)
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("n1")
                            .setType(TableFieldSchema.Type.STRING)
                            .setMode(TableFieldSchema.Mode.NULLABLE))
                    .addFields(
                        TableFieldSchema.newBuilder()
                            .setName("n2")
                            .setType(TableFieldSchema.Type.INT64)))
            .build();
    assertEquals(expected, merged);
    assertEquals(1, merged.getFieldsCount());
  }

  @Test
  public void testIsPayloadSchemaOutOfDate() throws Exception {
    byte[] hash1 =
        Hashing.goodFastHash(32).hashBytes("schema1".getBytes(StandardCharsets.UTF_8)).asBytes();
    byte[] hash2 =
        Hashing.goodFastHash(32).hashBytes("schema2".getBytes(StandardCharsets.UTF_8)).asBytes();

    DescriptorProtos.DescriptorProto descriptorProto =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("TestMessage")
            .addField(
                DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("field1")
                    .setNumber(1)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .build())
            .build();
    Descriptors.Descriptor descriptor =
        TableRowToStorageApiProto.wrapDescriptorProto(descriptorProto);

    DynamicMessage msg =
        DynamicMessage.newBuilder(descriptor)
            .setField(Preconditions.checkStateNotNull(descriptor.findFieldByNumber(1)), "abcd")
            .build();

    StorageApiWritePayload payload =
        new AutoValue_StorageApiWritePayload.Builder()
            .setPayload(msg.toByteArray())
            .setSchemaHash(hash1)
            .build();

    // 1. Missing hash in payload
    StorageApiWritePayload payloadNoHash =
        new AutoValue_StorageApiWritePayload.Builder().setPayload(new byte[0]).build();
    assertFalse(UpgradeTableSchema.isPayloadSchemaOutOfDate(payloadNoHash, () -> hash1, null));

    // 2. Equal hash
    assertFalse(UpgradeTableSchema.isPayloadSchemaOutOfDate(payload, () -> hash1, null));

    // 3. Different hash, but message doesn't have unknown fields (initialized and empty)
    assertFalse(
        UpgradeTableSchema.isPayloadSchemaOutOfDate(payload, () -> hash2, () -> descriptor));

    // 4. Different hash with unknown fields.
    DynamicMessage unknownFieldSet =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByNumber(1), "abcd")
            .setUnknownFields(
                UnknownFieldSet.newBuilder()
                    .addField(2, UnknownFieldSet.Field.newBuilder().addFixed64(42).build())
                    .build())
            .build();

    StorageApiWritePayload payloadWithUnknown =
        new AutoValue_StorageApiWritePayload.Builder()
            .setPayload(unknownFieldSet.toByteArray())
            .setSchemaHash(hash1)
            .build();
    assertTrue(
        UpgradeTableSchema.isPayloadSchemaOutOfDate(
            payloadWithUnknown, () -> hash2, () -> descriptor));
  }
}
