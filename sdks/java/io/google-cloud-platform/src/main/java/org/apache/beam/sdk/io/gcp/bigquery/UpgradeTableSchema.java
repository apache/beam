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

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashCode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Helper functions for SchemaUpdateOptions. */
public class UpgradeTableSchema {
  public static TableRowToStorageApiProto.ErrorCollector newErrorCollector() {
    return new TableRowToStorageApiProto.ErrorCollector(
        e ->
            (e instanceof TableRowToStorageApiProto.SchemaTooNarrowException
                    && !((TableRowToStorageApiProto.SchemaTooNarrowException) e)
                        .getMissingField()
                        .isEmpty())
                || e instanceof TableRowToStorageApiProto.SchemaMissingRequiredFieldException);
  }

  /**
   * Given a list of schema errors, generate a new schema that represents the minimal changes needed
   * to the schema in order to validate the new record. We only generate the incremental schema here
   * for performance reasons, as these schemas will be shuffled. The final transform will merge this
   * back into the existing table schema before updating the table.
   */
  public static TableSchema getIncrementalSchema(
      TableRowToStorageApiProto.ErrorCollector errorCollector, TableSchema oldSchema)
      throws TableRowToStorageApiProto.SchemaDoesntMatchException {
    // This isn't the most efficient, especially if we have deeply-nested schemas. However we don't
    // expect to be
    // upgrading schemas very regularly - if we do then we have other problems!
    Map<String, LinkedHashMap<String, TableFieldSchema>> newFields = Maps.newHashMap();
    Map<String, Set<String>> relaxedFields = Maps.newHashMap();

    for (TableRowToStorageApiProto.SchemaConversionException schemaConversionException :
        errorCollector.getExceptions()) {
      if (schemaConversionException instanceof TableRowToStorageApiProto.SchemaTooNarrowException) {
        TableRowToStorageApiProto.SchemaTooNarrowException e =
            (TableRowToStorageApiProto.SchemaTooNarrowException) schemaConversionException;
        List<String> components = Arrays.asList(e.getMissingField().toLowerCase().split("\\."));
        String prefix = String.join(".", components.subList(0, components.size() - 1));
        String name = components.get(components.size() - 1);
        TableFieldSchema.Mode mode =
            e.isRepeated() ? TableFieldSchema.Mode.REPEATED : TableFieldSchema.Mode.NULLABLE;
        // TODO(reuvenlax): Fix this so that arbitrary types can be selected.
        TableFieldSchema.Type type =
            e.isStruct() ? TableFieldSchema.Type.STRUCT : TableFieldSchema.Type.STRING;
        @Nullable
        TableFieldSchema oldValue =
            newFields
                .computeIfAbsent(prefix, p -> Maps.newLinkedHashMap())
                .put(
                    name,
                    TableFieldSchema.newBuilder()
                        .setName(name)
                        .setMode(mode)
                        .setType(type)
                        .build());
        if (oldValue != null) {
          // Duplicates are ok because we might run this over an entire bundle. However we must
          // ensure that they are compatible.
          if (!oldValue.getType().equals(type)) {
            throw new TableRowToStorageApiProto.SchemaDoesntMatchException(
                "Inconsistent types seen for field: "
                    + e.getMissingField()
                    + " "
                    + oldValue.getType()
                    + " v.s. "
                    + type);
          }
        }
      } else if (schemaConversionException
          instanceof TableRowToStorageApiProto.SchemaMissingRequiredFieldException) {
        ((TableRowToStorageApiProto.SchemaMissingRequiredFieldException) schemaConversionException)
            .getMissingFields()
            .forEach(
                f -> {
                  List<String> components = Arrays.asList(f.toLowerCase().split("\\."));
                  String prefix = String.join(".", components.subList(0, components.size() - 1));
                  String name = components.get(components.size() - 1);
                  relaxedFields.computeIfAbsent(prefix, p -> Sets.newHashSet()).add(name);
                });
      } else {
        throw new RuntimeException(
            "Unexpected error " + schemaConversionException, schemaConversionException);
      }
    }
    return TableSchema.newBuilder()
        .addAllFields(
            getIncrementalSchemaHelper(newFields, relaxedFields, oldSchema.getFieldsList(), ""))
        .build();
  }

  private static List<TableFieldSchema> getIncrementalSchemaHelper(
      Map<String, LinkedHashMap<String, TableFieldSchema>> newFields,
      Map<String, Set<String>> relaxedFields,
      List<TableFieldSchema> tableFields,
      String prefix) {
    List<TableFieldSchema> fields = Lists.newArrayList();

    Set<String> fieldsToRelax = relaxedFields.getOrDefault(prefix, Collections.emptySet());
    // Add existing fields in the same order.
    for (TableFieldSchema fieldSchema : tableFields) {
      String fieldName = fieldSchema.getName().toLowerCase();
      TableFieldSchema.Builder clonedField = null;
      if (fieldsToRelax.contains(fieldName)) {
        // Since we're only generating the incremental schema, existing fields are only examined if
        // they change - e.g. they are relaxed.
        clonedField = fieldSchema.toBuilder();
        clonedField.setMode(TableFieldSchema.Mode.NULLABLE);
      }
      if (fieldSchema.getType().equals(TableFieldSchema.Type.STRUCT)) {
        // Recursively walk the schema, looking for more field relaxations.
        String newPrefix = prefix.isEmpty() ? fieldName : String.join(".", prefix, fieldName);
        List<TableFieldSchema> newSubfields =
            getIncrementalSchemaHelper(
                newFields, relaxedFields, fieldSchema.getFieldsList(), newPrefix);
        if (!newSubfields.isEmpty()) {
          if (clonedField == null) {
            clonedField = fieldSchema.toBuilder();
          }
          clonedField.clearFields();
          clonedField.addAllFields(newSubfields);
        }
      }
      if (clonedField != null) {
        fields.add(clonedField.build());
      }
    }

    LinkedHashMap<String, TableFieldSchema> fieldsToAdd =
        newFields.getOrDefault(prefix, new LinkedHashMap<>());
    for (Map.Entry<String, TableFieldSchema> entry : fieldsToAdd.entrySet()) {
      TableFieldSchema.Builder field = entry.getValue().toBuilder();
      // We rely on the exception telling us intermediate struct fields.
      if (field.getType().equals(TableFieldSchema.Type.STRUCT)) {
        String fieldName = field.getName().toLowerCase();
        String newPrefix = prefix.isEmpty() ? fieldName : String.join(".", prefix, fieldName);
        field.addAllFields(
            getIncrementalSchemaHelper(
                newFields, relaxedFields, Collections.emptyList(), newPrefix));
      }
      fields.add(field.build());
    }

    return fields;
  }

  // Merge two schemas. schema1 is considered the primary schema, and will control what order
  // overlapping fields
  // are created in the final schema.
  public static TableSchema mergeSchemas(TableSchema schema1, TableSchema schema2) {
    List<TableFieldSchema> mergedFields =
        mergeFields(schema1.getFieldsList(), schema2.getFieldsList());
    return TableSchema.newBuilder().addAllFields(mergedFields).build();
  }

  private static List<TableFieldSchema> mergeFields(
      List<TableFieldSchema> fields1, List<TableFieldSchema> fields2) {
    // Use LinkedHashMap to preserve the order of fields
    Map<String, TableFieldSchema> mergedFieldsMap = Maps.newLinkedHashMap();

    // Add all fields from schema 1.
    fields1.forEach(f -> mergedFieldsMap.put(f.getName().toLowerCase(), f));

    // Merge or append fields from schema 2
    for (TableFieldSchema f2 : fields2) {
      String lowerName = f2.getName().toLowerCase();
      mergedFieldsMap.compute(lowerName, (k, v) -> v == null ? f2 : mergeField(v, f2));
    }
    return Lists.newArrayList(mergedFieldsMap.values());
  }

  private static TableFieldSchema mergeField(TableFieldSchema f1, TableFieldSchema f2) {
    if (!f1.getType().equals(f2.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "Conflicting field types for field '%s': %s vs %s",
              f1.getName(), f1.getType(), f2.getType()));
    }

    TableFieldSchema.Builder builder = f1.toBuilder().mergeFrom(f2);
    builder.clearFields();

    // Handle mode weakening (NULLABLE > REQUIRED)
    TableFieldSchema.Mode mode1 =
        f1.getMode() == TableFieldSchema.Mode.MODE_UNSPECIFIED
            ? TableFieldSchema.Mode.NULLABLE
            : f1.getMode();
    TableFieldSchema.Mode mode2 =
        f2.getMode() == TableFieldSchema.Mode.MODE_UNSPECIFIED
            ? TableFieldSchema.Mode.NULLABLE
            : f2.getMode();

    boolean isNull =
        ((mode1 == TableFieldSchema.Mode.NULLABLE) && (mode2 != TableFieldSchema.Mode.REPEATED))
            || (mode2 == TableFieldSchema.Mode.NULLABLE && mode1 != TableFieldSchema.Mode.REPEATED);
    if (isNull) {
      builder.setMode(TableFieldSchema.Mode.NULLABLE);
    } else if (mode1.equals(mode2)) {
      // Either merging REPEATED with REPEATED or REQUIRED with REQUIRED.
      builder.setMode(mode1);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Conflicting field modes for field '%s': %s vs %s",
              f1.getName(), f1.getMode(), f2.getMode()));
    }

    // Recursively merge nested fields if the type is STRUCT (Record)
    if (f1.getType() == TableFieldSchema.Type.STRUCT) {
      builder.addAllFields(mergeFields(f1.getFieldsList(), f2.getFieldsList()));
    }

    return builder.build();
  }

  public static boolean isPayloadSchemaOutOfDate(
      StorageApiWritePayload payload,
      ThrowingSupplier<byte[]> schemaHash,
      ThrowingSupplier<Descriptors.Descriptor> schemaDescriptor)
      throws Exception {
    byte @Nullable [] payloadSchemaHash = payload.getSchemaHash();
    if (payloadSchemaHash != null) {
      // Schema hash is only included in the payload if schema update options are set.
      HashCode lhs = HashCode.fromBytes(payloadSchemaHash);
      HashCode rhs = HashCode.fromBytes(schemaHash.get());
      if (!lhs.equals(rhs)) {
        DynamicMessage msg =
            DynamicMessage.newBuilder(schemaDescriptor.get())
                .mergeFrom(payload.getPayload())
                .buildPartial();
        return !msg.isInitialized() || hasUnknownFields(msg);
      }
    }
    return false;
  }

  private static boolean hasUnknownFields(Message message) {
    if (message == null) {
      return false;
    }

    // 1. Check if the current message level has any unknown fields
    UnknownFieldSet unknownFields = message.getUnknownFields();
    if (unknownFields != null && !unknownFields.asMap().isEmpty()) {
      return true;
    }

    // 2. Iterate through all populated fields in the current message
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      Descriptors.FieldDescriptor fieldDescriptor = entry.getKey();
      Object value = entry.getValue();

      // We only care about nested messages, as scalar types don't have sub-fields
      if (fieldDescriptor.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        if (fieldDescriptor.isRepeated()) {
          // For repeated message fields, recursively check each element in the list
          Iterable<?> repeatedMessages = (Iterable<?>) value;
          for (Object element : repeatedMessages) {
            if (element != null && hasUnknownFields((Message) element)) {
              return true;
            }
          }
        } else {
          // For singular message fields, recursively check the nested message
          if (value != null && hasUnknownFields((Message) value)) {
            return true;
          }
        }
      }
    }

    // If we reach here, neither this message nor its descendants have unknown fields
    return false;
  }
}
