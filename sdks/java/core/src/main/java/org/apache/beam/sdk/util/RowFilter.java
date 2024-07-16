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
package org.apache.beam.sdk.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RowFilter {
  private final Schema rowSchema;
  private @Nullable Schema transformedSchema;

  public RowFilter(Schema rowSchema) {
    this.rowSchema = rowSchema;
  }

  /**
   * Checks whether a {@link Schema} contains a list of field names. Nested fields can be expressed
   * with dot-notation. Throws a helpful error in the case where a field doesn't exist, or if a
   * nested field could not be reached.
   */
  @VisibleForTesting
  static void validateSchemaContainsFields(
      Schema schema, List<String> specifiedFields, String operation) {
    Set<String> notFound = new HashSet<>();
    Set<String> notRowField = new HashSet<>();

    for (String field : specifiedFields) {
      List<String> levels = Splitter.on(".").splitToList(field);

      Schema currentSchema = schema;

      for (int i = 0; i < levels.size(); i++) {
        String currentFieldName = String.join(".", levels.subList(0, i + 1));

        if (!currentSchema.hasField(levels.get(i))) {
          notFound.add(currentFieldName);
          break;
        }

        if (i + 1 < levels.size()) {
          Schema.Field nextField = currentSchema.getField(levels.get(i));
          if (!nextField.getType().getTypeName().equals(Schema.TypeName.ROW)) {
            notRowField.add(currentFieldName);
            break;
          }
          currentSchema = Preconditions.checkNotNull(nextField.getType().getRowSchema());
        }
      }
    }

    if (!notFound.isEmpty() || !notRowField.isEmpty()) {
      String message = "Validation failed for " + operation + ".";
      if (!notFound.isEmpty()) {
        message += "\nRow Schema does not contain the following specified fields: " + notFound;
      }
      if (!notRowField.isEmpty()) {
        message +=
            "\nThe following specified fields are not of type Row. Their nested fields could not be reached: "
                + notRowField;
      }
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Configures this {@link RowFilter} to filter {@link Row}s by keeping only the specified fields.
   * Nested fields can be specified using dot-notation.
   *
   * <p>For example, if we want to keep the list of fields {@code ["foo", "baz.nested_1"]}, for the
   * input {@link Row}:
   *
   * <pre>{@code
   * foo: 123
   * bar: 456
   * baz:
   *   nested_1: abc
   *   nested_2: xyz
   * }</pre>
   *
   * we will get the following output {@link Row}:
   *
   * <pre>{@code
   * foo: 123
   * baz
   *   nested_1: abc
   * }</pre>
   */
  public RowFilter keeping(List<String> fields) {
    Preconditions.checkState(
        transformedSchema == null,
        "This RowFilter has already been configured to filter to the following Schema: %s",
        transformedSchema);
    validateSchemaContainsFields(rowSchema, fields, "\"keep\"");
    transformedSchema = keepFields(rowSchema, fields);
    return this;
  }

  /**
   * Configures this {@link RowFilter} to filter {@link Row} by removing the specified fields.
   * Nested fields can be specified using dot-notation.
   *
   * <p>For example, if we want to elide thea list of fields {@code ["foo", "baz.nested_1"]}, for
   * the input {@link Row}:
   *
   * <pre>{@code
   * foo: 123
   * bar: 456
   * baz:
   *   nested_1: abc
   *   nested_2: xyz
   * }</pre>
   *
   * we will get the following output {@link Row}:
   *
   * <pre>{@code
   * bar: 456
   * baz:
   *   nested_2: xyz
   * }</pre>
   */
  public RowFilter eliding(List<String> fields) {
    Preconditions.checkState(
        transformedSchema == null,
        "This RowFilter has already been configured to filter to the following Schema: %s",
        transformedSchema);
    validateSchemaContainsFields(rowSchema, fields, "\"elide\"");
    transformedSchema = elideFields(rowSchema, fields);
    return this;
  }

  /**
   * Creates a field tree, separating each top-level field from its (potential) nested fields. E.g.
   * ["foo.bar.baz", "foo.abc", "xyz"] --> {"foo": ["bar.baz", "abc"], "xyz": []}
   */
  @VisibleForTesting
  static Map<String, List<String>> getFieldTree(List<String> fields) {
    Map<String, List<String>> fieldTree = Maps.newHashMap();

    for (String field : fields) {
      List<String> components = Splitter.on(".").splitToList(field);
      String root = components.get(0);
      fieldTree.computeIfAbsent(root, r -> new ArrayList<>());

      if (components.size() > 1) {
        String nestedFields = String.join(".", components.subList(1, components.size()));
        Preconditions.checkNotNull(fieldTree.get(root)).add(nestedFields);
      }
    }
    return fieldTree;
  }

  /**
   * Returns a new {@link Row} containing only the fields that intersect with the new {@link Schema}
   * Relies on a previous step to have validated the compatibility of the new {@link Schema}.
   */
  @VisibleForTesting
  @Nullable
  static Row copyWithNewSchema(@Nullable Row row, Schema newSchema) {
    if (row == null) {
      return null;
    }
    Map<String, Object> values = new HashMap<>(newSchema.getFieldCount());

    for (Schema.Field field : newSchema.getFields()) {
      String name = field.getName();
      Object value = row.getValue(name);
      if (field.getType().getTypeName().equals(Schema.TypeName.ROW)) {
        Schema nestedRowSchema = Preconditions.checkNotNull(field.getType().getRowSchema());
        value = copyWithNewSchema(row.getRow(name), nestedRowSchema);
      }
      if (value != null) {
        values.put(name, value);
      }
    }
    return Row.withSchema(newSchema).withFieldValues(values).build();
  }

  /**
   * Returns a new {@link Schema} with the specified fields removed.
   *
   * <p>No guarantee that field ordering will remain the same.
   */
  @VisibleForTesting
  static Schema elideFields(Schema schema, List<String> fieldsToElide) {
    if (fieldsToElide.isEmpty()) {
      return schema;
    }
    List<Schema.Field> newFieldsList = new ArrayList<>(schema.getFields());
    Map<String, List<String>> fieldTree = getFieldTree(fieldsToElide);

    for (Map.Entry<String, List<String>> fieldAndDescendents : fieldTree.entrySet()) {
      String root = fieldAndDescendents.getKey();
      List<String> nestedFields = fieldAndDescendents.getValue();
      Schema.Field fieldToRemove = schema.getField(root);
      Schema.FieldType typeToRemove = fieldToRemove.getType();

      // Base case: we're at the specified field to remove.
      if (nestedFields.isEmpty()) {
        newFieldsList.remove(fieldToRemove);
      } else {
        // Otherwise, we're asked to remove a nested field. Verify current field is ROW type
        Preconditions.checkArgument(
            typeToRemove.getTypeName().equals(Schema.TypeName.ROW),
            "Expected type %s for specified nested field '%s', but instead got type %s.",
            Schema.TypeName.ROW,
            root,
            typeToRemove.getTypeName());

        Schema nestedSchema = Preconditions.checkNotNull(typeToRemove.getRowSchema());
        Schema newNestedSchema = elideFields(nestedSchema, nestedFields);
        Schema.Field modifiedField =
            Schema.Field.of(root, Schema.FieldType.row(newNestedSchema))
                .withNullable(typeToRemove.getNullable());

        // Replace with modified field
        newFieldsList.set(newFieldsList.indexOf(fieldToRemove), modifiedField);
      }
    }
    return new Schema(newFieldsList);
  }

  /**
   * Returns a new {@link Schema} with only the specified fields kept.
   *
   * <p>No guarantee that field ordering will remain the same.
   */
  @VisibleForTesting
  static Schema keepFields(Schema schema, List<String> fieldsToKeep) {
    if (fieldsToKeep.isEmpty()) {
      return schema;
    }
    List<Schema.Field> newFieldsList = new ArrayList<>(fieldsToKeep.size());
    Map<String, List<String>> fieldTree = getFieldTree(fieldsToKeep);

    for (Map.Entry<String, List<String>> fieldAndDescendents : fieldTree.entrySet()) {
      String root = fieldAndDescendents.getKey();
      List<String> nestedFields = fieldAndDescendents.getValue();
      Schema.Field fieldToKeep = schema.getField(root);
      Schema.FieldType typeToKeep = fieldToKeep.getType();

      // Base case: we're at the specified field to keep, and we can skip this conditional.
      // Otherwise: we're asked to keep a nested field, so we dig deeper to determine which nested
      // fields to keep
      if (!nestedFields.isEmpty()) {
        Preconditions.checkArgument(
            typeToKeep.getTypeName().equals(Schema.TypeName.ROW),
            "Expected type %s for specified nested field '%s', but instead got type %s.",
            Schema.TypeName.ROW,
            root,
            typeToKeep.getTypeName());

        Schema nestedSchema = Preconditions.checkNotNull(typeToKeep.getRowSchema());
        Schema newNestedSchema = keepFields(nestedSchema, nestedFields);
        fieldToKeep =
            Schema.Field.of(root, Schema.FieldType.row(newNestedSchema))
                .withNullable(typeToKeep.getNullable());
      }
      newFieldsList.add(fieldToKeep);
    }

    return new Schema(newFieldsList);
  }

  /**
   * Performs a filter operation (keep or elide) on the input {@link Row}. Must have already
   * configured a filter operation with {@link #eliding(List)} or {@link #keeping(List)} for this
   * {@link RowFilter}.
   */
  public Row filter(Row row) {
    Preconditions.checkState(
        row.getSchema().assignableTo(rowSchema),
        "Encountered Row with schema that is incompatible with this RowFilter's schema.\nRow schema: %s\nSchema used to initialize this RowFilter: %s",
        row.getSchema(),
        rowSchema);
    Schema newSchema =
        Preconditions.checkNotNull(
            transformedSchema,
            "This RowFilter was not set up to filter fields. Please configure using eliding() or keeping().");

    return Preconditions.checkNotNull(copyWithNewSchema(row, newSchema));
  }
}
