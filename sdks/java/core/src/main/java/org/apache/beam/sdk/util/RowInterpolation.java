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

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RowInterpolation {
  private static final Pattern REPLACE_PATTERN = Pattern.compile("\\{(.+?)}");
  private final String template;
  private final Set<String> fieldsToReplace;
  private final Schema rowSchema;
  private @Nullable Schema transformedSchema;

  public RowInterpolation(String template, Schema rowSchema) {
    this.template = template;
    this.rowSchema = rowSchema;

    Matcher m = REPLACE_PATTERN.matcher(template);
    fieldsToReplace = new HashSet<>();
    while (m.find()) {
      fieldsToReplace.add(StringUtils.strip(m.group(), "{}"));
    }

    validateSchemaContainsFields(
        rowSchema, new ArrayList<>(fieldsToReplace), "string interpolation");
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
            "\nThe following specified fields are not of type Row, so could not reach nested fields: "
                + notRowField;
      }
      throw new IllegalArgumentException(message);
    }
  }

  /** Performs string interpolation on the template using values from the input {@link Row}. */
  public String interpolateFrom(Row row) {
    String interpolated = this.template;
    for (String field : fieldsToReplace) {
      List<String> levels = Splitter.on(".").splitToList(field);

      Object val = MoreObjects.firstNonNull(getValue(row, levels), "");

      interpolated = interpolated.replace("{" + field + "}", String.valueOf(val));
    }
    return interpolated;
  }

  private @Nullable Object getValue(@Nullable Row row, List<String> fieldLevels) {
    if (row == null) {
      return null;
    }
    if (fieldLevels.size() == 1) {
      return row.getValue(fieldLevels.get(0));
    }
    return getValue(row.getRow(fieldLevels.get(0)), fieldLevels.subList(1, fieldLevels.size()));
  }

  /**
   * Configures {@link RowInterpolation} to filter {@link Row} by keeping only the specified fields.
   * Nested fields can be specified using dot-notation. For example, with a list of fields to keep
   * {@code ["foo", "baz.nested_1"]}, and an input {@link Row}:
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
  public RowInterpolation keeping(List<String> fields) {
    Preconditions.checkState(
        transformedSchema == null,
        "This RowInterpolation object has already been configured to filter to the following Schema: %s",
        transformedSchema);
    validateSchemaContainsFields(rowSchema, fields, "\"keep\"");
    transformedSchema = keepFields(rowSchema, fields);
    return this;
  }

  /**
   * Configures {@link RowInterpolation} to filter {@link Row} by removing the specified fields.
   * Nested fields can be specified using dot-notation. For example, with a list of fields to elide
   * {@code ["foo", "baz.nested_1"]}, and an input {@link Row}:
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
  public RowInterpolation eliding(List<String> fields) {
    Preconditions.checkState(
        transformedSchema == null,
        "This RowInterpolation object has already been configured to filter to the following Schema: %s",
        transformedSchema);
    validateSchemaContainsFields(rowSchema, fields, "\"elide\"");
    transformedSchema = elideFields(rowSchema, fields);
    return this;
  }

  /**
   * Creates a field tree, separating each top level field from its (potential) nested fields. E.g.
   * ["foo.bar.baz", "foo.abc", "xyz"] --> {"foo": ["bar.baz", "abc"], "xyz": []}
   */
  @VisibleForTesting
  Map<String, List<String>> getFieldTree(List<String> fields) {
    Map<String, List<String>> fieldTree = Maps.newHashMap();

    for (String field : fields) {
      List<String> components = Splitter.on(".").splitToList(field);
      String root = components.get(0);
      fieldTree.computeIfAbsent(root, r -> new ArrayList<>());

      if (components.size() > 1) {
        String nestedField = String.join(".", components.subList(1, components.size()));
        Preconditions.checkNotNull(fieldTree.get(root)).add(nestedField);
      }
    }
    return fieldTree;
  }

  /**
   * Returns a new {@link Row} with just the fields in a new {@link Schema}. Relies on a previous
   * step to have validated the compatibility of the new {@link Schema}.
   */
  @VisibleForTesting
  @Nullable
  Row copyWithNewSchema(@Nullable Row row, Schema newSchema) {
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

  /** Returns a new {@link Schema} with the specified fields removed. */
  @VisibleForTesting
  Schema elideFields(Schema schema, List<String> fieldsToElide) {
    if (fieldsToElide.isEmpty()) {
      return schema;
    }
    List<Schema.Field> newFieldsList = new ArrayList<>(schema.getFields());
    Map<String, List<String>> fieldTree = getFieldTree(fieldsToElide);

    for (Map.Entry<String, List<String>> fieldAndDescendents : fieldTree.entrySet()) {
      String root = fieldAndDescendents.getKey();
      List<String> nestedFields = fieldAndDescendents.getValue();
      Schema.Field fieldToRemove = schema.getField(root);

      // Base case: we're at the specified field to remove.
      if (nestedFields.isEmpty()) {
        newFieldsList.remove(fieldToRemove);
      } else {
        // Otherwise, we're asked to remove a nested field. Verify current field is ROW type
        Preconditions.checkArgument(
            fieldToRemove.getType().getTypeName().equals(Schema.TypeName.ROW),
            "Expected type %s for specified nested field '%s', but instead got type %s.",
            Schema.TypeName.ROW,
            root,
            fieldToRemove.getType().getTypeName());

        Schema nestedSchema = Preconditions.checkNotNull(fieldToRemove.getType().getRowSchema());
        Schema newNestedSchema = elideFields(nestedSchema, nestedFields);
        Schema.Field modifiedField = Schema.Field.of(root, Schema.FieldType.row(newNestedSchema));

        // Replace with modified field
        newFieldsList.set(newFieldsList.indexOf(fieldToRemove), modifiedField);
      }
    }
    return new Schema(newFieldsList);
  }

  /** Returns a new {@link Schema} with only the specified fields kept. */
  @VisibleForTesting
  Schema keepFields(Schema schema, List<String> fieldsToKeep) {
    if (fieldsToKeep.isEmpty()) {
      return schema;
    }
    List<Schema.Field> newFieldsList = new ArrayList<>(fieldsToKeep.size());
    Map<String, List<String>> fieldTree = getFieldTree(fieldsToKeep);

    for (Map.Entry<String, List<String>> fieldAndDescendents : fieldTree.entrySet()) {
      String root = fieldAndDescendents.getKey();
      List<String> nestedFields = fieldAndDescendents.getValue();
      Schema.Field fieldToKeep = schema.getField(root);

      // Base case: we're at the specified field to keep, and we can skip this conditional.
      // Otherwise: we're asked to keep a nested field, so we dig deeper to determine which nested
      // fields to keep
      if (!nestedFields.isEmpty()) {
        Preconditions.checkArgument(
            fieldToKeep.getType().getTypeName().equals(Schema.TypeName.ROW),
            "Expected type %s for specified nested field '%s', but instead got type %s.",
            Schema.TypeName.ROW,
            root,
            fieldToKeep.getType().getTypeName());

        Schema nestedSchema = Preconditions.checkNotNull(fieldToKeep.getType().getRowSchema());
        Schema newNestedSchema = keepFields(nestedSchema, nestedFields);
        fieldToKeep = Schema.Field.of(root, Schema.FieldType.row(newNestedSchema));
      }
      newFieldsList.add(fieldToKeep);
    }

    return new Schema(newFieldsList);
  }

  /**
   * Performs a filter operation (keep or elide) on the input {@link Row}. Must have already configured a filter
   * operation with {@link #eliding(List)} or {@link #keeping(List)} for this {@link RowInterpolation}.
   */
  public Row filterRow(Row row) {
    Schema newSchema =
        Preconditions.checkNotNull(
            transformedSchema,
            "This RowInterpolation object was not set up to filter fields. Please configure using eliding() or keeping().");

    return Preconditions.checkNotNull(copyWithNewSchema(row, newSchema));
  }
}
