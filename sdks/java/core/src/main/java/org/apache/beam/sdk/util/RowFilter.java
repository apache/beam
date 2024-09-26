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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A utility that filters fields from Beam {@link Row}s. This filter can be configured to indicate
 * what fields you would like to either <strong>keep</strong> or <strong>drop</strong>. You may also
 * specify a singular {@link Row} field to extract with <strong>only</strong>. Afterward, call
 * {@link #filter(Row)} on a Schema-compatible Row to filter it. An un-configured filter will simply
 * return the input row untouched.
 *
 * <p>A configured {@link RowFilter} will naturally produce {@link Row}s with a new Beam {@link
 * Schema}. You can access this new Schema via the filter's {@link #outputSchema()}.
 *
 * <p>Configure a {@link RowFilter} as follows:
 *
 * <pre>{@code
 * // this is an un-configured filter
 * RowFilter unconfigured = new RowFilter(beamSchema);
 *
 * // this filter will exclusively keep these fields and drop everything else
 * List<String> fields = Arrays.asList("foo", "bar", "baz");
 * RowFilter keepingFilter = new RowFilter(beamSchema).keeping(fields);
 *
 * // this filter will drop these fields
 * RowFilter droppingFilter = new RowFilter(beamSchema).dropping(fields);
 *
 * // this filter will only output the contents of row field "my_record"
 * String field = "my_record";
 * RowFilter onlyFilter = new RowFilter(beamSchema).only(field);
 *
 * // produces a filtered row
 * Row outputRow = keepingFilter.filter(row);
 * }</pre>
 *
 * Check the documentation for {@link #keeping(List)}, {@link #dropping(List)}, and {@link
 * #only(String)} for further details on what an output Row can look like.
 */
public class RowFilter implements Serializable {
  private final Schema rowSchema;
  private @Nullable Schema transformedSchema;
  // for 'only' case
  private @Nullable String onlyField;

  public RowFilter(Schema rowSchema) {
    this.rowSchema = rowSchema;
  }

  /**
   * Configures this {@link RowFilter} to filter {@link Row}s by keeping only the specified fields.
   * Nested fields can be specified using dot-notation.
   *
   * <p>For example, if we want to keep the list of fields {@code ["foo", "baz"]}, for the input
   * {@link Row}:
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
   *   nested_2: xyz
   * }</pre>
   */
  public RowFilter keeping(List<String> fields) {
    checkUnconfigured();
    verifyNoNestedFields(fields, "keep");
    validateSchemaContainsFields(rowSchema, fields, "keep");
    transformedSchema = keepFields(rowSchema, fields);
    return this;
  }

  /**
   * Configures this {@link RowFilter} to filter {@link Row} by removing the specified fields.
   * Nested fields can be specified using dot-notation.
   *
   * <p>For example, if we want to drop the list of fields {@code ["foo", "baz"]}, for this input
   * {@link Row}:
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
   * }</pre>
   */
  public RowFilter dropping(List<String> fields) {
    checkUnconfigured();
    verifyNoNestedFields(fields, "drop");
    validateSchemaContainsFields(rowSchema, fields, "drop");
    transformedSchema = dropFields(rowSchema, fields);
    return this;
  }

  /**
   * Configures this {@link RowFilter} to only output the contents of a single row field.
   *
   * <p>For example, if we want to only extract the contents of field "foo" for this input {@link
   * Row}:
   *
   * <pre>{@code
   * abc: 123
   * bar: my_str
   * foo:
   *   xyz:
   *     baz: 456
   *     qwe: 789
   * }</pre>
   *
   * we will get the following output {@link Row}:
   *
   * <pre>{@code
   * xyz:
   *   baz: 456
   *   qwe: 789
   * }</pre>
   *
   * <p>Note that this will fail if the field is not of type {@link Row}, e.g. if {@code "abc"} is
   * specified for the example above.
   */
  public RowFilter only(String field) {
    checkUnconfigured();
    validateSchemaContainsFields(rowSchema, Collections.singletonList(field), "only");
    Schema.Field rowField = rowSchema.getField(field);
    Preconditions.checkArgument(
        rowField.getType().getTypeName().equals(Schema.TypeName.ROW),
        "Expected type '%s' for field '%s', but instead got type '%s'.",
        Schema.TypeName.ROW,
        rowField.getName(),
        rowField.getType().getTypeName());

    transformedSchema = rowField.getType().getRowSchema();
    onlyField = field;
    return this;
  }

  /**
   * Performs a filter operation (keep or drop) on the input {@link Row}. Must have already
   * configured a filter operation with {@link #dropping(List)} or {@link #keeping(List)} for this
   * {@link RowFilter}.
   *
   * <p>If not yet configured, will simply return the same {@link Row}.
   */
  public Row filter(Row row) {
    if (transformedSchema == null) {
      return row;
    }

    Preconditions.checkState(
        row.getSchema().assignableTo(rowSchema),
        "Encountered Row with schema that is incompatible with this RowFilter's schema."
            + "\nRow schema: %s"
            + "\nSchema used to initialize this RowFilter: %s",
        row.getSchema(),
        rowSchema);

    // 'only' case
    if (onlyField != null) {
      return checkStateNotNull(row.getRow(onlyField));
    }

    // 'keep' and 'drop'
    return Preconditions.checkNotNull(copyWithNewSchema(row, outputSchema()));
  }

  /** Returns the output {@link Row}'s {@link Schema}. */
  public Schema outputSchema() {
    return transformedSchema != null ? transformedSchema : rowSchema;
  }

  private void checkUnconfigured() {
    Preconditions.checkState(
        transformedSchema == null,
        "This RowFilter has already been configured to filter to the following Schema: %s",
        transformedSchema);
  }

  /** Verifies that this selection contains no nested fields. */
  private void verifyNoNestedFields(List<String> fields, String operation) {
    List<String> nestedFields = new ArrayList<>();
    for (String field : fields) {
      if (field.contains(".")) {
        nestedFields.add(field);
      }
    }
    if (!nestedFields.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "RowFilter does not support specifying nested fields to %s: %s",
              operation, nestedFields));
    }
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
      String message = "Validation failed for '" + operation + "'.";
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
  static Schema dropFields(Schema schema, List<String> fieldsToDrop) {
    if (fieldsToDrop.isEmpty()) {
      return schema;
    }
    List<Schema.Field> newFieldsList = new ArrayList<>(schema.getFields());
    Map<String, List<String>> fieldTree = getFieldTree(fieldsToDrop);

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
        Schema newNestedSchema = dropFields(nestedSchema, nestedFields);
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
}
