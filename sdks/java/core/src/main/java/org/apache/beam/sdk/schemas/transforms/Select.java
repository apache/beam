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
package org.apache.beam.sdk.schemas.transforms;

import static org.apache.beam.sdk.schemas.utils.SelectHelpers.CONCAT_FIELD_NAMES;
import static org.apache.beam.sdk.schemas.utils.SelectHelpers.KEEP_NESTED_NAME;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.schemas.utils.SelectHelpers.RowSelectorContainer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} for selecting a subset of fields from a schema type.
 *
 * <p>This transforms allows projecting out a subset of fields from a schema type. The output of
 * this transform is of type {@link Row}, though that can be converted into any other type with
 * matching schema using the {@link Convert} transform.
 *
 * <p>For example, consider the following POJO type:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class UserEvent {
 *   public String userId;
 *   public String eventId;
 *   public int eventType;
 *   public Location location;
 * }}</pre>
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class Location {
 *   public double latitude;
 *   public double longtitude;
 * }}</pre>
 *
 * Say you want to select just the set of userId, eventId pairs from each element, you would write
 * the following:
 *
 * <pre>{@code
 * PCollection<UserEvent> events = readUserEvents();
 * PCollection<Row> rows = event.apply(Select.fieldNames("userId", "eventId"));
 * }</pre>
 *
 * It's possible to select a nested field as well. For example, if you want just the location
 * information from each element:
 *
 * <pre>{@code
 * PCollection<UserEvent> events = readUserEvents();
 * PCollection<Location> rows = event.apply(Select.fieldNames("location")
 *                              .apply(Convert.to(Location.class));
 * }</pre>
 */
@Experimental(Kind.SCHEMAS)
public class Select {
  public static <T> Fields<T> create() {
    return fieldAccess(FieldAccessDescriptor.create());
  }

  /** Select a set of top-level field ids from the row. */
  public static <T> Fields<T> fieldIds(Integer... ids) {
    return fieldAccess(FieldAccessDescriptor.withFieldIds(ids));
  }

  /** Select a set of top-level field names from the row. */
  public static <T> Fields<T> fieldNames(String... names) {
    return fieldAccess(FieldAccessDescriptor.withFieldNames(names));
  }

  /**
   * Select a set of fields described in a {@link FieldAccessDescriptor}.
   *
   * <p>This allows for nested fields to be selected as well.
   */
  public static <T> Fields<T> fieldAccess(FieldAccessDescriptor fieldAccessDescriptor) {
    return new AutoValue_Select_Fields.Builder<T>()
        .setFieldAccessDescriptor(fieldAccessDescriptor)
        .build();
  }

  /**
   * Selects every leaf-level field. This results in a a nested schema being flattened into a single
   * top-level schema. By default nested field names will be concatenated with _ characters, though
   * this can be overridden using {@link Flattened#keepMostNestedFieldName()} and {@link
   * Flattened#withFieldNameAs}.
   */
  public static <T> Flattened<T> flattenedSchema() {
    return new AutoValue_Select_Flattened.Builder<T>()
        .setNameFn(CONCAT_FIELD_NAMES)
        .setNameOverrides(Collections.emptyMap())
        .build();
  }

  private static class SelectDoFn<T> extends DoFn<T, Row> {
    private final FieldAccessDescriptor fieldAccessDescriptor;
    private final Schema inputSchema;
    private final Schema outputSchema;
    RowSelector rowSelector;

    // TODO: This should be the same as resolved so that Beam knows which fields
    // are being accessed. Currently Beam only supports wildcard descriptors.
    // Once BEAM-4457 is fixed, fix this.
    @FieldAccess("selectFields")
    final FieldAccessDescriptor fieldAccess = FieldAccessDescriptor.withAllFields();

    public SelectDoFn(
        FieldAccessDescriptor fieldAccessDescriptor, Schema inputSchema, Schema outputSchema) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
      this.rowSelector = new RowSelectorContainer(inputSchema, fieldAccessDescriptor, true);
    }

    @ProcessElement
    public void process(@FieldAccess("selectFields") @Element Row row, OutputReceiver<Row> r) {
      r.output(rowSelector.select(row));
    }
  }

  @AutoValue
  public abstract static class Fields<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    abstract FieldAccessDescriptor getFieldAccessDescriptor();

    abstract @Nullable Schema getOutputSchema();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor);

      abstract Builder<T> setOutputSchema(Schema outputSchema);

      abstract Fields<T> build();
    }

    abstract Builder<T> toBuilder();

    /**
     * Add a single field to the selection, along with the name the field should take in the
     * selected schema. This Allows easily renaming fields while doing a select. In cases where the
     * output of a select would otherwise contain conflicting names, this function must be used to
     * rename one of the fields.
     */
    public Fields<T> withFieldNameAs(String fieldName, String fieldRename) {
      return toBuilder()
          .setFieldAccessDescriptor(
              getFieldAccessDescriptor().withFieldNameAs(fieldName, fieldRename))
          .build();
    }

    /**
     * Rename all output fields to match the specified schema. If the specified schema is not
     * compatible with the output schema a failure will be raised.
     */
    public Fields<T> withOutputSchema(Schema schema) {
      return toBuilder().setOutputSchema(schema).build();
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      FieldAccessDescriptor resolved = getFieldAccessDescriptor().resolve(inputSchema);
      Schema outputSchema = getOutputSchema();
      if (outputSchema == null) {
        outputSchema = SelectHelpers.getOutputSchema(inputSchema, resolved);
      } else {
        inputSchema = uniquifyNames(inputSchema);
        Schema inferredSchema = SelectHelpers.getOutputSchema(inputSchema, resolved);
        Preconditions.checkArgument(
            outputSchema.typesEqual(inferredSchema),
            "Types not equal. provided output schema: "
                + outputSchema
                + " Schema inferred from select: "
                + inferredSchema
                + " from input type: "
                + input.getSchema());
      }
      return input
          .apply(ParDo.of(new SelectDoFn<>(resolved, inputSchema, outputSchema)))
          .setRowSchema(outputSchema);
    }
  }

  private static Schema uniquifyNames(Schema schema) {
    Schema.Builder builder = new Schema.Builder();
    for (Field field : schema.getFields()) {
      builder.addField(UUID.randomUUID().toString(), uniquifyNames(field.getType()));
    }
    return builder.build();
  }

  private static FieldType uniquifyNames(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ROW:
        return FieldType.row(uniquifyNames(fieldType.getRowSchema()))
            .withNullable(fieldType.getNullable())
            .withMetadata(fieldType.getAllMetadata());
      case ARRAY:
        return FieldType.array(uniquifyNames(fieldType.getCollectionElementType()));
      case ITERABLE:
        return FieldType.iterable(uniquifyNames(fieldType.getCollectionElementType()));
      case MAP:
        return FieldType.map(
            uniquifyNames(fieldType.getMapKeyType()), uniquifyNames(fieldType.getMapValueType()));
      default:
        return fieldType;
    }
  }
  /** A {@link PTransform} representing a flattened schema. */
  @AutoValue
  public abstract static class Flattened<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    abstract SerializableFunction<List<String>, String> getNameFn();

    abstract Map<String, String> getNameOverrides();

    abstract @Nullable Schema getOutputSchema();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setNameFn(SerializableFunction<List<String>, String> nameFn);

      abstract Builder<T> setNameOverrides(Map<String, String> nameOverrides);

      abstract Builder<T> setOutputSchema(@Nullable Schema schema);

      abstract Flattened<T> build();
    }

    abstract Builder<T> toBuilder();

    /**
     * For nested fields, concatenate all the names separated by a _ character in the flattened
     * schema.
     */
    public Flattened<T> concatFieldNames() {
      return toBuilder().setNameFn(CONCAT_FIELD_NAMES).build();
    }

    /**
     * For nested fields, keep just the most-nested field name. Will fail if this name is not
     * unique.
     */
    public Flattened<T> keepMostNestedFieldName() {
      return toBuilder().setNameFn(KEEP_NESTED_NAME).build();
    }

    /**
     * Allows renaming a specific nested field. Can be used if two fields resolve to the same name
     * with the default name policies.
     */
    public Flattened<T> withFieldNameAs(String fieldName, String fieldRename) {
      Map<String, String> overrides =
          ImmutableMap.<String, String>builder()
              .putAll(getNameOverrides())
              .put(fieldName, fieldRename)
              .build();
      return toBuilder().setNameOverrides(overrides).build();
    }

    /**
     * Rename all output fields to match the specified schema. If the specified schema is not
     * compatible with the output schema a failure will be raised.
     */
    public Flattened<T> withOutputSchema(Schema schema) {
      return toBuilder().setOutputSchema(schema).build();
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();

      FieldAccessDescriptor fieldAccessDescriptor =
          SelectHelpers.allLeavesDescriptor(
              inputSchema,
              n ->
                  MoreObjects.firstNonNull(
                      getNameOverrides().get(String.join(".", n)), getNameFn().apply(n)));
      Schema inferredOutputSchema =
          SelectHelpers.getOutputSchema(inputSchema, fieldAccessDescriptor);
      Schema outputSchema = getOutputSchema();
      if (outputSchema != null) {
        Preconditions.checkArgument(outputSchema.typesEqual(inferredOutputSchema));
      } else {
        outputSchema = inferredOutputSchema;
      }
      return input
          .apply(ParDo.of(new SelectDoFn<>(fieldAccessDescriptor, inputSchema, outputSchema)))
          .setRowSchema(outputSchema);
    }
  }
}
