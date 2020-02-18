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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FieldAccess;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

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
    return new Fields<>(FieldAccessDescriptor.create());
  }

  /** Select a set of top-level field ids from the row. */
  public static <T> Fields<T> fieldIds(Integer... ids) {
    return new Fields<>(FieldAccessDescriptor.withFieldIds(ids));
  }

  /** Select a set of top-level field names from the row. */
  public static <T> Fields<T> fieldNames(String... names) {
    return new Fields<>(FieldAccessDescriptor.withFieldNames(names));
  }

  /**
   * Select a set of fields described in a {@link FieldAccessDescriptor}.
   *
   * <p>This allows for nested fields to be selected as well.
   */
  public static <T> Fields<T> fieldAccess(FieldAccessDescriptor fieldAccessDescriptor) {
    return new Fields<>(fieldAccessDescriptor);
  }

  /**
   * Selects every leaf-level field. This results in a a nested schema being flattened into a single
   * top-level schema. By default nested field names will be concatenated with _ characters, though
   * this can be overridden using {@link Flattened#keepMostNestedFieldName()} and {@link
   * Flattened#withFieldNameAs}.
   */
  public static <T> Flattened<T> flattenedSchema() {
    return new Flattened<>();
  }

  private static class SelectDoFn<T> extends DoFn<T, Row> {
    private FieldAccessDescriptor fieldAccessDescriptor;
    private Schema inputSchema;
    private Schema outputSchema;

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
    }

    @ProcessElement
    public void process(@FieldAccess("selectFields") @Element Row row, OutputReceiver<Row> r) {
      r.output(SelectHelpers.selectRow(row, fieldAccessDescriptor, inputSchema, outputSchema));
    }
  }

  public static class Fields<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private FieldAccessDescriptor fieldAccessDescriptor;

    public Fields(FieldAccessDescriptor fieldAccessDescriptor) {
      this.fieldAccessDescriptor = fieldAccessDescriptor;
    }

    /**
     * Add a single field to the selection, along with the name the field should take in the
     * selected schema. This Allows easily renaming fields while doing a select. In cases where the
     * output of a select would otherwise contain conflicting names, this function must be used to
     * rename one of the fields.
     */
    public Fields<T> withFieldNameAs(String fieldName, String fieldRename) {
      return new Fields<>(fieldAccessDescriptor.withFieldNameAs(fieldName, fieldRename));
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(inputSchema);
      Schema outputSchema = SelectHelpers.getOutputSchema(inputSchema, resolved);
      return input
          .apply(ParDo.of(new SelectDoFn<>(resolved, inputSchema, outputSchema)))
          .setRowSchema(outputSchema);
    }
  }

  /** A {@link PTransform} representing a flattened schema. */
  public static class Flattened<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private SerializableFunction<List<String>, String> nameFn;
    private Map<String, String> nameOverrides;

    Flattened() {
      this(CONCAT_FIELD_NAMES, Collections.emptyMap());
    }

    Flattened(
        SerializableFunction<List<String>, String> nameFn, Map<String, String> nameOverrides) {
      this.nameFn = nameFn;
      this.nameOverrides = nameOverrides;
    }

    /**
     * For nested fields, concatenate all the names separated by a _ character in the flattened
     * schema.
     */
    public Flattened<T> concatFieldNames() {
      return new Flattened<>(CONCAT_FIELD_NAMES, nameOverrides);
    }

    /**
     * For nested fields, keep just the most-nested field name. Will fail if this name is not
     * unique.
     */
    public Flattened<T> keepMostNestedFieldName() {
      return new Flattened<>(KEEP_NESTED_NAME, nameOverrides);
    }

    /**
     * Allows renaming a specific nested field. Can be used if two fields resolve to the same name
     * with the default name policies.
     */
    public Flattened<T> withFieldNameAs(String fieldName, String fieldRename) {
      return new Flattened<>(
          nameFn,
          ImmutableMap.<String, String>builder()
              .putAll(nameOverrides)
              .put(fieldName, fieldRename)
              .build());
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();

      FieldAccessDescriptor fieldAccessDescriptor =
          SelectHelpers.allLeavesDescriptor(
              inputSchema,
              n ->
                  MoreObjects.firstNonNull(
                      nameOverrides.get(String.join(".", n)), nameFn.apply(n)));
      Schema outputSchema = SelectHelpers.getOutputSchema(inputSchema, fieldAccessDescriptor);
      return input
          .apply(ParDo.of(new SelectDoFn<>(fieldAccessDescriptor, inputSchema, outputSchema)))
          .setRowSchema(outputSchema);
    }
  }
}
