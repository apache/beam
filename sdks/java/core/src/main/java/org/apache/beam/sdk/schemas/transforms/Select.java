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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

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
public class Select<T> extends PTransform<PCollection<T>, PCollection<Row>> {
  private final FieldAccessDescriptor fieldAccessDescriptor;

  private Select(FieldAccessDescriptor fieldAccessDescriptor) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
  }

  /** Select a set of top-level field ids from the row. */
  public static <T> Select<T> fieldIds(Integer... ids) {
    return new Select<>(FieldAccessDescriptor.withFieldIds(ids));
  }

  /** Select a set of top-level field names from the row. */
  public static <T> Select<T> fieldNames(String... names) {
    return new Select<>(FieldAccessDescriptor.withFieldNames(names));
  }

  /**
   * Select a set of fields described in a {@link FieldAccessDescriptor}.
   *
   * <p>This allows for nested fields to be selected as well.
   */
  public static <T> Select<T> fieldAccess(FieldAccessDescriptor fieldAccessDescriptor) {
    return new Select<>(fieldAccessDescriptor);
  }

  @Override
  public PCollection<Row> expand(PCollection<T> input) {
    Schema inputSchema = input.getSchema();
    FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(inputSchema);
    Schema outputSchema = SelectHelpers.getOutputSchema(inputSchema, resolved);

    return input
        .apply(
            ParDo.of(
                new DoFn<T, Row>() {
                  // TODO: This should be the same as resolved so that Beam knows which fields
                  // are being accessed. Currently Beam only supports wildcard descriptors.
                  // Once BEAM-4457 is fixed, fix this.
                  @FieldAccess("selectFields")
                  final FieldAccessDescriptor fieldAccessDescriptor =
                      FieldAccessDescriptor.withAllFields();

                  @ProcessElement
                  public void process(
                      @FieldAccess("selectFields") @Element Row row, OutputReceiver<Row> r) {
                    r.output(SelectHelpers.selectRow(row, resolved, inputSchema, outputSchema));
                  }
                }))
        .setRowSchema(outputSchema);
  }
}
