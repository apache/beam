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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} for filtering a collection of schema types.
 *
 * <p>Separate Predicates can be registered for different schema fields, and the result is allowed
 * to pass if all predicates return true. The output type is the same as the input type.
 *
 * <p>For example, consider the following schema type:
 *
 * <pre>{@code
 * public class Location {
 *   public double latitude;
 *   public double longitude;
 * }
 * }</pre>
 *
 * In order to examine only locations in south Manhattan, you would write:
 *
 * <pre>{@code
 * PCollection<Location> locations = readLocations();
 * locations.apply(Filter
 *    .whereFieldName("latitude", lat -> lat < 40.720 && lat > 40.699)
 *    .whereFieldName("longitude", long -> long < -73.969 && long > -74.747));
 * }</pre>
 *
 * Predicates that require examining multiple fields at once are also supported. For example,
 * consider the following class representing a user account:
 *
 * <pre>{@code
 * class UserAccount {
 *   public double spendOnBooks;
 *   public double spendOnMovies;
 *       ...
 * }
 * }</pre>
 *
 * Say you want to examine only users whos total spend is above $100. You could write:
 *
 * <pre>{@code
 * PCollection<UserAccount> users = readUsers();
 * users.apply(Filter
 *    .whereFieldNames(Lists.newArrayList("spendOnBooks", "spendOnMovies"),
 *        row -> return row.getDouble("spendOnBooks") + row.getDouble("spendOnMovies") > 100.00));
 * }</pre>
 */
@Experimental(Kind.SCHEMAS)
public class Filter {
  public static <T> Inner<T> create() {
    return new Inner<T>();
  }

  /** Implementation of the filter. */
  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private RowSelector rowSelector;

    @AutoValue
    abstract static class FilterDescription<FieldT> implements Serializable {
      abstract FieldAccessDescriptor getFieldAccessDescriptor();

      abstract SerializableFunction<FieldT, Boolean> getPredicate();

      abstract @Nullable Schema getSelectedSchema();

      abstract boolean getSelectsSingleField();

      abstract @Nullable Schema getInputSchema();

      abstract Builder<FieldT> toBuilder();

      @AutoValue.Builder
      abstract static class Builder<FieldT> {
        abstract Builder<FieldT> setFieldAccessDescriptor(
            FieldAccessDescriptor fieldAccessDescriptor);

        abstract Builder<FieldT> setPredicate(SerializableFunction<FieldT, Boolean> predicate);

        abstract Builder<FieldT> setSelectedSchema(@Nullable Schema selectedSchema);

        abstract Builder<FieldT> setSelectsSingleField(boolean unbox);

        abstract Builder<FieldT> setInputSchema(@Nullable Schema inputSchema);

        abstract FilterDescription<FieldT> build();
      }

      transient RowSelector rowSelector;

      public RowSelector getRowSelector() {
        if (rowSelector == null) {
          rowSelector =
              SelectHelpers.getRowSelectorOptimized(getInputSchema(), getFieldAccessDescriptor());
        }
        return rowSelector;
      }
    }

    private final List<FilterDescription<?>> filters = Lists.newArrayList();

    /** Set a predicate based on the value of a field, where the field is specified by name. */
    public <FieldT> Inner<T> whereFieldName(
        String fieldName, SerializableFunction<FieldT, Boolean> predicate) {
      filters.add(
          new AutoValue_Filter_Inner_FilterDescription.Builder<FieldT>()
              .setFieldAccessDescriptor(FieldAccessDescriptor.withFieldNames(fieldName))
              .setPredicate(predicate)
              .setSelectsSingleField(true)
              .build());
      return this;
    }

    /** Set a predicate based on the value of a field, where the field is specified by id. */
    public <FieldT> Inner<T> whereFieldId(
        int fieldId, SerializableFunction<FieldT, Boolean> predicate) {
      filters.add(
          new AutoValue_Filter_Inner_FilterDescription.Builder<FieldT>()
              .setFieldAccessDescriptor(FieldAccessDescriptor.withFieldIds(fieldId))
              .setPredicate(predicate)
              .setSelectsSingleField(true)
              .build());
      return this;
    }

    /** Set a predicate based on the value of multipled fields, specified by name. */
    public Inner<T> whereFieldNames(
        List<String> fieldNames, SerializableFunction<Row, Boolean> predicate) {
      filters.add(
          new AutoValue_Filter_Inner_FilterDescription.Builder<Row>()
              .setFieldAccessDescriptor(FieldAccessDescriptor.withFieldNames(fieldNames))
              .setPredicate(predicate)
              .setSelectsSingleField(false)
              .build());
      return this;
    }

    /** Set a predicate based on the value of multipled fields, specified by id. */
    public Inner<T> whereFieldIds(
        List<Integer> fieldIds, SerializableFunction<Row, Boolean> predicate) {
      filters.add(
          new AutoValue_Filter_Inner_FilterDescription.Builder<Row>()
              .setFieldAccessDescriptor(FieldAccessDescriptor.withFieldIds(fieldIds))
              .setPredicate(predicate)
              .setSelectsSingleField(false)
              .build());
      return this;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      List<FilterDescription> resolvedFilters =
          filters.stream()
              .map(
                  f ->
                      f.toBuilder()
                          .setFieldAccessDescriptor(
                              f.getFieldAccessDescriptor().resolve(inputSchema))
                          .build())
              .map(
                  f ->
                      f.toBuilder()
                          .setInputSchema(inputSchema)
                          .setSelectedSchema(
                              SelectHelpers.getOutputSchema(
                                  inputSchema, f.getFieldAccessDescriptor()))
                          .build())
              .collect(Collectors.toList());

      return input.apply(
          ParDo.of(
              new DoFn<T, T>() {
                @ProcessElement
                public void process(@Element Row row, OutputReceiver<Row> o) {
                  for (FilterDescription filter : resolvedFilters) {
                    Row selected = filter.getRowSelector().select(row);
                    if (filter.getSelectsSingleField()) {
                      SerializableFunction<Object, Boolean> predicate =
                          (SerializableFunction<Object, Boolean>) filter.getPredicate();
                      if (!predicate.apply(selected.getValue(0))) {
                        return;
                      }
                    } else {
                      SerializableFunction<Row, Boolean> predicate =
                          (SerializableFunction<Row, Boolean>) filter.getPredicate();
                      if (!predicate.apply(selected)) {
                        return;
                      }
                    }
                  }
                  // All filters passed. Output the row.
                  o.output(row);
                }
              }));
    }
  }
}
