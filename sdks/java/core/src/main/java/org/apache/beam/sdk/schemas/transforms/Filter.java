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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

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
    private final Map<String, SerializableFunction<?, Boolean>> fieldNameFilters =
        Maps.newHashMap();
    private final Map<Integer, SerializableFunction<?, Boolean>> fieldIdFilters = Maps.newHashMap();
    private final Map<List<String>, SerializableFunction<Row, Boolean>> fieldNamesFilters =
        Maps.newHashMap();
    private final Map<List<Integer>, SerializableFunction<Row, Boolean>> fieldIdsFilters =
        Maps.newHashMap();

    /** Set a predicate based on the value of a field, where the field is specified by name. */
    public Inner<T> whereFieldName(String fieldName, SerializableFunction<?, Boolean> predicate) {
      fieldNameFilters.put(fieldName, predicate);
      return this;
    }

    /** Set a predicate based on the value of a field, where the field is specified by id. */
    public Inner<T> whereFieldId(int fieldId, SerializableFunction<?, Boolean> predicate) {
      fieldIdFilters.put(fieldId, predicate);
      return this;
    }

    /** Set a predicate based on the value of multipled fields, specified by name. */
    public Inner<T> whereFieldNames(
        List<String> fieldNames, SerializableFunction<Row, Boolean> predicate) {
      fieldNamesFilters.put(fieldNames, predicate);
      return this;
    }

    /** Set a predicate based on the value of multipled fields, specified by id. */
    public Inner<T> whereFieldIds(
        List<Integer> fieldIds, SerializableFunction<Row, Boolean> predicate) {
      fieldIdsFilters.put(fieldIds, predicate);
      return this;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      // Validate that all referenced fields are in the schema.
      Schema schema = input.getSchema();
      for (String fieldName :
          Sets.union(
              fieldNameFilters.keySet(),
              fieldNamesFilters
                  .keySet()
                  .stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toSet()))) {
        schema.getField(fieldName);
      }
      for (int fieldIndex :
          Sets.union(
              fieldIdFilters.keySet(),
              fieldIdsFilters
                  .keySet()
                  .stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toSet()))) {
        if (fieldIndex >= schema.getFieldCount() || fieldIndex < 0) {
          throw new IllegalArgumentException(
              "Field index " + fieldIndex + " does not exist in the schema.");
        }
      }

      // TODO: Once BEAM-4457 is fixed, tag this ParDo with a FieldAccessDescriptor so that Beam
      // knows which fields are being accessed.

      return input.apply(
          ParDo.of(
              new DoFn<T, T>() {
                @ProcessElement
                public void process(@Element Row row, OutputReceiver<Row> o) {
                  for (Map.Entry<String, SerializableFunction<?, Boolean>> entry :
                      fieldNameFilters.entrySet()) {
                    if (!entry.getValue().apply(row.getValue(entry.getKey()))) {
                      return;
                    }
                  }

                  for (Map.Entry<Integer, SerializableFunction<?, Boolean>> entry :
                      fieldIdFilters.entrySet()) {
                    if (!entry.getValue().apply(row.getValue(entry.getKey()))) {
                      return;
                    }
                  }

                  for (SerializableFunction<Row, Boolean> predicate : fieldNamesFilters.values()) {
                    if (!predicate.apply(row)) {
                      return;
                    }
                  }

                  for (SerializableFunction<Row, Boolean> predicate : fieldIdsFilters.values()) {
                    if (!predicate.apply(row)) {
                      return;
                    }
                  }
                  // All filters passed. Output the row.
                  o.output(row);
                }
              }));
    }
  }
}
