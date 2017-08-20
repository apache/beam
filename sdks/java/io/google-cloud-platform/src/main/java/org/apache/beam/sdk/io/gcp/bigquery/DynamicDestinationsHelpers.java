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

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonTableRefToTableSpec;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Contains some useful helper instances of {@link DynamicDestinations}.
 */
class DynamicDestinationsHelpers {
  /**
   * Always returns a constant table destination.
   */
  static class ConstantTableDestinations<T> extends DynamicDestinations<T, TableDestination> {
    private final ValueProvider<String> tableSpec;
    @Nullable
    private final String tableDescription;

    ConstantTableDestinations(ValueProvider<String> tableSpec, @Nullable String tableDescription) {
      this.tableSpec = tableSpec;
      this.tableDescription = tableDescription;
    }

    static <T> ConstantTableDestinations<T> fromTableSpec(
        ValueProvider<String> tableSpec, String tableDescription) {
      return new ConstantTableDestinations<T>(tableSpec, tableDescription);
    }

    static <T> ConstantTableDestinations<T> fromJsonTableRef(
        ValueProvider<String> jsonTableRef, String tableDescription) {
      return new ConstantTableDestinations<T>(
          NestedValueProvider.of(jsonTableRef, new JsonTableRefToTableSpec()), tableDescription);
    }

    @Override
    public TableDestination getDestination(ValueInSingleWindow<T> element) {
      return new TableDestination(tableSpec.get(), tableDescription);
    }

    @Override
    public TableSchema getSchema(TableDestination destination) {
      return null;
    }

    @Override
    public TableDestination getTable(TableDestination destination) {
      return destination;
    }

    @Override
    public Coder<TableDestination> getDestinationCoder() {
      return TableDestinationCoder.of();
    }
  }

  /**
   * Returns a tables based on a user-supplied function.
   */
  static class TableFunctionDestinations<T> extends DynamicDestinations<T, TableDestination> {
    private final SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction;

    TableFunctionDestinations(
        SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction) {
      this.tableFunction = tableFunction;
    }

    @Override
    public TableDestination getDestination(ValueInSingleWindow<T> element) {
      return tableFunction.apply(element);
    }

    @Override
    public TableSchema getSchema(TableDestination destination) {
      return null;
    }

    @Override
    public TableDestination getTable(TableDestination destination) {
      return destination;
    }

    @Override
    public Coder<TableDestination> getDestinationCoder() {
      return TableDestinationCoder.of();
    }
  }

  /**
   * Delegates all calls to an inner instance of {@link DynamicDestinations}. This allows
   * subclasses to modify another instance of {@link DynamicDestinations} by subclassing and
   * overriding just the methods they want to alter.
   */
  static class DelegatingDynamicDestinations<T, DestinationT>
      extends DynamicDestinations<T, DestinationT> {
    private final DynamicDestinations<T, DestinationT> inner;
    DelegatingDynamicDestinations(DynamicDestinations<T, DestinationT> inner) {
      this.inner = inner;
    }
    @Override
    public DestinationT getDestination(ValueInSingleWindow<T> element) {
      return inner.getDestination(element);
    }

    @Override
    public TableSchema getSchema(DestinationT destination) {
      return inner.getSchema(destination);
    }

    @Override
    public TableDestination getTable(DestinationT destination) {
      return inner.getTable(destination);
    }

    @Override
    public Coder<DestinationT> getDestinationCoder() {
      return inner.getDestinationCoder();
    }
  }

  /**
   * Returns the same schema for every table.
   */
  static class ConstantSchemaDestinations<T>
      extends DelegatingDynamicDestinations<T, TableDestination> {
    @Nullable
    private final ValueProvider<String> jsonSchema;

    ConstantSchemaDestinations(DynamicDestinations<T, TableDestination> inner,
                               ValueProvider<String> jsonSchema) {
      super(inner);
      this.jsonSchema = jsonSchema;
    }

    @Override
    public TableSchema getSchema(TableDestination destination) {
      return BigQueryHelpers.fromJsonString(jsonSchema.get(), TableSchema.class);
    }
  }

  static class ConstantTimePartitioningDestinations<T>
      extends DelegatingDynamicDestinations<T, TableDestination> {

    @Nullable
    private final ValueProvider<String> jsonTimePartitioning;

    ConstantTimePartitioningDestinations(DynamicDestinations<T, TableDestination> inner,
        ValueProvider<String> jsonTimePartitioning) {
      super(inner);
      this.jsonTimePartitioning = jsonTimePartitioning;
    }

    @Override
    public TableDestination getDestination(ValueInSingleWindow<T> element) {
      TableDestination destination = super.getDestination(element);
      return new TableDestination(destination.getTableSpec(), destination.getTableDescription(),
          jsonTimePartitioning.get());
    }
  }

  /**
   * Takes in a side input mapping tablespec to json table schema, and always returns the
   * matching schema from the side input.
   */
  static class SchemaFromViewDestinations<T>
      extends DelegatingDynamicDestinations<T, TableDestination> {
    PCollectionView<Map<String, String>> schemaView;
    SchemaFromViewDestinations(DynamicDestinations<T, TableDestination> inner,
                               PCollectionView<Map<String, String>> schemaView) {
      super(inner);
      this.schemaView = schemaView;
    }


    @Override
    public List<PCollectionView<?>> getSideInputs() {
      return ImmutableList.<PCollectionView<?>>builder().add(schemaView).build();
    }

    @Override
    public TableSchema getSchema(TableDestination destination) {
      Map<String, String> mapValue = sideInput(schemaView);
      return BigQueryHelpers.fromJsonString(mapValue.get(destination.getTableSpec()),
          TableSchema.class);
    }
  }
}
