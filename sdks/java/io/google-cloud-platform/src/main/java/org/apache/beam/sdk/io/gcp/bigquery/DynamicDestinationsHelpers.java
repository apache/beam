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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonTableRefToTableSpec;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Contains some useful helper instances of {@link DynamicDestinations}. */
class DynamicDestinationsHelpers {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicDestinationsHelpers.class);

  /** Always returns a constant table destination. */
  static class ConstantTableDestinations<T> extends DynamicDestinations<T, TableDestination> {
    private final ValueProvider<String> tableSpec;
    private final @Nullable String tableDescription;
    private final boolean clusteringEnabled;

    ConstantTableDestinations(
        ValueProvider<String> tableSpec,
        @Nullable String tableDescription,
        boolean clusteringEnabled) {
      this.tableSpec = tableSpec;
      this.tableDescription = tableDescription;
      this.clusteringEnabled = clusteringEnabled;
    }

    static <T> ConstantTableDestinations<T> fromTableSpec(
        ValueProvider<String> tableSpec, String tableDescription, boolean clusteringEnabled) {
      return new ConstantTableDestinations<>(tableSpec, tableDescription, clusteringEnabled);
    }

    static <T> ConstantTableDestinations<T> fromJsonTableRef(
        ValueProvider<String> jsonTableRef, String tableDescription, boolean clusteringEnabled) {
      return new ConstantTableDestinations<>(
          NestedValueProvider.of(jsonTableRef, new JsonTableRefToTableSpec()),
          tableDescription,
          clusteringEnabled);
    }

    @Override
    public TableDestination getDestination(@Nullable ValueInSingleWindow<T> element) {
      String tableSpec = this.tableSpec.get();
      checkArgument(tableSpec != null, "tableSpec can not be null");
      return new TableDestination(tableSpec, tableDescription);
    }

    @Override
    public @Nullable TableSchema getSchema(TableDestination destination) {
      return null;
    }

    @Override
    public TableDestination getTable(TableDestination destination) {
      return destination;
    }

    @Override
    public Coder<TableDestination> getDestinationCoder() {
      if (clusteringEnabled) {
        return TableDestinationCoderV3.of();
      } else {
        return TableDestinationCoderV2.of();
      }
    }
  }

  /** Returns tables based on a user-supplied function. */
  static class TableFunctionDestinations<T> extends DynamicDestinations<T, TableDestination> {
    private final SerializableFunction<@Nullable ValueInSingleWindow<T>, TableDestination>
        tableFunction;
    private final boolean clusteringEnabled;

    TableFunctionDestinations(
        SerializableFunction<@Nullable ValueInSingleWindow<T>, TableDestination> tableFunction,
        boolean clusteringEnabled) {
      this.tableFunction = tableFunction;
      this.clusteringEnabled = clusteringEnabled;
    }

    @Override
    public TableDestination getDestination(@Nullable ValueInSingleWindow<T> element) {
      TableDestination res = tableFunction.apply(element);
      checkArgument(
          res != null,
          "result of tableFunction can not be null, but %s returned null for element: %s",
          tableFunction,
          element);
      return res;
    }

    @Override
    public @Nullable TableSchema getSchema(TableDestination destination) {
      return null;
    }

    @Override
    public TableDestination getTable(TableDestination destination) {
      return destination;
    }

    @Override
    public Coder<TableDestination> getDestinationCoder() {
      if (clusteringEnabled) {
        return TableDestinationCoderV3.of();
      } else {
        return TableDestinationCoderV2.of();
      }
    }
  }

  /**
   * Delegates all calls to an inner instance of {@link DynamicDestinations}. This allows subclasses
   * to modify another instance of {@link DynamicDestinations} by subclassing and overriding just
   * the methods they want to alter.
   */
  static class DelegatingDynamicDestinations<T, DestinationT>
      extends DynamicDestinations<T, DestinationT> {
    final DynamicDestinations<T, DestinationT> inner;

    DelegatingDynamicDestinations(DynamicDestinations<T, DestinationT> inner) {
      this.inner = inner;
    }

    @Override
    public DestinationT getDestination(@Nullable ValueInSingleWindow<T> element) {
      return inner.getDestination(element);
    }

    @Override
    public @Nullable TableSchema getSchema(DestinationT destination) {
      return inner.getSchema(destination);
    }

    @Override
    public @Nullable TableConstraints getTableConstraints(DestinationT destination) {
      return inner.getTableConstraints(destination);
    }

    @Override
    public TableDestination getTable(DestinationT destination) {
      return inner.getTable(destination);
    }

    @Override
    public @Nullable Coder<DestinationT> getDestinationCoder() {
      return inner.getDestinationCoder();
    }

    @Override
    Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
        throws CannotProvideCoderException {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder != null) {
        return destinationCoder;
      }
      return inner.getDestinationCoderWithDefault(registry);
    }

    @Override
    public List<PCollectionView<?>> getSideInputs() {
      return inner.getSideInputs();
    }

    @Override
    void setSideInputAccessorFromProcessContext(DoFn<?, ?>.ProcessContext context) {
      super.setSideInputAccessorFromProcessContext(context);
      inner.setSideInputAccessorFromProcessContext(context);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("inner", inner).toString();
    }
  }

  static class ConstantTableConstraintsDestinations<T, DestinationT>
      extends DelegatingDynamicDestinations<T, DestinationT> {
    private final String jsonTableConstraints;

    ConstantTableConstraintsDestinations(
        DynamicDestinations<T, DestinationT> inner, TableConstraints tableConstraints) {
      super(inner);
      this.jsonTableConstraints = BigQueryHelpers.toJsonString(tableConstraints);
    }

    @Override
    public TableConstraints getTableConstraints(DestinationT destination) {
      return BigQueryHelpers.fromJsonString(jsonTableConstraints, TableConstraints.class);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("inner", inner)
          .add("tableConstraints", jsonTableConstraints)
          .toString();
    }
  }

  /** Returns the same schema for every table. */
  static class ConstantSchemaDestinations<T, DestinationT>
      extends DelegatingDynamicDestinations<T, DestinationT> {
    private final ValueProvider<String> jsonSchema;

    ConstantSchemaDestinations(
        DynamicDestinations<T, DestinationT> inner, ValueProvider<String> jsonSchema) {
      super(inner);
      Preconditions.checkArgumentNotNull(jsonSchema, "jsonSchema can not be null");
      this.jsonSchema = jsonSchema;
    }

    @Override
    public TableSchema getSchema(DestinationT destination) {
      String jsonSchema = this.jsonSchema.get();
      checkArgument(jsonSchema != null, "jsonSchema can not be null");
      return BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("inner", inner)
          .add("jsonSchema", jsonSchema)
          .toString();
    }
  }

  static class ConstantTimePartitioningClusteringDestinations<T>
      extends DelegatingDynamicDestinations<T, TableDestination> {

    private final @Nullable ValueProvider<String> jsonTimePartitioning;
    private final @Nullable ValueProvider<String> jsonClustering;

    ConstantTimePartitioningClusteringDestinations(
        DynamicDestinations<T, TableDestination> inner,
        ValueProvider<String> jsonTimePartitioning,
        ValueProvider<String> jsonClustering) {
      super(inner);

      checkArgument(
          (jsonTimePartitioning != null
                  && jsonTimePartitioning.isAccessible()
                  && jsonTimePartitioning.get() != null)
              || (jsonClustering != null
                  && jsonClustering.isAccessible()
                  && jsonClustering.get() != null),
          "at least one of jsonTimePartitioning or jsonClustering must be non-null, accessible "
              + "and present");

      this.jsonTimePartitioning = jsonTimePartitioning;
      this.jsonClustering = jsonClustering;
    }

    @Override
    public TableDestination getDestination(@Nullable ValueInSingleWindow<T> element) {
      TableDestination destination = super.getDestination(element);
      String partitioning =
          Optional.ofNullable(jsonTimePartitioning).map(ValueProvider::get).orElse(null);
      if (partitioning == null
          || JsonParser.parseString(partitioning).getAsJsonObject().isEmpty()) {
        partitioning = destination.getJsonTimePartitioning();
      }
      String clustering = Optional.ofNullable(jsonClustering).map(ValueProvider::get).orElse(null);
      if (clustering == null || JsonParser.parseString(clustering).getAsJsonObject().isEmpty()) {
        clustering = destination.getJsonClustering();
      }

      return new TableDestination(
          destination.getTableSpec(), destination.getTableDescription(), partitioning, clustering);
    }

    @Override
    public Coder<TableDestination> getDestinationCoder() {
      if (jsonClustering != null) {
        return TableDestinationCoderV3.of();
      } else {
        return TableDestinationCoderV2.of();
      }
    }

    @Override
    public String toString() {
      MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this).add("inner", inner);
      if (jsonTimePartitioning != null) {
        helper.add("jsonTimePartitioning", jsonTimePartitioning);
      }
      if (jsonClustering != null) {
        helper.add("jsonClustering", jsonClustering);
      }
      return helper.toString();
    }
  }

  /**
   * Takes in a side input mapping tablespec to json table schema, and always returns the matching
   * schema from the side input.
   */
  static class SchemaFromViewDestinations<T>
      extends DelegatingDynamicDestinations<T, TableDestination> {
    PCollectionView<Map<String, String>> schemaView;

    SchemaFromViewDestinations(
        DynamicDestinations<T, TableDestination> inner,
        PCollectionView<Map<String, String>> schemaView) {
      super(inner);
      checkArgument(schemaView != null, "schemaView can not be null");
      this.schemaView = schemaView;
    }

    @Override
    public List<PCollectionView<?>> getSideInputs() {
      return ImmutableList.<PCollectionView<?>>builder().add(schemaView).build();
    }

    @Override
    public TableSchema getSchema(TableDestination destination) {
      Map<String, String> mapValue = sideInput(schemaView);
      String schema = mapValue.get(destination.getTableSpec());
      Preconditions.checkArgumentNotNull(
          schema,
          "Schema view must contain data for every destination used, "
              + "but view %s does not contain data for table destination %s "
              + "produced by %s",
          schemaView,
          destination.getTableSpec(),
          inner);
      return BigQueryHelpers.fromJsonString(schema, TableSchema.class);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("inner", inner)
          .add("schemaView", schemaView)
          .toString();
    }
  }

  static <T, DestinationT> DynamicDestinations<T, DestinationT> matchTableDynamicDestinations(
      DynamicDestinations<T, DestinationT> inner, BigQueryServices bqServices) {
    return new MatchTableDynamicDestinations<>(inner, bqServices);
  }

  static class MatchTableDynamicDestinations<T, DestinationT>
      extends DelegatingDynamicDestinations<T, DestinationT> {
    private final BigQueryServices bqServices;

    private MatchTableDynamicDestinations(
        DynamicDestinations<T, DestinationT> inner, BigQueryServices bqServices) {
      super(inner);
      this.bqServices = bqServices;
    }

    private @Nullable Table getBigQueryTable(TableReference tableReference) {
      BackOff backoff =
          BackOffAdapter.toGcpBackOff(
              FluentBackoff.DEFAULT
                  .withMaxRetries(3)
                  .withInitialBackoff(Duration.standardSeconds(1))
                  .withMaxBackoff(Duration.standardSeconds(2))
                  .backoff());
      try {
        do {
          try {
            PipelineOptions options = getPipelineOptions();
            if (options == null) {
              throw new IllegalStateException("pipeline options cannot be null");
            }
            BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
            if (tableReference.getProjectId() == null) {
              tableReference.setProjectId(
                  bqOptions.getBigQueryProject() == null
                      ? bqOptions.getProject()
                      : bqOptions.getBigQueryProject());
            }
            try (DatasetService datasetService = bqServices.getDatasetService(bqOptions)) {
              return datasetService.getTable(tableReference);
            } catch (InterruptedException | IOException e) {
              LOG.info("Failed to get BigQuery table " + tableReference);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } while (nextBackOff(Sleeper.DEFAULT, backoff));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    /** Identical to {@link BackOffUtils#next} but without checked IOException. */
    private static boolean nextBackOff(Sleeper sleeper, BackOff backoff)
        throws InterruptedException {
      try {
        return BackOffUtils.next(sleeper, backoff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /** Returns a {@link TableDestination} object for the destination. May not return null. */
    @Override
    public TableDestination getTable(DestinationT destination) {
      TableDestination wrappedDestination = super.getTable(destination);
      Table existingTable = getBigQueryTable(wrappedDestination.getTableReference());

      if (existingTable == null) {
        return wrappedDestination;
      } else {
        return new TableDestination(
            wrappedDestination.getTableSpec(),
            existingTable.getDescription(),
            existingTable.getTimePartitioning(),
            existingTable.getClustering());
      }
    }

    /**
     * Returns the table schema for the destination. If possible, will return the existing table
     * schema.
     */
    @Override
    public @Nullable TableSchema getSchema(DestinationT destination) {
      TableDestination wrappedDestination = super.getTable(destination);
      @Nullable Table existingTable = getBigQueryTable(wrappedDestination.getTableReference());
      if (existingTable == null
          || existingTable.getSchema() == null
          || existingTable.getSchema().isEmpty()) {
        return super.getSchema(destination);
      } else {
        return existingTable.getSchema();
      }
    }
  }
}
