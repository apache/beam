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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Assigns destination metadata for each input record.
 *
 * <p>The output will have the format { {destination, partition}, data }
 */
class AssignDestinationsAndPartitions
    extends PTransform<PCollection<Row>, PCollection<KV<Row, Row>>> {

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
  private final DistributionMode distributionMode;
  private final @Nullable SerializableFunction<Row, Integer> distributionFunction;

  static final String DESTINATION = "destination";
  static final String PARTITION = "partition";
  static final String SHARD = "shard";

  /**
   * Grouping key used for {@link DistributionMode#NONE} and {@link DistributionMode#HASH} writes.
   *
   * <p>This encoding is load-bearing for pipeline update compatibility: it is the key coder of the
   * {@link org.apache.beam.sdk.transforms.GroupIntoBatches} in {@link WriteToPartitions}, so
   * changing its shape invalidates the buffered state of in-flight streaming jobs. Do not add
   * fields here.
   */
  static final org.apache.beam.sdk.schemas.Schema OUTPUT_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addStringField(DESTINATION)
          .addStringField(PARTITION)
          .build();

  /**
   * Grouping key used for {@link DistributionMode#RANGE} writes, which additionally fan each
   * partition out into non-overlapping shards so that every shard becomes its own sorted file.
   */
  static final org.apache.beam.sdk.schemas.Schema OUTPUT_SCHEMA_WITH_SHARD =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addStringField(DESTINATION)
          .addStringField(PARTITION)
          .addInt32Field(SHARD)
          .build();

  /** Returns the grouping key schema that the given distribution mode emits. */
  static org.apache.beam.sdk.schemas.Schema outputSchemaFor(DistributionMode distributionMode) {
    return distributionMode == DistributionMode.RANGE ? OUTPUT_SCHEMA_WITH_SHARD : OUTPUT_SCHEMA;
  }

  public AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations, IcebergCatalogConfig catalogConfig) {
    this(dynamicDestinations, catalogConfig, null, DistributionMode.HASH, null);
  }

  public AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations,
      IcebergCatalogConfig catalogConfig,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
    this(dynamicDestinations, catalogConfig, metadataView, DistributionMode.HASH, null);
  }

  public AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations,
      IcebergCatalogConfig catalogConfig,
      DistributionMode distributionMode,
      @Nullable SerializableFunction<Row, Integer> distributionFunction) {
    this(dynamicDestinations, catalogConfig, null, distributionMode, distributionFunction);
  }

  public AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations,
      IcebergCatalogConfig catalogConfig,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
      DistributionMode distributionMode,
      @Nullable SerializableFunction<Row, Integer> distributionFunction) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
    this.metadataView = metadataView;
    this.distributionMode = distributionMode;
    this.distributionFunction = distributionFunction;
  }

  @Override
  public PCollection<KV<Row, Row>> expand(PCollection<Row> input) {
    ParDo.SingleOutput<Row, KV<Row, Row>> parDo =
        ParDo.of(
            new AssignDoFn(
                dynamicDestinations,
                catalogConfig,
                metadataView,
                distributionMode,
                distributionFunction));
    if (metadataView != null) {
      parDo = parDo.withSideInputs(metadataView);
    }
    return input
        .apply(parDo)
        .setCoder(
            KvCoder.of(
                RowCoder.of(outputSchemaFor(distributionMode)),
                RowCoder.of(dynamicDestinations.getDataSchema())));
  }

  static class AssignDoFn extends DoFn<Row, KV<Row, Row>> {

    private static final Duration REFRESH_INTERVAL = Duration.standardMinutes(5);

    private transient @MonotonicNonNull Map<String, PartitionKey> partitionKeys;
    private transient @MonotonicNonNull Map<String, BeamRowWrapper> wrappers;
    private transient @MonotonicNonNull Map<String, Instant> lastRefreshTimes;
    private transient @MonotonicNonNull Map<String, Integer> cachedSpecIds;

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
    private final DistributionMode distributionMode;
    private final @Nullable SerializableFunction<Row, Integer> distributionFunction;

    AssignDoFn(DynamicDestinations dynamicDestinations, IcebergCatalogConfig catalogConfig) {
      this(dynamicDestinations, catalogConfig, null, DistributionMode.HASH, null);
    }

    AssignDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
      this(dynamicDestinations, catalogConfig, metadataView, DistributionMode.HASH, null);
    }

    AssignDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        DistributionMode distributionMode,
        @Nullable SerializableFunction<Row, Integer> distributionFunction) {
      this(dynamicDestinations, catalogConfig, null, distributionMode, distributionFunction);
    }

    AssignDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
        DistributionMode distributionMode,
        @Nullable SerializableFunction<Row, Integer> distributionFunction) {
      this.dynamicDestinations = dynamicDestinations;
      this.catalogConfig = catalogConfig;
      this.metadataView = metadataView;
      this.distributionMode = distributionMode;
      this.distributionFunction = distributionFunction;
    }

    @Setup
    public void setup() {
      this.wrappers = new HashMap<>();
      this.partitionKeys = new HashMap<>();
      this.lastRefreshTimes = new HashMap<>();
      this.cachedSpecIds = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element Row element,
        BoundedWindow window,
        PaneInfo paneInfo,
        @Timestamp Instant timestamp,
        OutputReceiver<KV<Row, Row>> out) {

      String tableIdentifier =
          dynamicDestinations.getTableStringIdentifier(
              ValueInSingleWindow.of(element, timestamp, window, paneInfo));

      SerializableTableSpec tableSpec = null;
      if (metadataView != null) {
        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
        if (viewMap != null) {
          tableSpec = viewMap.get(tableIdentifier);
        }
      }

      Row data = dynamicDestinations.getData(element);

      @Nullable PartitionKey partitionKey = checkStateNotNull(partitionKeys).get(tableIdentifier);
      @Nullable BeamRowWrapper wrapper = checkStateNotNull(wrappers).get(tableIdentifier);
      @Nullable Instant lastRefresh = checkStateNotNull(lastRefreshTimes).get(tableIdentifier);
      @Nullable Integer cachedSpecId = checkStateNotNull(cachedSpecIds).get(tableIdentifier);

      Instant now = Instant.now();

      boolean specChanged =
          tableSpec != null
              && (cachedSpecId == null || !cachedSpecId.equals(tableSpec.getSpecId()));

      boolean shouldRefresh =
          partitionKey == null
              || wrapper == null
              || specChanged
              || (tableSpec == null
                  && (lastRefresh == null || now.isAfter(lastRefresh.plus(REFRESH_INTERVAL))));

      if (shouldRefresh) {

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Schema schema = IcebergUtils.beamSchemaToIcebergSchema(data.getSchema());

        @Nullable IcebergTableCreateConfig createConfig =
            dynamicDestinations.instantiateDestination(tableIdentifier).getTableCreateConfig();

        if (tableSpec != null) {
          spec = tableSpec.getPartitionSpec();
          if (data.getSchema().getFieldCount() == tableSpec.getSchema().columns().size()) {
            schema = tableSpec.getSchema();
          }
          checkStateNotNull(cachedSpecIds).put(tableIdentifier, tableSpec.getSpecId());
        } else if (createConfig != null && createConfig.getPartitionFields() != null) {
          spec =
              PartitionUtils.toPartitionSpec(createConfig.getPartitionFields(), data.getSchema());
        } else {
          try {
            // see if table already exists with a spec
            Table table =
                TableCache.getAndRefreshIfStale(
                    catalogConfig, IcebergUtils.parseTableIdentifier(tableIdentifier));
            spec = table.spec();
            if (data.getSchema().getFieldCount() == table.schema().columns().size()) {
              schema = table.schema();
            }
          } catch (NoSuchTableException ignored) {
            // no partition to apply
          }
        }

        partitionKey = new PartitionKey(spec, schema);
        wrapper = new BeamRowWrapper(data.getSchema(), schema.asStruct());

        checkStateNotNull(partitionKeys).put(tableIdentifier, partitionKey);
        checkStateNotNull(wrappers).put(tableIdentifier, wrapper);
        checkStateNotNull(lastRefreshTimes).put(tableIdentifier, now);
      }

      partitionKey = checkStateNotNull(partitionKey);
      wrapper = checkStateNotNull(wrapper);

      partitionKey.partition(wrapper.wrap(data));

      String partitionPath = partitionKey.toPath();

      Row.Builder keyBuilder =
          Row.withSchema(outputSchemaFor(distributionMode))
              .addValue(tableIdentifier)
              .addValue(partitionPath);

      if (distributionMode == DistributionMode.RANGE) {
        SerializableFunction<Row, Integer> shardFn =
            checkStateNotNull(
                distributionFunction,
                "A distribution function is required when using RANGE distribution mode.");
        keyBuilder =
            keyBuilder.addValue(
                checkStateNotNull(
                    shardFn.apply(data),
                    "The RANGE distribution function returned a null shard id. It must return a"
                        + " non-null Integer for every row."));
      }

      out.output(KV.of(keyBuilder.build(), data));
    }
  }
}
