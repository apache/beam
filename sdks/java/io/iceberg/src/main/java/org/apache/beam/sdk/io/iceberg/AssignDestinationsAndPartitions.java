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
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
  private final DistributionMode distributionMode;
  private final @Nullable SerializableFunction<Row, Integer> distributionFunction;

  static final String DESTINATION = "destination";
  static final String PARTITION = "partition";
  static final String SHARD = "shard";
  static final org.apache.beam.sdk.schemas.Schema OUTPUT_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addStringField(DESTINATION)
          .addStringField(PARTITION)
          .addNullableField(SHARD, org.apache.beam.sdk.schemas.Schema.FieldType.INT32)
          .build();

  public AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations, IcebergCatalogConfig catalogConfig) {
    this(dynamicDestinations, catalogConfig, DistributionMode.HASH, null);
  }

  public AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations,
      IcebergCatalogConfig catalogConfig,
      DistributionMode distributionMode,
      @Nullable SerializableFunction<Row, Integer> distributionFunction) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
    this.distributionMode = distributionMode;
    this.distributionFunction = distributionFunction;
  }

  @Override
  public PCollection<KV<Row, Row>> expand(PCollection<Row> input) {
    return input
        .apply(
            ParDo.of(
                new AssignDoFn(
                    dynamicDestinations, catalogConfig, distributionMode, distributionFunction)))
        .setCoder(
            KvCoder.of(
                RowCoder.of(OUTPUT_SCHEMA), RowCoder.of(dynamicDestinations.getDataSchema())));
  }

  @SuppressWarnings("nullness")
  static class AssignDoFn extends DoFn<Row, KV<Row, Row>> {
    private transient @MonotonicNonNull Map<String, PartitionKey> partitionKeys;
    private transient @MonotonicNonNull Map<String, BeamRowWrapper> wrappers;
    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final DistributionMode distributionMode;
    private final @Nullable SerializableFunction<Row, Integer> distributionFunction;

    AssignDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        DistributionMode distributionMode,
        @Nullable SerializableFunction<Row, Integer> distributionFunction) {
      this.dynamicDestinations = dynamicDestinations;
      this.catalogConfig = catalogConfig;
      this.distributionMode = distributionMode;
      this.distributionFunction = distributionFunction;
    }

    @Setup
    public void setup() {
      this.wrappers = new HashMap<>();
      this.partitionKeys = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        @Element Row element,
        BoundedWindow window,
        PaneInfo paneInfo,
        @Timestamp Instant timestamp,
        OutputReceiver<KV<Row, Row>> out) {
      String tableIdentifier =
          dynamicDestinations.getTableStringIdentifier(
              ValueInSingleWindow.of(element, timestamp, window, paneInfo));
      Row data = dynamicDestinations.getData(element);

      @Nullable PartitionKey partitionKey = checkStateNotNull(partitionKeys).get(tableIdentifier);
      @Nullable BeamRowWrapper wrapper = checkStateNotNull(wrappers).get(tableIdentifier);
      if (partitionKey == null || wrapper == null) {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Schema schema = IcebergUtils.beamSchemaToIcebergSchema(data.getSchema());
        @Nullable
        IcebergTableCreateConfig createConfig =
            dynamicDestinations.instantiateDestination(tableIdentifier).getTableCreateConfig();
        if (createConfig != null && createConfig.getPartitionFields() != null) {
          spec =
              PartitionUtils.toPartitionSpec(createConfig.getPartitionFields(), data.getSchema());
        } else {
          try {
            // see if table already exists with a spec
            // TODO(https://github.com/apache/beam/issues/38337): improve this by periodically
            // refreshing the table to fetch updated specs
            spec = catalogConfig.catalog().loadTable(TableIdentifier.parse(tableIdentifier)).spec();
          } catch (NoSuchTableException ignored) {
            // no partition to apply
          }
        }
        partitionKey = new PartitionKey(spec, schema);
        wrapper = new BeamRowWrapper(data.getSchema(), schema.asStruct());
        checkStateNotNull(partitionKeys).put(tableIdentifier, partitionKey);
        checkStateNotNull(wrappers).put(tableIdentifier, wrapper);
      }
      partitionKey.partition(wrapper.wrap(data));
      String partitionPath = partitionKey.toPath();

      Integer shardId = null;
      if (distributionMode == DistributionMode.RANGE && distributionFunction != null) {
        shardId = distributionFunction.apply(data);
      }

      Row destAndPartition =
          Row.withSchema(OUTPUT_SCHEMA)
              .withFieldValue(DESTINATION, tableIdentifier)
              .withFieldValue(PARTITION, partitionPath)
              .withFieldValue(SHARD, shardId)
              .build();
      out.output(KV.of(destAndPartition, data));
    }
  }
}
