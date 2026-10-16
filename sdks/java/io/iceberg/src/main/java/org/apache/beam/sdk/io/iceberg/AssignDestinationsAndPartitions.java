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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Assigns destination and partition metadata for each input record.
 *
 * <p>The output has the format {@code { {destination, partition}, data }} for {@link
 * DistributionMode#HASH} and {@code { {destination, partition, shard}, data }} for {@link
 * DistributionMode#RANGE}.
 */
class AssignDestinationsAndPartitions
    extends PTransform<PCollection<Row>, PCollection<KV<Row, Row>>> {

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
  private final DistributionMode distributionMode;
  private final @Nullable SerializableFunction<Row, Integer> distributionFunction;
  private final long targetBytesPerRangeShard;

  static final String DESTINATION = "destination";
  static final String PARTITION = "partition";
  static final String SHARD = "shard";

  /** Maximum number of sort-key samples retained per partition within a single bundle. */
  static final int MAX_SAMPLES_PER_BUNDLE = 200;

  /** Maximum number of sort-key samples retained per partition across combined bundles. */
  static final int MAX_COMBINED_SAMPLES = 2048;

  /** Maximum number of dynamic range shards created for a single partition. */
  static final int MAX_RANGE_SHARDS = 1000;

  private static final TupleTag<KV<Row, Row>> PARTITIONED_ROWS_TAG =
      new TupleTag<KV<Row, Row>>() {};
  private static final TupleTag<KV<Row, PartitionSample>> PARTITION_SAMPLES_TAG =
      new TupleTag<KV<Row, PartitionSample>>() {};

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
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
      DistributionMode distributionMode,
      @Nullable SerializableFunction<Row, Integer> distributionFunction) {
    this(
        dynamicDestinations,
        catalogConfig,
        metadataView,
        distributionMode,
        distributionFunction,
        WriteToPartitions.DEFAULT_BYTES_PER_FILE);
  }

  @VisibleForTesting
  AssignDestinationsAndPartitions(
      DynamicDestinations dynamicDestinations,
      IcebergCatalogConfig catalogConfig,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
      DistributionMode distributionMode,
      @Nullable SerializableFunction<Row, Integer> distributionFunction,
      long targetBytesPerRangeShard) {
    this.dynamicDestinations = dynamicDestinations;
    this.catalogConfig = catalogConfig;
    this.metadataView = metadataView;
    this.distributionMode = distributionMode;
    this.distributionFunction = distributionFunction;
    this.targetBytesPerRangeShard = targetBytesPerRangeShard;
  }

  @Override
  public PCollection<KV<Row, Row>> expand(PCollection<Row> input) {
    if (distributionMode == DistributionMode.RANGE
        && distributionFunction == null
        && !IcebergUtils.isUnbounded(input)) {
      return expandWithDynamicRangeShards(input);
    }

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

  private PCollection<KV<Row, Row>> expandWithDynamicRangeShards(PCollection<Row> input) {
    RowCoder partitionKeyCoder = RowCoder.of(OUTPUT_SCHEMA);
    RowCoder rangeKeyCoder = RowCoder.of(OUTPUT_SCHEMA_WITH_SHARD);
    RowCoder dataCoder = RowCoder.of(dynamicDestinations.getDataSchema());

    ParDo.MultiOutput<Row, KV<Row, Row>> extractParDo =
        ParDo.of(
                new ExtractPartitionAndSampleSortKeyDoFn(
                    dynamicDestinations, catalogConfig, metadataView))
            .withOutputTags(PARTITIONED_ROWS_TAG, TupleTagList.of(PARTITION_SAMPLES_TAG));
    if (metadataView != null) {
      extractParDo = extractParDo.withSideInputs(metadataView);
    }

    PCollectionTuple extracted = input.apply("ExtractPartitionAndSampleSortKey", extractParDo);

    PCollection<KV<Row, Row>> partitionedRows =
        extracted.get(PARTITIONED_ROWS_TAG).setCoder(KvCoder.of(partitionKeyCoder, dataCoder));

    PCollection<KV<Row, PartitionSample>> partitionSamples =
        extracted
            .get(PARTITION_SAMPLES_TAG)
            .setCoder(KvCoder.of(partitionKeyCoder, PartitionSampleCoder.of()));

    PCollectionView<Map<Row, List<byte[]>>> splitPointsView =
        partitionSamples
            .apply(
                "ComputePartitionQuantileSplits",
                Combine.perKey(new PartitionQuantilesCombineFn(targetBytesPerRangeShard)))
            .setCoder(KvCoder.of(partitionKeyCoder, ListCoder.of(ByteArrayCoder.of())))
            .apply("SplitsAsMapView", View.asMap());

    ParDo.SingleOutput<KV<Row, Row>, KV<Row, Row>> assignShardParDo =
        ParDo.of(
                new AssignDynamicRangeShardDoFn(
                    dynamicDestinations, catalogConfig, metadataView, splitPointsView))
            .withSideInputs(
                metadataView != null
                    ? Arrays.asList(splitPointsView, metadataView)
                    : Collections.singletonList(splitPointsView));

    return partitionedRows
        .apply("AssignDynamicRangeShard", assignShardParDo)
        .setCoder(KvCoder.of(rangeKeyCoder, dataCoder));
  }

  /**
   * Maps a {@code sortKey} to a shard index {@code 0 .. splits.size()} using half-open intervals
   * {@code (-inf, splits[0]), [splits[0], splits[1]), ..., [splits[k-1], +inf)}.
   */
  @VisibleForTesting
  static int findRangeShard(@Nullable List<byte[]> splits, byte[] sortKey) {
    if (splits == null || splits.isEmpty()) {
      return 0;
    }
    int pos = Collections.binarySearch(splits, sortKey, UnsignedBytes.lexicographicalComparator());
    return pos >= 0 ? (pos + 1) : -(pos + 1);
  }

  /**
   * Resolved per-destination partition and sort-key metadata cached on the worker across bundles.
   */
  private static class ResolvedDestination {
    final String tableIdentifier;
    final Row data;
    final String partitionPath;
    final IcebergRowSorter.@Nullable KeyEncoder keyEncoder;

    ResolvedDestination(
        String tableIdentifier,
        Row data,
        String partitionPath,
        IcebergRowSorter.@Nullable KeyEncoder keyEncoder) {
      this.tableIdentifier = tableIdentifier;
      this.data = data;
      this.partitionPath = partitionPath;
      this.keyEncoder = keyEncoder;
    }
  }

  private static class CachedTableMetadata {
    final PartitionKey partitionKey;
    final BeamRowWrapper wrapper;
    final IcebergRowSorter.@Nullable KeyEncoder keyEncoder;
    final Instant lastRefresh;
    final @Nullable Integer specId;
    final @Nullable Integer orderId;

    CachedTableMetadata(
        PartitionKey partitionKey,
        BeamRowWrapper wrapper,
        IcebergRowSorter.@Nullable KeyEncoder keyEncoder,
        Instant lastRefresh,
        @Nullable Integer specId,
        @Nullable Integer orderId) {
      this.partitionKey = partitionKey;
      this.wrapper = wrapper;
      this.keyEncoder = keyEncoder;
      this.lastRefresh = lastRefresh;
      this.specId = specId;
      this.orderId = orderId;
    }
  }

  /**
   * Shared helper that resolves a destination table's {@link PartitionSpec}, {@link SortOrder}, and
   * {@link Schema} from the side-input cache, {@link TableCache}, or {@link
   * IcebergTableCreateConfig}, and computes the record's partition path and optional {@link
   * IcebergRowSorter.KeyEncoder}.
   */
  private static class DestinationMetadataResolver implements Serializable {
    private static final Duration REFRESH_INTERVAL = Duration.standardMinutes(5);

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
    private final boolean includeSortKeyEncoder;
    private transient @MonotonicNonNull Map<String, CachedTableMetadata> cache;

    DestinationMetadataResolver(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
        boolean includeSortKeyEncoder) {
      this.dynamicDestinations = dynamicDestinations;
      this.catalogConfig = catalogConfig;
      this.metadataView = metadataView;
      this.includeSortKeyEncoder = includeSortKeyEncoder;
    }

    void setup() {
      this.cache = new HashMap<>();
    }

    private CachedTableMetadata getOrRefreshMetadata(
        DoFn<?, ?>.ProcessContext c,
        String tableIdentifier,
        org.apache.beam.sdk.schemas.Schema dataSchema) {
      SerializableTableSpec tableSpec = null;
      if (metadataView != null) {
        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
        if (viewMap != null) {
          tableSpec = viewMap.get(tableIdentifier);
        }
      }

      Map<String, CachedTableMetadata> metadataCache = checkStateNotNull(cache);
      @Nullable CachedTableMetadata cached = metadataCache.get(tableIdentifier);

      Instant now = Instant.now();
      boolean specOrOrderChanged =
          tableSpec != null
              && (cached == null
                  || !Objects.equals(cached.specId, tableSpec.getSpecId())
                  || !Objects.equals(cached.orderId, tableSpec.getOrderId()));

      boolean shouldRefresh =
          cached == null
              || specOrOrderChanged
              || (tableSpec == null && now.isAfter(cached.lastRefresh.plus(REFRESH_INTERVAL)));

      if (shouldRefresh) {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Schema schema = IcebergUtils.beamSchemaToIcebergSchema(dataSchema);
        Schema tableSchema = schema;
        SortOrder sortOrder = SortOrder.unsorted();
        Integer specId = null;
        Integer orderId = null;

        if (tableSpec != null) {
          spec = tableSpec.getPartitionSpec();
          sortOrder = tableSpec.getSortOrder();
          tableSchema = tableSpec.getSchema();
          if (dataSchema.getFieldCount() == tableSchema.columns().size()) {
            schema = tableSchema;
          }
          specId = tableSpec.getSpecId();
          orderId = tableSpec.getOrderId();
        } else {
          try {
            Table table =
                TableCache.getAndRefreshIfStale(
                    catalogConfig, IcebergUtils.parseTableIdentifier(tableIdentifier));
            spec = table.spec();
            sortOrder = table.sortOrder();
            tableSchema = table.schema();
            if (dataSchema.getFieldCount() == tableSchema.columns().size()) {
              schema = tableSchema;
            }
          } catch (NoSuchTableException ignored) {
            @Nullable IcebergTableCreateConfig createConfig =
                dynamicDestinations.instantiateDestination(tableIdentifier).getTableCreateConfig();
            if (createConfig != null) {
              if (createConfig.getPartitionFields() != null) {
                spec = PartitionUtils.toPartitionSpec(createConfig.getPartitionFields(), schema);
              }
              if (createConfig.getSortOrder() != null) {
                sortOrder = createConfig.getSortOrder();
              }
            }
          }
        }

        PartitionKey partitionKey = new PartitionKey(spec, schema);
        BeamRowWrapper wrapper = new BeamRowWrapper(dataSchema, schema.asStruct());
        IcebergRowSorter.@Nullable KeyEncoder encoder =
            includeSortKeyEncoder && sortOrder != null && sortOrder.isSorted()
                ? new IcebergRowSorter.KeyEncoder(sortOrder, tableSchema, dataSchema)
                : null;

        cached = new CachedTableMetadata(partitionKey, wrapper, encoder, now, specId, orderId);
        metadataCache.put(tableIdentifier, cached);
      }

      return checkStateNotNull(cached);
    }

    ResolvedDestination resolve(
        DoFn<Row, ?>.ProcessContext c,
        Row element,
        BoundedWindow window,
        PaneInfo paneInfo,
        Instant timestamp) {
      String tableIdentifier =
          dynamicDestinations.getTableStringIdentifier(
              ValueInSingleWindow.of(element, timestamp, window, paneInfo));
      Row data = dynamicDestinations.getData(element);
      CachedTableMetadata cached = getOrRefreshMetadata(c, tableIdentifier, data.getSchema());
      cached.partitionKey.partition(cached.wrapper.wrap(data));
      return new ResolvedDestination(
          tableIdentifier, data, cached.partitionKey.toPath(), cached.keyEncoder);
    }

    IcebergRowSorter.@Nullable KeyEncoder resolveKeyEncoder(
        DoFn<?, ?>.ProcessContext c,
        String tableIdentifier,
        org.apache.beam.sdk.schemas.Schema dataSchema) {
      return getOrRefreshMetadata(c, tableIdentifier, dataSchema).keyEncoder;
    }
  }

  static class AssignDoFn extends DoFn<Row, KV<Row, Row>> {

    private final DestinationMetadataResolver metadataResolver;
    private final DistributionMode distributionMode;
    private final @Nullable SerializableFunction<Row, Integer> distributionFunction;

    AssignDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
        DistributionMode distributionMode,
        @Nullable SerializableFunction<Row, Integer> distributionFunction) {
      this.metadataResolver =
          new DestinationMetadataResolver(dynamicDestinations, catalogConfig, metadataView, false);
      this.distributionMode = distributionMode;
      this.distributionFunction = distributionFunction;
    }

    @Setup
    public void setup() {
      metadataResolver.setup();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element Row element,
        BoundedWindow window,
        PaneInfo paneInfo,
        @Timestamp Instant timestamp,
        OutputReceiver<KV<Row, Row>> out) {

      ResolvedDestination resolved =
          metadataResolver.resolve(c, element, window, paneInfo, timestamp);

      Row.Builder keyBuilder =
          Row.withSchema(outputSchemaFor(distributionMode))
              .addValue(resolved.tableIdentifier)
              .addValue(resolved.partitionPath);

      if (distributionMode == DistributionMode.RANGE) {
        int shard = 0;
        if (distributionFunction != null) {
          shard =
              checkStateNotNull(
                  distributionFunction.apply(resolved.data),
                  "The RANGE distribution function returned a null shard id. It must return a"
                      + " non-null Integer for every row.");
        }
        keyBuilder = keyBuilder.addValue(shard);
      }

      out.output(KV.of(keyBuilder.build(), resolved.data));
    }
  }

  /**
   * Accumulator and wire representation of a partition's estimated byte count, total row count, and
   * reservoir-sampled sort keys.
   */
  @VisibleForTesting
  static class PartitionSample {
    long totalBytes;
    long rowCount;
    List<byte[]> samples;

    PartitionSample(long totalBytes, long rowCount, List<byte[]> samples) {
      this.totalBytes = totalBytes;
      this.rowCount = rowCount;
      this.samples = samples;
    }
  }

  /** Deterministic {@link StructuredCoder} for {@link PartitionSample}. */
  static class PartitionSampleCoder extends StructuredCoder<PartitionSample> {
    private static final PartitionSampleCoder INSTANCE = new PartitionSampleCoder();
    private static final VarLongCoder LONG_CODER = VarLongCoder.of();
    private static final ListCoder<byte[]> SAMPLES_CODER = ListCoder.of(ByteArrayCoder.of());

    static PartitionSampleCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(PartitionSample value, OutputStream outStream)
        throws CoderException, IOException {
      LONG_CODER.encode(value.totalBytes, outStream);
      LONG_CODER.encode(value.rowCount, outStream);
      SAMPLES_CODER.encode(value.samples, outStream);
    }

    @Override
    public PartitionSample decode(InputStream inStream) throws CoderException, IOException {
      long totalBytes = LONG_CODER.decode(inStream);
      long rowCount = LONG_CODER.decode(inStream);
      List<byte[]> samples = new ArrayList<>(SAMPLES_CODER.decode(inStream));
      return new PartitionSample(totalBytes, rowCount, samples);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      LONG_CODER.verifyDeterministic();
      SAMPLES_CODER.verifyDeterministic();
    }
  }

  /** Per-bundle reservoir sample and byte counter for a single {@code (destination, partition)}. */
  private static class BundlePartitionSample {
    long sampledBytes = 0L;
    long sampledRows = 0L;
    long rowCount = 0L;
    final List<byte[]> reservoir = new ArrayList<>(MAX_SAMPLES_PER_BUNDLE);
    final Row partitionKeyRow;
    final BoundedWindow window;
    final Instant timestamp;

    BundlePartitionSample(Row partitionKeyRow, BoundedWindow window, Instant timestamp) {
      this.partitionKeyRow = partitionKeyRow;
      this.window = window;
      this.timestamp = timestamp;
    }

    /**
     * Returns the reservoir slot index {@code [0, MAX_SAMPLES_PER_BUNDLE)} if the next row should
     * be sampled into the reservoir, or {@code -1} if it is skipped.
     */
    int nextReservoirSlot() {
      rowCount++;
      if (reservoir.size() < MAX_SAMPLES_PER_BUNDLE) {
        return reservoir.size();
      }
      long replaceIdx = ThreadLocalRandom.current().nextLong(rowCount);
      return replaceIdx < MAX_SAMPLES_PER_BUNDLE ? (int) replaceIdx : -1;
    }

    void recordSample(int slotIndex, byte[] sortKeyBytes, long estimatedBytes) {
      sampledBytes += estimatedBytes;
      sampledRows++;
      if (slotIndex == reservoir.size()) {
        reservoir.add(sortKeyBytes);
      } else {
        reservoir.set(slotIndex, sortKeyBytes);
      }
    }

    long estimatedTotalBytes() {
      if (sampledRows <= 0L) {
        return 0L;
      }
      if (sampledRows == rowCount) {
        return sampledBytes;
      }
      return Math.round((double) sampledBytes * rowCount / sampledRows);
    }
  }

  /**
   * Computes {@code (destination, partition)} for each row while accumulating a lightweight
   * per-bundle reservoir sample of sort keys and estimated partition bytes emitted once per bundle
   * in {@link FinishBundle}. Sort keys and row byte sizes are only computed for rows selected into
   * the reservoir.
   */
  static class ExtractPartitionAndSampleSortKeyDoFn extends DoFn<Row, KV<Row, Row>> {

    private static final byte[] EMPTY_SORT_KEY = new byte[0];

    private final DestinationMetadataResolver metadataResolver;
    private transient @MonotonicNonNull Map<KV<Row, BoundedWindow>, BundlePartitionSample>
        bundleSamples;
    private transient @MonotonicNonNull ByteArrayOutputStream reusableKeyBaos;

    ExtractPartitionAndSampleSortKeyDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
      this.metadataResolver =
          new DestinationMetadataResolver(dynamicDestinations, catalogConfig, metadataView, true);
    }

    @Setup
    public void setup() {
      this.metadataResolver.setup();
      this.reusableKeyBaos = new ByteArrayOutputStream();
    }

    @StartBundle
    public void startBundle() {
      this.bundleSamples = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element Row element,
        BoundedWindow window,
        PaneInfo paneInfo,
        @Timestamp Instant timestamp,
        MultiOutputReceiver out)
        throws IOException {

      ResolvedDestination resolved =
          metadataResolver.resolve(c, element, window, paneInfo, timestamp);
      Row data = resolved.data;

      Row partitionKeyRow =
          Row.withSchema(OUTPUT_SCHEMA)
              .addValue(resolved.tableIdentifier)
              .addValue(resolved.partitionPath)
              .build();

      BundlePartitionSample sample =
          checkStateNotNull(bundleSamples)
              .computeIfAbsent(
                  KV.of(partitionKeyRow, window),
                  k -> new BundlePartitionSample(partitionKeyRow, window, timestamp));
      int slot = sample.nextReservoirSlot();
      if (slot >= 0) {
        byte[] sortKeyBytes =
            resolved.keyEncoder != null
                ? resolved.keyEncoder.encodeToBytes(data, checkStateNotNull(reusableKeyBaos))
                : EMPTY_SORT_KEY;
        long estimatedBytes =
            Math.max(16L, IcebergUtils.estimateRowBytes(data) + sortKeyBytes.length);
        sample.recordSample(slot, sortKeyBytes, estimatedBytes);
      }

      out.get(PARTITIONED_ROWS_TAG).output(KV.of(partitionKeyRow, data));
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      if (bundleSamples == null || bundleSamples.isEmpty()) {
        return;
      }
      for (BundlePartitionSample sample : bundleSamples.values()) {
        c.output(
            PARTITION_SAMPLES_TAG,
            KV.of(
                sample.partitionKeyRow,
                new PartitionSample(
                    sample.estimatedTotalBytes(), sample.rowCount, sample.reservoir)),
            sample.timestamp,
            sample.window);
      }
      bundleSamples.clear();
    }
  }

  /**
   * Combines per-bundle partition byte counts, row counts, and sort-key reservoirs to compute
   * right-sized, deduplicated quantile split points per {@code (destination, partition)}.
   */
  static class PartitionQuantilesCombineFn
      extends Combine.CombineFn<PartitionSample, PartitionSample, List<byte[]>> {

    private final long targetBytesPerShard;

    PartitionQuantilesCombineFn(long targetBytesPerShard) {
      this.targetBytesPerShard = Math.max(1L, targetBytesPerShard);
    }

    @Override
    public PartitionSample createAccumulator() {
      return new PartitionSample(0L, 0L, new ArrayList<>());
    }

    @Override
    public PartitionSample addInput(PartitionSample accum, PartitionSample input) {
      mergeIntoReservoir(accum, input);
      return accum;
    }

    @Override
    public PartitionSample mergeAccumulators(Iterable<PartitionSample> accumulators) {
      PartitionSample merged = createAccumulator();
      for (PartitionSample accum : accumulators) {
        mergeIntoReservoir(merged, accum);
      }
      return merged;
    }

    /**
     * Merges {@code incoming} into {@code accum} in place. When the combined sample count exceeds
     * {@link #MAX_COMBINED_SAMPLES} or the two reservoirs have different per-sample weights (e.g. a
     * small bundle merged with a large bundle that exceeded {@link #MAX_SAMPLES_PER_BUNDLE}), uses
     * Efraimidis-Spirakis weighted reservoir sampling without replacement so each retained sample
     * represents an equal number of rows and no sample is ever duplicated.
     */
    private static void mergeIntoReservoir(PartitionSample accum, PartitionSample incoming) {
      accum.totalBytes += Math.max(0L, incoming.totalBytes);
      List<byte[]> validIncoming = new ArrayList<>(incoming.samples.size());
      for (byte[] sample : incoming.samples) {
        if (sample != null && sample.length > 0) {
          validIncoming.add(sample);
        }
      }
      if (validIncoming.isEmpty()) {
        accum.rowCount += Math.max(0L, incoming.rowCount);
        return;
      }

      long effectiveIncomingRows = Math.max((long) validIncoming.size(), incoming.rowCount);
      if (accum.samples.isEmpty()) {
        if (validIncoming.size() <= MAX_COMBINED_SAMPLES) {
          accum.samples.addAll(validIncoming);
        } else {
          accum.samples.addAll(validIncoming.subList(0, MAX_COMBINED_SAMPLES));
        }
        accum.rowCount += effectiveIncomingRows;
        return;
      }

      long effectiveAccumRows = Math.max((long) accum.samples.size(), accum.rowCount);
      long combinedRows = effectiveAccumRows + effectiveIncomingRows;
      int nA = accum.samples.size();
      int nB = validIncoming.size();
      int totalCandidates = nA + nB;
      double weightA = (double) effectiveAccumRows / nA;
      double weightB = (double) effectiveIncomingRows / nB;

      if (totalCandidates <= MAX_COMBINED_SAMPLES && Math.abs(weightA - weightB) < 1e-9) {
        accum.samples.addAll(validIncoming);
        accum.rowCount = combinedRows;
        return;
      }

      double targetWeight =
          Math.max(Math.max(weightA, weightB), (double) combinedRows / MAX_COMBINED_SAMPLES);
      int targetSamples =
          (int)
              Math.min(
                  totalCandidates,
                  Math.min(
                      MAX_COMBINED_SAMPLES, Math.max(1L, Math.round(combinedRows / targetWeight))));

      double[] keys = new double[totalCandidates];
      Integer[] indices = new Integer[totalCandidates];
      ThreadLocalRandom rng = ThreadLocalRandom.current();
      for (int i = 0; i < nA; i++) {
        double u = Math.max(Double.MIN_NORMAL, rng.nextDouble());
        keys[i] = -Math.log(u) / weightA;
        indices[i] = i;
      }
      for (int j = 0; j < nB; j++) {
        double u = Math.max(Double.MIN_NORMAL, rng.nextDouble());
        int idx = nA + j;
        keys[idx] = -Math.log(u) / weightB;
        indices[idx] = idx;
      }
      Arrays.sort(indices, Comparator.comparingDouble(i -> keys[i]));

      List<byte[]> selected = new ArrayList<>(targetSamples);
      for (int k = 0; k < targetSamples; k++) {
        int chosen = indices[k];
        selected.add(chosen < nA ? accum.samples.get(chosen) : validIncoming.get(chosen - nA));
      }
      accum.samples = selected;
      accum.rowCount = combinedRows;
    }

    @Override
    public List<byte[]> extractOutput(PartitionSample accum) {
      long totalBytes = accum.totalBytes;
      List<byte[]> samples = new ArrayList<>(accum.samples);
      int numShards =
          (int)
              Math.min(
                  MAX_RANGE_SHARDS,
                  Math.max(1L, (totalBytes + targetBytesPerShard - 1) / targetBytesPerShard));
      if (numShards <= 1 || samples.isEmpty()) {
        return Collections.emptyList();
      }

      Comparator<byte[]> cmp = UnsignedBytes.lexicographicalComparator();
      samples.sort(cmp);

      List<byte[]> splits = new ArrayList<>(numShards - 1);
      int n = samples.size();
      for (int i = 1; i < numShards; i++) {
        int idx = (int) ((long) i * n / numShards);
        if (idx >= n) {
          idx = n - 1;
        }
        byte[] candidate = samples.get(idx);
        if (splits.isEmpty() || cmp.compare(splits.get(splits.size() - 1), candidate) < 0) {
          splits.add(candidate);
        }
      }
      return splits;
    }

    @Override
    public Coder<PartitionSample> getAccumulatorCoder(
        CoderRegistry registry, Coder<PartitionSample> inputCoder) {
      return PartitionSampleCoder.of();
    }

    @Override
    public Coder<List<byte[]>> getDefaultOutputCoder(
        CoderRegistry registry, Coder<PartitionSample> inputCoder) {
      return ListCoder.of(ByteArrayCoder.of());
    }
  }

  /**
   * Assigns the final {@code (destination, partition, shard)} key using the dynamically computed
   * quantile split points for each partition. Sort keys are only encoded for partitions that span
   * multiple range shards.
   */
  private static class AssignDynamicRangeShardDoFn extends DoFn<KV<Row, Row>, KV<Row, Row>> {

    private static final byte[] EMPTY_SORT_KEY = new byte[0];

    private final DestinationMetadataResolver metadataResolver;
    private final PCollectionView<Map<Row, List<byte[]>>> splitPointsView;
    private transient @MonotonicNonNull ByteArrayOutputStream reusableKeyBaos;

    AssignDynamicRangeShardDoFn(
        DynamicDestinations dynamicDestinations,
        IcebergCatalogConfig catalogConfig,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
        PCollectionView<Map<Row, List<byte[]>>> splitPointsView) {
      this.metadataResolver =
          new DestinationMetadataResolver(dynamicDestinations, catalogConfig, metadataView, true);
      this.splitPointsView = splitPointsView;
    }

    @Setup
    public void setup() {
      this.metadataResolver.setup();
      this.reusableKeyBaos = new ByteArrayOutputStream();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c, @Element KV<Row, Row> element, OutputReceiver<KV<Row, Row>> out)
        throws IOException {
      Row partitionKey = element.getKey();
      Row data = element.getValue();
      String tableIdentifier = checkStateNotNull(partitionKey.getString(DESTINATION));
      String partitionPath = checkStateNotNull(partitionKey.getString(PARTITION));

      Map<Row, List<byte[]>> splitMap = c.sideInput(splitPointsView);
      @Nullable List<byte[]> splits = splitMap != null ? splitMap.get(partitionKey) : null;
      int shard = 0;
      if (splits != null && !splits.isEmpty()) {
        IcebergRowSorter.@Nullable KeyEncoder keyEncoder =
            metadataResolver.resolveKeyEncoder(c, tableIdentifier, data.getSchema());
        byte[] sortKeyBytes =
            keyEncoder != null
                ? keyEncoder.encodeToBytes(data, checkStateNotNull(reusableKeyBaos))
                : EMPTY_SORT_KEY;
        shard = findRangeShard(splits, sortKeyBytes);
      }

      Row rangeKey =
          Row.withSchema(OUTPUT_SCHEMA_WITH_SHARD)
              .addValue(tableIdentifier)
              .addValue(partitionPath)
              .addValue(shard)
              .build();
      out.output(KV.of(rangeKey, data));
    }
  }
}
