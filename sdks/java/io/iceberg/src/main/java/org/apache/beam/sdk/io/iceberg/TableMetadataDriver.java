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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A driver transform that extracts table identifiers from incoming {@link Row}s, deduplicates them
 * per window, optionally bounds the cache size up to {@code maximumCacheSize}, loads their
 * declarative metadata from the Iceberg catalog, and emits {@link KV} pairs of table identifier
 * strings to {@link SerializableTableSpec}. This is intended to be used in Beam pipelines that may
 * utilize a large number of workers to handle Iceberg writes, where having every worker thread
 * query for table metadata results in an excessive amount of requests and a high level of
 * redundancy.
 *
 * <p>Can also be materialized into a broadcasted {@link PCollectionView} via {@link
 * #asView(IcebergCatalogConfig, DynamicDestinations)}. By default, the cache size is uncapped. If
 * {@code maximumCacheSize} is configured and the number of distinct tables in a window exceeds it,
 * up to {@code maximumCacheSize} tables are sampled into the broadcasted view, while remaining
 * destinations fall back to worker-local catalog loading.
 *
 * <p>For unbounded streaming pipelines in {@link GlobalWindows}, {@link Deduplicate} is used to
 * deduplicate table identifiers over the configured {@code refreshInterval} (defaulting to {@link
 * #DEFAULT_REFRESH_INTERVAL}), allowing periodic refresh of table metadata when schemas evolve.
 */
@Internal
@AutoValue
public abstract class TableMetadataDriver
    extends PTransform<PCollection<Row>, PCollection<KV<String, SerializableTableSpec>>> {

  public static final Duration DEFAULT_REFRESH_INTERVAL = Duration.standardMinutes(5);
  public static final int DEFAULT_POLLING_BUCKETS = 1;

  public abstract IcebergCatalogConfig getCatalogConfig();

  public abstract DynamicDestinations getDynamicDestinations();

  public abstract @Nullable Integer getMaximumCacheSize();

  public abstract @Nullable Duration getRefreshInterval();

  /**
   * Returns the number of parallel buckets/workers used to query the Iceberg catalog, or {@code
   * null} for default.
   */
  public abstract @Nullable Integer getPollingBuckets();

  public static Builder builder() {
    return new AutoValue_TableMetadataDriver.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCatalogConfig(IcebergCatalogConfig catalogConfig);

    public abstract Builder setDynamicDestinations(DynamicDestinations dynamicDestinations);

    public abstract Builder setMaximumCacheSize(@Nullable Integer maximumCacheSize);

    public abstract Builder setRefreshInterval(@Nullable Duration refreshInterval);

    /**
     * Sets the number of parallel buckets (worker tasks) used to query the Iceberg catalog.
     *
     * <p>Defaults to {@link #DEFAULT_POLLING_BUCKETS} (1), which serializes all catalog lookups to
     * avoid overwhelming catalog metastores (e.g. Hive Metastore, REST catalog). For pipelines
     * writing to a large number of distinct dynamic tables (e.g. hundreds of tables per window),
     * consider increasing this value (e.g. 5–10) to parallelize catalog lookups while still
     * bounding load.
     */
    public abstract Builder setPollingBuckets(@Nullable Integer pollingBuckets);

    abstract TableMetadataDriver autoBuild();

    public TableMetadataDriver build() {
      TableMetadataDriver driver = autoBuild();
      Integer maxCacheSize = driver.getMaximumCacheSize();
      if (maxCacheSize != null) {
        Preconditions.checkArgument(
            maxCacheSize > 0, "maximumCacheSize must be greater than 0, got %s", maxCacheSize);
      }
      Duration refreshInterval = driver.getRefreshInterval();
      if (refreshInterval != null) {
        Preconditions.checkArgument(
            refreshInterval.isLongerThan(Duration.ZERO),
            "refreshInterval must be positive, got %s",
            refreshInterval);
      }
      Integer pollingBuckets = driver.getPollingBuckets();
      if (pollingBuckets != null) {
        Preconditions.checkArgument(
            pollingBuckets > 0, "pollingBuckets must be greater than 0, got %s", pollingBuckets);
      }
      return driver;
    }
  }

  /**
   * Helper that applies {@link TableMetadataDriver} and creates an uncapped {@link PCollectionView}
   * of {@link Map} of table identifier strings to {@link SerializableTableSpec}.
   */
  public static PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>
      asView(IcebergCatalogConfig catalogConfig, DynamicDestinations dynamicDestinations) {
    return asView(catalogConfig, dynamicDestinations, null, null, null);
  }

  /**
   * Helper that applies {@link TableMetadataDriver} with an optional {@code maximumCacheSize} limit
   * and creates a {@link PCollectionView} of {@link Map} of table identifier strings to {@link
   * SerializableTableSpec}.
   *
   * @param catalogConfig the catalog configuration used to poll metadata.
   * @param dynamicDestinations destination strategy extracting table IDs from rows.
   * @param maximumCacheSize optional maximum distinct tables to poll and broadcast per window (null
   *     for uncapped).
   */
  public static PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>
      asView(
          IcebergCatalogConfig catalogConfig,
          DynamicDestinations dynamicDestinations,
          @Nullable Integer maximumCacheSize) {
    return asView(catalogConfig, dynamicDestinations, maximumCacheSize, null, null);
  }

  /**
   * Helper that applies {@link TableMetadataDriver} with an optional {@code maximumCacheSize} limit
   * and custom {@code refreshInterval}, creating a {@link PCollectionView} of {@link Map} of table
   * identifier strings to {@link SerializableTableSpec}.
   *
   * @param catalogConfig the catalog configuration used to poll metadata.
   * @param dynamicDestinations destination strategy extracting table IDs from rows.
   * @param maximumCacheSize optional maximum distinct tables to poll and broadcast per window (null
   *     for uncapped).
   * @param refreshInterval optional refresh interval for streaming global window triggers.
   */
  public static PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>
      asView(
          IcebergCatalogConfig catalogConfig,
          DynamicDestinations dynamicDestinations,
          @Nullable Integer maximumCacheSize,
          @Nullable Duration refreshInterval) {
    return asView(catalogConfig, dynamicDestinations, maximumCacheSize, refreshInterval, null);
  }

  /**
   * Helper that applies {@link TableMetadataDriver} with an optional {@code maximumCacheSize}
   * limit, custom {@code refreshInterval}, and custom {@code pollingBuckets}, creating a {@link
   * PCollectionView} of {@link Map} of table identifier strings to {@link SerializableTableSpec}.
   *
   * @param catalogConfig the catalog configuration used to poll metadata.
   * @param dynamicDestinations destination strategy extracting table IDs from rows.
   * @param maximumCacheSize optional maximum distinct tables to poll and broadcast per window (null
   *     for uncapped).
   * @param refreshInterval optional refresh interval for streaming global window triggers.
   * @param pollingBuckets optional number of parallel buckets/workers for catalog polling.
   */
  public static PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>
      asView(
          IcebergCatalogConfig catalogConfig,
          DynamicDestinations dynamicDestinations,
          @Nullable Integer maximumCacheSize,
          @Nullable Duration refreshInterval,
          @Nullable Integer pollingBuckets) {
    return new PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>() {
      @Override
      public PCollectionView<Map<String, SerializableTableSpec>> expand(PCollection<Row> input) {
        boolean isStreaming = input.isBounded() == PCollection.IsBounded.UNBOUNDED;

        PCollection<KV<String, SerializableTableSpec>> specs =
            input.apply(
                "GenerateTableMetadata",
                TableMetadataDriver.builder()
                    .setCatalogConfig(catalogConfig)
                    .setDynamicDestinations(dynamicDestinations)
                    .setMaximumCacheSize(maximumCacheSize)
                    .setRefreshInterval(refreshInterval)
                    .setPollingBuckets(pollingBuckets)
                    .build());

        if (isStreaming) {
          return specs
              .apply("KeyForGlobalCache", WithKeys.of((Void) null))
              .setCoder(KvCoder.of(VoidCoder.of(), specs.getCoder()))
              .apply("AccumulateCacheMap", ParDo.of(new AccumulateTableMetadataMapDoFn()))
              .setCoder(MapCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder()))
              .apply(
                  "StreamingCacheWindow",
                  Window.<Map<String, SerializableTableSpec>>into(new GlobalWindows())
                      .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                      .discardingFiredPanes())
              .apply(
                  "CreateMetadataSingletonView",
                  Combine.globally(new MapMergerFn()).asSingletonView());
        }

        return specs.apply("CreateTableMetadataView", View.asMap());
      }
    };
  }

  @Override
  public PCollection<KV<String, SerializableTableSpec>> expand(PCollection<Row> input) {
    PCollection<String> tableIds =
        input
            .apply("ExtractTableIds", ParDo.of(new ExtractTableIdsDoFn(getDynamicDestinations())))
            .setCoder(StringUtf8Coder.of())
            .apply("MetadataGlobalWindow", Window.into(new GlobalWindows()));

    boolean isStreaming = input.isBounded() == PCollection.IsBounded.UNBOUNDED;

    PCollection<String> distinctTableIds;
    if (isStreaming) {
      Duration customInterval = getRefreshInterval();
      Duration interval =
          checkNotNull(customInterval != null ? customInterval : DEFAULT_REFRESH_INTERVAL);
      distinctTableIds =
          tableIds.apply(
              "DeduplicateTableIds", Deduplicate.<String>values().withDuration(interval));
    } else {
      distinctTableIds = tableIds.apply("DistinctTableIds", Distinct.create());
    }

    PCollection<String> cachedTableIds;
    Integer maxCacheSize = getMaximumCacheSize();
    if (maxCacheSize != null) {
      if (isStreaming) {
        throw new UnsupportedOperationException(
            "maximumCacheSize is currently not supported for unbounded streaming pipelines.");
      }
      cachedTableIds = distinctTableIds.apply("CapCacheSize", Sample.any(maxCacheSize));
    } else {
      cachedTableIds = distinctTableIds;
    }

    @Nullable Integer configuredBuckets = getPollingBuckets();
    int pollingBuckets = configuredBuckets != null ? configuredBuckets : DEFAULT_POLLING_BUCKETS;
    PCollection<String> pollingTableIds =
        cachedTableIds.apply(
            "ReshufflePollingBuckets",
            Reshuffle.<String>viaRandomKey().withNumBuckets(pollingBuckets));

    PCollection<KV<String, SerializableTableSpec>> specs =
        pollingTableIds
            .apply("PollTableMetadata", ParDo.of(new CatalogPollingDoFn(getCatalogConfig())))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder()));

    if (isStreaming) {
      return specs.apply(
          "ApplyStreamingTrigger",
          Window.<KV<String, SerializableTableSpec>>into(new GlobalWindows())
              .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
              .discardingFiredPanes());
    }
    return specs;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.addIfNotNull(
        DisplayData.item("maximumCacheSize", getMaximumCacheSize())
            .withLabel("Maximum Cache Size"));
    builder.addIfNotNull(
        DisplayData.item("refreshInterval", getRefreshInterval())
            .withLabel("Table Metadata Refresh Interval"));
    builder.addIfNotNull(
        DisplayData.item("pollingBuckets", getPollingBuckets())
            .withLabel("Catalog Polling Buckets"));
  }

  static class ExtractTableIdsDoFn extends DoFn<Row, String> {
    private final DynamicDestinations dynamicDestinations;

    ExtractTableIdsDoFn(DynamicDestinations dynamicDestinations) {
      this.dynamicDestinations = dynamicDestinations;
    }

    @ProcessElement
    public void processElement(
        @Element Row element,
        BoundedWindow window,
        PaneInfo paneInfo,
        @Timestamp Instant timestamp,
        OutputReceiver<String> out) {
      String tableIdentifier =
          dynamicDestinations.getTableStringIdentifier(
              ValueInSingleWindow.of(element, timestamp, window, paneInfo));
      if (tableIdentifier != null && !tableIdentifier.trim().isEmpty()) {
        out.output(tableIdentifier.trim());
      }
    }
  }

  static class CatalogPollingDoFn extends DoFn<String, KV<String, SerializableTableSpec>> {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogPollingDoFn.class);
    private static final Counter TABLES_POLLED_COUNTER =
        Metrics.counter(TableMetadataDriver.class, "tablesPolled");
    private static final Counter TABLES_SKIPPED_MISSING_COUNTER =
        Metrics.counter(TableMetadataDriver.class, "tablesSkippedMissing");

    private final IcebergCatalogConfig catalogConfig;

    CatalogPollingDoFn(IcebergCatalogConfig catalogConfig) {
      this.catalogConfig = catalogConfig;
    }

    @ProcessElement
    public void processElement(
        @Element String tableIdString, OutputReceiver<KV<String, SerializableTableSpec>> out) {
      TableIdentifier tableId;
      try {
        tableId = IcebergUtils.parseTableIdentifier(tableIdString);
      } catch (IllegalArgumentException e) {
        LOG.warn(
            "Failed to parse table identifier '{}'. Skipping metadata emission for side-input view.",
            tableIdString,
            e);
        TABLES_SKIPPED_MISSING_COUNTER.inc();
        return;
      }

      Table table;
      try {
        table = catalogConfig.catalog().loadTable(tableId);
      } catch (NoSuchTableException e) {
        LOG.info(
            "Table '{}' does not exist in catalog. Skipping metadata emission for side-input view.",
            tableIdString);
        TABLES_SKIPPED_MISSING_COUNTER.inc();
        return;
      }
      SerializableTableSpec spec;
      try {
        spec = SerializableTableSpec.fromTable(tableIdString, table);
      } catch (IllegalArgumentException e) {
        LOG.warn(
            "Failed to create SerializableTableSpec for table '{}'. Skipping metadata emission for side-input view.",
            tableIdString,
            e);
        TABLES_SKIPPED_MISSING_COUNTER.inc();
        return;
      }
      TABLES_POLLED_COUNTER.inc();
      out.output(KV.of(tableIdString, spec));
    }
  }

  static class AccumulateTableMetadataMapDoFn
      extends DoFn<
          KV<Void, KV<String, SerializableTableSpec>>, Map<String, SerializableTableSpec>> {
    @StateId("tableCache")
    private final StateSpec<MapState<String, SerializableTableSpec>> cacheStateSpec =
        StateSpecs.map(StringUtf8Coder.of(), SerializableTableSpec.getCoder());

    @ProcessElement
    public void processElement(
        @Element KV<Void, KV<String, SerializableTableSpec>> element,
        @StateId("tableCache") MapState<String, SerializableTableSpec> cacheState,
        OutputReceiver<Map<String, SerializableTableSpec>> out) {
      KV<String, SerializableTableSpec> kv = element.getValue();
      String tableId = kv.getKey();
      SerializableTableSpec newSpec = kv.getValue();

      ReadableState<SerializableTableSpec> existingState = cacheState.get(tableId);
      SerializableTableSpec existingSpec = existingState != null ? existingState.read() : null;
      if (existingSpec == null
          || newSpec.getLastUpdatedMillis() > existingSpec.getLastUpdatedMillis()
          || (newSpec.getLastUpdatedMillis() == existingSpec.getLastUpdatedMillis()
              && newSpec.getSchemaId() >= existingSpec.getSchemaId())) {
        cacheState.put(tableId, newSpec);
      }

      Map<String, SerializableTableSpec> mapSnapshot = new HashMap<>();
      for (Map.Entry<String, SerializableTableSpec> entry : cacheState.entries().read()) {
        mapSnapshot.put(entry.getKey(), entry.getValue());
      }
      out.output(Collections.unmodifiableMap(mapSnapshot));
    }
  }

  static class MapMergerFn extends Combine.BinaryCombineFn<Map<String, SerializableTableSpec>> {
    @Override
    public Map<String, SerializableTableSpec> apply(
        Map<String, SerializableTableSpec> left, Map<String, SerializableTableSpec> right) {
      if (left == null || left.isEmpty()) {
        return right != null ? right : Collections.emptyMap();
      }
      if (right == null || right.isEmpty()) {
        return left;
      }
      Map<String, SerializableTableSpec> merged = new HashMap<>(left);
      for (Map.Entry<String, SerializableTableSpec> entry : right.entrySet()) {
        String tableId = entry.getKey();
        SerializableTableSpec rightSpec = entry.getValue();
        SerializableTableSpec leftSpec = merged.get(tableId);
        if (leftSpec == null) {
          merged.put(tableId, rightSpec);
        } else if (rightSpec.getLastUpdatedMillis() > leftSpec.getLastUpdatedMillis()) {
          merged.put(tableId, rightSpec);
        } else if (rightSpec.getLastUpdatedMillis() == leftSpec.getLastUpdatedMillis()) {
          // Deterministic tie-breaker for strict commutativity
          if (rightSpec.getSchemaId() > leftSpec.getSchemaId()) {
            merged.put(tableId, rightSpec);
          }
        }
      }
      return Collections.unmodifiableMap(merged);
    }

    @Override
    public Map<String, SerializableTableSpec> identity() {
      return Collections.emptyMap();
    }
  }
}
