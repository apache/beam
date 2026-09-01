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

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A driver transform that extracts table identifiers from incoming {@link Row}s, deduplicates them
 * per window, optionally bounds the cache size up to {@code maximumCacheSize}, loads their
 * declarative metadata from the Iceberg catalog, and emits {@link KV} pairs of table identifier
 * strings to {@link SerializableTableSpec}.
 *
 * <p>Can also be materialized into a broadcasted {@link PCollectionView} via {@link
 * #asView(IcebergCatalogConfig, DynamicDestinations)}. By default, the cache size is uncapped. If
 * {@code maximumCacheSize} is configured and the number of distinct tables in a window exceeds it,
 * up to {@code maximumCacheSize} tables are sampled into the broadcasted view, while remaining
 * destinations fall back to worker-local catalog loading.
 */
@Internal
@AutoValue
public abstract class TableMetadataDriver
    extends PTransform<PCollection<Row>, PCollection<KV<String, SerializableTableSpec>>> {

  public abstract IcebergCatalogConfig getCatalogConfig();

  public abstract DynamicDestinations getDynamicDestinations();

  public abstract @Nullable Integer getMaximumCacheSize();

  public static Builder builder() {
    return new AutoValue_TableMetadataDriver.Builder();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCatalogConfig(IcebergCatalogConfig catalogConfig);

    public abstract Builder setDynamicDestinations(DynamicDestinations dynamicDestinations);

    public abstract Builder setMaximumCacheSize(@Nullable Integer maximumCacheSize);

    abstract TableMetadataDriver autoBuild();

    public TableMetadataDriver build() {
      TableMetadataDriver driver = autoBuild();
      Integer maxCacheSize = driver.getMaximumCacheSize();
      if (maxCacheSize != null) {
        Preconditions.checkArgument(
            maxCacheSize > 0, "maximumCacheSize must be greater than 0, got %s", maxCacheSize);
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
    return asView(catalogConfig, dynamicDestinations, null);
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
    return new PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>() {
      @Override
      public PCollectionView<Map<String, SerializableTableSpec>> expand(PCollection<Row> input) {
        return input
            .apply(
                "GenerateTableMetadata",
                TableMetadataDriver.builder()
                    .setCatalogConfig(catalogConfig)
                    .setDynamicDestinations(dynamicDestinations)
                    .setMaximumCacheSize(maximumCacheSize)
                    .build())
            .apply("CreateTableMetadataView", View.asMap());
      }
    };
  }

  @Override
  public PCollection<KV<String, SerializableTableSpec>> expand(PCollection<Row> input) {
    PCollection<String> tableIds =
        input
            .apply("ExtractTableIds", ParDo.of(new ExtractTableIdsDoFn(getDynamicDestinations())))
            .setCoder(StringUtf8Coder.of());

    PCollection<String> distinctTableIds = tableIds.apply("DistinctTableIds", Distinct.create());

    PCollection<String> cachedTableIds;
    Integer maxCacheSize = getMaximumCacheSize();
    if (maxCacheSize != null) {
      cachedTableIds = distinctTableIds.apply("CapCacheSize", Sample.any(maxCacheSize));
    } else {
      cachedTableIds = distinctTableIds;
    }

    return cachedTableIds
        .apply("PollTableMetadata", ParDo.of(new CatalogPollingDoFn(getCatalogConfig())))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder()));
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

    private final IcebergCatalogConfig catalogConfig;

    CatalogPollingDoFn(IcebergCatalogConfig catalogConfig) {
      this.catalogConfig = catalogConfig;
    }

    @ProcessElement
    public void processElement(
        @Element String tableIdString, OutputReceiver<KV<String, SerializableTableSpec>> out) {
      TableIdentifier tableId = IcebergUtils.parseTableIdentifier(tableIdString);
      try {
        Table table = catalogConfig.catalog().loadTable(tableId);
        SerializableTableSpec spec = SerializableTableSpec.fromTable(tableIdString, table);
        TABLES_POLLED_COUNTER.inc();
        out.output(KV.of(tableIdString, spec));
      } catch (NoSuchTableException e) {
        LOG.debug(
            "Table '{}' does not exist in catalog. Skipping metadata emission for side-input view.",
            tableIdString);
      }
    }
  }
}
