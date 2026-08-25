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
import org.joda.time.Instant;

/**
 * A driver transform that extracts table identifiers from incoming {@link Row}s, deduplicates them
 * per window, samples up to a maximum number of tables, loads their declarative metadata from the
 * Iceberg catalog, and emits {@link KV} pairs of table identifier strings to {@link
 * SerializableTableSpec}.
 *
 * <p>Can also be materialized into a broadcasted {@link PCollectionView} via {@link
 * #asView(IcebergCatalogConfig, DynamicDestinations)}. If the number of distinct tables in a window
 * exceeds {@code maxTables}, up to {@code maxTables} tables are sampled into the broadcasted view,
 * while remaining destinations can fall back to worker-local catalog loading.
 */
@Internal
@AutoValue
public abstract class TableMetadataDriver
    extends PTransform<PCollection<Row>, PCollection<KV<String, SerializableTableSpec>>> {

  public static final int DEFAULT_MAX_TABLES = 100;

  public abstract IcebergCatalogConfig getCatalogConfig();

  public abstract DynamicDestinations getDynamicDestinations();

  public abstract int getMaxTables();

  public static Builder builder() {
    return new AutoValue_TableMetadataDriver.Builder().setMaxTables(DEFAULT_MAX_TABLES);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setCatalogConfig(IcebergCatalogConfig catalogConfig);

    public abstract Builder setDynamicDestinations(DynamicDestinations dynamicDestinations);

    public abstract Builder setMaxTables(int maxTables);

    abstract TableMetadataDriver autoBuild();

    public TableMetadataDriver build() {
      TableMetadataDriver driver = autoBuild();
      Preconditions.checkArgument(
          driver.getMaxTables() > 0,
          "maxTables must be greater than 0, got %s",
          driver.getMaxTables());
      return driver;
    }
  }

  /**
   * Helper that applies {@link TableMetadataDriver} and creates a {@link PCollectionView} of {@link
   * Map} of table identifier strings to {@link SerializableTableSpec} using {@link
   * #DEFAULT_MAX_TABLES}.
   */
  public static PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>
      asView(IcebergCatalogConfig catalogConfig, DynamicDestinations dynamicDestinations) {
    return asView(catalogConfig, dynamicDestinations, DEFAULT_MAX_TABLES);
  }

  /**
   * Helper that applies {@link TableMetadataDriver} with a custom {@code maxTables} limit and
   * creates a {@link PCollectionView} of {@link Map} of table identifier strings to {@link
   * SerializableTableSpec}.
   *
   * @param catalogConfig the catalog configuration used to poll metadata.
   * @param dynamicDestinations destination strategy extracting table IDs from rows.
   * @param maxTables maximum distinct tables to poll and broadcast per window.
   */
  public static PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>
      asView(
          IcebergCatalogConfig catalogConfig,
          DynamicDestinations dynamicDestinations,
          int maxTables) {
    return new PTransform<PCollection<Row>, PCollectionView<Map<String, SerializableTableSpec>>>() {
      @Override
      public PCollectionView<Map<String, SerializableTableSpec>> expand(PCollection<Row> input) {
        return input
            .apply(
                "GenerateTableMetadata",
                TableMetadataDriver.builder()
                    .setCatalogConfig(catalogConfig)
                    .setDynamicDestinations(dynamicDestinations)
                    .setMaxTables(maxTables)
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

    PCollection<String> sampledTableIds =
        distinctTableIds.apply("SampleTableIds", Sample.any(getMaxTables()));

    return sampledTableIds
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
      Table table = catalogConfig.catalog().loadTable(tableId);
      SerializableTableSpec spec = SerializableTableSpec.fromTable(tableIdString, table);
      TABLES_POLLED_COUNTER.inc();
      out.output(KV.of(tableIdString, spec));
    }
  }
}
