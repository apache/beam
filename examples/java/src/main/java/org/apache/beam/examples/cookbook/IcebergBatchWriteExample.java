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
package org.apache.beam.examples.cookbook;

import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * This pipeline demonstrates a batch write to an Iceberg table using the BigQuery Metastore
 * catalog.
 *
 * <p>The pipeline reads from a public BigQuery table containing Google Analytics session data,
 * extracts and aggregates the total number of transactions per web browser, and writes the results
 * to a new Iceberg table managed by the BigQuery Metastore.
 *
 * <p>This example is a demonstration of the Iceberg BigQuery Metastore. For more information, see
 * the documentation at https://cloud.google.com/bigquery/docs/blms-use-dataproc.
 */
public class IcebergBatchWriteExample {

  public static final Schema BQ_SCHEMA =
      Schema.builder().addStringField("browser").addInt64Field("transactions").build();

  public static final Schema AGGREGATED_SCHEMA =
      Schema.builder().addStringField("browser").addInt64Field("transaction_count").build();

  public static final String BQ_TABLE =
      "bigquery-public-data.google_analytics_sample.ga_sessions_20170801";

  private static Row flattenAnalyticsRow(Row row) {
    Row device = Preconditions.checkStateNotNull(row.getRow("device"));
    Row totals = Preconditions.checkStateNotNull(row.getRow("totals"));
    return Row.withSchema(BQ_SCHEMA)
        .withFieldValue("browser", Preconditions.checkStateNotNull(device.getString("browser")))
        .withFieldValue(
            "transactions", Preconditions.checkStateNotNull(totals.getInt64("transactions")))
        .build();
  }

  static class ExtractBrowserTransactionsFn extends DoFn<Row, KV<String, Long>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      c.output(
          KV.of(
              Preconditions.checkStateNotNull(row.getString("browser")),
              Preconditions.checkStateNotNull(row.getInt64("transactions"))));
    }
  }

  static class FormatCountsFn extends DoFn<KV<String, Long>, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row =
          Row.withSchema(AGGREGATED_SCHEMA)
              .withFieldValue("browser", c.element().getKey())
              .withFieldValue("transaction_count", c.element().getValue())
              .build();
      c.output(row);
    }
  }

  static class CountTransactions extends PTransform<PCollection<Row>, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollection<Row> rows) {
      PCollection<KV<String, Long>> browserTransactions =
          rows.apply(ParDo.of(new ExtractBrowserTransactionsFn()));
      PCollection<KV<String, Long>> browserCounts = browserTransactions.apply(Sum.longsPerKey());
      return browserCounts.apply(ParDo.of(new FormatCountsFn()));
    }
  }

  /** Pipeline options for this example. */
  public interface IcebergPipelineOptions extends GcpOptions {
    @Description(
        "Warehouse location where the table's data will be written to. "
            + "As of 07/14/25 BigLake only supports Single Region buckets")
    @Validation.Required
    @Default.String("gs://analytics_warehouse")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The Iceberg table to write to, in the format 'dataset.table'.")
    @Validation.Required
    @Default.String("analytics_dataset.transactions_by_browser")
    String getIcebergTable();

    void setIcebergTable(String value);

    @Description("The name of the catalog to use.")
    @Validation.Required
    @Default.String("analytics")
    String getCatalogName();

    void setCatalogName(String catalogName);

    @Description("The implementation of the Iceberg catalog.")
    @Default.String("org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog")
    String getCatalogImpl();

    void setCatalogImpl(String catalogImpl);

    @Description("The GCP location for the BigQuery Metastore.")
    @Default.String("us-central1")
    String getGcpLocation();

    void setGcpLocation(String gcpLocation);

    @Description("The implementation of the Iceberg FileIO.")
    @Default.String("org.apache.iceberg.gcp.gcs.GCSFileIO")
    String getIoImpl();

    void setIoImpl(String ioImpl);
  }

  /**
   * Main entry point for the pipeline.
   *
   * @param args Command line arguments
   * @throws IOException if there's an issue with the pipeline setup
   */
  public static void main(String[] args) throws IOException {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergPipelineOptions.class);

    final String tableIdentifier = options.getIcebergTable();
    final String warehouseLocation = options.getWarehouse();
    final String catalogName = options.getCatalogName();
    final String projectName = options.getProject();
    final String catalogImpl = options.getCatalogImpl();
    final String gcpLocation = options.getGcpLocation();
    final String ioImpl = options.getIoImpl();

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("warehouse", warehouseLocation)
            .put("catalog-impl", catalogImpl)
            .put("gcp_project", projectName)
            .put("gcp_location", gcpLocation)
            .put("io-impl", ioImpl)
            .build();

    Map<String, Object> icebergWriteConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", tableIdentifier)
            .put("catalog_properties", catalogProps)
            .put("catalog_name", catalogName)
            .build();

    Map<String, Object> bigQueryReadConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", BQ_TABLE)
            .put("fields", ImmutableList.of("device.browser", "totals.transactions"))
            .put("row_restriction", "totals.transactions is not null")
            .build();

    Pipeline p = Pipeline.create(options);

    p.apply("ReadFromBigQuery", Managed.read(Managed.BIGQUERY).withConfig(bigQueryReadConfig))
        .get("output")
        .apply(
            "Flatten",
            MapElements.into(TypeDescriptors.rows())
                .via(IcebergBatchWriteExample::flattenAnalyticsRow))
        .setRowSchema(BQ_SCHEMA)
        .apply("CountTransactions", new CountTransactions())
        .setRowSchema(AGGREGATED_SCHEMA)
        .apply("WriteToIceberg", Managed.write(Managed.ICEBERG).withConfig(icebergWriteConfig));

    p.run().waitUntilFinish();
  }
}
