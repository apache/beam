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

import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergExpireSnapshots {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergExpireSnapshots.class);

  public static void main(String[] args) throws IOException {
    IcebergPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IcebergPipelineOptions.class);

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", options.getCatalogUri())
            .put("catalog-name", options.getCatalogName())
            .put("warehouse", options.getWarehouse())
            .put("header.x-goog-user-project", options.getProject())
            .put("oauth2-server-uri", "https://oauth2.googleapis.com/token")
            .put(
                "token",
                GoogleCredentials.getApplicationDefault().refreshAccessToken().getTokenValue())
            .put("rest-metrics-reporting-enabled", "false")
            .build();

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    p.apply("StartPipeline", Create.of((Void) null))
        .apply(
            "ExpireSnapshots",
            ParDo.of(
                new ExpireSnapshotsPlanner(
                    options.getIcebergTable(), options.getCatalogName(), catalogProps)))
        .apply("GroupIntoBatches", GroupIntoBatches.ofSize(2))
        .apply(
            "DeleteFiles",
            ParDo.of(
                new DeleteFileBatch(
                    options.getIcebergTable(), options.getCatalogName(), catalogProps)));
    p.run().waitUntilFinish();
  }

  static class ExpireSnapshotsPlanner extends DoFn<Void, KV<String, String>> {
    private final String tableIdentifier;
    private final String catalogName;
    private final Map<String, String> catalogProps;
    private transient Table table;

    @SuppressWarnings("initialization")
    public ExpireSnapshotsPlanner(
        String tableIdentifier, String catalogName, Map<String, String> catalogProps) {
      this.tableIdentifier = tableIdentifier;
      this.catalogName = catalogName;
      this.catalogProps = catalogProps;
    }

    @Setup
    public void setup() {
      IcebergCatalogConfig catalogConfig =
          IcebergCatalogConfig.builder()
              .setCatalogName(catalogName)
              .setCatalogProperties(catalogProps)
              .build();
      Catalog catalog = catalogConfig.catalog();
      this.table = catalog.loadTable(TableIdentifier.parse(tableIdentifier));
    }

    @ProcessElement
    public void processElement(ProcessContext c, OutputReceiver<KV<String, String>> out) {
      ExpireSnapshots expire = table.expireSnapshots();
      expire
          .expireOlderThan(System.currentTimeMillis())
          .deleteWith(
              file -> {
                LOG.info("selected file info to delete {}", file);
                out.output(KV.of("key", file));
              })
          .cleanExpiredFiles(true)
          .commit();
    }
  }

  static class DeleteFileBatch extends DoFn<KV<String, Iterable<String>>, Void> {
    private final String tableIdentifier;
    private final String catalogName;
    private final Map<String, String> catalogProperties;
    private transient Table table;

    @SuppressWarnings("initialization")
    public DeleteFileBatch(
        String tableIdentifier, String catalogName, Map<String, String> catalogProperties) {
      this.tableIdentifier = tableIdentifier;
      this.catalogName = catalogName;
      this.catalogProperties = catalogProperties;
    }

    @Setup
    public void setup() {
      IcebergCatalogConfig catalogConfig =
          IcebergCatalogConfig.builder()
              .setCatalogName(catalogName)
              .setCatalogProperties(catalogProperties)
              .build();
      Catalog catalog = catalogConfig.catalog();
      this.table = catalog.loadTable(TableIdentifier.parse(tableIdentifier));
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      java.util.List<String> files = Lists.newArrayList(c.element().getValue());
      if (table.io() instanceof SupportsBulkOperations) {
        LOG.info("Executing Bulk delete");
        SupportsBulkOperations bulkOperations = (SupportsBulkOperations) table.io();
        bulkOperations.deleteFiles(files);
      } else {
        LOG.info("Executing single file delete");
        files.forEach(file -> table.io().deleteFile(file));
      }
      LOG.info("deleted files {}", files);
    }
  }

  public interface IcebergPipelineOptions extends GcpOptions {
    @Description(
        "Warehouse location where the table's data will be written to. "
            + "As of 07/14/25 BigLake only supports Single Region buckets")
    @Validation.Required
    @Default.String("gs://biglake_taxi_ride_metrics")
    String getWarehouse();

    void setWarehouse(String warehouse);

    @Description("The URI for the REST catalog.")
    @Validation.Required
    @Default.String("https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
    String getCatalogUri();

    void setCatalogUri(String value);

    @Description("The iceberg table to write to.")
    @Validation.Required
    @Default.String("taxi_dataset.ride_metrics_by_minute_test2")
    String getIcebergTable();

    void setIcebergTable(String value);

    @Validation.Required
    @Default.String("taxi_rides")
    String getCatalogName();

    void setCatalogName(String catalogName);
  }
}
