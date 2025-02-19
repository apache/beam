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
package org.apache.beam.temp;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergShardingRuns {
  static String PROJECT = "apache-beam-testing";
  static String DATASET = "ahmedabualsaud";
  static String WAREHOUSE = "gs://ahmedabualsaud-apache-beam-testing";
  static String BQMS_CATALOG = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog";
  static Schema SCHEMA = Schema.builder().addByteArrayField("bytes").build();

  public static void main(String[] args) throws IOException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject(PROJECT);
    options.setRegion("us-central1");
    options.setRunner(DataflowRunner.class);
    options.setExperiments(Arrays.asList("use_runner_v2", GcpOptions.STREAMING_ENGINE_EXPERIMENT));

    // ======== experiment with these numbers ===========
    int numShards = 1;
    long numRecords = 5_000_000L;
    long payloadSize = 1 << 10; // 1KB
    int numIcebergPartitions = 0;
    // ==================================================

    int initialSplits = 20;
    long recordsPerSecond = 1_000;
    long mbps = Math.round((double) payloadSize * recordsPerSecond / (1 << 20));
    String name =
        String.format(
            "test-iceberg-bqms-%sshards-%smbps-%spartitions-%s",
            numShards, mbps, numIcebergPartitions, System.nanoTime());
    options.setJobName(name);
    String tableId = String.format("%s.%s", DATASET, name);
    if (numIcebergPartitions > 1) {
      createTableWithPartitions(tableId, numIcebergPartitions);
    }

    long rowsPerSecondPerSplit = recordsPerSecond / initialSplits;
    double delayMillis = 1000d / rowsPerSecondPerSplit;
    SyntheticSourceOptions syntheticSourceOptions =
        SyntheticSourceOptions.fromJsonString(
            String.format(
                "{\"numRecords\":%s, \"valueSizeBytes\":%s, "
                    + "\"delayDistribution\":{\"type\":\"const\",\"const\":%s}, "
                    + "\"forceNumInitialBundles\":%s}",
                numRecords, payloadSize, delayMillis, initialSplits),
            SyntheticSourceOptions.class);

    Pipeline p = Pipeline.create(options);
    PCollection<Row> rows =
        p.apply(Read.from(new SyntheticUnboundedSource(syntheticSourceOptions)))
            .apply(
                "Convert to Rows",
                MapElements.into(TypeDescriptors.rows())
                    .via(kv -> Row.withSchema(SCHEMA).addValue(kv.getValue()).build()))
            .setRowSchema(SCHEMA);

    int multiplier = (int) Math.ceil(1 / delayMillis);
    if (multiplier > 1) {
      rows = rows.apply("Multiply by " + multiplier, ParDo.of(Multiplier.of(multiplier)));
    }

    rows.apply(
        Managed.write(Managed.ICEBERG)
            .withConfig(
                ImmutableMap.of(
                    "table",
                    tableId,
                    "catalog_properties",
                    catalogProps(numShards),
                    "triggering_frequency_seconds",
                    30)));

    System.out.println("Dataflow job and table name: " + tableId);

    p.run();
  }

  static class Multiplier extends DoFn<Row, Row> {
    int multiplier;

    Multiplier(int multiplier) {
      this.multiplier = multiplier;
    }

    static Multiplier of(int multiplier) {
      return new Multiplier(multiplier);
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> out) {
      for (int i = 0; i < multiplier; i++) {
        out.output(row);
      }
    }
  }

  static void createTableWithPartitions(String tableId, int numPartitions) {
    org.apache.iceberg.Schema icebergSchema = IcebergUtils.beamSchemaToIcebergSchema(SCHEMA);
    PartitionSpec spec =
        PartitionSpec.builderFor(icebergSchema).bucket("bytes", numPartitions).build();
    catalog.createTable(TableIdentifier.parse(tableId), icebergSchema, spec);
  }

  static Map<String, String> catalogProps(int numShards) {
    return ImmutableMap.of(
        "catalog-impl",
        BQMS_CATALOG,
        "io-impl",
        "org.apache.iceberg.gcp.gcs.GCSFileIO",
        "gcp_project",
        PROJECT,
        "gcp_location",
        "us-central1",
        "warehouse",
        WAREHOUSE,
        "num_shards",
        String.valueOf(numShards));
  }

  static Catalog catalog =
      CatalogUtil.loadCatalog(BQMS_CATALOG, "bqms_catalog", catalogProps(0), new Configuration());
}
