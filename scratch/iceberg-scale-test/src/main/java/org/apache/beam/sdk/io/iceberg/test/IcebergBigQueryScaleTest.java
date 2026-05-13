package org.apache.beam.sdk.io.iceberg.test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergDestination;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergTableCreateConfig;
import org.apache.iceberg.DistributionMode;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;

public class IcebergBigQueryScaleTest implements Serializable {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    Pipeline pipeline = Pipeline.create(options);

    // 1. Define Hadoop Catalog on GCS Biglake bucket
    Map<String, String> catalogProps = new HashMap<>();
    catalogProps.put("type", "hadoop");
    catalogProps.put("warehouse", "gs://at-euw4-biglake-bucket/iceberg-warehouse");
    catalogProps.put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO");

    Map<String, String> configProps = new HashMap<>();
    configProps.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    configProps.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("hadoop_catalog")
            .setCatalogProperties(catalogProps)
            .setConfigProperties(configProps)
            .build();

    // 2. Read crypto_ethereum.blocks partitioned table from BigQuery using High-Performance Direct Read API
    PCollection<Row> bqRows =
        pipeline.apply(
            "Read Ethereum Blocks from BigQuery",
            BigQueryIO.readTableRowsWithSchema()
                .from("bigquery-public-data.crypto_ethereum.blocks")
                .withMethod(Method.DIRECT_READ))
            .apply("Convert to Beam Rows", Convert.toRows());

    final Schema dataSchema = bqRows.getSchema();
    final String salt = UUID.randomUUID().toString().substring(0, 8);

    // 3. Configure dynamic destinations to create partitioned and sorted Iceberg tables
    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          @Override
          public Schema getDataSchema() {
            return dataSchema;
          }

          @Override
          public Row getData(Row element) {
            return element;
          }

          @Override
          public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
            // Write all blocks to a single Ethereum blocks table
            return "hadoop_catalog.default.ethereum_blocks_" + salt;
          }

          @Override
          public IcebergDestination instantiateDestination(String dest) {
            java.util.Map<String, String> properties = new java.util.HashMap<>();
            properties.put("write.format.default", "parquet");
            properties.put("write.target-file-size-bytes", "10485760"); // 10MB target
            properties.put("write.parquet.page-size-bytes", "1048576");  // 1MB page size

            return IcebergDestination.builder()
                .setTableIdentifier(TableIdentifier.parse(dest))
                .setFileFormat(FileFormat.PARQUET)
                .setTableCreateConfig(
                    IcebergTableCreateConfig.builder()
                        .setSchema(getDataSchema())
                        // Partition on timestamp column (day-based) and sort by block hash
                        .setPartitionFields(Arrays.asList("month(timestamp)"))
                        .setSortFields(Arrays.asList("hash asc"))
                        .setTableProperties(properties)
                        .build())
                .build();
          }
        };

    bqRows.apply(
        "Write Sorted Partitioned Blocks to GCS Iceberg Warehouse",
        IcebergIO.writeRows(catalogConfig)
            .to(dynamicDestinations)
            .withDistributionMode(DistributionMode.HASH)
            .withAutosharding());

    System.out.println("Staging Dataflow pipeline graph...");
    pipeline.run();
    System.out.println("Dataflow pipeline launched successfully!");
  }
}
