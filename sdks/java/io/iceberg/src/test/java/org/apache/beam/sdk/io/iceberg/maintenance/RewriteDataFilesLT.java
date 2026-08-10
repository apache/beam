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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Large load test for {@link RewriteDataFiles} (bin-pack compaction).
 *
 * <p>Populates a partitioned table with many small data files (written by a distributed Beam
 * pipeline), runs the rewrite, and asserts the row count is preserved while the data-file count is
 * drastically reduced.
 *
 * <p>Runs at two sizes via {@code --testSize}:
 *
 * <ul>
 *   <li>{@code small} (default): a few hundred files, runnable on the DirectRunner for CI.
 *   <li>{@code large}: tens of thousands of files across hundreds of partitions, for Dataflow.
 * </ul>
 *
 * <p>Dataflow invocation (the {@code loadTest} task already passes {@code --testSize=large
 * --runner=DataflowRunner}); pass a GCS warehouse via {@code beamTestPipelineOptions}:
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:iceberg:loadTest \
 *   --tests "org.apache.beam.sdk.io.iceberg.maintenance.RewriteDataFilesLT" \
 *   -PgcpProject=apache-beam-testing
 * }</pre>
 */
@RunWith(JUnit4.class)
public class RewriteDataFilesLT {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesLT.class);

  /** Options controlling the load-test scale and warehouse location. */
  public interface Options extends PipelineOptions {
    @Default.String("small")
    String getTestSize();

    void setTestSize(String value);

    @Default.String("")
    String getWarehouse();

    void setWarehouse(String value);
  }

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "shard", Types.IntegerType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("shard").build();
  private static final long TARGET_FILE_SIZE_BYTES = 256L * 1024 * 1024; // 256 MB

  @BeforeClass
  public static void registerOptions() {
    PipelineOptionsFactory.register(Options.class);
  }

  @Test
  public void rewriteManySmallFiles() throws Exception {
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    boolean large = "large".equalsIgnoreCase(options.getTestSize());

    int numFiles = large ? 20_000 : 250;
    int recordsPerFile = large ? 2_500 : 80;
    int numPartitions = large ? 200 : 8;
    long expectedRows = (long) numFiles * recordsPerFile;

    String warehouse =
        options.getWarehouse().isEmpty()
            ? "file://" + Files.createTempDirectory("rewrite-lt-")
            : options.getWarehouse();
    Map<String, String> catalogProps = ImmutableMap.of("type", "hadoop", "warehouse", warehouse);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder().setCatalogProperties(catalogProps).build();

    TableIdentifier tableId =
        TableIdentifier.of("loadtest", "rewrite_" + UUID.randomUUID().toString().replace('-', '_'));
    Catalog catalog = catalogConfig.catalog();
    if (catalog instanceof SupportsNamespaces) {
      Namespace ns = tableId.namespace();
      if (!((SupportsNamespaces) catalog).namespaceExists(ns)) {
        ((SupportsNamespaces) catalog).createNamespace(ns);
      }
    }
    Table table = catalog.createTable(tableId, SCHEMA, SPEC);

    // --- Populate: write `numFiles` small data files (distributed), then commit them all. ---
    Pipeline writePipeline = Pipeline.create(options);
    PCollection<SerializableDataFile> written =
        writePipeline
            .apply("File indices", GenerateSequence.from(0).to(numFiles))
            .apply("Spread", Redistribute.arbitrarily())
            .apply(
                "Write small files",
                ParDo.of(
                    new WriteSmallFileFn(
                        warehouse, tableId.toString(), recordsPerFile, numPartitions)));
    written
        .apply("Key for commit", WithKeys.of(0))
        .apply("Group files", GroupByKey.create())
        .apply("Commit append", ParDo.of(new CommitAppendFn(warehouse, tableId.toString())));
    writePipeline.run().waitUntilFinish();

    table.refresh();
    long filesBefore = totalDataFiles(table);
    LOG.info("[RewriteLT] populated {} rows across {} data files", expectedRows, filesBefore);
    assertTrue("expected many small files before rewrite", filesBefore >= numFiles);

    // --- Rewrite (compaction). ---
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(large)
            .setMaxCommits(large ? 20 : 2)
            .setRewriteOptions(
                ImmutableMap.of(
                    "target-file-size-bytes",
                    String.valueOf(TARGET_FILE_SIZE_BYTES),
                    "min-input-files",
                    "2"))
            .build();
    PipelineResult result =
        IcebergMaintenance.create(tableId.toString(), catalogProps, options)
            .rewriteDataFiles(config)
            .run();
    result.waitUntilFinish();

    // --- Assert: rows preserved, files drastically reduced, only replace snapshots created. ---
    Table reloaded = catalog.loadTable(tableId);
    long filesAfter = totalDataFiles(reloaded);
    LOG.info("[RewriteLT] after rewrite: {} data files (was {})", filesAfter, filesBefore);

    assertEquals(
        "row count must be preserved (exact Iceberg total-records summary)",
        expectedRows,
        totalRecords(reloaded));
    assertTrue(
        "compaction must drastically reduce the data-file count",
        filesAfter < filesBefore && filesAfter <= Math.max(numPartitions * 4L, filesBefore / 2));
    assertEquals(
        "the latest snapshot must be a replace", "replace", reloaded.currentSnapshot().operation());

    // Content check for the small (CI) case: the fixture writes id = fileIndex*recordsPerFile + i,
    // so every id in [0, expectedRows) must survive EXACTLY once. A sum+xor checksum over the
    // scanned ids (derived from the GenerateSequence bounds) catches dropped, duplicated, or
    // swapped
    // rows that a bare count would miss. The large case trusts the exact total-records summary
    // (maintained atomically by the rewrite commit) rather than scan tens of millions of rows in
    // the test driver.
    if (!large) {
      long expectedSum = 0;
      long expectedXor = 0;
      for (long id = 0; id < expectedRows; id++) {
        expectedSum += id;
        expectedXor ^= id;
      }
      long scanned = 0;
      long actualSum = 0;
      long actualXor = 0;
      try (CloseableIterable<Record> rows =
          org.apache.iceberg.data.IcebergGenerics.read(reloaded).build()) {
        for (Record r : rows) {
          long id = (Long) r.getField("id");
          actualSum += id;
          actualXor ^= id;
          scanned++;
        }
      }
      assertEquals("read-back row count must match", expectedRows, scanned);
      assertEquals(
          "id sum checksum must match — no dropped/duplicated/swapped rows",
          expectedSum,
          actualSum);
      assertEquals("id xor checksum must match", expectedXor, actualXor);
    }
  }

  private static long totalDataFiles(Table table) {
    String total = table.currentSnapshot().summary().get("total-data-files");
    return total != null ? Long.parseLong(total) : -1;
  }

  private static long totalRecords(Table table) {
    String total = table.currentSnapshot().summary().get("total-records");
    return total != null ? Long.parseLong(total) : -1;
  }

  /** Writes a single small Iceberg data file into one partition and emits its serializable form. */
  private static class WriteSmallFileFn extends DoFn<Long, SerializableDataFile> {
    private final String warehouse;
    private final String tableIdentifier;
    private final int recordsPerFile;
    private final int numPartitions;
    private transient Table table;

    WriteSmallFileFn(
        String warehouse, String tableIdentifier, int recordsPerFile, int numPartitions) {
      this.warehouse = warehouse;
      this.tableIdentifier = tableIdentifier;
      this.recordsPerFile = recordsPerFile;
      this.numPartitions = numPartitions;
    }

    private Table table() {
      if (table == null) {
        table =
            IcebergCatalogConfig.builder()
                .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse))
                .build()
                .catalog()
                .loadTable(TableIdentifier.parse(tableIdentifier));
      }
      return table;
    }

    @ProcessElement
    public void process(@Element Long fileIndex, OutputReceiver<SerializableDataFile> out)
        throws IOException {
      Table table = table();
      int shard = (int) (fileIndex % numPartitions);

      Record partitionHolder = GenericRecord.create(SCHEMA);
      partitionHolder.setField("shard", shard);
      PartitionKey partitionKey = new PartitionKey(SPEC, SCHEMA);
      partitionKey.partition(new InternalRecordWrapper(SCHEMA.asStruct()).wrap(partitionHolder));

      String path =
          table.location() + "/data/lt-" + fileIndex + "-" + UUID.randomUUID() + ".parquet";
      OutputFile outputFile = table.io().newOutputFile(path);
      DataWriter<Record> writer =
          Parquet.writeData(outputFile)
              .schema(SCHEMA)
              .createWriterFunc(GenericParquetWriter::create)
              .withSpec(SPEC)
              .withPartition(partitionKey)
              .overwrite()
              .build();
      try {
        for (int i = 0; i < recordsPerFile; i++) {
          Record record = GenericRecord.create(SCHEMA);
          record.setField("id", fileIndex * recordsPerFile + i);
          record.setField("data", "v" + fileIndex + "_" + i);
          record.setField("shard", shard);
          writer.write(record);
        }
      } finally {
        writer.close();
      }
      DataFile dataFile = writer.toDataFile();
      out.output(SerializableDataFile.from(dataFile, SPEC));
    }
  }

  /** Appends all written data files to the table in a single commit. */
  private static class CommitAppendFn
      extends DoFn<KV<Integer, Iterable<SerializableDataFile>>, Void> {
    private final String warehouse;
    private final String tableIdentifier;

    CommitAppendFn(String warehouse, String tableIdentifier) {
      this.warehouse = warehouse;
      this.tableIdentifier = tableIdentifier;
    }

    @ProcessElement
    public void process(@Element KV<Integer, Iterable<SerializableDataFile>> element) {
      Table table =
          IcebergCatalogConfig.builder()
              .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouse))
              .build()
              .catalog()
              .loadTable(TableIdentifier.parse(tableIdentifier));
      AppendFiles append = table.newAppend();
      for (SerializableDataFile sdf : element.getValue()) {
        append.appendFile(sdf.createDataFile(table.specs()));
      }
      append.commit();
    }
  }
}
