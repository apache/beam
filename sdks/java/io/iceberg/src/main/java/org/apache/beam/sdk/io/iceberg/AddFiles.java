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

import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.DATA_FILES;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.ERRORS;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.ERROR_SCHEMA;
import static org.apache.beam.sdk.metrics.Metrics.counter;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transform that takes in a stream of file paths, converts them to Iceberg {@link DataFile}s with
 * partition metadata and metrics, then commits them to an Iceberg {@link Table}.
 */
public class AddFiles extends SchemaTransform {
  private static final Duration DEFAULT_TRIGGER_INTERVAL = Duration.standardMinutes(10);
  private static final Counter numFilesAdded = counter(AddFiles.class, "numFilesAdded");
  private static final Counter numErrorFiles = counter(AddFiles.class, "numErrorFiles");
  private static final Logger LOG = LoggerFactory.getLogger(AddFiles.class);
  private static final int DEFAULT_FILES_TRIGGER = 100_000;
  private final IcebergCatalogConfig catalogConfig;
  private final String tableIdentifier;
  private final Duration intervalTrigger;
  private final int numFilesTrigger;
  private final @Nullable String locationPrefix;

  public AddFiles(
      IcebergCatalogConfig catalogConfig,
      String tableIdentifier,
      @Nullable String locationPrefix,
      @Nullable Integer numFilesTrigger,
      @Nullable Duration intervalTrigger) {
    this.catalogConfig = catalogConfig;
    this.tableIdentifier = tableIdentifier;
    this.intervalTrigger = intervalTrigger != null ? intervalTrigger : DEFAULT_TRIGGER_INTERVAL;
    this.numFilesTrigger = numFilesTrigger != null ? numFilesTrigger : DEFAULT_FILES_TRIGGER;
    this.locationPrefix = locationPrefix;
  }

  @Override
  public PCollectionRowTuple expand(PCollectionRowTuple input) {
    LOG.info(
        "AddFiles configured to commit after accumulating {} files, or after {} seconds.",
        numFilesTrigger,
        intervalTrigger.getStandardSeconds());
    if (!Strings.isNullOrEmpty(locationPrefix)) {
      LOG.info(
          "AddFiles configured to build partition metadata after the prefix: '{}'", locationPrefix);
    }

    PCollection<Row> filePaths = input.getSinglePCollection();
    Schema inputSchema = filePaths.getSchema();
    Preconditions.checkState(
        inputSchema.getFieldCount() == 1
            && inputSchema.getField(0).getType().getTypeName().equals(Schema.TypeName.STRING),
        "Incoming Row Schema must contain only one field of type String. Instead, got schema: %s",
        inputSchema);

    PCollectionTuple dataFiles =
        filePaths
            .apply("Filter empty paths", Filter.by(row -> row.getString(0) != null))
            .apply(
                "ExtractPaths",
                MapElements.into(TypeDescriptors.strings())
                    .via(row -> checkStateNotNull(row.getString(0))))
            .apply(Redistribute.arbitrarily())
            .apply(
                "ConvertToDataFiles",
                ParDo.of(new ConvertToDataFile(catalogConfig, tableIdentifier, locationPrefix))
                    .withOutputTags(DATA_FILES, TupleTagList.of(ERRORS)));
    SchemaCoder<SerializableDataFile> sdfSchema;
    try {
      sdfSchema = SchemaRegistry.createDefault().getSchemaCoder(SerializableDataFile.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    PCollection<Row> snapshots =
        dataFiles
            .get(DATA_FILES)
            .setCoder(sdfSchema)
            .apply("AddStaticKey", WithKeys.of((Void) null))
            .apply(
                GroupIntoBatches.<Void, SerializableDataFile>ofSize(numFilesTrigger)
                    .withMaxBufferingDuration(intervalTrigger))
            .apply("DropKey", Values.create())
            .apply(
                "CommitFilesToIceberg",
                ParDo.of(new CommitFilesDoFn(catalogConfig, tableIdentifier)))
            .setRowSchema(SnapshotInfo.getSchema());

    return PCollectionRowTuple.of(
        "snapshots", snapshots, "errors", dataFiles.get(ERRORS).setRowSchema(ERROR_SCHEMA));
  }

  static class ConvertToDataFile extends DoFn<String, SerializableDataFile> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    public static final TupleTag<Row> ERRORS = new TupleTag<>();
    public static final TupleTag<SerializableDataFile> DATA_FILES = new TupleTag<>();
    public static final Schema ERROR_SCHEMA =
        Schema.builder().addStringField("file").addStringField("error").build();
    private final @Nullable String prefix;
    private static final Map<String, Table> tableCache = new ConcurrentHashMap<>();
    private boolean isPartitioned = false;

    public ConvertToDataFile(
        IcebergCatalogConfig catalogConfig, String identifier, @Nullable String prefix) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
      this.prefix = prefix;
    }

    @Setup
    public void start() {
      Table table =
          tableCache.computeIfAbsent(
              identifier, (id) -> catalogConfig.catalog().loadTable(TableIdentifier.parse(id)));
      isPartitioned = table.spec().isPartitioned();
      Preconditions.checkArgument(
          !isPartitioned || prefix != null,
          "A location prefix must be specified when adding files to a partitioned table.");
    }

    private static final String UNKNOWN_FORMAT_ERROR = "Could not determine the file's format";
    private static final String PREFIX_ERROR = "File path did not start with the specified prefix";

    @ProcessElement
    public void process(@Element String filePath, MultiOutputReceiver output) {
      final Table table = checkStateNotNull(tableCache.get(identifier));
      boolean isPartitioned = table.spec().isPartitioned();
      if (isPartitioned && !filePath.startsWith(checkStateNotNull(prefix))) {
        output
            .get(ERRORS)
            .output(Row.withSchema(ERROR_SCHEMA).addValues(filePath, PREFIX_ERROR).build());
        numErrorFiles.inc();
        return;
      }

      InputFile inputFile = table.io().newInputFile(filePath);
      FileFormat format;
      try {
        format = inferFormat(inputFile.location());
      } catch (UnknownFormat e) {
        output
            .get(ERRORS)
            .output(Row.withSchema(ERROR_SCHEMA).addValues(filePath, UNKNOWN_FORMAT_ERROR).build());
        numErrorFiles.inc();
        return;
      }
      Metrics metrics = getFileMetrics(inputFile, format, MetricsConfig.forTable(table));

      String partitionPath = getPartitionPath(filePath);
      DataFile df =
          DataFiles.builder(table.spec())
              .withPath(filePath)
              .withFormat(format)
              .withMetrics(metrics)
              .withFileSizeInBytes(inputFile.getLength())
              .withPartitionPath(partitionPath)
              .build();

      output.get(DATA_FILES).output(SerializableDataFile.from(df, partitionPath));
    }

    private String getPartitionPath(String filePath) {
      if (!isPartitioned) {
        return "";
      }
      String partitionPath = filePath.substring(checkStateNotNull(prefix).length());
      int lastSlashIndex = partitionPath.lastIndexOf('/');

      return lastSlashIndex > 0 ? partitionPath.substring(0, lastSlashIndex) : "";
    }
  }

  static class CommitFilesDoFn extends DoFn<Iterable<SerializableDataFile>, Row> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private transient @MonotonicNonNull Table table = null;

    public CommitFilesDoFn(IcebergCatalogConfig catalogConfig, String identifier) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
    }

    @Setup
    public void start() {
      table = catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
    }

    @ProcessElement
    public void process(@Element Iterable<SerializableDataFile> files, OutputReceiver<Row> output) {
      if (!files.iterator().hasNext()) {
        return;
      }
      Table table = checkStateNotNull(this.table);

      int numFiles = 0;
      AppendFiles appendFiles = table.newAppend();
      for (SerializableDataFile file : files) {
        DataFile df = file.createDataFile(table.specs());
        appendFiles.appendFile(df);
        numFiles++;
      }

      appendFiles.commit();
      numFilesAdded.inc(numFiles);

      Snapshot snapshot = table.currentSnapshot();
      output.output(SnapshotInfo.fromSnapshot(snapshot).toRow());
    }
  }

  @SuppressWarnings("argument")
  public static Metrics getFileMetrics(InputFile file, FileFormat format, MetricsConfig config) {
    switch (format) {
      case PARQUET:
        return ParquetUtil.fileMetrics(file, config);
      case ORC:
        return OrcMetrics.fromInputFile(file, config);
      case AVRO:
        return new Metrics(Avro.rowCount(file), null, null, null, null);
      default:
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }
  }

  public static FileFormat inferFormat(String path) {
    String lowerPath = path.toLowerCase();

    if (lowerPath.endsWith(".parquet") || lowerPath.endsWith(".pqt")) {
      return FileFormat.PARQUET;
    } else if (lowerPath.endsWith(".orc")) {
      return FileFormat.ORC;
    } else if (lowerPath.endsWith(".avro")) {
      return FileFormat.AVRO;
    }

    throw new UnknownFormat();
  }

  static class UnknownFormat extends IllegalArgumentException {}
}
