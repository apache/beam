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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.DoFn;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
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

import java.util.List;

import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.DATA_FILES;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.ERRORS;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.ERROR_SCHEMA;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

public class AddFiles extends SchemaTransform {
  private final IcebergCatalogConfig catalogConfig;
  private final String tableIdentifier;
  private final Duration intervalTrigger;
  private final int numFilesTrigger;
  private final @Nullable String locationPrefix;
  private static final int DEFAULT_FILES_TRIGGER = 100_000;
  private static final Duration DEFAULT_TRIGGER_INTERVAL = Duration.standardMinutes(10);

  public AddFiles(
      IcebergCatalogConfig catalogConfig,
      String tableIdentifier,
      @Nullable String locationPrefix,
      @Nullable Integer numFilesTrigger,
      @Nullable Duration intervalTrigger) {
    System.out.println("got catalog config: " + catalogConfig);
    this.catalogConfig = catalogConfig;
    this.tableIdentifier = tableIdentifier;
    this.intervalTrigger = intervalTrigger != null ? intervalTrigger : DEFAULT_TRIGGER_INTERVAL;
    this.numFilesTrigger = numFilesTrigger != null ? numFilesTrigger : DEFAULT_FILES_TRIGGER;
    this.locationPrefix = locationPrefix;
  }

  @Override
  public PCollectionRowTuple expand(PCollectionRowTuple input) {
    PCollection<Row> filePaths = input.getSinglePCollection();
    Schema inputSchema = filePaths.getSchema();
    Preconditions.checkState(
        inputSchema.getFieldCount() == 1
            && inputSchema.getField(0).getType().getTypeName().equals(Schema.TypeName.STRING)
            && !inputSchema.getField(0).getType().getNullable(),
        "Incoming Row Schema contain only one (required) field of type String.");

    PCollectionTuple dataFiles =
        filePaths
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
    dataFiles.get(ERRORS).setRowSchema(ERROR_SCHEMA);
    PCollection<Row> snapshots =
        dataFiles
            .get(DATA_FILES)
            .setCoder(sdfSchema)
            .apply("AddStaticKey", WithKeys.of((Void) null))
            .apply(
                GroupIntoBatches.<Void, SerializableDataFile>ofSize(numFilesTrigger)
                    .withMaxBufferingDuration(intervalTrigger))
            .apply("DropKey", Values.create())
            .apply("AddFilesToIceberg", ParDo.of(new AddFilesDoFn(catalogConfig, tableIdentifier)))
            .setRowSchema(SnapshotInfo.getSchema());

    return PCollectionRowTuple.of("snapshots", snapshots);
  }

  static class ConvertToDataFile extends DoFn<String, SerializableDataFile> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    public static final TupleTag<Row> ERRORS = new TupleTag<>();
    public static final TupleTag<SerializableDataFile> DATA_FILES = new TupleTag<>();
    public static final Schema ERROR_SCHEMA =
        Schema.builder().addStringField("file").addStringField("error").build();
    private final @Nullable String prefix;
    private static volatile @MonotonicNonNull Table sharedTable;
    private boolean isPartitioned = false;

    public ConvertToDataFile(
        IcebergCatalogConfig catalogConfig, String identifier, @Nullable String prefix) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
      this.prefix = prefix;
    }

    @Setup
    public void start() {
      if (sharedTable == null) {
        synchronized (ConvertToDataFile.class) {
          if (sharedTable == null) {
            sharedTable = catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
          }
        }
      }
      isPartitioned = sharedTable.spec().isPartitioned();
      Preconditions.checkArgument(
          !isPartitioned || prefix != null,
          "A location prefix must be specified when adding files to a partitioned table.");
    }

    private static final String UNKNOWN_FORMAT_ERROR = "Could not determine the file's format";
    private static final String PREFIX_ERROR = "File did not start with the specified prefix";

    @ProcessElement
    public void process(@Element String filePath, MultiOutputReceiver output) {
      final Table table = checkStateNotNull(sharedTable);
      boolean isPartitioned = table.spec().isPartitioned();
      if (isPartitioned && !filePath.startsWith(checkStateNotNull(prefix))) {
        output
            .get(ERRORS)
            .output(Row.withSchema(ERROR_SCHEMA).addValues(filePath, PREFIX_ERROR).build());
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
      List<String> components = Lists.newArrayList(Splitter.on('/').split(partitionPath));
      if (!components.isEmpty()) {
        components = components.subList(0, components.size() - 1);
      }
      return String.join("/", components);
    }
  }

  static class AddFilesDoFn extends DoFn<Iterable<SerializableDataFile>, Row> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private @MonotonicNonNull Table table = null;

    public AddFilesDoFn(IcebergCatalogConfig catalogConfig, String identifier) {
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

      AppendFiles appendFiles = table.newAppend();
      for (SerializableDataFile file : files) {
        DataFile df = file.createDataFile(table.specs());
        appendFiles.appendFile(df);
      }

      appendFiles.commit();
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
