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

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO.ReadFiles.BeamParquetInputFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.DATA_FILES;
import static org.apache.beam.sdk.io.iceberg.AddFiles.ConvertToDataFile.ERRORS;
import static org.apache.beam.sdk.metrics.Metrics.counter;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.PCollection.IsBounded.BOUNDED;

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
  static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("file").addStringField("error").build();
  private final IcebergCatalogConfig catalogConfig;
  private final String tableIdentifier;
  private final Duration intervalTrigger;
  private final int numFilesTrigger;
  private final @Nullable String locationPrefix;
  private final @Nullable List<String> partitionFields;
  private final @Nullable Map<String, String> tableProps;
  private final String jobId = UUID.randomUUID().toString();

  public AddFiles(
      IcebergCatalogConfig catalogConfig,
      String tableIdentifier,
      @Nullable String locationPrefix,
      @Nullable List<String> partitionFields,
      @Nullable Map<String, String> tableProps,
      @Nullable Integer numFilesTrigger,
      @Nullable Duration intervalTrigger) {
    this.catalogConfig = catalogConfig;
    this.tableIdentifier = tableIdentifier;
    this.partitionFields = partitionFields;
    this.tableProps = tableProps;
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

    Schema inputSchema = input.getSinglePCollection().getSchema();
    Preconditions.checkState(
        inputSchema.getFieldCount() == 1
            && inputSchema.getField(0).getType().getTypeName().equals(Schema.TypeName.STRING),
        "Incoming Row Schema must contain only one field of type String. Instead, got schema: %s",
        inputSchema);

    PCollectionTuple dataFiles =
        input
            .getSinglePCollection()
            .apply("Filter empty paths", Filter.by(row -> row.getString(0) != null))
            .apply(
                "ExtractPaths",
                MapElements.into(TypeDescriptors.strings())
                    .via(row -> checkStateNotNull(row.getString(0))))
            .apply(Redistribute.arbitrarily())
            .apply(
                "ConvertToDataFiles",
                ParDo.of(
                        new ConvertToDataFile(
                            catalogConfig,
                            tableIdentifier,
                            locationPrefix,
                            partitionFields,
                            tableProps))
                    .withOutputTags(DATA_FILES, TupleTagList.of(ERRORS)));
    SchemaCoder<SerializableDataFile> sdfSchema;
    try {
      sdfSchema = SchemaRegistry.createDefault().getSchemaCoder(SerializableDataFile.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    PCollection<KV<Void, SerializableDataFile>> keyedFiles =
        dataFiles
            .get(DATA_FILES)
            .setCoder(sdfSchema)
            .apply("AddStaticKey", WithKeys.of((Void) null));

    PCollection<KV<Void, Iterable<SerializableDataFile>>> groupedFiles =
        keyedFiles.isBounded().equals(BOUNDED)
            ? keyedFiles.apply(GroupByKey.create())
            : keyedFiles.apply(
                GroupIntoBatches.<Void, SerializableDataFile>ofSize(numFilesTrigger)
                    .withMaxBufferingDuration(intervalTrigger));

    PCollection<Row> snapshots =
        groupedFiles
            .apply(
                "CommitFilesToIceberg",
                ParDo.of(new CommitFilesDoFn(catalogConfig, tableIdentifier, jobId)))
            .setRowSchema(SnapshotInfo.getSchema());

    return PCollectionRowTuple.of(
        "snapshots", snapshots, "errors", dataFiles.get(ERRORS).setRowSchema(ERROR_SCHEMA));
  }

  static class ConvertToDataFile extends DoFn<String, SerializableDataFile> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    public static final TupleTag<Row> ERRORS = new TupleTag<>();
    public static final TupleTag<SerializableDataFile> DATA_FILES = new TupleTag<>();
    private final @Nullable String prefix;
    private final @Nullable List<String> partitionFields;
    private final @Nullable Map<String, String> tableProps;
    private transient @MonotonicNonNull Table table;

    public ConvertToDataFile(
        IcebergCatalogConfig catalogConfig,
        String identifier,
        @Nullable String prefix,
        @Nullable List<String> partitionFields,
        @Nullable Map<String, String> tableProps) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
      this.prefix = prefix;
      this.partitionFields = partitionFields;
      this.tableProps = tableProps;
    }

    private static final String PREFIX_ERROR = "File path did not start with the specified prefix";
    private static final String UNKNOWN_FORMAT_ERROR = "Could not determine the file's format";
    static final String UNKNOWN_PARTITION_ERROR = "Could not determine the file's partition";

    @ProcessElement
    public void process(@Element String filePath, MultiOutputReceiver output) throws IOException {
      FileFormat format;
      try {
        format = inferFormat(filePath);
      } catch (UnknownFormatException e) {
        output
            .get(ERRORS)
            .output(Row.withSchema(ERROR_SCHEMA).addValues(filePath, UNKNOWN_FORMAT_ERROR).build());
        numErrorFiles.inc();
        return;
      }

      if (table == null) {
        table = getOrCreateTable(getSchema(filePath, format));
      }

      // Check if the file path contains the provided prefix
      if (table.spec().isPartitioned()
          && !Strings.isNullOrEmpty(prefix)
          && !filePath.startsWith(checkStateNotNull(prefix))) {
        output
            .get(ERRORS)
            .output(Row.withSchema(ERROR_SCHEMA).addValues(filePath, PREFIX_ERROR).build());
        numErrorFiles.inc();
        return;
      }

      InputFile inputFile = table.io().newInputFile(filePath);

      Metrics metrics = getFileMetrics(inputFile, format, MetricsConfig.forTable(table));

      // Figure out which partition this DataFile should go to
      String partitionPath;
      if (table.spec().isUnpartitioned()) {
        partitionPath = "";
      } else if (!Strings.isNullOrEmpty(prefix)) {
        // option 1: use directory structure to determine partition
        // Note: we don't validate the DataFile content here
        partitionPath = getPartitionFromFilePath(filePath);
      } else {
        try {
          // option 2: examine DataFile min/max statistics to determine partition
          partitionPath = getPartitionFromMetrics(metrics, inputFile);
        } catch (UnknownPartitionException e) {
          output
              .get(ERRORS)
              .output(
                  Row.withSchema(ERROR_SCHEMA)
                      .addValues(filePath, UNKNOWN_PARTITION_ERROR + ": " + e.getMessage())
                      .build());
          numErrorFiles.inc();
          return;
        }
      }

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

    private <W, T> T transformValue(Transform<W, T> transform, Type type, ByteBuffer bytes) {
      return transform.bind(type).apply(Conversions.fromByteBuffer(type, bytes));
    }

    private Table getOrCreateTable(org.apache.iceberg.Schema schema) {
      PartitionSpec spec = PartitionUtils.toPartitionSpec(partitionFields, schema);
      try {
        return tableProps == null
            ? catalogConfig.catalog().createTable(TableIdentifier.parse(identifier), schema, spec)
            : catalogConfig
                .catalog()
                .createTable(TableIdentifier.parse(identifier), schema, spec, tableProps);
      } catch (AlreadyExistsException e) { // if table already exists, just load it
        return catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
      }
    }

    /**
     * We don't have a table yet, so we don't know which FileIO to use to read these files. Instead,
     * we use Beam's FileSystem utilities to read the file and extract its schema to create the
     * table
     */
    private static org.apache.iceberg.Schema getSchema(String filePath, FileFormat format)
        throws IOException {
      Preconditions.checkArgument(
          format.equals(FileFormat.PARQUET), "Table creation is only supported for Parquet files.");
      FileSystems.registerFileSystemsOnce(PipelineOptionsFactory.create());

      MatchResult result = FileSystems.match(filePath);
      ResourceId resourceId = Iterables.getOnlyElement(result.metadata()).resourceId();
      Compression compression = Compression.detect(checkStateNotNull(resourceId.getFilename()));
      SeekableByteChannel channel =
          (SeekableByteChannel) compression.readDecompressed(FileSystems.open(resourceId));
      BeamParquetInputFile file = new BeamParquetInputFile(channel);

      try (ParquetFileReader reader = ParquetFileReader.open(file)) {
        MessageType messageType = reader.getFooter().getFileMetaData().getSchema();
        return ParquetSchemaUtil.convert(messageType);
      }
    }

    private String getPartitionFromFilePath(String filePath) {
      if (checkStateNotNull(table).spec().isUnpartitioned()) {
        return "";
      }
      String partitionPath = filePath.substring(checkStateNotNull(prefix).length());
      int lastSlashIndex = partitionPath.lastIndexOf('/');

      return lastSlashIndex > 0 ? partitionPath.substring(0, lastSlashIndex) : "";
    }

    /**
     * Examines the min/max values of each partition column to determine the destination partition.
     *
     * <p>If the transformed min/max values are not equal for any given column, we won't be able to
     * determine the partition. In this case, we output the DataFile to the DLQ, because assigning
     * an incorrect partition may lead to it being hidden from some queries.
     */
    private String getPartitionFromMetrics(Metrics metrics, InputFile inputFile)
        throws UnknownPartitionException {
      Table table = checkStateNotNull(this.table);
      List<PartitionField> fields = table.spec().fields();
      List<Integer> sourceIds =
          fields.stream().map(PartitionField::sourceId).collect(Collectors.toList());
      Metrics partitionMetrics;
      // Check if metrics already includes partition columns (this is configured by table
      // properties):
      if (metrics.lowerBounds().keySet().containsAll(sourceIds)
          && metrics.upperBounds().keySet().containsAll(sourceIds)) {
        partitionMetrics = metrics;
      } else {
        // Otherwise, recollect metrics and ensure it includes all partition fields.
        // Note: we don't attach these additional metrics to the DataFile because we can't assume
        // that's in the user's best interest.
        // Some tables are very wide and users may not want to store excessive metadata.
        List<String> sourceNames =
            fields.stream()
                .map(pf -> checkStateNotNull(table.schema().idToName().get(pf.sourceId())))
                .collect(Collectors.toList());
        Map<String, String> configProps =
            sourceNames.stream()
                .collect(Collectors.toMap(s -> "write.metadata.metrics.column." + s, s -> "full"));
        MetricsConfig configWithPartitionFields = MetricsConfig.fromProperties(configProps);
        partitionMetrics =
            getFileMetrics(inputFile, inferFormat(inputFile.location()), configWithPartitionFields);
      }

      PartitionKey pk = new PartitionKey(table.spec(), table.schema());

      for (int i = 0; i < fields.size(); i++) {
        PartitionField field = fields.get(i);
        Type type = table.schema().findType(field.sourceId());
        Transform<?, ?> transform = field.transform();

        // Make a best effort estimate by comparing the lower and upper transformed values.
        // If the transformed values are equal, assume that the DataFile's data safely
        // aligns with the same partition.
        // TODO(ahmedabu98): is comparing min/max safe enough?
        //  or should we compare ALL the records in a DF?
        ByteBuffer lowerBytes = partitionMetrics.lowerBounds().get(field.sourceId());
        ByteBuffer upperBytes = partitionMetrics.upperBounds().get(field.sourceId());
        if (lowerBytes == null && upperBytes == null) {
          pk.set(i, null);
          continue;
        } else if (lowerBytes == null || upperBytes == null) {
          throw new UnknownPartitionException("Only one of the min/max was was null");
        }
        Object lowerTransformedValue = transformValue(transform, type, lowerBytes);
        Object upperTransformedValue = transformValue(transform, type, upperBytes);

        if (!Objects.deepEquals(lowerTransformedValue, upperTransformedValue)) {
          // The DataFile contains values that align to different partitions, so we cannot
          // safely determine a partition.
          // If we commit the DataFile with an incorrect partition, downstream queries may
          // completely ignore it (due to Iceberg's smart partition scan-planning).
          // We also cannot commit the DataFile with a "null" partition, because that will
          // also get skipped by most queries.
          // The safe thing to do is to output the file to DLQ.
          throw new UnknownPartitionException("Min and max transformed values were not equal");
        }

        pk.set(i, lowerTransformedValue);
      }

      return pk.toPath();
    }
  }

  static class CommitFilesDoFn extends DoFn<KV<Void, Iterable<SerializableDataFile>>, Row> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private transient @MonotonicNonNull Table table = null;
    private final String jobId;
    private static final String COMMIT_ID_KEY = "beam.add-files-commit-id";

    public CommitFilesDoFn(IcebergCatalogConfig catalogConfig, String identifier, String jobId) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
      this.jobId = jobId;
    }

    @StartBundle
    public void start() {
      if (table == null) {
        table = catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
      }
    }

    @ProcessElement
    public void process(
        @Element KV<Void, Iterable<SerializableDataFile>> files,
        PaneInfo pane,
        OutputReceiver<Row> output) {
      String commitId = commitHash(files.getValue());
      Table table = checkStateNotNull(this.table);
      if (shouldSkip(commitId)) {
        return;
      }

      int numFiles = 0;
      AppendFiles appendFiles = table.newAppend();
      for (SerializableDataFile file : files.getValue()) {
        DataFile df = file.createDataFile(table.specs());
        appendFiles.appendFile(df);
        numFiles++;
      }
      appendFiles.set(COMMIT_ID_KEY, commitId);
      appendFiles.commit();

      Snapshot snapshot = table.currentSnapshot();
      output.output(SnapshotInfo.fromSnapshot(snapshot).toRow());
      numFilesAdded.inc(numFiles);
    }

    private String commitHash(Iterable<SerializableDataFile> files) {
      int hash = 0;
      for (SerializableDataFile file : files) {
        hash = 31 * hash + file.getPath().hashCode();
      }
      return String.valueOf(Math.abs(hash));
    }

    /**
     * If the process call fails immediately after committing files, but before registering with the
     * runner, then runner will retry committing the same batch of files, possibly leading to data
     * duplication.
     *
     * <p>To mitigate, we create a unique ID per commit and store it in the snapshot summary. We
     * skip the pane's batch of files if we see a snapshot with the same unique ID.
     */
    private boolean shouldSkip(String commitUID) {
      Table table = checkStateNotNull(this.table);
      table.refresh();

      // check the last 10 snapshots to see if it contains the commit ID
      int i = 0;
      @Nullable Snapshot current = table.currentSnapshot();
      while (current != null && i < 10) {
        Map<String, String> summary = current.summary();
        if (summary != null && commitUID.equals(summary.get(COMMIT_ID_KEY))) {
          return true; // commit already happened, we should skip
        }
        if (current.parentId() == null) {
          break;
        }
        current = table.snapshot(current.parentId());
        i++;
      }

      return false;
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

    throw new UnknownFormatException();
  }

  static class UnknownFormatException extends IllegalArgumentException {}

  static class UnknownPartitionException extends IllegalStateException {
    UnknownPartitionException(String msg) {
      super(msg);
    }
  }
}
