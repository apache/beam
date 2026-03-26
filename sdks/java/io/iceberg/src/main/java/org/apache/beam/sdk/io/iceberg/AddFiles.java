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
import static org.apache.beam.sdk.metrics.Metrics.counter;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.PCollection.IsBounded.BOUNDED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO.ReadFiles.BeamParquetInputFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transform that takes in a stream of file paths, converts them to Iceberg {@link DataFile}s with
 * partition metadata and metrics, then commits them to an Iceberg {@link Table}.
 */
public class AddFiles extends PTransform<PCollection<String>, PCollectionRowTuple> {
  static final String OUTPUT_TAG = "snapshots";
  static final String ERROR_TAG = "errors";
  private static final Duration DEFAULT_TRIGGER_INTERVAL = Duration.standardMinutes(10);
  private static final Counter numFilesAdded = counter(AddFiles.class, "numFilesAdded");
  private static final Counter numErrorFiles = counter(AddFiles.class, "numErrorFiles");
  private static final Logger LOG = LoggerFactory.getLogger(AddFiles.class);
  private static final int DEFAULT_FILES_TRIGGER = 1_000;
  static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("file").addStringField("error").build();
  private final IcebergCatalogConfig catalogConfig;
  private final String tableIdentifier;
  private final Duration intervalTrigger;
  private final int numFilesTrigger;
  private final @Nullable String locationPrefix;
  private final @Nullable List<String> partitionFields;
  private final @Nullable Map<String, String> tableProps;

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
  public PCollectionRowTuple expand(PCollection<String> input) {
    LOG.info(
        "AddFiles configured to commit after accumulating {} files, or after {} seconds.",
        numFilesTrigger,
        intervalTrigger.getStandardSeconds());
    if (!Strings.isNullOrEmpty(locationPrefix)) {
      LOG.info(
          "AddFiles configured to build partition metadata after the prefix: '{}'", locationPrefix);
    }

    PCollectionTuple dataFiles =
        input.apply(
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
                ParDo.of(new CommitFilesDoFn(catalogConfig, tableIdentifier)))
            .setRowSchema(SnapshotInfo.getSchema());

    return PCollectionRowTuple.of(
        OUTPUT_TAG, snapshots, ERROR_TAG, dataFiles.get(ERRORS).setRowSchema(ERROR_SCHEMA));
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
    // Limit open readers to avoid blowing up memory on one worker
    private static final int MAX_READERS = 10;
    private static final Semaphore ACTIVE_READERS = new Semaphore(MAX_READERS);

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

    static final String PREFIX_ERROR = "File path did not start with the specified prefix";
    private static final String UNKNOWN_FORMAT_ERROR = "Could not determine the file's format";
    static final String UNKNOWN_PARTITION_ERROR = "Could not determine the file's partition: ";

    @ProcessElement
    public void process(@Element String filePath, MultiOutputReceiver output)
        throws IOException, InterruptedException {
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
        try {
          table = getOrCreateTable(filePath, format);
        } catch (FileNotFoundException e) {
          output
              .get(ERRORS)
              .output(
                  Row.withSchema(ERROR_SCHEMA)
                      .addValues(filePath, checkStateNotNull(e.getMessage()))
                      .build());
          numErrorFiles.inc();
          return;
        }
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

      Metrics metrics;
      try {
        metrics =
            getFileMetrics(
                inputFile,
                format,
                MetricsConfig.forTable(table),
                MappingUtil.create(table.schema()));
      } catch (FileNotFoundException e) {
        output
            .get(ERRORS)
            .output(
                Row.withSchema(ERROR_SCHEMA)
                    .addValues(filePath, checkStateNotNull(e.getMessage()))
                    .build());
        numErrorFiles.inc();
        return;
      }

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
          partitionPath = getPartitionFromMetrics(metrics, inputFile, table);
        } catch (UnknownPartitionException e) {
          output
              .get(ERRORS)
              .output(
                  Row.withSchema(ERROR_SCHEMA)
                      .addValues(filePath, UNKNOWN_PARTITION_ERROR + e.getMessage())
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

    static <W, T> T transformValue(Transform<W, T> transform, Type type, ByteBuffer bytes) {
      return transform.bind(type).apply(Conversions.fromByteBuffer(type, bytes));
    }

    private static <W, T> T transformValue(Transform<W, T> transform, Type type, Object value) {
      return transform.bind(type).apply((W) value);
    }

    private Table getOrCreateTable(String filePath, FileFormat format) throws IOException {
      TableIdentifier tableId = TableIdentifier.parse(identifier);
      try {
        return catalogConfig.catalog().loadTable(tableId);
      } catch (NoSuchTableException e) {
        try {
          org.apache.iceberg.Schema schema = getSchema(filePath, format);
          PartitionSpec spec = PartitionUtils.toPartitionSpec(partitionFields, schema);

          return tableProps == null
              ? catalogConfig.catalog().createTable(TableIdentifier.parse(identifier), schema, spec)
              : catalogConfig
                  .catalog()
                  .createTable(TableIdentifier.parse(identifier), schema, spec, tableProps);
        } catch (AlreadyExistsException e2) { // if table already exists, just load it
          return catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
        }
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
      try (ParquetFileReader reader = ParquetFileReader.open(getParquetInputFile(filePath))) {
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
     * determine the partition. We also cannot fall back to a "null" partition, because that will
     * also get skipped by most queries.
     *
     * <p>The Bucket partition transform is an exceptional case because it is not monotonic, meaning
     * it's not enough to just compare the min and max values. There may be a middle value somewhere
     * that gets hashed to a different value. For this transform, we'll need to read all the values
     * in the column ensure they all get transformed to the same partition value.
     *
     * <p>In these cases, we output the DataFile to the DLQ, because assigning an incorrect
     * partition may lead to it being incorrectly ignored by downstream queries.
     */
    static String getPartitionFromMetrics(Metrics metrics, InputFile inputFile, Table table)
        throws UnknownPartitionException, IOException, InterruptedException {
      List<PartitionField> fields = table.spec().fields();
      List<Integer> sourceIds =
          fields.stream().map(PartitionField::sourceId).collect(Collectors.toList());
      Metrics partitionMetrics;
      // Check if metrics already includes partition columns (configured by table properties):
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
                .map(pf -> table.schema().findColumnName(pf.sourceId()))
                .collect(Collectors.toList());
        Map<String, String> configProps =
            sourceNames.stream()
                .collect(Collectors.toMap(s -> "write.metadata.metrics.column." + s, s -> "full"));
        MetricsConfig configWithPartitionFields = MetricsConfig.fromProperties(configProps);
        partitionMetrics =
            getFileMetrics(
                inputFile,
                inferFormat(inputFile.location()),
                configWithPartitionFields,
                MappingUtil.create(table.schema()));
      }

      PartitionKey pk = new PartitionKey(table.spec(), table.schema());

      HashMap<Integer, PartitionField> bucketPartitions = new HashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        PartitionField field = fields.get(i);
        Transform<?, ?> transform = field.transform();
        if (transform.toString().contains("bucket[")) {
          bucketPartitions.put(i, field);
        }
      }

      // first, read only metadata for the non-bucket partition types
      for (int i = 0; i < fields.size(); i++) {
        PartitionField field = fields.get(i);
        // skip bucket partitions (we will process them below)
        if (bucketPartitions.containsKey(i)) {
          continue;
        }
        Type type = table.schema().findType(field.sourceId());
        Transform<?, ?> transform = field.transform();

        // Make a best effort estimate by comparing the lower and upper transformed values.
        // If the transformed values are equal, assume that the DataFile's data safely
        // aligns with the same partition.
        ByteBuffer lowerBytes = partitionMetrics.lowerBounds().get(field.sourceId());
        ByteBuffer upperBytes = partitionMetrics.upperBounds().get(field.sourceId());
        if (lowerBytes == null && upperBytes == null) {
          continue;
        } else if (lowerBytes == null || upperBytes == null) {
          throw new UnknownPartitionException(
              "Only one of the min/max was was null, for field "
                  + table.schema().findColumnName(field.sourceId()));
        }
        Object lowerTransformedValue = transformValue(transform, type, lowerBytes);
        Object upperTransformedValue = transformValue(transform, type, upperBytes);

        if (!Objects.deepEquals(lowerTransformedValue, upperTransformedValue)) {
          // The DataFile contains values that align to different partitions, so we cannot
          // safely determine a partition.
          throw new UnknownPartitionException(
              "Min and max transformed values were not equal, for column: " + field.name());
        }

        pk.set(i, lowerTransformedValue);
      }

      // bucket transform needs extra processing (see java doc above)
      if (!bucketPartitions.isEmpty()) {
        // Optimize by only reading bucket-transformed columns into memory
        org.apache.iceberg.Schema bucketCols =
            TypeUtil.select(
                table.schema(),
                bucketPartitions.values().stream()
                    .map(PartitionField::sourceId)
                    .collect(Collectors.toSet()));

        // Keep one instance of transformed value per column. Use this to compare against each
        // record's transformed value.
        // Values in the same columns must yield the same transformed value, otherwise we cannot
        // determine a partition
        // from this file.
        Map<Integer, Object> transformedValues = new HashMap<>();

        // Do a one-time read of the file and compare all bucket-transformed columns
        ACTIVE_READERS.acquire();
        try (CloseableIterable<Record> reader = ReadUtils.createReader(inputFile, bucketCols)) {
          for (Record record : reader) {
            for (Map.Entry<Integer, PartitionField> entry : bucketPartitions.entrySet()) {
              int partitionIndex = entry.getKey();
              PartitionField partitionField = entry.getValue();
              Transform<?, ?> transform = partitionField.transform();
              Types.NestedField field = table.schema().findField(partitionField.sourceId());
              Object value = record.getField(field.name());

              // set initial transformed value for this column
              @Nullable Object transformedValue = transformedValues.get(partitionIndex);
              Object currentTransformedValue = transformValue(transform, field.type(), value);
              if (transformedValue == null) {
                transformedValues.put(partitionIndex, checkStateNotNull(currentTransformedValue));
                continue;
              }

              if (!Objects.deepEquals(currentTransformedValue, transformedValue)) {
                throw new UnknownPartitionException(
                    "Found records with conflicting transformed values, for column: "
                        + field.name());
              }
            }
          }
        } finally {
          ACTIVE_READERS.release();
        }

        for (Map.Entry<Integer, Object> partitionCol : transformedValues.entrySet()) {
          pk.set(partitionCol.getKey(), partitionCol.getValue());
        }
      }
      return pk.toPath();
    }
  }

  /**
   * A stateful {@link DoFn} that commits batches of files to an Iceberg table.
   *
   * <p>Addresses two primary concerns:
   *
   * <ul>
   *   <li><b>Concurrency:</b> Being stateful on a dummy {@code Void} key forces the runner to
   *       process batches sequentially, preventing concurrent commit conflicts on the Iceberg
   *       table.
   *   <li><b>Idempotency:</b> Prevents duplicate commits during bundle failures by calculating a
   *       deterministic hash for the file set. This ID is stored in the Iceberg {@code Snapshot}
   *       summary, under the key {@code "beam.add-files-commit-id"}. Before committing, the DoFn
   *       travereses backwards through recent snapshots to check if the current batch's ID is
   *       already present.
   * </ul>
   *
   * <p>Outputs the resulting Iceberg {@link Snapshot} information.
   */
  static class CommitFilesDoFn extends DoFn<KV<Void, Iterable<SerializableDataFile>>, Row> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private transient @MonotonicNonNull Table table = null;
    private static final String COMMIT_ID_KEY = "beam.add-files-commit-id";

    @StateId("lastCommitTimestamp")
    private final StateSpec<ValueState<Long>> lastCommitTimestamp =
        StateSpecs.value(VarLongCoder.of());

    public CommitFilesDoFn(IcebergCatalogConfig catalogConfig, String identifier) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
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
        @AlwaysFetched @StateId("lastCommitTimestamp") ValueState<Long> lastCommitTimestamp,
        OutputReceiver<Row> output) {
      String commitId = commitHash(files.getValue());
      Table table = checkStateNotNull(this.table);
      table.refresh();

      if (shouldSkip(commitId, lastCommitTimestamp.read())) {
        return;
      }

      int numFiles = 0;
      AppendFiles appendFiles = table.newFastAppend();
      for (SerializableDataFile file : files.getValue()) {
        DataFile df = file.createDataFile(table.specs());
        appendFiles.appendFile(df);
        numFiles++;
      }
      appendFiles.set(COMMIT_ID_KEY, commitId);
      LOG.info("Committing {} files, with commit ID: {}", numFiles, commitId);
      appendFiles.commit();

      Snapshot snapshot = table.currentSnapshot();
      output.output(SnapshotInfo.fromSnapshot(snapshot).toRow());
      lastCommitTimestamp.write(snapshot.timestampMillis());
      numFilesAdded.inc(numFiles);
    }

    private String commitHash(Iterable<SerializableDataFile> files) {
      Hasher hasher = Hashing.sha256().newHasher();

      // Extract, sort, and hash to ensure deterministic output
      List<String> paths = new ArrayList<>();
      for (SerializableDataFile file : files) {
        paths.add(file.getPath());
      }
      Collections.sort(paths);

      for (String path : paths) {
        hasher.putString(path, StandardCharsets.UTF_8);
      }
      return hasher.hash().toString();
    }

    /**
     * Performs a look-back through Iceberg table history to determine if this specific batch of
     * files has already been successfully committed.
     */
    private boolean shouldSkip(String commitUID, @Nullable Long lastCommitTimestamp) {
      if (lastCommitTimestamp == null) {
        return false;
      }
      Table table = checkStateNotNull(this.table);

      // check past snapshots to see if they contain the commit ID
      @Nullable Snapshot current = table.currentSnapshot();
      while (current != null && current.timestampMillis() > lastCommitTimestamp) {
        Map<String, String> summary = current.summary();
        if (summary != null && commitUID.equals(summary.get(COMMIT_ID_KEY))) {
          return true; // commit already happened, we should skip
        }
        if (current.parentId() == null) {
          break;
        }
        current = table.snapshot(current.parentId());
      }

      return false;
    }
  }

  @SuppressWarnings("argument")
  public static Metrics getFileMetrics(
      InputFile file, FileFormat format, MetricsConfig config, NameMapping mapping)
      throws IOException {
    switch (format) {
      case PARQUET:
        try (ParquetFileReader reader =
            ParquetFileReader.open(getParquetInputFile(file.location()))) {
          ParquetMetadata footer = reader.getFooter();
          MessageType originalMessageType = footer.getFileMetaData().getSchema();
          if (!ParquetSchemaUtil.hasIds(originalMessageType)) {
            footer = getFooterWithTypeIds(originalMessageType, footer, mapping);
          }

          return ParquetUtil.footerMetrics(footer, Stream.empty(), config, mapping);
        }
      case ORC:
        return OrcMetrics.fromInputFile(file, config, mapping);
      case AVRO:
        return new Metrics(Avro.rowCount(file), null, null, null, null);
      default:
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }
  }

  /** Tries to infer other file formats. Defaults to Parquet. */
  public static FileFormat inferFormat(String path) {
    String lowerPath = path.toLowerCase();

    if (lowerPath.endsWith(".parquet") || lowerPath.endsWith(".pqt")) {
      return FileFormat.PARQUET;
    } else if (lowerPath.endsWith(".orc")) {
      return FileFormat.ORC;
    } else if (lowerPath.endsWith(".avro")) {
      return FileFormat.AVRO;
    } else {
      throw new UnknownFormatException();
    }
  }

  static ParquetMetadata getFooterWithTypeIds(
      MessageType originalMessageType, ParquetMetadata footer, NameMapping mapping) {
    originalMessageType = ParquetSchemaUtil.applyNameMapping(originalMessageType, mapping);
    FileMetaData oldFileMeta = footer.getFileMetaData();
    FileMetaData newFileMeta =
        new FileMetaData(
            originalMessageType, oldFileMeta.getKeyValueMetaData(), oldFileMeta.getCreatedBy());
    return new ParquetMetadata(newFileMeta, footer.getBlocks());
  }

  static org.apache.parquet.io.InputFile getParquetInputFile(String filePath) throws IOException {
    ResourceId resourceId =
        Iterables.getOnlyElement(FileSystems.match(filePath).metadata()).resourceId();
    Compression compression = Compression.detect(checkStateNotNull(resourceId.getFilename()));
    SeekableByteChannel channel =
        (SeekableByteChannel) compression.readDecompressed(FileSystems.open(resourceId));
    return new BeamParquetInputFile(channel);
  }

  static class UnknownFormatException extends IllegalArgumentException {}

  static class UnknownPartitionException extends IllegalStateException {
    UnknownPartitionException(String msg) {
      super(msg);
    }
  }
}
