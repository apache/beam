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
import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
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
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ShardedKey;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hasher;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
  private static final Counter numManifestFilesAdded =
      counter(AddFiles.class, "numManifestFilesAdded");
  private static final Counter numDataFilesAdded = counter(AddFiles.class, "numDataFilesAdded");
  private static final Counter numErrorFiles = counter(AddFiles.class, "numErrorFiles");
  private static final Logger LOG = LoggerFactory.getLogger(AddFiles.class);
  private static final int DEFAULT_DATAFILES_PER_MANIFEST = 10_000;
  private static final int DEFAULT_MAX_MANIFESTS_PER_SNAPSHOT = 100;
  static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("file").addStringField("error").build();
  private static final long MANIFEST_PREFIX = UUID.randomUUID().getMostSignificantBits();
  private final IcebergCatalogConfig catalogConfig;
  private final String tableIdentifier;
  private @Nullable Duration intervalTrigger;
  private final int manifestFileSize;
  private final @Nullable String locationPrefix;
  private final @Nullable List<String> partitionFields;
  private final @Nullable Map<String, String> tableProps;

  public AddFiles(
      IcebergCatalogConfig catalogConfig,
      String tableIdentifier,
      @Nullable String locationPrefix,
      @Nullable List<String> partitionFields,
      @Nullable Map<String, String> tableProps,
      @Nullable Integer manifestFileSize,
      @Nullable Duration intervalTrigger) {
    this.catalogConfig = catalogConfig;
    this.tableIdentifier = tableIdentifier;
    this.partitionFields = partitionFields;
    this.tableProps = tableProps;
    this.intervalTrigger = intervalTrigger;
    this.manifestFileSize =
        manifestFileSize != null ? manifestFileSize : DEFAULT_DATAFILES_PER_MANIFEST;
    this.locationPrefix = locationPrefix;
  }

  @Override
  public PCollectionRowTuple expand(PCollection<String> input) {
    if (input.isBounded().equals(UNBOUNDED)) {
      intervalTrigger = intervalTrigger != null ? intervalTrigger : DEFAULT_TRIGGER_INTERVAL;
      LOG.info(
          "AddFiles configured to generate a new manifest after accumulating {} files, or after {} seconds.",
          manifestFileSize,
          intervalTrigger.getStandardSeconds());
    } else {
      checkState(
          intervalTrigger == null,
          "Specifying an interval trigger is only supported for streaming pipelines.");
    }

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
    SchemaCoder<SerializableDataFile> sdfCoder;
    try {
      sdfCoder = SchemaRegistry.createDefault().getSchemaCoder(SerializableDataFile.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    PCollection<KV<Integer, SerializableDataFile>> keyedFiles =
        dataFiles
            .get(DATA_FILES)
            .setCoder(sdfCoder)
            .apply("AddSpecIdKey", WithKeys.of(SerializableDataFile::getPartitionSpecId))
            .setCoder(KvCoder.of(VarIntCoder.of(), sdfCoder));

    GroupIntoBatches<Integer, SerializableDataFile> batchDataFiles =
        GroupIntoBatches.ofSize(manifestFileSize);
    GroupIntoBatches<String, byte[]> batchManifestFiles =
        GroupIntoBatches.ofSize(DEFAULT_MAX_MANIFESTS_PER_SNAPSHOT);

    if (keyedFiles.isBounded().equals(UNBOUNDED)) {
      batchDataFiles = batchDataFiles.withMaxBufferingDuration(checkStateNotNull(intervalTrigger));
      batchManifestFiles =
          batchManifestFiles.withMaxBufferingDuration(checkStateNotNull(intervalTrigger));
    }

    PCollection<KV<ShardedKey<Integer>, Iterable<SerializableDataFile>>> groupedFiles =
        keyedFiles.apply("GroupDataFilesIntoBatches", batchDataFiles.withShardedKey());

    PCollection<KV<String, byte[]>> manifests =
        groupedFiles.apply(
            "CreateManifests", ParDo.of(new CreateManifests(catalogConfig, tableIdentifier)));

    PCollection<Row> snapshots =
        manifests
            .apply("GatherManifests", batchManifestFiles)
            .apply(
                "CommitManifests",
                ParDo.of(new CommitManifestFilesDoFn(catalogConfig, tableIdentifier)))
            .setRowSchema(SnapshotInfo.getSchema());

    return PCollectionRowTuple.of(
        OUTPUT_TAG, snapshots, ERROR_TAG, dataFiles.get(ERRORS).setRowSchema(ERROR_SCHEMA));
  }

  /**
   * Reads incoming file paths, extracts Iceberg metadata, and converts them into {@link
   * SerializableDataFile} objects.
   *
   * <p><b>Asynchronous Bundle Processing:</b> Because file I/O, catalog lookups, and metadata
   * inference can be highly latency-bound, this DoFn implements an asynchronous processing pattern
   * to maximize throughput. By default, Beam processes elements in a bundle sequentially. To avoid
   * bottlenecking the pipeline, we use an internal {@link ExecutorService} to process multiple
   * files concurrently within a single DoFn instance.
   *
   * <p><b>Lifecycle & Thread Safety:</b>
   *
   * <ul>
   *   <li><b>{@link ProcessElement}:</b> Submits the heavy lifting (format inference, metrics
   *       collection, and partition resolution) to a background thread pool and stores the
   *       resulting {@link Future}.
   *   <li><b>{@link FinishBundle}:</b> Blocks and awaits the completion of all futures in the
   *       current bundle. It safely emits the successfully parsed {@link DataFile}s, or error rows,
   *       back to the runner on the main thread, as {@link MultiOutputReceiver} is not thread-safe.
   * </ul>
   */
  static class ConvertToDataFile extends DoFn<String, SerializableDataFile> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    public static final TupleTag<Row> ERRORS = new TupleTag<>();
    public static final TupleTag<SerializableDataFile> DATA_FILES = new TupleTag<>();
    private final @Nullable String prefix;
    private final @Nullable List<String> partitionFields;
    private final @Nullable Map<String, String> tableProps;
    private transient @MonotonicNonNull ExecutorService executor;
    private transient @MonotonicNonNull LinkedList<Future<ProcessResult>> activeTasks;
    private transient volatile @MonotonicNonNull Table table;

    // Number of parallel threads processing incoming files
    private static final int THREAD_POOL_SIZE = 10;
    private static final int MAX_IN_FLIGHT_TASKS = 100;

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

    private static class ProcessResult {
      final @Nullable SerializableDataFile dataFile;
      final @Nullable Row errorRow;
      final Instant timestamp;
      final BoundedWindow window;
      final PaneInfo paneInfo;

      ProcessResult(
          @Nullable SerializableDataFile dataFile,
          @Nullable Row errorRow,
          Instant timestamp,
          BoundedWindow window,
          PaneInfo paneInfo) {
        checkState(
            dataFile == null || errorRow == null,
            "Expected only one of dataFile or errorRow, but got both:%n\tfile: %s%n\terror: %s",
            dataFile != null ? dataFile.getPath() : null,
            errorRow);
        this.dataFile = dataFile;
        this.errorRow = errorRow;
        this.timestamp = timestamp;
        this.window = window;
        this.paneInfo = paneInfo;
      }
    }

    @Setup
    public void setup() {
      executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }

    @Teardown
    public void teardown() {
      if (executor != null) {
        executor.shutdownNow();
      }
    }

    @StartBundle
    public void startBundle() {
      activeTasks = Lists.newLinkedList();
    }

    @ProcessElement
    public void process(
        @Element String filePath,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo paneInfo,
        MultiOutputReceiver output)
        throws IOException, InterruptedException, ExecutionException {
      LinkedList<Future<ProcessResult>> activeTasks = checkStateNotNull(this.activeTasks);

      // start draining finished tasks, but don't block
      Iterator<Future<ProcessResult>> iterator = activeTasks.iterator();
      while (iterator.hasNext()) {
        Future<ProcessResult> future = iterator.next();
        if (future.isDone()) {
          outputResult(future.get(), output);
          iterator.remove();
        }
      }

      // if we have too many active tasks, wait until some finish
      while (activeTasks.size() >= MAX_IN_FLIGHT_TASKS) {
        Future<ProcessResult> oldestTask = activeTasks.removeFirst();
        outputResult(oldestTask.get(), output); // .get() blocks until the task completes
      }

      // create a new task for the current element and add to queue
      Callable<ProcessResult> task = createProcessTask(filePath, timestamp, window, paneInfo);
      activeTasks.add(checkStateNotNull(executor).submit(task));
    }

    private void outputResult(ProcessResult result, MultiOutputReceiver output) {
      if (result.errorRow != null) {
        output
            .get(ERRORS)
            .outputWindowedValue(
                result.errorRow,
                result.timestamp,
                Collections.singleton(result.window),
                result.paneInfo);
        numErrorFiles.inc();
      } else if (result.dataFile != null) {
        output
            .get(DATA_FILES)
            .outputWindowedValue(
                result.dataFile,
                result.timestamp,
                Collections.singleton(result.window),
                result.paneInfo);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      // Block and wait for threads to finish their work
      int numErrors = 0;
      for (Future<ProcessResult> future : checkStateNotNull(activeTasks)) {
        ProcessResult result = future.get();
        if (result.errorRow != null) {
          context.output(ERRORS, result.errorRow, result.timestamp, result.window);
          numErrors++;
        } else if (result.dataFile != null) {
          context.output(DATA_FILES, result.dataFile, result.timestamp, result.window);
        }
      }
      numErrorFiles.inc(numErrors);
    }

    private Callable<ProcessResult> createProcessTask(
        String filePath, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {

      return () -> {
        FileFormat format;
        try {
          format = inferFormat(filePath);
        } catch (UnknownFormatException e) {
          return new ProcessResult(
              null,
              Row.withSchema(ERROR_SCHEMA).addValues(filePath, UNKNOWN_FORMAT_ERROR).build(),
              timestamp,
              window,
              paneInfo);
        }

        // Synchronize table initialization
        if (table == null) {
          synchronized (this) {
            if (table == null) {
              try {
                table = getOrCreateTable(filePath, format);
              } catch (FileNotFoundException e) {
                return new ProcessResult(
                    null,
                    Row.withSchema(ERROR_SCHEMA)
                        .addValues(filePath, checkStateNotNull(e.getMessage()))
                        .build(),
                    timestamp,
                    window,
                    paneInfo);
              }
            }
          }
        }

        // Check if the file path contains the provided prefix
        if (table.spec().isPartitioned()
            && !Strings.isNullOrEmpty(prefix)
            && !filePath.startsWith(checkStateNotNull(prefix))) {
          return new ProcessResult(
              null,
              Row.withSchema(ERROR_SCHEMA).addValues(filePath, PREFIX_ERROR).build(),
              timestamp,
              window,
              paneInfo);
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
        } catch (Exception e) {
          return new ProcessResult(
              null,
              Row.withSchema(ERROR_SCHEMA)
                  .addValues(filePath, checkStateNotNull(e.getMessage()))
                  .build(),
              timestamp,
              window,
              paneInfo);
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
            return new ProcessResult(
                null,
                Row.withSchema(ERROR_SCHEMA)
                    .addValues(filePath, UNKNOWN_PARTITION_ERROR + e.getMessage())
                    .build(),
                timestamp,
                window,
                paneInfo);
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
        return new ProcessResult(
            SerializableDataFile.from(df, partitionPath), null, timestamp, window, paneInfo);
      };
    }

    static <W, T> T transformValue(Transform<W, T> transform, Type type, ByteBuffer bytes) {
      return transform.bind(type).apply(Conversions.fromByteBuffer(type, bytes));
    }

    private Table getOrCreateTable(String filePath, FileFormat format) throws IOException {
      TableIdentifier tableId = TableIdentifier.parse(identifier);
      @Nullable Table t;
      try {
        t = catalogConfig.catalog().loadTable(tableId);
      } catch (NoSuchTableException e) {
        try {
          org.apache.iceberg.Schema schema = getSchema(filePath, format);
          PartitionSpec spec = PartitionUtils.toPartitionSpec(partitionFields, schema);

          t =
              tableProps == null
                  ? catalogConfig
                      .catalog()
                      .createTable(TableIdentifier.parse(identifier), schema, spec)
                  : catalogConfig
                      .catalog()
                      .createTable(TableIdentifier.parse(identifier), schema, spec, tableProps);
        } catch (AlreadyExistsException e2) { // if table already exists, just load it
          t = catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
        }
      }
      ensureNameMappingPresent(t);
      return t;
    }

    private static void ensureNameMappingPresent(Table table) {
      if (table.properties().get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
        // Forces Name based resolution instead of position based resolution
        NameMapping mapping = MappingUtil.create(table.schema());
        String mappingJson = NameMappingParser.toJson(mapping);
        table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();
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

      // read metadata from footer and set partition based on min/max transformed values
      for (int i = 0; i < fields.size(); i++) {
        PartitionField field = fields.get(i);
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

      return pk.toPath();
    }
  }

  /**
   * Writes batches of {@link SerializableDataFile}s (grouped by Partition Spec ID) into {@link
   * ManifestFile}s.
   *
   * <p>Returns the byte-encoded {@link ManifestFile}, to be reconstructed and committed by
   * downstream {@link CommitManifestFilesDoFn}.
   */
  static class CreateManifests
      extends DoFn<KV<ShardedKey<Integer>, Iterable<SerializableDataFile>>, KV<String, byte[]>> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private transient @MonotonicNonNull Table table;

    public CreateManifests(IcebergCatalogConfig catalogConfig, String identifier) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
    }

    @ProcessElement
    public void process(
        @Element KV<ShardedKey<Integer>, Iterable<SerializableDataFile>> batch,
        OutputReceiver<KV<String, byte[]>> output)
        throws IOException {
      if (!batch.getValue().iterator().hasNext()) {
        return;
      }
      if (table == null) {
        table = catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
      }

      PartitionSpec spec = checkStateNotNull(table.specs().get(batch.getKey().getKey()));

      String manifestPath =
          String.format(
              "%s/metadata/%s-%s-m0.avro", table.location(), MANIFEST_PREFIX, UUID.randomUUID());
      OutputFile outputFile = table.io().newOutputFile(manifestPath);

      int numDataFiles = 0;
      ManifestFile manifestFile;
      try (ManifestWriter<DataFile> writer = ManifestFiles.write(spec, outputFile)) {
        for (SerializableDataFile sdf : batch.getValue()) {
          DataFile df = sdf.createDataFile(table.specs());
          writer.add(df);
          numDataFiles++;
        }
        writer.close();
        manifestFile = writer.toManifestFile();

        // Provide a non-null dummy Snapshot ID to avoid encoding/decoding Null exceptions.
        // The snapshot ID will be overwritten when the file is committed.
        ((GenericManifestFile) manifestFile).set(6, -1L);
      }

      output.output(KV.of(identifier, ManifestFiles.encode(manifestFile)));
      numDataFilesAdded.inc(numDataFiles);
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
   *       traverses backwards through recent snapshots to check if the current batch's ID is
   *       already present.
   * </ul>
   *
   * <p>Outputs the resulting Iceberg {@link Snapshot} information.
   */
  static class CommitManifestFilesDoFn extends DoFn<KV<String, Iterable<byte[]>>, Row> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private transient @MonotonicNonNull Table table = null;
    private static final String COMMIT_ID_KEY = "beam.add-files-commit-id";

    @StateId("lastCommitTimestamp")
    private final StateSpec<ValueState<Long>> lastCommitTimestamp =
        StateSpecs.value(VarLongCoder.of());

    public CommitManifestFilesDoFn(IcebergCatalogConfig catalogConfig, String identifier) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
    }

    @ProcessElement
    public void process(
        @Element KV<String, Iterable<byte[]>> batch,
        @AlwaysFetched @StateId("lastCommitTimestamp") ValueState<Long> lastCommitTimestamp,
        OutputReceiver<Row> output)
        throws IOException {
      List<ManifestFile> manifests = new ArrayList<>();
      for (byte[] bytes : batch.getValue()) {
        manifests.add(ManifestFiles.decode(bytes));
      }
      String commitId = commitHash(manifests);
      if (table == null) {
        table = catalogConfig.catalog().loadTable(TableIdentifier.parse(identifier));
      }
      table.refresh();

      if (shouldSkip(commitId, lastCommitTimestamp.read())) {
        return;
      }

      int numManifests = 0;
      AppendFiles appendFiles = table.newFastAppend();
      for (ManifestFile file : manifests) {
        appendFiles.appendManifest(file);
        numManifests++;
      }
      appendFiles.set(COMMIT_ID_KEY, commitId);
      LOG.info("Committing {} files, with commit ID: {}", numManifests, commitId);
      appendFiles.commit();

      Snapshot snapshot = table.currentSnapshot();
      output.output(SnapshotInfo.fromSnapshot(snapshot).toRow());
      lastCommitTimestamp.write(snapshot.timestampMillis());
      numManifestFilesAdded.inc(numManifests);
    }

    private String commitHash(Iterable<ManifestFile> files) {
      Hasher hasher = Hashing.sha256().newHasher();

      // Extract, sort, and hash to ensure deterministic output
      List<String> paths = new ArrayList<>();
      for (ManifestFile file : files) {
        paths.add(file.path());
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
