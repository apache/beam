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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.UnverifiableFileHandling;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
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
import org.apache.iceberg.SortOrder;
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
import org.apache.iceberg.types.Types;
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
 * Registers existing Parquet, ORC or Avro files in an Iceberg table without rewriting them: each
 * path becomes a {@link DataFile} with partition metadata and column stats, batched into manifests
 * and committed as snapshots.
 *
 * <p>Outputs: {@code snapshots} (one row per commit), {@code errors} (one row per file that could
 * not be registered: {@code file}, {@code error}), and {@code dry_run_report} when a dry run is
 * configured.
 *
 * <p><b>Schema evolution.</b> With a {@link SchemaEvolutionConfig} whose options are set, a
 * pre-pass reads every Parquet footer, classifies the change each distinct file schema needs on the
 * table (add a column, relax a required column, promote a type), commits the allowed changes in one
 * transaction, and only then registers the files. Manifest entries are immutable, so this ordering
 * is what guarantees that every registered file carries stats for every column it has. Files whose
 * schema needs a change that is not allowed, or that conflicts with the table or with another file,
 * are incompatible: by default the pipeline fails before committing anything, or routes them to
 * {@code errors} (see {@link SchemaEvolutionConfig.IncompatibleSchemaHandling}). The per-file
 * checks read Parquet footers: an ORC or Avro file, or a pinned column the footer has no null count
 * for, cannot be verified and goes to {@code errors} unless {@link
 * SchemaEvolutionConfig.UnverifiableFileHandling#ACCEPT} registers it on trust. When the table does
 * not exist, the pre-pass creates it from the union of the file schemas; if no readable Parquet
 * schema can seed it, nothing is created and every file goes to {@code errors}. Schema evolution
 * currently requires bounded input; unbounded input with options set is rejected at construction.
 *
 * <pre>{@code
 * SchemaEvolutionConfig evolution =
 *     SchemaEvolutionConfig.builder()
 *         .setOptions(EnumSet.of(ALLOW_FIELD_ADDITION, ALLOW_TYPE_PROMOTION))
 *         .setRequiredColumns(Collections.singleton("id"))
 *         .build();
 * paths.apply(new AddFiles(catalog, "db.sales", null, null, null, null, null, null, evolution));
 * }</pre>
 *
 * <p>Without options the table schema is never changed and files register as-is: columns the table
 * does not have get no stats and are not readable, and a nested column the table does not know can
 * make that file, and any scan that includes it, fail in an Iceberg reader.
 */
public class AddFiles extends PTransform<PCollection<String>, PCollectionRowTuple> {
  static final String OUTPUT_TAG = "snapshots";
  static final String ERROR_TAG = "errors";

  /** Only present with {@link SchemaEvolutionConfig#getDryRun()}. */
  static final String DRY_RUN_TAG = "dry_run_report";

  private static final Duration DEFAULT_TRIGGER_INTERVAL = Duration.standardMinutes(10);
  private static final Counter numManifestFilesAdded =
      counter(AddFiles.class, "numManifestFilesAdded");
  private static final Counter numDataFilesAdded = counter(AddFiles.class, "numDataFilesAdded");
  private static final Counter numErrorFiles = counter(AddFiles.class, "numErrorFiles");
  static final String UNCHECKED_FORMAT_COUNTER = "numUncheckedFormatFiles";
  static final String UNPROVEN_PINS_COUNTER = "numUnprovenPinFiles";
  private static final Counter numUncheckedFormatFiles =
      counter(AddFiles.class, UNCHECKED_FORMAT_COUNTER);
  private static final Counter numUnprovenPinFiles = counter(AddFiles.class, UNPROVEN_PINS_COUNTER);
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
  private final @Nullable List<String> sortFields;
  private final @Nullable Map<String, String> tableProps;
  private final SchemaEvolutionConfig evolution;
  private CommitSchemaUnion.Committer committer = CommitSchemaUnion.DEFAULT_COMMITTER;

  public AddFiles(
      IcebergCatalogConfig catalogConfig,
      String tableIdentifier,
      @Nullable String locationPrefix,
      @Nullable List<String> partitionFields,
      @Nullable List<String> sortFields,
      @Nullable Map<String, String> tableProps,
      @Nullable Integer manifestFileSize,
      @Nullable Duration intervalTrigger) {
    this(
        catalogConfig,
        tableIdentifier,
        locationPrefix,
        partitionFields,
        sortFields,
        tableProps,
        manifestFileSize,
        intervalTrigger,
        null);
  }

  /**
   * @param locationPrefix when set on a partitioned table, the partition is read from the path
   *     after this prefix instead of from the file's column stats
   * @param partitionFields partition spec applied when the table is created by this transform
   * @param sortFields sort order applied when the table is created by this transform
   * @param tableProps table properties applied when the table is created by this transform
   * @param manifestFileSize data files per manifest
   * @param intervalTrigger streaming only: how often manifests are committed
   * @param evolution schema evolution settings; null or no options means the schema is never
   *     changed
   */
  public AddFiles(
      IcebergCatalogConfig catalogConfig,
      String tableIdentifier,
      @Nullable String locationPrefix,
      @Nullable List<String> partitionFields,
      @Nullable List<String> sortFields,
      @Nullable Map<String, String> tableProps,
      @Nullable Integer manifestFileSize,
      @Nullable Duration intervalTrigger,
      @Nullable SchemaEvolutionConfig evolution) {
    this.evolution = evolution != null ? evolution : SchemaEvolutionConfig.disabled();
    this.catalogConfig = catalogConfig;
    this.tableIdentifier = tableIdentifier;
    this.partitionFields = partitionFields;
    this.sortFields = sortFields;
    this.tableProps = tableProps;
    this.intervalTrigger = intervalTrigger;
    this.manifestFileSize =
        manifestFileSize != null ? manifestFileSize : DEFAULT_DATAFILES_PER_MANIFEST;
    this.locationPrefix = locationPrefix;
  }

  @Override
  public PCollectionRowTuple expand(PCollection<String> input) {
    if (input.isBounded().equals(UNBOUNDED)) {
      Preconditions.checkArgument(
          !evolution.isEnabled(),
          "Schema evolution is not yet supported for unbounded input: run a batch pipeline or"
              + " remove the schema evolution options.");
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

    PCollection<String> paths = input;
    if (evolution.isEnabled()) {
      // one commit per window, and for bounded input the window is the whole input
      PCollection<String> windowed =
          input.apply("PrePassGlobalWindow", Window.into(new GlobalWindows()));
      PCollection<List<CollectDistinctSchemas.SchemaGroup>> schemas = distinctSchemas(windowed);
      CommitSchemaUnion.Settings settings =
          new CommitSchemaUnion.Settings(
              evolution,
              evolution.incompatibleSchemaHandlingFor(input.isBounded()),
              new CommitSchemaUnion.NewTableSettings(partitionFields, sortFields, tableProps));
      if (evolution.getDryRun()) {
        return report(schemas, settings);
      }
      PCollection<Long> committed = commitSchema(schemas, settings);
      paths = windowed.apply("WaitForSchemaCommit", Wait.on(committed));
    }

    PCollectionTuple dataFiles =
        paths.apply(
            "ConvertToDataFiles",
            ParDo.of(
                    new ConvertToDataFile(
                        catalogConfig,
                        tableIdentifier,
                        locationPrefix,
                        partitionFields,
                        sortFields,
                        tableProps,
                        evolution))
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

    PCollection<KV<Integer, Iterable<SerializableDataFile>>> groupedFiles =
        keyedFiles.apply("GroupDataFilesIntoBatches", batchDataFiles);

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

  private PCollection<List<CollectDistinctSchemas.SchemaGroup>> distinctSchemas(
      PCollection<String> windowed) {
    return windowed
        .apply("ReadFooterSchema", ParDo.of(new ReadFooterSchema(evolution)))
        .setCoder(CollectDistinctSchemas.groupCoder())
        .apply(
            "CollectDistinctSchemas",
            Combine.globally(new CollectDistinctSchemas()).withoutDefaults());
  }

  /** Commits the plan for the window's schemas once; the signal releases the gated paths. */
  private PCollection<Long> commitSchema(
      PCollection<List<CollectDistinctSchemas.SchemaGroup>> schemas,
      CommitSchemaUnion.Settings settings) {
    return schemas.apply(
        "CommitSchemaOnce",
        ParDo.of(new CommitSchemaOnce(catalogConfig, tableIdentifier, settings, committer)));
  }

  /** Reports the plan for the window's schemas instead; nothing is committed or registered. */
  private PCollectionRowTuple report(
      PCollection<List<CollectDistinctSchemas.SchemaGroup>> schemas,
      CommitSchemaUnion.Settings settings) {
    PCollection<Row> report =
        schemas
            .apply(
                "DryRunReport",
                ParDo.of(new DryRunReport(catalogConfig, tableIdentifier, settings)))
            .setRowSchema(DryRunReport.REPORT_SCHEMA);

    PCollection<Row> emptySnapshots =
        schemas
            .getPipeline()
            .apply("NoSnapshots", Create.empty(RowCoder.of(SnapshotInfo.getSchema())))
            .setRowSchema(SnapshotInfo.getSchema());
    PCollection<Row> emptyErrors =
        schemas
            .getPipeline()
            .apply("NoErrors", Create.empty(RowCoder.of(ERROR_SCHEMA)))
            .setRowSchema(ERROR_SCHEMA);
    return PCollectionRowTuple.of(OUTPUT_TAG, emptySnapshots)
        .and(ERROR_TAG, emptyErrors)
        .and(DRY_RUN_TAG, report);
  }

  /** Test hook: how the schema pre-pass commits its transaction. */
  AddFiles withSchemaCommitter(CommitSchemaUnion.Committer committer) {
    this.committer = committer;
    return this;
  }

  /**
   * Reads incoming file paths, extracts Iceberg metadata, and converts them into {@link
   * SerializableDataFile} objects.
   *
   * <p><b>Asynchronous Bundle Processing:</b> Because file I/O, catalog lookups, and metadata
   * inference can be highly latency-bound, this DoFn implements an asynchronous processing pattern
   * to maximize throughput. By default, Beam processes elements in a bundle sequentially. To avoid
   * bottlenecking the pipeline, we use an internal {@link BoundedAsyncTasks} to process multiple
   * files concurrently within a single DoFn instance.
   *
   * <p><b>Lifecycle & Thread Safety:</b>
   *
   * <ul>
   *   <li><b>{@link ProcessElement}:</b> Submits the heavy lifting (format inference, metrics
   *       collection, and partition resolution) to a background thread pool and emits results as
   *       they complete.
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
    private final @Nullable List<String> sortFields;
    private final @Nullable Map<String, String> tableProps;
    private final SchemaEvolutionConfig evolution;
    private transient @MonotonicNonNull BoundedAsyncTasks<ProcessResult> tasks;
    private transient volatile @MonotonicNonNull Table table;
    private transient volatile boolean tableMissing;
    private transient @MonotonicNonNull Set<String> warned;
    private final AtomicBoolean refreshedThisBundle = new AtomicBoolean();

    // Number of parallel threads processing incoming files
    private static final int THREAD_POOL_SIZE = 10;
    private static final int MAX_IN_FLIGHT_TASKS = 100;

    public ConvertToDataFile(
        IcebergCatalogConfig catalogConfig,
        String identifier,
        @Nullable String prefix,
        @Nullable List<String> partitionFields,
        @Nullable List<String> sortFields,
        @Nullable Map<String, String> tableProps) {
      this(
          catalogConfig,
          identifier,
          prefix,
          partitionFields,
          sortFields,
          tableProps,
          SchemaEvolutionConfig.disabled());
    }

    public ConvertToDataFile(
        IcebergCatalogConfig catalogConfig,
        String identifier,
        @Nullable String prefix,
        @Nullable List<String> partitionFields,
        @Nullable List<String> sortFields,
        @Nullable Map<String, String> tableProps,
        SchemaEvolutionConfig evolution) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
      this.prefix = prefix;
      this.partitionFields = partitionFields;
      this.sortFields = sortFields;
      this.tableProps = tableProps;
      this.evolution = evolution;
    }

    static final String PREFIX_ERROR = "File path did not start with the specified prefix";
    private static final String UNKNOWN_FORMAT_ERROR = "Could not determine the file's format";
    static final String UNKNOWN_PARTITION_ERROR = "Could not determine the file's partition: ";
    static final String UNREADABLE_SCHEMA_ERROR = "Could not read the file's schema: ";
    static final String UNCOVERED_ERROR = "Table schema does not cover the file after refresh: ";
    static final String PINNED_COLUMN_ERROR = "Pinned required column ";
    static final String MISSING_TABLE_ERROR =
        "Table does not exist and the schema pre-pass could not create it (no readable Parquet"
            + " schema seeded it, or every file schema was refused): ";
    static final String UNCHECKED_FORMAT_ERROR =
        "Schema evolution is enabled but coverage and pin checks support only Parquet;"
            + " refusing to register an unchecked file of format ";

    /**
     * What a file registered under {@link UnverifiableFileHandling#ACCEPT} could not be checked
     * for.
     */
    enum Unverified {
      FORMAT,
      PIN_STATISTICS
    }

    /** Verdict of the per-file checks: an error, or none plus what was left unverified. */
    private static final class Verdict {
      static final Verdict OK = new Verdict(null, null);

      final @Nullable String error;
      final @Nullable Unverified unverified;

      private Verdict(@Nullable String error, @Nullable Unverified unverified) {
        this.error = error;
        this.unverified = unverified;
      }

      static Verdict error(String message) {
        return new Verdict(message, null);
      }

      static Verdict unverified(Unverified what) {
        return new Verdict(null, what);
      }
    }

    private static class ProcessResult {
      final @Nullable SerializableDataFile dataFile;
      final @Nullable Row errorRow;

      /** Counted on the processing thread: metrics touched from the executor are lost. */
      final @Nullable Unverified unverified;

      final Instant timestamp;
      final BoundedWindow window;
      final PaneInfo paneInfo;

      ProcessResult(
          @Nullable SerializableDataFile dataFile,
          @Nullable Row errorRow,
          @Nullable Unverified unverified,
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
        this.unverified = unverified;
        this.timestamp = timestamp;
        this.window = window;
        this.paneInfo = paneInfo;
      }
    }

    @Setup
    public void setup() {
      tasks = new BoundedAsyncTasks<>(THREAD_POOL_SIZE, MAX_IN_FLIGHT_TASKS);
      warned = ConcurrentHashMap.newKeySet();
    }

    /** Clears anything left behind if the runner reuses this instance after a failed bundle. */
    @StartBundle
    public void startBundle() {
      checkStateNotNull(tasks).cancelAll();
      refreshedThisBundle.set(false);
    }

    @Teardown
    public void teardown() {
      if (tasks != null) {
        tasks.shutdown();
      }
    }

    @ProcessElement
    public void process(
        @Element String filePath,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo paneInfo,
        MultiOutputReceiver output)
        throws Exception {
      Callable<ProcessResult> task = createProcessTask(filePath, timestamp, window, paneInfo);
      checkStateNotNull(tasks).submit(task, result -> outputResult(result, output));
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
        countUnverified(result);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      checkStateNotNull(tasks).awaitAll(result -> outputAtFinish(result, context));
    }

    private static void outputAtFinish(ProcessResult result, FinishBundleContext context) {
      if (result.errorRow != null) {
        context.output(ERRORS, result.errorRow, result.timestamp, result.window);
        numErrorFiles.inc();
      } else if (result.dataFile != null) {
        context.output(DATA_FILES, result.dataFile, result.timestamp, result.window);
        countUnverified(result);
      }
    }

    private static void countUnverified(ProcessResult result) {
      if (result.unverified == Unverified.FORMAT) {
        numUncheckedFormatFiles.inc();
      } else if (result.unverified == Unverified.PIN_STATISTICS) {
        numUnprovenPinFiles.inc();
      }
    }

    private Callable<ProcessResult> createProcessTask(
        String filePath, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {

      return () -> {
        FileFormat format;
        try {
          format = inferFormat(filePath);
        } catch (UnknownFormatException e) {
          return errorResult(filePath, UNKNOWN_FORMAT_ERROR, timestamp, window, paneInfo);
        }

        // ---- Infrastructure phase. Failures propagate so the runner retries the bundle;
        // per-file error rows here would silently drop in-flight files on a transient blip.
        // Only conditions that are properties of the file go to the error output.
        if (tableMissing) {
          return errorResult(
              filePath, MISSING_TABLE_ERROR + identifier, timestamp, window, paneInfo);
        }
        if (table == null) {
          synchronized (this) {
            if (table == null) {
              try {
                table = getOrCreateTable(filePath, format);
              } catch (FileNotFoundException e) {
                return errorResult(filePath, errorMessage(e), timestamp, window, paneInfo);
              } catch (NoSuchTableException e) {
                tableMissing = true;
                return errorResult(
                    filePath, MISSING_TABLE_ERROR + identifier, timestamp, window, paneInfo);
              }
            }
          }
        }

        // Check if the file path contains the provided prefix
        if (table.spec().isPartitioned()
            && !Strings.isNullOrEmpty(prefix)
            && !filePath.startsWith(checkStateNotNull(prefix))) {
          return errorResult(filePath, PREFIX_ERROR, timestamp, window, paneInfo);
        }

        if (table.schema().columns().isEmpty() && firstTime("empty schema")) {
          LOG.warn(
              "Table {} has no columns: files register with no readable columns and no stats."
                  + " Enable schema evolution to infer the schema from the files.",
              identifier);
        }

        // ---- Per-file phase: every failure below is one error row, never a failed bundle.
        @Nullable ParquetMetadata parquetFooter = null;
        if (format.equals(FileFormat.PARQUET)) {
          try {
            parquetFooter = ParquetFooters.read(filePath);
          } catch (Exception e) {
            return errorResult(filePath, errorMessage(e), timestamp, window, paneInfo);
          }
        }
        Verdict verdict = Verdict.OK;
        if (evolution.isEnabled()) {
          verdict = verify(filePath, format, parquetFooter);
        }
        if (verdict.error != null) {
          return errorResult(filePath, verdict.error, timestamp, window, paneInfo);
        }

        InputFile inputFile = table.io().newInputFile(filePath);

        Metrics metrics;
        try {
          metrics =
              getFileMetrics(
                  inputFile,
                  format,
                  MetricsConfig.forTable(table),
                  MappingUtil.create(table.schema()),
                  table.schema(),
                  parquetFooter);
        } catch (Exception e) {
          return errorResult(filePath, errorMessage(e), timestamp, window, paneInfo);
        }

        // Figure out which partition this DataFile should go to
        String partitionPath = "";
        @Nullable PartitionKey partitionFromMetrics = null;
        boolean partitioned = table.spec().isPartitioned();
        if (partitioned && !Strings.isNullOrEmpty(prefix)) {
          // option 1: use directory structure to determine partition
          // Note: we don't validate the DataFile content here
          partitionPath = getPartitionFromFilePath(filePath);
        } else if (partitioned) {
          try {
            // option 2: examine DataFile min/max statistics to determine partition
            partitionFromMetrics =
                getPartitionFromMetrics(metrics, inputFile, table, parquetFooter);
          } catch (UnknownPartitionException e) {
            return errorResult(
                filePath, UNKNOWN_PARTITION_ERROR + e.getMessage(), timestamp, window, paneInfo);
          }
        }

        try {
          DataFiles.Builder builder =
              DataFiles.builder(table.spec())
                  .withPath(filePath)
                  .withFormat(format)
                  .withMetrics(metrics)
                  .withFileSizeInBytes(inputFile.getLength());
          if (partitionFromMetrics != null) {
            // Set as values: a path string cannot carry a null ("flag=null" parses as false).
            builder = builder.withPartition(partitionFromMetrics);
          } else {
            builder = builder.withPartitionPath(partitionPath);
          }
          DataFile df = builder.build();
          return new ProcessResult(
              SerializableDataFile.from(df, table.spec()),
              null,
              verdict.unverified,
              timestamp,
              window,
              paneInfo);
        } catch (Exception e) {
          // getLength is a per-file read (e.g. the file was deleted mid-flight).
          return errorResult(filePath, errorMessage(e), timestamp, window, paneInfo);
        }
      };
    }

    /**
     * The checks the options promise, in order: a format the checks can read, a convertible schema,
     * coverage by the table, pins. The first failure is the verdict.
     */
    private Verdict verify(String filePath, FileFormat format, @Nullable ParquetMetadata footer) {
      if (!format.equals(FileFormat.PARQUET)) {
        if (evolution.getUnverifiableFileHandling() == UnverifiableFileHandling.REJECT) {
          return Verdict.error(UNCHECKED_FORMAT_ERROR + format.name());
        }
        if (firstTime("unchecked format")) {
          LOG.warn(
              "Registering {} files in table {} unchecked (UnverifiableFileHandling.ACCEPT):"
                  + " coverage and pin checks read only Parquet, so a required column such a file"
                  + " lacks or holds nulls in fails reads of the table, not registration. First"
                  + " file: {}",
              format,
              identifier,
              filePath);
        }
        return Verdict.unverified(Unverified.FORMAT);
      }
      ParquetMetadata parquetFooter = checkStateNotNull(footer, "Parquet checks need the footer");
      org.apache.iceberg.Schema fileSchema;
      try {
        fileSchema = FileSchemas.effective(parquetFooter);
      } catch (Exception e) {
        return Verdict.error(UNREADABLE_SCHEMA_ERROR + errorMessage(e));
      }
      @Nullable String uncovered = uncoveredReason(fileSchema);
      if (uncovered != null) {
        return Verdict.error(uncovered);
      }
      return checkPins(filePath, fileSchema, parquetFooter);
    }

    /**
     * The pre-pass commits the schema before paths reach this stage, so the cached table normally
     * covers every file. If not, refresh once per bundle (a commit may have landed since the table
     * was cached) and report the remaining delta. Never changes the schema.
     */
    private @Nullable String uncoveredReason(org.apache.iceberg.Schema fileSchema) {
      Table table = checkStateNotNull(this.table);
      SchemaDelta delta = SchemaDelta.classify(table, fileSchema);
      if (delta.isEmpty()) {
        return null;
      }
      // a commit can land after the table was cached; one refresh per bundle is enough to see it,
      // and a bundle full of routed files must not load the table once per file
      if (refreshedThisBundle.compareAndSet(false, true)) {
        synchronized (this) {
          table.refresh();
        }
      }
      delta = SchemaDelta.classify(table, fileSchema);
      if (delta.isEmpty()) {
        return null;
      }
      String reason = delta.disallowedReason(evolution);
      if (reason.isEmpty()) {
        reason = "changes not applied: " + String.join("; ", delta.descriptions());
      }
      return UNCOVERED_ERROR + reason;
    }

    /**
     * A pinned column must be present and provably null-free; a zero-row file is vacuously fine.
     * The evidence is the footer's own null counts read by the tighten rules ({@link
     * FileSchemas#nullCount}), never the Metrics built for the DataFile: the table's
     * write.metadata.metrics configuration shapes those (mode none, or the inferred-column cap on
     * wide schemas, drops the counts) and must not be able to turn pin enforcement off. A pin with
     * no count is a violation under REJECT; under ACCEPT it is recorded as unproven and the walk
     * goes on, so a pin the footer does count nulls for still fails the file.
     */
    private Verdict checkPins(
        String filePath, org.apache.iceberg.Schema fileSchema, ParquetMetadata footer) {
      Table table = checkStateNotNull(this.table);
      List<String> unproven = new ArrayList<>();
      for (String pinned : evolution.getRequiredColumns()) {
        if (table.schema().findField(pinned) == null) {
          continue;
        }
        if (fileSchema.findField(pinned) == null) {
          return Verdict.error(PINNED_COLUMN_ERROR + pinned + " is absent from the file");
        }
        @Nullable Long nulls = FileSchemas.nullCount(footer, fileSchema, pinned);
        if (nulls == null) {
          if (evolution.getUnverifiableFileHandling() == UnverifiableFileHandling.REJECT) {
            return Verdict.error(
                PINNED_COLUMN_ERROR + pinned + " has no null count statistics in the file");
          }
          unproven.add(pinned);
          continue;
        }
        if (nulls > 0) {
          return Verdict.error(
              PINNED_COLUMN_ERROR + pinned + " has " + nulls + " null(s) in the file");
        }
      }
      if (unproven.isEmpty()) {
        return Verdict.OK;
      }
      if (firstTime("unproven pins")) {
        LOG.warn(
            "Registering files in table {} whose footer has no null count statistics for pinned"
                + " column(s) {} on trust (UnverifiableFileHandling.ACCEPT): nulls there fail"
                + " reads of the table, not registration. First file: {}",
            identifier,
            unproven,
            filePath);
      }
      return Verdict.unverified(Unverified.PIN_STATISTICS);
    }

    /** Once per instance per key: at volume a line per file would drown the log. */
    private boolean firstTime(String key) {
      return checkStateNotNull(warned).add(key);
    }

    private static ProcessResult errorResult(
        String filePath, String message, Instant timestamp, BoundedWindow window, PaneInfo pane) {
      return new ProcessResult(
          null,
          Row.withSchema(ERROR_SCHEMA).addValues(filePath, message).build(),
          null,
          timestamp,
          window,
          pane);
    }

    static <W, T> T transformValue(Transform<W, T> transform, Type type, ByteBuffer bytes) {
      return transform.bind(type).apply(Conversions.fromByteBuffer(type, bytes));
    }

    private Table getOrCreateTable(String filePath, FileFormat format) throws IOException {
      TableIdentifier tableId = IcebergUtils.parseTableIdentifier(identifier);
      try {
        return catalogConfig.catalog().loadTable(tableId);
      } catch (NoSuchTableException e) {
        if (evolution.isEnabled()) {
          // the pre-pass is the only creator then, and it has already declined
          throw e;
        }
        try {
          org.apache.iceberg.Schema schema = getSchema(filePath, format);
          PartitionSpec spec = PartitionUtils.toPartitionSpec(partitionFields, schema);
          SortOrder sortOrder = SortOrderUtils.toSortOrder(sortFields, schema);
          Map<String, String> properties =
              tableProps != null ? new HashMap<>(tableProps) : new HashMap<>();
          if (properties.get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
            // Forces Name based resolution instead of position based resolution
            NameMapping mapping = MappingUtil.create(schema);
            String mappingJson = NameMappingParser.toJson(mapping);
            properties.put(TableProperties.DEFAULT_NAME_MAPPING, mappingJson);
          }

          return catalogConfig
              .catalog()
              .buildTable(tableId, schema)
              .withPartitionSpec(spec)
              .withSortOrder(sortOrder)
              .withProperties(properties)
              .create();

        } catch (AlreadyExistsException e2) { // if table already exists, just load it
          return catalogConfig.catalog().loadTable(IcebergUtils.parseTableIdentifier(identifier));
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
      MessageType messageType = ParquetFooters.read(filePath).getFileMetaData().getSchema();
      return ParquetSchemaUtil.convert(messageType);
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
    static PartitionKey getPartitionFromMetrics(
        Metrics metrics, InputFile inputFile, Table table, @Nullable ParquetMetadata preReadFooter)
        throws UnknownPartitionException {
      List<PartitionField> fields = table.spec().fields();
      List<Integer> sourceIds =
          fields.stream().map(PartitionField::sourceId).collect(Collectors.toList());
      Metrics partitionMetrics;
      // Check if metrics already includes partition columns (configured by table properties):
      if (orEmpty(metrics.lowerBounds()).keySet().containsAll(sourceIds)
          && orEmpty(metrics.upperBounds()).keySet().containsAll(sourceIds)) {
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
        // Only the partition columns: an unrelated column whose bounds cannot be collected must
        // not fail the inference.
        Map<String, String> configProps = new HashMap<>();
        configProps.put(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");
        for (String sourceName : sourceNames) {
          configProps.put(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + sourceName, "full");
        }
        MetricsConfig configWithPartitionFields = MetricsConfig.fromProperties(configProps);
        partitionMetrics =
            getFileMetrics(
                inputFile,
                inferFormat(inputFile.location()),
                configWithPartitionFields,
                MappingUtil.create(table.schema()),
                table.schema(),
                preReadFooter);
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
        ByteBuffer lowerBytes = orEmpty(partitionMetrics.lowerBounds()).get(field.sourceId());
        ByteBuffer upperBytes = orEmpty(partitionMetrics.upperBounds()).get(field.sourceId());
        if (lowerBytes == null && upperBytes == null) {
          // No bounds. The null partition is right only when every value is known to be null;
          // otherwise the partition is unknowable and must not be guessed.
          if (allValuesNull(partitionMetrics, field.sourceId())
              || lacksColumn(preReadFooter, table, field.sourceId())) {
            continue;
          }
          throw new UnknownPartitionException(
              "No column bounds for partition source column "
                  + table.schema().findColumnName(field.sourceId())
                  + " (statistics are missing, or are not collected for its type, e.g. INT96, or"
                  + " for this file format); set a location prefix to partition by path instead");
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
        // Bounds ignore nulls, and a null row belongs to the null partition.
        if (lowerTransformedValue != null && hasNulls(partitionMetrics, field.sourceId())) {
          throw new UnknownPartitionException(
              "Column has both null and non-null values, which belong to different partitions: "
                  + table.schema().findColumnName(field.sourceId()));
        }

        pk.set(i, lowerTransformedValue);
      }

      return pk;
    }

    /** Avro metrics carry null bound maps. */
    private static Map<Integer, ByteBuffer> orEmpty(@Nullable Map<Integer, ByteBuffer> bounds) {
      if (bounds == null) {
        return Collections.emptyMap();
      }
      return bounds;
    }

    /** True when the file is empty or the column's null count equals its value count. */
    private static boolean allValuesNull(Metrics metrics, int fieldId) {
      Long records = metrics.recordCount();
      if (records != null && records == 0) {
        return true;
      }
      Map<Integer, Long> valueCounts = metrics.valueCounts();
      Map<Integer, Long> nullCounts = metrics.nullValueCounts();
      if (valueCounts == null || nullCounts == null) {
        return false;
      }
      Long valueCount = valueCounts.get(fieldId);
      Long nullCount = nullCounts.get(fieldId);
      return valueCount != null && nullCount != null && valueCount.equals(nullCount);
    }

    private static boolean hasNulls(Metrics metrics, int fieldId) {
      Map<Integer, Long> nullCounts = metrics.nullValueCounts();
      if (nullCounts == null) {
        return false;
      }
      Long nullCount = nullCounts.get(fieldId);
      return nullCount != null && nullCount > 0;
    }

    /**
     * True when a Parquet file does not contain the column at all, e.g. it was written before the
     * column existed. Every row then reads as null. Unknown for other formats.
     */
    private static boolean lacksColumn(@Nullable ParquetMetadata footer, Table table, int fieldId) {
      if (footer == null) {
        return false;
      }
      MessageType fileType = footer.getFileMetaData().getSchema();
      if (!ParquetSchemaUtil.hasIds(fileType)) {
        fileType = ParquetSchemaUtil.applyNameMapping(fileType, MappingUtil.create(table.schema()));
      }
      return !containsFieldId(fileType, fieldId);
    }

    private static boolean containsFieldId(org.apache.parquet.schema.GroupType group, int fieldId) {
      for (org.apache.parquet.schema.Type field : group.getFields()) {
        if (field.getId() != null && field.getId().intValue() == fieldId) {
          return true;
        }
        if (!field.isPrimitive() && containsFieldId(field.asGroupType(), fieldId)) {
          return true;
        }
      }
      return false;
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
      extends DoFn<KV<Integer, Iterable<SerializableDataFile>>, KV<String, byte[]>> {
    private final IcebergCatalogConfig catalogConfig;
    private final String identifier;
    private transient @MonotonicNonNull Table table;

    public CreateManifests(IcebergCatalogConfig catalogConfig, String identifier) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
    }

    @ProcessElement
    public void process(
        @Element KV<Integer, Iterable<SerializableDataFile>> batch,
        OutputReceiver<KV<String, byte[]>> output)
        throws IOException {
      if (!batch.getValue().iterator().hasNext()) {
        return;
      }
      if (table == null) {
        table = catalogConfig.catalog().loadTable(IcebergUtils.parseTableIdentifier(identifier));
      }

      PartitionSpec spec = checkStateNotNull(table.specs().get(batch.getKey()));

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

    private static void ensureNameMappingPresent(Table table) {
      // Forces name-based resolution: zero-copy files typically don't carry
      // field ids, so any schema column missing from the mapping is unreadable
      // in registered files.
      @Nullable NameMapping existing =
          NameMappingUtils.parseOrNull(
              table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
      if (existing != null && NameMappingUtils.covers(existing, table.schema().asStruct())) {
        return;
      }
      if (existing != null) {
        LOG.info(
            "Name mapping of table {} does not cover its schema; regenerating it, preserving "
                + "custom names where possible.",
            table.name());
      }
      String mappingJson = NameMappingUtils.regenerate(table.schema(), existing);
      table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();
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
        table = catalogConfig.catalog().loadTable(IcebergUtils.parseTableIdentifier(identifier));
      }
      table.refresh();
      ensureNameMappingPresent(table);

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
      InputFile file,
      FileFormat format,
      MetricsConfig config,
      NameMapping mapping,
      org.apache.iceberg.Schema tableSchema,
      @Nullable ParquetMetadata preReadFooter) {
    switch (format) {
      case PARQUET:
        ParquetMetadata footer =
            checkStateNotNull(preReadFooter, "Parquet metrics require the pre-read footer");
        MessageType originalMessageType = footer.getFileMetaData().getSchema();
        if (!ParquetSchemaUtil.hasIds(originalMessageType)) {
          footer = getFooterWithTypeIds(originalMessageType, footer, mapping);
        }
        Map<Integer, BoundAdjustment> adjustments =
            BoundAdjustment.forSchema(footer.getFileMetaData().getSchema(), tableSchema);
        if (adjustments.isEmpty()) {
          return ParquetUtil.footerMetrics(footer, Stream.empty(), config, mapping);
        }
        Metrics raw =
            ParquetUtil.footerMetrics(
                BoundAdjustment.withNeutralTypes(footer, adjustments),
                Stream.empty(),
                config,
                mapping);
        return BoundAdjustment.apply(raw, adjustments);
      case ORC:
        return OrcMetrics.fromInputFile(file, config, mapping);
      case AVRO:
        return new Metrics(Avro.rowCount(file), null, null, null, null);
      default:
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }
  }

  /**
   * Some exceptions carry a null message (bare EOFException, NPE); the error-routing path must
   * never throw on one.
   */
  static String errorMessage(Throwable e) {
    return e.getMessage() != null ? e.getMessage() : e.toString();
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

  /**
   * Iceberg collects bounds in the file column's unit and width, but readers decode them with the
   * table column's type: a millis or nanos timestamp under a micros column, or a millis or micros
   * one under a nanos column, would be off by a factor of 1000 or more, and an unsigned 32-bit int
   * under a long column throws when the int is cast to a long. Bounds are what partition inference
   * and query pruning read, so they are rewritten in the table column's unit: the affected INT32
   * columns are presented to Iceberg without their annotation (so it computes plain int bounds
   * instead of throwing) and every affected bound is converted afterwards, or dropped when it does
   * not fit the table's unit. Value counts and null counts are unaffected.
   */
  enum BoundAdjustment {
    /** Millis stored under a micros type: times 1000. */
    MILLIS_TO_MICROS,
    /** Millis stored under a nanos type: times 1,000,000. */
    MILLIS_TO_NANOS,
    /** Micros stored under a nanos type: times 1000. */
    MICROS_TO_NANOS,
    /** Nanos stored under a micros type: divided by 1000, lower rounded down, upper rounded up. */
    NANOS_TO_MICROS,
    /** Unsigned 32-bit int stored under a long. */
    UINT32_TO_LONG;

    static Map<Integer, BoundAdjustment> forSchema(
        MessageType fileSchema, org.apache.iceberg.Schema tableSchema) {
      Map<Integer, BoundAdjustment> adjustments = new HashMap<>();
      collect(fileSchema, tableSchema, adjustments);
      return adjustments;
    }

    private static void collect(
        org.apache.parquet.schema.GroupType group,
        org.apache.iceberg.Schema tableSchema,
        Map<Integer, BoundAdjustment> out) {
      for (org.apache.parquet.schema.Type field : group.getFields()) {
        if (!field.isPrimitive()) {
          collect(field.asGroupType(), tableSchema, out);
          continue;
        }
        org.apache.parquet.schema.Type.ID id = field.getId();
        if (id == null) {
          continue;
        }
        @Nullable Type tableType = tableSchema.findType(id.intValue());
        if (tableType == null) {
          continue;
        }
        @Nullable BoundAdjustment adjustment = forPrimitive(field.asPrimitiveType(), tableType);
        if (adjustment != null) {
          out.put(id.intValue(), adjustment);
        }
      }
    }

    /**
     * Null when the file and table units agree, or when the table type is not the matching
     * timestamp, time or long type: such a column is left as Iceberg computes it.
     */
    private static @Nullable BoundAdjustment forPrimitive(
        org.apache.parquet.schema.PrimitiveType primitive, Type tableType) {
      org.apache.parquet.schema.LogicalTypeAnnotation annotation =
          primitive.getLogicalTypeAnnotation();
      if (annotation
          instanceof
          org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
        org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit fileUnit =
            ((org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                    annotation)
                .getUnit();
        if (tableType.typeId() == Type.TypeID.TIMESTAMP) {
          return toMicros(fileUnit);
        }
        if (tableType.typeId() == Type.TypeID.TIMESTAMP_NANO) {
          return toNanos(fileUnit);
        }
        return null;
      }
      if (annotation
          instanceof org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
        if (tableType.typeId() != Type.TypeID.TIME) {
          return null;
        }
        return toMicros(
            ((org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation)
                .getUnit());
      }
      if (annotation
          instanceof org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
            (org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
        if (intType.getBitWidth() == 32
            && !intType.isSigned()
            && tableType.typeId() == Type.TypeID.LONG) {
          return UINT32_TO_LONG;
        }
      }
      return null;
    }

    private static @Nullable BoundAdjustment toMicros(
        org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit fileUnit) {
      switch (fileUnit) {
        case MILLIS:
          return MILLIS_TO_MICROS;
        case NANOS:
          return NANOS_TO_MICROS;
        default:
          return null;
      }
    }

    private static @Nullable BoundAdjustment toNanos(
        org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit fileUnit) {
      switch (fileUnit) {
        case MILLIS:
          return MILLIS_TO_NANOS;
        case MICROS:
          return MICROS_TO_NANOS;
        default:
          return null;
      }
    }

    /** The footer with annotations removed from adjusted INT32 columns. */
    static ParquetMetadata withNeutralTypes(
        ParquetMetadata footer, Map<Integer, BoundAdjustment> adjustments) {
      MessageType schema = footer.getFileMetaData().getSchema();
      List<org.apache.parquet.schema.Type> fields = neutralFields(schema, adjustments);
      MessageType neutral = new MessageType(schema.getName(), fields);
      FileMetaData meta = footer.getFileMetaData();
      return new ParquetMetadata(
          new FileMetaData(neutral, meta.getKeyValueMetaData(), meta.getCreatedBy()),
          footer.getBlocks());
    }

    private static List<org.apache.parquet.schema.Type> neutralFields(
        org.apache.parquet.schema.GroupType group, Map<Integer, BoundAdjustment> adjustments) {
      List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
      for (org.apache.parquet.schema.Type field : group.getFields()) {
        if (field.isPrimitive()) {
          fields.add(neutralPrimitive(field.asPrimitiveType(), adjustments));
        } else {
          fields.add(
              field.asGroupType().withNewFields(neutralFields(field.asGroupType(), adjustments)));
        }
      }
      return fields;
    }

    private static org.apache.parquet.schema.Type neutralPrimitive(
        org.apache.parquet.schema.PrimitiveType primitive,
        Map<Integer, BoundAdjustment> adjustments) {
      boolean adjusted =
          primitive.getId() != null && adjustments.containsKey(primitive.getId().intValue());
      if (!adjusted
          || primitive.getPrimitiveTypeName()
              != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32) {
        return primitive;
      }
      return org.apache.parquet.schema.Types.primitive(
              primitive.getPrimitiveTypeName(), primitive.getRepetition())
          .id(primitive.getId().intValue())
          .named(primitive.getName());
    }

    static Metrics apply(Metrics metrics, Map<Integer, BoundAdjustment> adjustments) {
      Map<Integer, ByteBuffer> lower = metrics.lowerBounds();
      Map<Integer, ByteBuffer> upper = metrics.upperBounds();
      if (lower == null || upper == null) {
        return metrics;
      }
      return new Metrics(
          metrics.recordCount(),
          metrics.columnSizes(),
          metrics.valueCounts(),
          metrics.nullValueCounts(),
          metrics.nanValueCounts(),
          adjust(lower, adjustments, false),
          adjust(upper, adjustments, true));
    }

    private static Map<Integer, ByteBuffer> adjust(
        Map<Integer, ByteBuffer> bounds, Map<Integer, BoundAdjustment> adjustments, boolean upper) {
      Map<Integer, ByteBuffer> adjusted = new HashMap<>(bounds);
      for (Map.Entry<Integer, BoundAdjustment> entry : adjustments.entrySet()) {
        ByteBuffer bytes = bounds.get(entry.getKey());
        if (bytes == null) {
          continue;
        }
        try {
          long value = entry.getValue().convert(bytes, upper);
          adjusted.put(entry.getKey(), Conversions.toByteBuffer(Types.LongType.get(), value));
        } catch (ArithmeticException e) {
          // Beyond the table unit's range (e.g. year 9999 in nanos): a missing bound is safe, a
          // wrapped one is not.
          adjusted.remove(entry.getKey());
        }
      }
      return adjusted;
    }

    private long convert(ByteBuffer bytes, boolean upper) {
      ByteBuffer little = bytes.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
      switch (this) {
        case UINT32_TO_LONG:
          return Integer.toUnsignedLong(little.getInt(little.position()));
        case MILLIS_TO_MICROS:
          return Math.multiplyExact(readLong(little), 1000L);
        case MILLIS_TO_NANOS:
          return Math.multiplyExact(readLong(little), 1_000_000L);
        case MICROS_TO_NANOS:
          return Math.multiplyExact(readLong(little), 1000L);
        case NANOS_TO_MICROS:
          long nanos = little.getLong(little.position());
          return upper ? -Math.floorDiv(-nanos, 1000L) : Math.floorDiv(nanos, 1000L);
        default:
          throw new IllegalStateException(name());
      }
    }

    /** A millis TIME bound has 4 bytes: its INT32 column is presented without the annotation. */
    private static long readLong(ByteBuffer little) {
      if (little.remaining() == 4) {
        return little.getInt(little.position());
      }
      return little.getLong(little.position());
    }
  }

  static class UnknownFormatException extends IllegalArgumentException {}

  static class UnknownPartitionException extends IllegalStateException {
    UnknownPartitionException(String msg) {
      super(msg);
    }
  }
}
