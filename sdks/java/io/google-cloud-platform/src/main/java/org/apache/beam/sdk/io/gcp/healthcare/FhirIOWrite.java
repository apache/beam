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
package org.apache.beam.sdk.io.gcp.healthcare;

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIO.BASE_METRIC_PREFIX;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.healthcare.v1.model.Operation;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The type Write. */
@AutoValue
public abstract class FhirIOWrite extends PTransform<PCollection<String>, FhirIOWrite.Result> {

  /** The tag for successful writes to FHIR store. */
  public static final TupleTag<String> SUCCESSFUL_BODY = new TupleTag<String>() {};
  /** The tag for the failed writes to FHIR store. */
  public static final TupleTag<HealthcareIOError<String>> FAILED_BODY =
      new TupleTag<HealthcareIOError<String>>() {};
  /** The tag for the files that failed to FHIR store. */
  public static final TupleTag<HealthcareIOError<String>> FAILED_FILES =
      new TupleTag<HealthcareIOError<String>>() {};
  /** The tag for temp files for import to FHIR store. */
  public static final TupleTag<ResourceId> TEMP_FILES = new TupleTag<ResourceId>() {};

  /** The enum Write method. */
  public enum WriteMethod {
    /**
     * Execute Bundle Method executes a batch of requests as a single transaction @see <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
     */
    EXECUTE_BUNDLE,
    /**
     * Import Method bulk imports resources from GCS. This is ideal for initial loads to empty FHIR
     * stores. <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import></a>.
     */
    IMPORT
  }

  /** The Content Structure for imports. */
  public enum ContentStructure {
    /** If the content structure is not specified, the default value BUNDLE will be used. */
    CONTENT_STRUCTURE_UNSPECIFIED,
    /**
     * The source file contains one or more lines of newline-delimited JSON (ndjson). Each line is a
     * bundle, which contains one or more resources. Set the bundle type to history to import
     * resource versions.
     */
    BUNDLE,
    /**
     * The source file contains one or more lines of newline-delimited JSON (ndjson). Each line is a
     * single resource.
     */
    RESOURCE
  }

  /** The type Result. */
  public static class Result implements POutput {

    private final Pipeline pipeline;
    private final PCollection<String> successfulBodies;
    private final PCollection<HealthcareIOError<String>> failedBodies;
    private final PCollection<HealthcareIOError<String>> failedFiles;

    /**
     * Creates a {@link Result} in the given {@link Pipeline}.
     *
     * @param pipeline the pipeline
     * @param bodies the successful and failing bodies results.
     * @return the result
     */
    static Result in(Pipeline pipeline, PCollectionTuple bodies) throws IllegalArgumentException {
      if (bodies.has(SUCCESSFUL_BODY) && bodies.has(FAILED_BODY)) {
        return new Result(pipeline, bodies.get(SUCCESSFUL_BODY), bodies.get(FAILED_BODY), null);
      } else {
        throw new IllegalArgumentException(
            "The PCollection tuple bodies must have the FhirIO.Write.SUCCESSFUL_BODY "
                + "and FhirIO.Write.FAILED_BODY tuple tags.");
      }
    }

    static Result in(
        Pipeline pipeline,
        PCollection<HealthcareIOError<String>> failedBodies,
        PCollection<HealthcareIOError<String>> failedFiles) {
      return new Result(pipeline, null, failedBodies, failedFiles);
    }

    /**
     * Gets successful bodies from Write.
     *
     * @return the entries that were inserted
     */
    public PCollection<String> getSuccessfulBodies() {
      return this.successfulBodies;
    }

    /**
     * Gets failed bodies with err.
     *
     * @return the failed inserts with err
     */
    public PCollection<HealthcareIOError<String>> getFailedBodies() {
      return this.failedBodies;
    }

    /**
     * Gets failed file imports with err.
     *
     * @return the failed GCS uri with err
     */
    public PCollection<HealthcareIOError<String>> getFailedFiles() {
      return this.failedFiles;
    }

    @Override
    public Pipeline getPipeline() {
      return this.pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          SUCCESSFUL_BODY, successfulBodies, FAILED_BODY, failedBodies, FAILED_FILES, failedFiles);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}

    private Result(
        Pipeline pipeline,
        @Nullable PCollection<String> successfulBodies,
        PCollection<HealthcareIOError<String>> failedBodies,
        @Nullable PCollection<HealthcareIOError<String>> failedFiles) {
      this.pipeline = pipeline;
      if (successfulBodies == null) {
        successfulBodies = pipeline.apply(Create.empty(StringUtf8Coder.of()));
      }
      this.successfulBodies = successfulBodies;
      this.failedBodies = failedBodies;
      if (failedFiles == null) {
        failedFiles = pipeline.apply(Create.empty(HealthcareIOErrorCoder.of(StringUtf8Coder.of())));
      }
      this.failedFiles = failedFiles;
    }
  }

  /**
   * Gets Fhir store.
   *
   * @return the Fhir store
   */
  abstract ValueProvider<String> getFhirStore();

  /**
   * Gets write method.
   *
   * @return the write method
   */
  abstract WriteMethod getWriteMethod();

  /**
   * <<<<<<< HEAD Gets content structure for an import.
   *
   * @return the import content structure ======= Gets content structure.
   * @return the content structure >>>>>>> c22bdba81c30ae67b04c72bc16287a3261f00f88
   */
  abstract Optional<ContentStructure> getContentStructure();

  /**
   * Gets import gcs temp path.
   *
   * @return the import gcs temp path
   */
  abstract Optional<ValueProvider<String>> getImportGcsTempPath();

  /**
   * Gets import gcs dead letter path.
   *
   * @return the import gcs dead letter path
   */
  abstract Optional<ValueProvider<String>> getImportGcsDeadLetterPath();

  /** The type Builder. */
  @AutoValue.Builder
  abstract static class Builder {

    /**
     * Sets Fhir store.
     *
     * @param fhirStore the Fhir store
     * @return the write builder
     */
    abstract Builder setFhirStore(ValueProvider<String> fhirStore);

    /**
     * Sets write method.
     *
     * @param writeMethod the write method
     * @return the write builder
     */
    abstract Builder setWriteMethod(WriteMethod writeMethod);

    /**
     * Sets content structure.
     *
     * @param contentStructure the content structure
     * @return the write builder
     */
    abstract Builder setContentStructure(@Nullable ContentStructure contentStructure);

    /**
     * Sets import gcs temp path.
     *
     * @param gcsTempPath the gcs temp path
     * @return the write builder
     */
    abstract Builder setImportGcsTempPath(ValueProvider<String> gcsTempPath);

    /**
     * Sets import gcs dead letter path.
     *
     * @param gcsDeadLetterPath the gcs dead letter path
     * @return the write builder
     */
    abstract Builder setImportGcsDeadLetterPath(ValueProvider<String> gcsDeadLetterPath);

    /**
     * Build write.
     *
     * @return the write
     */
    abstract FhirIOWrite build();
  }

  /**
   * Create Method creates a single FHIR resource. @see <a
   * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/create></a>
   *
   * @param fhirStore the hl 7 v 2 store
   * @param gcsTempPath the gcs temp path
   * @param gcsDeadLetterPath the gcs dead letter path
   * @param contentStructure the content structure
   * @return the write
   */
  public static FhirIOWrite fhirStoresImport(
      String fhirStore,
      String gcsTempPath,
      String gcsDeadLetterPath,
      @Nullable ContentStructure contentStructure) {
    return new AutoValue_FhirIOWrite.Builder()
        .setFhirStore(StaticValueProvider.of(fhirStore))
        .setWriteMethod(WriteMethod.IMPORT)
        .setContentStructure(contentStructure)
        .setImportGcsTempPath(StaticValueProvider.of(gcsTempPath))
        .setImportGcsDeadLetterPath(StaticValueProvider.of(gcsDeadLetterPath))
        .build();
  }

  public static FhirIOWrite fhirStoresImport(
      String fhirStore, String gcsDeadLetterPath, @Nullable ContentStructure contentStructure) {
    return new AutoValue_FhirIOWrite.Builder()
        .setFhirStore(StaticValueProvider.of(fhirStore))
        .setWriteMethod(WriteMethod.IMPORT)
        .setContentStructure(contentStructure)
        .setImportGcsDeadLetterPath(StaticValueProvider.of(gcsDeadLetterPath))
        .build();
  }

  public static FhirIOWrite fhirStoresImport(
      ValueProvider<String> fhirStore,
      ValueProvider<String> gcsTempPath,
      ValueProvider<String> gcsDeadLetterPath,
      @Nullable ContentStructure contentStructure) {
    return new AutoValue_FhirIOWrite.Builder()
        .setFhirStore(fhirStore)
        .setWriteMethod(WriteMethod.IMPORT)
        .setContentStructure(contentStructure)
        .setImportGcsTempPath(gcsTempPath)
        .setImportGcsDeadLetterPath(gcsDeadLetterPath)
        .build();
  }

  /**
   * Execute Bundle Method executes a batch of requests as a single transaction @see <a
   * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
   *
   * @param fhirStore the fhir store
   * @return the write
   */
  public static FhirIOWrite executeBundles(String fhirStore) {
    return new AutoValue_FhirIOWrite.Builder()
        .setFhirStore(StaticValueProvider.of(fhirStore))
        .setWriteMethod(WriteMethod.EXECUTE_BUNDLE)
        .build();
  }

  /**
   * Execute bundles write.
   *
   * @param fhirStore the fhir store
   * @return the write
   */
  public static FhirIOWrite executeBundles(ValueProvider<String> fhirStore) {
    return new AutoValue_FhirIOWrite.Builder()
        .setFhirStore(fhirStore)
        .setWriteMethod(WriteMethod.EXECUTE_BUNDLE)
        .build();
  }

  private static final Logger LOG = LoggerFactory.getLogger(FhirIOWrite.class);

  @Override
  public Result expand(PCollection<String> input) {
    switch (this.getWriteMethod()) {
      case IMPORT:
        LOG.warn(
            "Make sure the Cloud Healthcare Service Agent has permissions when using import:"
                + " https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions");
        ValueProvider<String> deadPath =
            getImportGcsDeadLetterPath().orElseThrow(IllegalArgumentException::new);
        ContentStructure contentStructure =
            getContentStructure().orElse(ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED);
        ValueProvider<String> tempPath =
            getImportGcsTempPath()
                .orElse(StaticValueProvider.of(input.getPipeline().getOptions().getTempLocation()));

        return input.apply(new Import(getFhirStore(), tempPath, deadPath, contentStructure));
      case EXECUTE_BUNDLE:
      default:
        return input.apply(new ExecuteBundles(this.getFhirStore()));
    }
  }

  /**
   * Writes each bundle of elements to a new-line delimited JSON file on GCS and issues a
   * fhirStores.import Request for that file. This is intended for batch use only to facilitate
   * large backfills to empty FHIR stores and should not be used with unbounded PCollections. If
   * your use case is streaming checkout using {@link ExecuteBundles} to more safely execute bundles
   * as transactions which is safer practice for a use on a "live" FHIR store.
   */
  public static class Import extends FhirIOWrite {

    private static final Logger LOG = LoggerFactory.getLogger(Import.class);

    private final ValueProvider<String> fhirStore;
    private final ValueProvider<String> deadLetterGcsPath;
    private final ValueProvider<String> tempGcsPath;
    private final ContentStructure contentStructure;

    /*
     * Instantiates a new Import.
     *
     * @param fhirStore the fhir store
     * @param tempGcsPath the temp gcs path
     * @param deadLetterGcsPath the dead letter gcs path
     * @param contentStructure the content structure
     */
    Import(
        ValueProvider<String> fhirStore,
        ValueProvider<String> tempGcsPath,
        ValueProvider<String> deadLetterGcsPath,
        ContentStructure contentStructure) {
      this.fhirStore = fhirStore;
      this.tempGcsPath = tempGcsPath;
      this.deadLetterGcsPath = deadLetterGcsPath;
      this.contentStructure = contentStructure;
    }

    /**
     * Instantiates a new Import.
     *
     * @param fhirStore the fhir store
     * @param tempGcsPath the temp gcs path
     * @param deadLetterGcsPath the dead letter gcs path
     * @param contentStructure the content structure
     */
    Import(
        String fhirStore,
        String tempGcsPath,
        String deadLetterGcsPath,
        ContentStructure contentStructure) {
      this.fhirStore = StaticValueProvider.of(fhirStore);
      this.tempGcsPath = StaticValueProvider.of(tempGcsPath);
      this.deadLetterGcsPath = StaticValueProvider.of(deadLetterGcsPath);
      this.contentStructure = contentStructure;
    }

    @Override
    ValueProvider<String> getFhirStore() {
      return fhirStore;
    }

    @Override
    WriteMethod getWriteMethod() {
      return WriteMethod.IMPORT;
    }

    @Override
    Optional<ContentStructure> getContentStructure() {
      return Optional.of(contentStructure);
    }

    @Override
    Optional<ValueProvider<String>> getImportGcsTempPath() {
      return Optional.of(tempGcsPath);
    }

    @Override
    Optional<ValueProvider<String>> getImportGcsDeadLetterPath() {
      return Optional.of(deadLetterGcsPath);
    }

    @Override
    public Result expand(PCollection<String> input) {
      checkState(
          input.isBounded() == IsBounded.BOUNDED,
          "FhirIO.Import should only be used on bounded PCollections as it is"
              + "intended for batch use only.");

      // fall back on pipeline's temp location.
      ValueProvider<String> tempPath =
          getImportGcsTempPath()
              .orElse(StaticValueProvider.of(input.getPipeline().getOptions().getTempLocation()));

      // Write input json in batches to GCS.
      PCollectionTuple writeTmpFileResults =
          input.apply(
              "Write input to GCS",
              ParDo.of(new WriteBatchToFilesFn(tempGcsPath))
                  .withOutputTags(TEMP_FILES, TupleTagList.of(FAILED_BODY)));

      PCollection<HealthcareIOError<String>> failedBodies =
          writeTmpFileResults
              .get(FAILED_BODY)
              .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));

      PCollection<HealthcareIOError<String>> failedFiles =
          writeTmpFileResults
              .get(TEMP_FILES)
              .apply(
                  "Import Batches",
                  ParDo.of(new ImportFn(fhirStore, tempPath, deadLetterGcsPath, contentStructure)))
              .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));

      input
          .getPipeline()
          .apply("Instantiate Temp Path", Create.ofProvider(tempPath, StringUtf8Coder.of()))
          .apply(
              "Resolve SubDirs",
              MapElements.into(TypeDescriptors.strings())
                  .via((String path) -> path.endsWith("/") ? path + "*" : path + "/*"))
          .apply("Wait On File Writing", Wait.on(failedBodies))
          .apply("Wait On FHIR Importing", Wait.on(failedFiles))
          .apply(
              "Match tempGcsPath",
              FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
          .apply(
              "Delete tempGcsPath",
              ParDo.of(
                  new DoFn<Metadata, Void>() {
                    @ProcessElement
                    public void delete(@Element Metadata path, ProcessContext context) {
                      // Wait til window closes for failedBodies and failedFiles to ensure we are
                      // done processing anything under tempGcsPath because it has been
                      // successfully imported to FHIR store or copies have been moved to the
                      // dead letter path.
                      // Clean up all of tempGcsPath. This will handle removing phantom temporary
                      // objects from failed / rescheduled ImportFn::importBatch.
                      try {
                        FileSystems.delete(
                            Collections.singleton(path.resourceId()),
                            StandardMoveOptions.IGNORE_MISSING_FILES);
                      } catch (IOException e) {
                        LOG.error("error cleaning up tempGcsDir: %s", e);
                      }
                    }
                  }))
          .setCoder(VoidCoder.of());

      return Result.in(input.getPipeline(), failedBodies, failedFiles);
    }

    /**
     * WriteToFilesFn writes the input JSON to files in the provided temporary GCS location.
     * Multiple inputs are written to the same file, and therefore the input JSON is additionally
     * processed into NDJSON. The size of a single file is determined according to the size of a
     * batch's window (large for bounded PCollections).
     */
    @SuppressWarnings("initialization.fields.uninitialized")
    static class WriteBatchToFilesFn extends DoFn<String, ResourceId> {

      private final ValueProvider<String> tempGcsPath;

      private final ObjectMapper mapper;

      private ResourceId resourceId;
      private WritableByteChannel ndJsonChannel;
      private BoundedWindow window;

      /**
       * Writes batches of NDJSON to the gcs path.
       *
       * @param tempGcsPath the gcs path to write files to
       */
      WriteBatchToFilesFn(ValueProvider<String> tempGcsPath) {
        this.tempGcsPath = tempGcsPath;
        this.mapper = new ObjectMapper();
      }

      /**
       * Init the NDJSON file for the current batch.
       *
       * @throws IOException the io exception
       */
      @StartBundle
      public void initFile() throws IOException {
        String filename = String.format("fhirImportBatch-%s.ndjson", UUID.randomUUID());
        ResourceId tempDir = FileSystems.matchNewResource(this.tempGcsPath.get(), true);
        this.resourceId = tempDir.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
        this.ndJsonChannel = FileSystems.create(resourceId, "application/ld+json");
      }

      /**
       * Add the input JSON to the batch, converting to NDJSON in the process.
       *
       * @param context the context
       * @throws IOException the io exception
       */
      @ProcessElement
      public void addToFile(ProcessContext context, BoundedWindow window) throws IOException {
        this.window = window;
        String httpBody = context.element();
        try {
          // This will error if not valid JSON an convert Pretty JSON to raw JSON.
          Object data = this.mapper.readValue(httpBody, Object.class);
          String ndJson = this.mapper.writeValueAsString(data) + "\n";
          this.ndJsonChannel.write(ByteBuffer.wrap(ndJson.getBytes(StandardCharsets.UTF_8)));
        } catch (JsonProcessingException e) {
          String resource =
              String.format(
                  "Failed to parse payload: %s as json at: %s : %s."
                      + "Dropping resource from batch import.",
                  httpBody, e.getLocation().getCharOffset(), e.getMessage());
          LOG.warn(resource);
          context.output(FAILED_BODY, HealthcareIOError.of(httpBody, new IOException(resource)));
        }
      }

      /**
       * Close file.
       *
       * @param context the context
       * @throws IOException the io exception
       */
      @FinishBundle
      public void closeFile(FinishBundleContext context) throws IOException {
        // Write the file with all elements in this batch to GCS.
        ndJsonChannel.close();
        context.output(resourceId, window.maxTimestamp(), window);
      }
    }

    /** Import batches of new line delimited json files to FHIR Store. */
    @SuppressWarnings("initialization.fields.uninitialized")
    static class ImportFn extends DoFn<ResourceId, HealthcareIOError<String>> {

      private static final Counter IMPORT_OPERATION_SUCCESS =
          Metrics.counter(ImportFn.class, BASE_METRIC_PREFIX + "import_operation_success_count");
      private static final Counter IMPORT_OPERATION_ERRORS =
          Metrics.counter(ImportFn.class, BASE_METRIC_PREFIX + "import_operation_failure_count");
      private static final Counter RESOURCES_IMPORTED_SUCCESS =
          Metrics.counter(ImportFn.class, BASE_METRIC_PREFIX + "resources_imported_success_count");
      private static final Counter RESOURCES_IMPORTED_ERRORS =
          Metrics.counter(ImportFn.class, BASE_METRIC_PREFIX + "resources_imported_failure_count");
      private static final Logger LOG = LoggerFactory.getLogger(ImportFn.class);

      private final ValueProvider<String> fhirStore;
      private final ValueProvider<String> tempGcsPath;
      private final ValueProvider<String> deadLetterGcsPath;
      private final ContentStructure contentStructure;

      private ResourceId tempDir;
      private HealthcareApiClient client;

      private BoundedWindow window;
      private List<ResourceId> files;
      private List<ResourceId> tempDestinations;
      private List<ResourceId> deadLetterDestinations;

      ImportFn(
          ValueProvider<String> fhirStore,
          ValueProvider<String> tempGcsPath,
          ValueProvider<String> deadLetterGcsPath,
          ContentStructure contentStructure) {
        this.fhirStore = fhirStore;
        this.tempGcsPath = tempGcsPath;
        this.deadLetterGcsPath = deadLetterGcsPath;
        this.contentStructure = contentStructure;
      }

      @Setup
      public void init() throws IOException {
        client = new HttpHealthcareApiClient();
      }

      @StartBundle
      public void initBatch() {
        tempDir =
            FileSystems.matchNewResource(tempGcsPath.get(), true)
                .resolve(
                    String.format("tmp-%s", UUID.randomUUID()),
                    StandardResolveOptions.RESOLVE_DIRECTORY);

        files = new ArrayList<>();
        tempDestinations = new ArrayList<>();
        deadLetterDestinations = new ArrayList<>();
      }

      @ProcessElement
      public void process(ProcessContext context, BoundedWindow window) throws IOException {
        this.window = window;

        ResourceId file = context.element();
        assert file != null;
        String filename = file.getFilename();
        if (filename != null) {
          files.add(file);
          tempDestinations.add(tempDir.resolve(filename, StandardResolveOptions.RESOLVE_FILE));
          deadLetterDestinations.add(
              FileSystems.matchNewResource(deadLetterGcsPath.get(), true)
                  .resolve(filename, StandardResolveOptions.RESOLVE_FILE));
        } else {
          throw new IllegalArgumentException(String.format("Expected temp file, got: %s", file));
        }
      }

      @FinishBundle
      public void importBatch(FinishBundleContext context) throws IOException {
        // Move files to a temporary subdir (to provide common prefix) to execute import with single
        // GCS URI and allow for retries.
        // IGNORE_MISSING_FILES ignores missing source files, we enable this as if this is a retry
        // files should have already been moved over.
        FileSystems.rename(
            ImmutableList.copyOf(files),
            tempDestinations,
            StandardMoveOptions.IGNORE_MISSING_FILES);
        // Even in a retry we need to check that all temporary files are present in the temporary
        // destination.
        boolean hasMissingFile =
            FileSystems.matchResources(tempDestinations).stream()
                .anyMatch((MatchResult r) -> r.status() != Status.OK);
        if (hasMissingFile) {
          throw new IllegalStateException("Not all temporary files are present for importing.");
        }
        ResourceId importUri = tempDir.resolve("*", StandardResolveOptions.RESOLVE_FILE);

        try {
          // Blocking fhirStores.import request.
          assert contentStructure != null;
          Operation operation =
              client.importFhirResource(
                  fhirStore.get(), importUri.toString(), contentStructure.name());
          operation = client.pollOperation(operation, 15000L);
          FhirIO.incrementLroCounters(
              operation,
              IMPORT_OPERATION_SUCCESS,
              IMPORT_OPERATION_ERRORS,
              RESOURCES_IMPORTED_SUCCESS,
              RESOURCES_IMPORTED_ERRORS);

          // Clean up temp files on GCS as they we successfully imported to FHIR store and no longer
          // needed.
          FileSystems.delete(tempDestinations);
        } catch (IOException | InterruptedException e) {
          ResourceId deadLetterResourceId =
              FileSystems.matchNewResource(deadLetterGcsPath.get(), true);
          LOG.warn(
              String.format(
                  "Failed to import %s with error: %s. Moving to deadletter path %s",
                  importUri, e.getMessage(), deadLetterResourceId.toString()));
          IMPORT_OPERATION_ERRORS.inc();

          FileSystems.rename(tempDestinations, deadLetterDestinations);
          context.output(
              HealthcareIOError.of(importUri.toString(), e), window.maxTimestamp(), window);
        }
      }
    }
  }

  /**
   * The type ExecuteBundles which executes FHIR bundles on the provided FHIR store. According to
   * the JSON input, bundles are either executed in batch or transactionally.
   */
  public static class ExecuteBundles extends FhirIOWrite {

    private final ValueProvider<String> fhirStore;

    /**
     * Instantiates a new ExecuteBundles.
     *
     * @param fhirStore the fhir store
     */
    ExecuteBundles(ValueProvider<String> fhirStore) {
      this.fhirStore = fhirStore;
    }

    /**
     * Instantiates a new ExecuteBundles.
     *
     * @param fhirStore the fhir store
     */
    ExecuteBundles(String fhirStore) {
      this.fhirStore = StaticValueProvider.of(fhirStore);
    }

    @Override
    ValueProvider<String> getFhirStore() {
      return fhirStore;
    }

    @Override
    WriteMethod getWriteMethod() {
      return WriteMethod.EXECUTE_BUNDLE;
    }

    @Override
    Optional<ContentStructure> getContentStructure() {
      return Optional.empty();
    }

    @Override
    Optional<ValueProvider<String>> getImportGcsTempPath() {
      return Optional.empty();
    }

    @Override
    Optional<ValueProvider<String>> getImportGcsDeadLetterPath() {
      return Optional.empty();
    }

    @Override
    public Result expand(PCollection<String> input) {
      PCollectionTuple bodies =
          input.apply(
              ParDo.of(new ExecuteBundlesFn(fhirStore))
                  .withOutputTags(SUCCESSFUL_BODY, TupleTagList.of(FAILED_BODY)));
      bodies.get(SUCCESSFUL_BODY).setCoder(StringUtf8Coder.of());
      bodies.get(FAILED_BODY).setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
      return Result.in(input.getPipeline(), bodies);
    }

    /** ExecuteBundlesFn executes the FHIR JSON bundle. */
    static class ExecuteBundlesFn extends DoFn<String, String> {

      private static final Counter EXECUTE_BUNDLE_ERRORS =
          Metrics.counter(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_error_count");
      private static final Counter EXECUTE_BUNDLE_SUCCESS =
          Metrics.counter(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_success_count");
      private static final Distribution EXECUTE_BUNDLE_LATENCY_MS =
          Metrics.distribution(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_latency_ms");

      private final ValueProvider<String> fhirStore;
      private final ObjectMapper mapper;

      @SuppressWarnings("initialization.fields.uninitialized")
      private transient HealthcareApiClient client;

      ExecuteBundlesFn(ValueProvider<String> fhirStore) {
        this.fhirStore = fhirStore;
        this.mapper = new ObjectMapper();
      }

      /**
       * Initialize healthcare client.
       *
       * @throws IOException If the Healthcare client cannot be created.
       */
      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      @ProcessElement
      public void executeBundles(ProcessContext context) {
        String body = context.element();
        try {
          long startTime = Instant.now().toEpochMilli();
          // Validate that data was set to valid JSON.
          mapper.readTree(body);
          client.executeFhirBundle(fhirStore.get(), body);
          EXECUTE_BUNDLE_LATENCY_MS.update(Instant.now().toEpochMilli() - startTime);
          EXECUTE_BUNDLE_SUCCESS.inc();
          context.output(SUCCESSFUL_BODY, body);
        } catch (IOException | HealthcareHttpException e) {
          EXECUTE_BUNDLE_ERRORS.inc();
          context.output(FAILED_BODY, HealthcareIOError.of(body, e));
        }
      }
    }
  }
}
