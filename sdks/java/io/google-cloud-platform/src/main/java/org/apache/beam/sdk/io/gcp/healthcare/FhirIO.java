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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.Operation;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FhirIO} provides an API for reading and writing resources to <a
 * href="https://cloud.google.com/healthcare/docs/concepts/fhir">Google Cloud Healthcare Fhir API.
 * </a>
 *
 * <h3>Reading</h3>
 *
 * <p>FHIR resources can be read with {@link FhirIO.Read}, which supports use cases where you have a
 * ${@link PCollection} of message IDs. This is appropriate for reading the Fhir notifications from
 * a Pub/Sub subscription with {@link PubsubIO#readStrings()} or in cases where you have a manually
 * prepared list of messages that you need to process (e.g. in a text file read with {@link
 * org.apache.beam.sdk.io.TextIO}*) .
 *
 * <p>Fetch Resource contents from Fhir Store based on the {@link PCollection} of message ID strings
 * {@link FhirIO.Read.Result} where one can call {@link Read.Result#getResources()} to retrieve a
 * {@link PCollection} containing the successfully fetched {@link String}s and/or {@link
 * FhirIO.Read.Result#getFailedReads()}* to retrieve a {@link PCollection} of {@link
 * HealthcareIOError}* containing the resource ID that could not be fetched and the exception as a
 * {@link HealthcareIOError}, this can be used to write to the dead letter storage system of your
 * choosing. This error handling is mainly to transparently surface errors where the upstream {@link
 * PCollection}* contains IDs that are not valid or are not reachable due to permissions issues.
 *
 * <h3>Writing</h3>
 *
 * <p>Write Resources can be written to FHIR with two different methods: Import or Execute Bundle.
 *
 * <p>Execute Bundle This is best for use cases where you are writing to a non-empty FHIR store with
 * other clients or otherwise need referential integrity (e.g. A Streaming HL7v2 to FHIR ETL
 * pipeline).
 *
 * <p>Import This is best for use cases where you are populating an empty FHIR store with no other
 * clients. It is faster than the execute bundles method but does not respect referential integrity
 * and the resources are not written transactionally (e.g. a historicaly backfill on a new FHIR
 * store) This requires each resource to contain a client provided ID. It is important that when
 * using import you give the appropriate permissions to the Google Cloud Healthcare Service Agent.
 *
 * @see <a
 *     href=>https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>
 * @see <a
 *     href=>https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions></a>
 * @see <a
 *     href=>https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores/import></a>
 *     A {@link PCollection} of {@link String} can be ingested into an Fhir store using {@link
 *     FhirIO.Write#fhirStoresImport(String, String, String, FhirIO.Import.ContentStructure)} This
 *     will return a {@link FhirIO.Write.Result} on which you can call {@link
 *     FhirIO.Write.Result#getFailedBodies()} to retrieve a {@link PCollection} of {@link
 *     HealthcareIOError} containing the {@link String} that failed to be ingested and the
 *     exception.
 *     <p>Example
 *     <pre>{@code
 * Pipeline pipeline = ...
 *
 * // Tail the FHIR store by retrieving resources based on Pub/Sub notifications.
 * FhirIO.Read.Result readResult = p
 *   .apply("Read FHIR notifications",
 *     PubsubIO.readStrings().fromSubscription(options.getNotificationSubscription()))
 *   .apply(FhirIO.readResources());
 *
 * // happily retrived messages
 * PCollection<String> resources = readResult.getResources();
 * // message IDs that couldn't be retrieved + error context
 * PCollection<HealthcareIOError<String>> failedReads = readResult.getFailedReads();
 *
 * failedReads.apply("Write Message IDs / Stacktrace for Failed Reads to BigQuery",
 *     BigQueryIO
 *         .write()
 *         .to(option.getBQFhirExecuteBundlesDeadLetterTable())
 *         .withFormatFunction(new HealthcareIOErrorToTableRow()));
 *
 * output = resources.apply("Happy path transformations", ...);
 * FhirIO.Write.Result writeResult =
 *     output.apply("Execute FHIR Bundles", FhirIO.executeBundles(options.getExistingFhirStore()));
 *
 * PCollection<HealthcareIOError<String>> failedBundles = writeResult.getFailedInsertsWithErr();
 *
 * failedBundles.apply("Write failed bundles to BigQuery",
 *     BigQueryIO
 *         .write()
 *         .to(option.getBQFhirExecuteBundlesDeadLetterTable())
 *         .withFormatFunction(new HealthcareIOErrorToTableRow()));
 *
 * // Alternatively you could use import for high throughput to a new store.
 * FhirIO.Write.Result writeResult =
 *     output.apply("Import FHIR Resources", FhirIO.executeBundles(options.getNewFhirStore()));
 * }***
 * </pre>
 */
public class FhirIO {

  /**
   * Read resources from a PCollection of resource IDs (e.g. when subscribing the pubsub
   * notifications)
   *
   * @return the read
   * @see Read
   */
  public static Read readResources() {
    return new Read();
  }

  /**
   * Import resources. Intended for use on empty FHIR stores
   *
   * @param fhirStore the fhir store
   * @param tempDir the temp dir
   * @param deadLetterDir the dead letter dir
   * @param contentStructure the content structure
   * @return the import
   * @see Import
   */
  public static Import importResources(
      String fhirStore,
      String tempDir,
      String deadLetterDir,
      FhirIO.Import.@Nullable ContentStructure contentStructure) {
    return new Import(fhirStore, tempDir, deadLetterDir, contentStructure);
  }

  /**
   * Import resources. Intended for use on empty FHIR stores
   *
   * @param fhirStore the fhir store
   * @param tempDir the temp dir
   * @param deadLetterDir the dead letter dir
   * @param contentStructure the content structure
   * @return the import
   * @see Import
   */
  public static Import importResources(
      ValueProvider<String> fhirStore,
      ValueProvider<String> tempDir,
      ValueProvider<String> deadLetterDir,
      FhirIO.Import.@Nullable ContentStructure contentStructure) {
    return new Import(fhirStore, tempDir, deadLetterDir, contentStructure);
  }

  /**
   * Export resources to GCS. Intended for use on non-empty FHIR stores
   *
   * @param fhirStore the fhir store, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param exportGcsUriPrefix the destination GCS dir, in the format:
   *     gs://YOUR_BUCKET_NAME/path/to/a/dir
   * @return the export
   * @see ExportGcs
   */
  public static ExportGcs exportResourcesToGcs(String fhirStore, String exportGcsUriPrefix) {
    return new ExportGcs(
        StaticValueProvider.of(fhirStore), StaticValueProvider.of(exportGcsUriPrefix));
  }

  /** The type Read. */
  public static class Read extends PTransform<PCollection<String>, FhirIO.Read.Result> {
    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    /** Instantiates a new Read. */
    public Read() {}

    /** The type Result. */
    public static class Result implements POutput, PInput {
      private PCollection<String> resources;

      private PCollection<HealthcareIOError<String>> failedReads;
      /** The Pct. */
      PCollectionTuple pct;

      /**
       * Create FhirIO.Read.Result form PCollectionTuple with OUT and DEAD_LETTER tags.
       *
       * @param pct the pct
       * @return the read result
       * @throws IllegalArgumentException the illegal argument exception
       */
      static FhirIO.Read.Result of(PCollectionTuple pct) throws IllegalArgumentException {
        if (pct.getAll()
            .keySet()
            .containsAll((Collection<?>) TupleTagList.of(OUT).and(DEAD_LETTER))) {
          return new FhirIO.Read.Result(pct);
        } else {
          throw new IllegalArgumentException(
              "The PCollection tuple must have the FhirIO.Read.OUT "
                  + "and FhirIO.Read.DEAD_LETTER tuple tags");
        }
      }

      private Result(PCollectionTuple pct) {
        this.pct = pct;
        this.resources = pct.get(OUT);
        this.failedReads =
            pct.get(DEAD_LETTER).setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
      }

      /**
       * Gets failed reads.
       *
       * @return the failed reads
       */
      public PCollection<HealthcareIOError<String>> getFailedReads() {
        return failedReads;
      }

      /**
       * Gets resources.
       *
       * @return the resources
       */
      public PCollection<String> getResources() {
        return resources;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pct.getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(OUT, resources);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /** The tag for the main output of Fhir Messages. */
    public static final TupleTag<String> OUT = new TupleTag<String>() {};
    /** The tag for the deadletter output of Fhir Messages. */
    public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
        new TupleTag<HealthcareIOError<String>>() {};

    @Override
    public FhirIO.Read.Result expand(PCollection<String> input) {
      return input.apply("Fetch Fhir messages", new FetchResourceJsonString());
    }

    /**
     * DoFn to fetch a resource from an Google Cloud Healthcare FHIR store based on resourceID
     *
     * <p>This DoFn consumes a {@link PCollection} of notifications {@link String}s from the FHIR
     * store, and fetches the actual {@link String} object based on the id in the notification and
     * will output a {@link PCollectionTuple} which contains the output and dead-letter {@link
     * PCollection}*.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link FhirIO.Read#OUT} - Contains all {@link PCollection} records successfully read
     *       from the Fhir store.
     *   <li>{@link FhirIO.Read#DEAD_LETTER} - Contains all {@link PCollection} of {@link
     *       HealthcareIOError}* of message IDs which failed to be fetched from the Fhir store, with
     *       error message and stacktrace.
     * </ul>
     */
    static class FetchResourceJsonString
        extends PTransform<PCollection<String>, FhirIO.Read.Result> {

      /** Instantiates a new Fetch Fhir message DoFn. */
      public FetchResourceJsonString() {}

      @Override
      public FhirIO.Read.Result expand(PCollection<String> resourceIds) {
        return new FhirIO.Read.Result(
            resourceIds.apply(
                ParDo.of(new ReadResourceFn())
                    .withOutputTags(FhirIO.Read.OUT, TupleTagList.of(FhirIO.Read.DEAD_LETTER))));
      }

      /** DoFn for fetching messages from the Fhir store with error handling. */
      static class ReadResourceFn extends DoFn<String, String> {

        private Counter failedMessageGets =
            Metrics.counter(ReadResourceFn.class, "failed-message-reads");
        private static final Logger LOG = LoggerFactory.getLogger(ReadResourceFn.class);
        private final Counter successfulStringGets =
            Metrics.counter(ReadResourceFn.class, "successful-hl7v2-message-gets");
        private HealthcareApiClient client;
        private ObjectMapper mapper;

        /** Instantiates a new Hl 7 v 2 message get fn. */
        ReadResourceFn() {}

        /**
         * Instantiate healthcare client.
         *
         * @throws IOException the io exception
         */
        @Setup
        public void instantiateHealthcareClient() throws IOException {
          this.client = new HttpHealthcareApiClient();
          this.mapper = new ObjectMapper();
        }

        /**
         * Process element.
         *
         * @param context the context
         */
        @ProcessElement
        public void processElement(ProcessContext context) {
          String resourceId = context.element();
          try {
            context.output(fetchResource(this.client, resourceId));
          } catch (Exception e) {
            failedMessageGets.inc();
            LOG.warn(
                String.format(
                    "Error fetching Fhir message with ID %s writing to Dead Letter "
                        + "Queue. Cause: %s Stack Trace: %s",
                    resourceId, e.getMessage(), Throwables.getStackTraceAsString(e)));
            context.output(FhirIO.Read.DEAD_LETTER, HealthcareIOError.of(resourceId, e));
          }
        }

        private String fetchResource(HealthcareApiClient client, String resourceId)
            throws IOException, IllegalArgumentException {
          long startTime = System.currentTimeMillis();

          HttpBody resource = client.readFhirResource(resourceId);

          if (resource == null) {
            throw new IOException(String.format("GET request for %s returned null", resourceId));
          }
          this.successfulStringGets.inc();
          return mapper.writeValueAsString(resource);
        }
      }
    }
  }

  /** The type Write. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, Write.Result> {

    /** The tag for the failed writes to FHIR store`. */
    public static final TupleTag<HealthcareIOError<String>> FAILED_BODY =
        new TupleTag<HealthcareIOError<String>>() {};
    /** The tag for the files that failed to FHIR store`. */
    public static final TupleTag<HealthcareIOError<String>> FAILED_FILES =
        new TupleTag<HealthcareIOError<String>>() {};
    /** The tag for temp files for import to FHIR store`. */
    public static final TupleTag<ResourceId> TEMP_FILES = new TupleTag<ResourceId>() {};

    /** The enum Write method. */
    public enum WriteMethod {
      /**
       * Execute Bundle Method executes a batch of requests as a single transaction @see <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
       */
      EXECUTE_BUNDLE,
      /**
       * Import Method bulk imports resources from GCS. This is ideal for initial loads to empty
       * FHIR stores. <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores/import></a>.
       */
      IMPORT
    }

    /** The type Result. */
    public static class Result implements POutput {
      private final Pipeline pipeline;
      private final PCollection<HealthcareIOError<String>> failedBodies;
      private final PCollection<HealthcareIOError<String>> failedFiles;

      /**
       * Creates a {@link FhirIO.Write.Result} in the given {@link Pipeline}. @param pipeline the
       * pipeline
       *
       * @param failedBodies the failed inserts
       * @return the result
       */
      static Result in(Pipeline pipeline, PCollection<HealthcareIOError<String>> failedBodies) {
        return new Result(pipeline, failedBodies, null);
      }

      static Result in(
          Pipeline pipeline,
          PCollection<HealthcareIOError<String>> failedBodies,
          PCollection<HealthcareIOError<String>> failedFiles) {
        return new Result(pipeline, failedBodies, failedFiles);
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
        return ImmutableMap.of(Write.FAILED_BODY, failedBodies, Write.FAILED_FILES, failedFiles);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}

      private Result(
          Pipeline pipeline,
          PCollection<HealthcareIOError<String>> failedBodies,
          @Nullable PCollection<HealthcareIOError<String>> failedFiles) {
        this.pipeline = pipeline;
        this.failedBodies = failedBodies;
        if (failedFiles == null) {
          failedFiles =
              (PCollection<HealthcareIOError<String>>)
                  pipeline.apply(Create.empty(HealthcareIOErrorCoder.of(StringUtf8Coder.of())));
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
     * Gets content structure.
     *
     * @return the content structure
     */
    abstract Optional<FhirIO.Import.ContentStructure> getContentStructure();

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
       * @return the Fhir store
       */
      abstract Builder setFhirStore(ValueProvider<String> fhirStore);

      /**
       * Sets write method.
       *
       * @param writeMethod the write method
       * @return the write method
       */
      abstract Builder setWriteMethod(WriteMethod writeMethod);

      /**
       * Sets content structure.
       *
       * @param contentStructure the content structure
       * @return the content structure
       */
      abstract Builder setContentStructure(FhirIO.Import.ContentStructure contentStructure);

      /**
       * Sets import gcs temp path.
       *
       * @param gcsTempPath the gcs temp path
       * @return the import gcs temp path
       */
      abstract Builder setImportGcsTempPath(ValueProvider<String> gcsTempPath);

      /**
       * Sets import gcs dead letter path.
       *
       * @param gcsDeadLetterPath the gcs dead letter path
       * @return the import gcs dead letter path
       */
      abstract Builder setImportGcsDeadLetterPath(ValueProvider<String> gcsDeadLetterPath);

      /**
       * Build write.
       *
       * @return the write
       */
      abstract Write build();
    }

    private static Write.Builder write(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder().setFhirStore(StaticValueProvider.of(fhirStore));
    }

    /**
     * Create Method creates a single FHIR resource. @see <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/create></a>
     *
     * @param fhirStore the hl 7 v 2 store
     * @param gcsTempPath the gcs temp path
     * @param gcsDeadLetterPath the gcs dead letter path
     * @param contentStructure the content structure
     * @return the write
     */
    public static Write fhirStoresImport(
        String fhirStore,
        String gcsTempPath,
        String gcsDeadLetterPath,
        FhirIO.Import.@Nullable ContentStructure contentStructure) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(StaticValueProvider.of(fhirStore))
          .setWriteMethod(Write.WriteMethod.IMPORT)
          .setContentStructure(contentStructure)
          .setImportGcsTempPath(StaticValueProvider.of(gcsTempPath))
          .setImportGcsDeadLetterPath(StaticValueProvider.of(gcsDeadLetterPath))
          .build();
    }

    public static Write fhirStoresImport(
        String fhirStore,
        String gcsDeadLetterPath,
        FhirIO.Import.@Nullable ContentStructure contentStructure) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(StaticValueProvider.of(fhirStore))
          .setWriteMethod(Write.WriteMethod.IMPORT)
          .setContentStructure(contentStructure)
          .setImportGcsDeadLetterPath(StaticValueProvider.of(gcsDeadLetterPath))
          .build();
    }

    public static Write fhirStoresImport(
        ValueProvider<String> fhirStore,
        ValueProvider<String> gcsTempPath,
        ValueProvider<String> gcsDeadLetterPath,
        FhirIO.Import.@Nullable ContentStructure contentStructure) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(fhirStore)
          .setWriteMethod(Write.WriteMethod.IMPORT)
          .setContentStructure(contentStructure)
          .setImportGcsTempPath(gcsTempPath)
          .setImportGcsDeadLetterPath(gcsDeadLetterPath)
          .build();
    }

    /**
     * Execute Bundle Method executes a batch of requests as a single transaction @see <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
     *
     * @param fhirStore the hl 7 v 2 store
     * @return the write
     */
    public static Write executeBundles(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder()
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
    public static Write executeBundles(ValueProvider<String> fhirStore) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(fhirStore)
          .setWriteMethod(WriteMethod.EXECUTE_BUNDLE)
          .build();
    }

    private static final Logger LOG = LoggerFactory.getLogger(Write.class);

    @Override
    public Result expand(PCollection<String> input) {
      PCollection<HealthcareIOError<String>> failedBundles;
      PCollection<HealthcareIOError<String>> failedImports;
      switch (this.getWriteMethod()) {
        case IMPORT:
          LOG.warn(
              "Make sure the Cloud Healthcare Service Agent has permissions when using import:"
                  + " https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions");
          ValueProvider<String> deadPath =
              getImportGcsDeadLetterPath().orElseThrow(IllegalArgumentException::new);
          FhirIO.Import.ContentStructure contentStructure =
              getContentStructure().orElseThrow(IllegalArgumentException::new);
          ValueProvider<String> tempPath =
              getImportGcsTempPath()
                  .orElse(
                      StaticValueProvider.of(input.getPipeline().getOptions().getTempLocation()));

          return input.apply(new Import(getFhirStore(), tempPath, deadPath, contentStructure));
        case EXECUTE_BUNDLE:
        default:
          failedBundles =
              input
                  .apply(
                      "Execute FHIR Bundles",
                      ParDo.of(new ExecuteBundles.ExecuteBundlesFn(this.getFhirStore())))
                  .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
      }
      return Result.in(input.getPipeline(), failedBundles);
    }
  }

  /**
   * Writes each bundle of elements to a new-line delimited JSON file on GCS and issues a
   * fhirStores.import Request for that file. This is intended for batch use only to facilitate
   * large backfills to empty FHIR stores and should not be used with unbounded PCollections. If
   * your use case is streaming checkout using {@link ExecuteBundles} to more safely execute bundles
   * as transactions which is safer practice for a use on a "live" FHIR store.
   */
  public static class Import extends Write {

    private final ValueProvider<String> fhirStore;
    private final ValueProvider<String> deadLetterGcsPath;
    private final ContentStructure contentStructure;
    private static final int DEFAULT_FILES_PER_BATCH = 10000;
    private static final Logger LOG = LoggerFactory.getLogger(Import.class);
    private ValueProvider<String> tempGcsPath;

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
        @Nullable ContentStructure contentStructure) {
      this.fhirStore = fhirStore;
      this.tempGcsPath = tempGcsPath;
      this.deadLetterGcsPath = deadLetterGcsPath;
      if (contentStructure == null) {
        this.contentStructure = ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED;
      } else {
        this.contentStructure = contentStructure;
      }
    }

    Import(
        ValueProvider<String> fhirStore,
        ValueProvider<String> deadLetterGcsPath,
        @Nullable ContentStructure contentStructure) {
      this(fhirStore, null, deadLetterGcsPath, contentStructure);
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
        @Nullable ContentStructure contentStructure) {
      this.fhirStore = StaticValueProvider.of(fhirStore);
      this.tempGcsPath = StaticValueProvider.of(tempGcsPath);
      this.deadLetterGcsPath = StaticValueProvider.of(deadLetterGcsPath);
      if (contentStructure == null) {
        this.contentStructure = ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED;
      } else {
        this.contentStructure = contentStructure;
      }
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
    public Write.Result expand(PCollection<String> input) {
      checkState(
          input.isBounded() == IsBounded.BOUNDED,
          "FhirIO.Import should only be used on unbounded PCollections as it is"
              + "intended for batch use only.");

      // fall back on pipeline's temp location.
      ValueProvider<String> tempPath =
          getImportGcsTempPath()
              .orElse(StaticValueProvider.of(input.getPipeline().getOptions().getTempLocation()));

      // Write bundles of String to GCS
      PCollectionTuple writeTmpFileResults =
          input.apply(
              "Write nd json to GCS",
              ParDo.of(new WriteBundlesToFilesFn(fhirStore, tempPath, deadLetterGcsPath))
                  .withOutputTags(Write.TEMP_FILES, TupleTagList.of(Write.FAILED_BODY)));

      PCollection<HealthcareIOError<String>> failedBodies =
          writeTmpFileResults
              .get(Write.FAILED_BODY)
              .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
      int numShards = 100;
      PCollection<HealthcareIOError<String>> failedFiles =
          writeTmpFileResults
              .get(Write.TEMP_FILES)
              .apply(
                  "Shard files", // to paralelize group into batches
                  WithKeys.of(elm -> ThreadLocalRandom.current().nextInt(0, numShards)))
              .setCoder(KvCoder.of(TextualIntegerCoder.of(), ResourceIdCoder.of()))
              .apply("Assemble File Batches", GroupIntoBatches.ofSize(DEFAULT_FILES_PER_BATCH))
              .setCoder(
                  KvCoder.of(TextualIntegerCoder.of(), IterableCoder.of(ResourceIdCoder.of())))
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
                      // done processing
                      // anything under tempGcsPath because it has been successfully imported to
                      // FHIR store or
                      // copies have been moved to the dead letter path.
                      // Clean up all of tempGcsPath. This will handle removing phantom temporary
                      // objects from
                      // failed / rescheduled ImportFn::importBatch.
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

      return Write.Result.in(input.getPipeline(), failedBodies, failedFiles);
    }

    /** The Write bundles to new line delimited json files. */
    static class WriteBundlesToFilesFn extends DoFn<String, ResourceId> {

      private final ValueProvider<String> fhirStore;
      private final ValueProvider<String> tempGcsPath;
      private final ValueProvider<String> deadLetterGcsPath;
      private ObjectMapper mapper;
      private ResourceId resourceId;
      private WritableByteChannel ndJsonChannel;
      private BoundedWindow window;

      private transient HealthcareApiClient client;
      private static final Logger LOG = LoggerFactory.getLogger(WriteBundlesToFilesFn.class);

      WriteBundlesToFilesFn(
          ValueProvider<String> fhirStore,
          ValueProvider<String> tempGcsPath,
          ValueProvider<String> deadLetterGcsPath) {
        this.fhirStore = fhirStore;
        this.tempGcsPath = tempGcsPath;
        this.deadLetterGcsPath = deadLetterGcsPath;
      }

      /**
       * Instantiates a new Import fn.
       *
       * @param fhirStore the fhir store
       * @param tempGcsPath the temp gcs path
       * @param deadLetterGcsPath the dead letter gcs path
       */
      WriteBundlesToFilesFn(String fhirStore, String tempGcsPath, String deadLetterGcsPath) {
        this.fhirStore = StaticValueProvider.of(fhirStore);
        this.tempGcsPath = StaticValueProvider.of(tempGcsPath);
        this.deadLetterGcsPath = StaticValueProvider.of(deadLetterGcsPath);
      }

      /**
       * Init client.
       *
       * @throws IOException the io exception
       */
      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      /**
       * Init batch.
       *
       * @throws IOException the io exception
       */
      @StartBundle
      public void initFile() throws IOException {
        // Write each bundle to newline delimited JSON file.
        String filename = String.format("fhirImportBatch-%s.ndjson", UUID.randomUUID().toString());
        ResourceId tempDir = FileSystems.matchNewResource(this.tempGcsPath.get(), true);
        this.resourceId = tempDir.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
        this.ndJsonChannel = FileSystems.create(resourceId, "application/ld+json");
        if (mapper == null) {
          this.mapper = new ObjectMapper();
        }
      }

      /**
       * Add to batch.
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
                      + "Dropping message from batch import.",
                  httpBody.toString(), e.getLocation().getCharOffset(), e.getMessage());
          LOG.warn(resource);
          context.output(
              Write.FAILED_BODY, HealthcareIOError.of(httpBody, new IOException(resource)));
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
        // Write the file with all elements in this bundle to GCS.
        ndJsonChannel.close();
        context.output(resourceId, window.maxTimestamp(), window);
      }
    }

    /** Import batches of new line delimited json files to FHIR Store. */
    static class ImportFn
        extends DoFn<KV<Integer, Iterable<ResourceId>>, HealthcareIOError<String>> {

      private static final Logger LOG = LoggerFactory.getLogger(ImportFn.class);
      private final ValueProvider<String> tempGcsPath;
      private final ValueProvider<String> deadLetterGcsPath;
      private ResourceId tempDir;
      private final ContentStructure contentStructure;
      private HealthcareApiClient client;
      private final ValueProvider<String> fhirStore;

      ImportFn(
          ValueProvider<String> fhirStore,
          ValueProvider<String> tempGcsPath,
          ValueProvider<String> deadLetterGcsPath,
          @Nullable ContentStructure contentStructure) {
        this.fhirStore = fhirStore;
        this.tempGcsPath = tempGcsPath;
        this.deadLetterGcsPath = deadLetterGcsPath;
        if (contentStructure == null) {
          this.contentStructure = ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED;
        } else {
          this.contentStructure = contentStructure;
        }
      }

      @Setup
      public void init() throws IOException {
        tempDir =
            FileSystems.matchNewResource(tempGcsPath.get(), true)
                .resolve(
                    String.format("tmp-%s", UUID.randomUUID().toString()),
                    StandardResolveOptions.RESOLVE_DIRECTORY);
        client = new HttpHealthcareApiClient();
      }

      /**
       * Move files to a temporary subdir (to provide common prefix) to execute import with single
       * GCS URI.
       */
      @ProcessElement
      public void importBatch(
          @Element KV<Integer, Iterable<ResourceId>> element,
          OutputReceiver<HealthcareIOError<String>> output)
          throws IOException {
        Iterable<ResourceId> batch = element.getValue();
        List<ResourceId> tempDestinations = new ArrayList<>();
        List<ResourceId> deadLetterDestinations = new ArrayList<>();
        assert batch != null;
        for (ResourceId file : batch) {
          tempDestinations.add(
              tempDir.resolve(file.getFilename(), StandardResolveOptions.RESOLVE_FILE));
          deadLetterDestinations.add(
              FileSystems.matchNewResource(deadLetterGcsPath.get(), true)
                  .resolve(file.getFilename(), StandardResolveOptions.RESOLVE_FILE));
        }
        // Ignore missing files since this might be a retry, which means files
        // should have been copied over.
        FileSystems.copy(
            ImmutableList.copyOf(batch),
            tempDestinations,
            StandardMoveOptions.IGNORE_MISSING_FILES);
        // Check whether any temporary files are not present.
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
          client.pollOperation(operation, 500L);
          // Clean up temp files on GCS as they we successfully imported to FHIR store and no longer
          // needed.
          FileSystems.delete(tempDestinations);
        } catch (IOException | InterruptedException e) {
          ResourceId deadLetterResourceId =
              FileSystems.matchNewResource(deadLetterGcsPath.get(), true);
          LOG.warn(
              String.format(
                  "Failed to import %s with error: %s. Moving to deadletter path %s",
                  importUri.toString(), e.getMessage(), deadLetterResourceId.toString()));
          FileSystems.rename(tempDestinations, deadLetterDestinations);
          output.output(HealthcareIOError.of(importUri.toString(), e));
        } finally {
          // If we've reached this point files have either been successfully import to FHIR store
          // or moved to Dead Letter Queue.
          // Clean up original files for this batch on GCS.
          FileSystems.delete(ImmutableList.copyOf(batch));
        }
      }
    }

    /** The enum Content structure. */
    public enum ContentStructure {
      /** If the content structure is not specified, the default value BUNDLE will be used. */
      CONTENT_STRUCTURE_UNSPECIFIED,
      /**
       * The source file contains one or more lines of newline-delimited JSON (ndjson). Each line is
       * a bundle, which contains one or more resources. Set the bundle type to history to import
       * resource versions.
       */
      BUNDLE,
      /**
       * The source file contains one or more lines of newline-delimited JSON (ndjson). Each line is
       * a single resource.
       */
      RESOURCE,
      /** The entire file is one JSON bundle. The JSON can span multiple lines. */
      BUNDLE_PRETTY,
      /** The entire file is one JSON resource. The JSON can span multiple lines. */
      RESOURCE_PRETTY
    }
  }

  /** The type Execute bundles. */
  public static class ExecuteBundles extends PTransform<PCollection<String>, Write.Result> {
    private final ValueProvider<String> fhirStore;

    /**
     * Instantiates a new Execute bundles.
     *
     * @param fhirStore the fhir store
     */
    ExecuteBundles(ValueProvider<String> fhirStore) {
      this.fhirStore = fhirStore;
    }

    /**
     * Instantiates a new Execute bundles.
     *
     * @param fhirStore the fhir store
     */
    ExecuteBundles(String fhirStore) {
      this.fhirStore = StaticValueProvider.of(fhirStore);
    }

    @Override
    public FhirIO.Write.Result expand(PCollection<String> input) {
      return Write.Result.in(
          input.getPipeline(),
          input
              .apply(ParDo.of(new ExecuteBundlesFn(fhirStore)))
              .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of())));
    }

    /** The type Write Fhir fn. */
    static class ExecuteBundlesFn extends DoFn<String, HealthcareIOError<String>> {

      private Counter failedBundles = Metrics.counter(ExecuteBundlesFn.class, "failed-bundles");
      private transient HealthcareApiClient client;
      private final ObjectMapper mapper = new ObjectMapper();
      /** The Fhir store. */
      private final ValueProvider<String> fhirStore;

      /**
       * Instantiates a new Write Fhir fn.
       *
       * @param fhirStore the Fhir store
       */
      ExecuteBundlesFn(ValueProvider<String> fhirStore) {
        this.fhirStore = fhirStore;
      }

      /**
       * Initialize healthcare client.
       *
       * @throws IOException the io exception
       */
      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      /**
       * Execute Bundles.
       *
       * @param context the context
       */
      @ProcessElement
      public void executeBundles(ProcessContext context) {
        String body = context.element();
        try {
          // Validate that data was set to valid JSON.
          mapper.readTree(body);
          client.executeFhirBundle(fhirStore.get(), body);
        } catch (IOException | HealthcareHttpException e) {
          failedBundles.inc();
          context.output(HealthcareIOError.of(body, e));
        }
      }
    }
  }

  /** Export FHIR resources from a FHIR store to new line delimited json files on GCS. */
  public static class ExportGcs extends PTransform<PBegin, ExportGcs.Result> {
    public static final TupleTag<String> OUT = new TupleTag<String>() {};

    /**
     * Represents the result of an export, including both the successful parsed messages, and
     * invalid ones.
     */
    public static class Result implements POutput, PInput {
      private PCollection<String> resources;

      public static Result of(PCollection<String> resources) {
        return new Result(resources);
      }

      private Result(PCollection<String> resources) {
        this.resources = resources;
      }

      public PCollection<String> getResources() {
        return resources;
      }

      @Override
      public Pipeline getPipeline() {
        return this.resources.getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(OUT, resources);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    private final ValueProvider<String> fhirStore;
    private final ValueProvider<String> exportGcsUriPrefix;

    public ExportGcs(ValueProvider<String> fhirStore, ValueProvider<String> exportGcsUriPrefix) {
      this.fhirStore = fhirStore;
      this.exportGcsUriPrefix = exportGcsUriPrefix;
    }

    @Override
    public ExportGcs.Result expand(PBegin input) {
      return ExportGcs.Result.of(
          input
              .apply(Create.ofProvider(fhirStore, StringUtf8Coder.of()))
              .apply(
                  "ScheduleExportOperations",
                  ParDo.of(new ExportResourcesToGcsFn(this.exportGcsUriPrefix)))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches())
              .apply("ReadResourcesFromFiles", TextIO.readFiles()));
    }

    /** A function that schedules an export operation and monitors the status. */
    public static class ExportResourcesToGcsFn extends DoFn<String, String> {

      private HealthcareApiClient client;
      private final ValueProvider<String> exportGcsUriPrefix;

      public ExportResourcesToGcsFn(ValueProvider<String> exportGcsUriPrefix) {
        this.exportGcsUriPrefix = exportGcsUriPrefix;
      }

      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      @ProcessElement
      public void exportResourcesToGcs(ProcessContext context)
          throws IOException, InterruptedException, HealthcareHttpException {
        String fhirStore = context.element();
        String gcsPrefix = this.exportGcsUriPrefix.get();
        Operation operation = client.exportFhirResourceToGcs(fhirStore, gcsPrefix);
        operation = client.pollOperation(operation, 1000L);
        if (operation.getError() != null) {
          throw new RuntimeException(
              String.format("Export operation (%s) failed.", operation.getName()));
        }
        context.output(String.format("%s/*", gcsPrefix.replaceAll("/+$", "")));
      }
    }
  }
}
