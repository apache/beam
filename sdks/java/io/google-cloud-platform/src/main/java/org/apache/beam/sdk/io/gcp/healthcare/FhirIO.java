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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.Operation;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Write.Result;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FhirIO} provides an API for reading and writing resources to <a
 * href="https://cloud.google.com/healthcare/docs/concepts/fhir">Google Cloud Healthcare Fhir API.
 * </a>
 *
 * <p>Read
 *
 * <p>FHIR resources can be read with {@link FhirIO.Read} supports use cases where you have a
 * ${@link PCollection} of message IDS. This is appropriate for reading the Fhir notifications from
 * a Pub/Sub subscription with {@link PubsubIO#readStrings()} or in cases where you have a manually
 * prepared list of messages that you need to process (e.g. in a text file read with {@link
 * org.apache.beam.sdk.io.TextIO}*) .
 *
 * <p>Fetch Resource contents from Fhir Store based on the {@link PCollection} of message ID strings
 * {@link FhirIO.Read.Result} where one can call {@link Read.Result#getResources()} to retrieved a
 * {@link PCollection} containing the successfully fetched {@link HttpBody}s and/or {@link
 * FhirIO.Read.Result#getFailedReads()}* to retrieve a {@link PCollection} of {@link
 * HealthcareIOError}* containing the resource ID that could not be fetched and the exception as a
 * {@link HealthcareIOError}, this can be used to write to the dead letter storage system of your
 * choosing. This error handling is mainly to transparently surface errors where the upstream {@link
 * PCollection}* contains IDs that are not valid or are not reachable due to permissions issues.
 *
 * <p>Write Resources can be written to FHIR with two different methods: Import or Execute Bundle.
 *
 * <p>Execute Bundle This is best for use cases where you are writing to a non-empty FHIR store with
 * other clients or otherwise need referential integrity (e.g. A Streaming HL7v2 to FHIR ETL
 * pipeline).
 *
 * @see <a
 *     href=>https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>
 *     <p>Import This is best for use cases where you are populating an empty FHIR store with no
 *     other clients. It is faster than the execute bundles method but does not respect referential
 *     integrity and the resources are not written transactionally (e.g. a historicaly backfill on a
 *     new FHIR store) This requires each resource to contain a client provided ID. It is important
 *     that when using import you give the appropriate permissions to the Google Cloud Healthcare
 *     Service Agent
 * @see <a
 *     href=>https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions></a>
 * @see <a
 *     href=>https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores/import></a>
 *     A {@link PCollection} of {@link HttpBody} can be ingested into an Fhir store using {@link
 *     FhirIO.Write#fhirStoresImport(String, String, String, ContentStructure)} This will return a
 *     {@link FhirIO.Write.Result} on which you can call {@link
 *     FhirIO.Write.Result#getFailedInsertsWithErr()} to retrieve a {@link PCollection} of {@link
 *     HealthcareIOError} containing the {@link HttpBody} that failed to be ingested and the
 *     exception.
 *     <p>Example
 *     <pre>{@code
 * Pipeline pipeline = ...
 *
 * // Tail the FHIR store by retrieving resources based on Pub/Sub notifications.
 * FHIRIO.Read.Result readResult = p
 *   .apply("Read FHIR notifications",
 *     PubsubIO.readStrings().fromSubscription(options.getNotificationSubscription()))
 *   .apply(FhirIO.readResources());
 *
 * // happily retrived messages
 * PCollection<HttpBody> resources = readResult.getResources();
 * // message IDs that couldn't be retrieved + error context
 * PCollection<HealthcareIOError<String>> failedReads = readResult.getFailedReads();
 *
 * failedReads.apply("Write Message IDs of Failed Reads to BigQuery",
 *     BigQueryIO
 *         .write()
 *         .to(option.getBQFhirExecuteBundlesDeadLetterTable())
 *         .withFormatFunction(new HealthcareIOErrorToTableRow()));
 *
 * output = resources.apply("Happy path transformations", ...);
 * FhirIO.Write.Result writeResult =
 *     output.apply("Execute FHIR Bundles", FhirIO.executeBundles(options.getFhirStore()));
 *
 * PCollection<HealthcareIOError<HttpBody>> failedBundles = writeResult.getFailedInsertsWithErr();
 *
 * failedBundles.apply("Write failed bundles to BigQuery",
 *     BigQueryIO
 *         .write()
 *         .to(option.getBQFhirExecuteBundlesDeadLetterTable())
 *         .withFormatFunction(new HealthcareIOErrorToTableRow()));
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
      @Nullable ContentStructure contentStructure) {
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
      @Nullable ContentStructure contentStructure) {
    return new Import(fhirStore, tempDir, deadLetterDir, contentStructure);
  }

  /** The type Read. */
  @Experimental // TODO(jaketf) add IT and remove this tag.
  public static class Read extends PTransform<PCollection<String>, FhirIO.Read.Result> {
    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    /** Instantiates a new Read. */
    public Read() {
      // TODO(jaketf) add IT and remove this tag.
      LOG.warn("FhirIO.Read is experimental and has not been tested.");
    }

    /** The type Result. */
    public static class Result implements POutput, PInput {
      private PCollection<HttpBody> resources;

      private PCollection<HealthcareIOError<String>> failedReads;
      /** The Pct. */
      PCollectionTuple pct;

      /**
       * Of fhir io . read . result.
       *
       * @param pct the pct
       * @return the fhir io . read . result
       * @throws IllegalArgumentException the illegal argument exception
       */
      public static FhirIO.Read.Result of(PCollectionTuple pct) throws IllegalArgumentException {
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
        this.resources = pct.get(OUT).setCoder(new HttpBodyCoder());
        this.failedReads =
            pct.get(DEAD_LETTER).setCoder(new HealthcareIOErrorCoder<>(StringUtf8Coder.of()));
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
      public PCollection<HttpBody> getResources() {
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
    public static final TupleTag<HttpBody> OUT = new TupleTag<HttpBody>() {};
    /** The tag for the deadletter output of Fhir Messages. */
    public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
        new TupleTag<HealthcareIOError<String>>() {};

    @Override
    public FhirIO.Read.Result expand(PCollection<String> input) {
      return input.apply("Fetch Fhir messages", new FhirIO.Read.FetchHttpBody());
    }

    /**
     * DoFn to fetch a resource from an Google Cloud Healthcare FHIR store based on resourceID
     *
     * <p>This DoFn consumes a {@link PCollection} of notifications {@link String}s from the FHIR
     * store, and fetches the actual {@link HttpBody} object based on the id in the notification and
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
    public static class FetchHttpBody extends PTransform<PCollection<String>, FhirIO.Read.Result> {

      /** Instantiates a new Fetch Fhir message DoFn. */
      public FetchHttpBody() {}

      @Override
      public FhirIO.Read.Result expand(PCollection<String> resourceIds) {
        return new FhirIO.Read.Result(
            resourceIds.apply(
                ParDo.of(new FhirIO.Read.FetchHttpBody.HttpBodyGetFn())
                    .withOutputTags(FhirIO.Read.OUT, TupleTagList.of(FhirIO.Read.DEAD_LETTER))));
      }

      /** DoFn for fetching messages from the Fhir store with error handling. */
      public static class HttpBodyGetFn extends DoFn<String, HttpBody> {

        private Counter failedMessageGets =
            Metrics.counter(FhirIO.Read.FetchHttpBody.HttpBodyGetFn.class, "failed-message-reads");
        private static final Logger LOG =
            LoggerFactory.getLogger(FhirIO.Read.FetchHttpBody.HttpBodyGetFn.class);
        private final Counter successfulHttpBodyGets =
            Metrics.counter(
                FhirIO.Read.FetchHttpBody.HttpBodyGetFn.class, "successful-hl7v2-message-gets");
        private HealthcareApiClient client;

        /** Instantiates a new Hl 7 v 2 message get fn. */
        HttpBodyGetFn() {}

        /**
         * Instantiate healthcare client.
         *
         * @throws IOException the io exception
         */
        @Setup
        public void instantiateHealthcareClient() throws IOException {
          this.client = new HttpHealthcareApiClient();
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

        private HttpBody fetchResource(HealthcareApiClient client, String resourceId)
            throws IOException, IllegalArgumentException, InterruptedException {
          long startTime = System.currentTimeMillis();
          Sleeper sleeper = Sleeper.DEFAULT;

          com.google.api.services.healthcare.v1beta1.model.HttpBody resource =
              client.readFhirResource(resourceId);

          if (resource == null) {
            throw new IOException(String.format("GET request for %s returned null", resourceId));
          }
          this.successfulHttpBodyGets.inc();
          return resource;
        }
      }
    }
  }

  /** The type Write. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<HttpBody>, Write.Result> {

    /** The tag for the failed writes to FHIR store`. */
    public static final TupleTag<HealthcareIOError<HttpBody>> FAILED_BODY =
        new TupleTag<HealthcareIOError<HttpBody>>() {};

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
      private final PCollection<HealthcareIOError<HttpBody>> failedInsertsWithErr;

      /**
       * Creates a {@link FhirIO.Write.Result} in the given {@link Pipeline}. @param pipeline the
       * pipeline
       *
       * @param failedInserts the failed inserts
       * @return the result
       */
      static Result in(Pipeline pipeline, PCollection<HealthcareIOError<HttpBody>> failedInserts) {
        return new Result(pipeline, failedInserts);
      }

      /**
       * Gets failed inserts with err.
       *
       * @return the failed inserts with err
       */
      public PCollection<HealthcareIOError<HttpBody>> getFailedInsertsWithErr() {
        return this.failedInsertsWithErr;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pipeline;
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        failedInsertsWithErr.setCoder(new HealthcareIOErrorCoder<HttpBody>(new HttpBodyCoder()));
        return ImmutableMap.of(Write.FAILED_BODY, failedInsertsWithErr);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}

      private Result(
          Pipeline pipeline, PCollection<HealthcareIOError<HttpBody>> failedInsertsWithErr) {
        this.pipeline = pipeline;
        this.failedInsertsWithErr = failedInsertsWithErr;
      }
    }

    /**
     * Gets Fhir store.
     *
     * @return the Fhir store
     */
    abstract String getFhirStore();

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
    abstract Optional<ContentStructure> getContentStructure();

    /**
     * Gets import gcs temp path.
     *
     * @return the import gcs temp path
     */
    abstract Optional<String> getImportGcsTempPath();

    /**
     * Gets import gcs dead letter path.
     *
     * @return the import gcs dead letter path
     */
    abstract Optional<String> getImportGcsDeadLetterPath();

    /** The type Builder. */
    @AutoValue.Builder
    abstract static class Builder {

      /**
       * Sets Fhir store.
       *
       * @param fhirStore the Fhir store
       * @return the Fhir store
       */
      abstract Builder setFhirStore(String fhirStore);

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
      abstract Builder setContentStructure(ContentStructure contentStructure);

      /**
       * Sets import gcs temp path.
       *
       * @param gcsTempPath the gcs temp path
       * @return the import gcs temp path
       */
      abstract Builder setImportGcsTempPath(String gcsTempPath);

      /**
       * Sets import gcs dead letter path.
       *
       * @param gcsDeadLetterPath the gcs dead letter path
       * @return the import gcs dead letter path
       */
      abstract Builder setImportGcsDeadLetterPath(String gcsDeadLetterPath);

      /**
       * Build write.
       *
       * @return the write
       */
      abstract Write build();
    }

    private static Write.Builder write(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder().setFhirStore(fhirStore);
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
        @Nullable ContentStructure contentStructure) {
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
          .setFhirStore(fhirStore)
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
          .setFhirStore(fhirStore.get())
          .setWriteMethod(WriteMethod.EXECUTE_BUNDLE)
          .build();
    }

    private static final Logger LOG = LoggerFactory.getLogger(Write.class);

    @Override
    public Result expand(PCollection<HttpBody> input) {
      PCollection<HealthcareIOError<HttpBody>> failedBundles;
      PCollection<HealthcareIOError<String>> failedImports;
      switch (this.getWriteMethod()) {
        case IMPORT:
          LOG.warn(
              "Make sure the Cloud Healthcare Service Agent has permissions when using import:"
                  + " https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions");
          String tempPath = getImportGcsTempPath().orElseThrow(IllegalArgumentException::new);
          String deadPath = getImportGcsDeadLetterPath().orElseThrow(IllegalArgumentException::new);
          ContentStructure contentStructure =
              getContentStructure().orElseThrow(IllegalArgumentException::new);

          failedBundles =
              input
                  .apply(new Import(getFhirStore(), tempPath, deadPath, contentStructure))
                  .setCoder(new HealthcareIOErrorCoder<>(new HttpBodyCoder()));
          // fall through
        case EXECUTE_BUNDLE:
        default:
          failedBundles =
              input.apply(
                  "Execute FHIR Bundles",
                  ParDo.of(new ExecuteBundles.ExecuteBundlesFn(this.getFhirStore())));
      }
      return Result.in(input.getPipeline(), failedBundles);
    }
  }

  /**
   * Writes each bundle of elements to a new-line delimited JSON file on GCS and issues a
   * fhirStores.import Request for that file.
   */
  public static class Import
      extends PTransform<PCollection<HttpBody>, PCollection<HealthcareIOError<HttpBody>>> {

    private final String fhirStore;
    private final String tempGcsPath;
    private final String deadLetterGcsPath;
    private final ContentStructure contentStructure;

    /**
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
      this.fhirStore = fhirStore.get();
      this.tempGcsPath = tempGcsPath.get();
      this.deadLetterGcsPath = deadLetterGcsPath.get();
      if (contentStructure == null) {
        this.contentStructure = ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED;
      } else {
        this.contentStructure = contentStructure;
      }
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
      this.fhirStore = fhirStore;
      this.tempGcsPath = tempGcsPath;
      this.deadLetterGcsPath = deadLetterGcsPath;
      if (contentStructure == null) {
        this.contentStructure = ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED;
      } else {
        this.contentStructure = contentStructure;
      }
    }

    @Override
    public PCollection<HealthcareIOError<HttpBody>> expand(PCollection<HttpBody> input) {
      return input.apply(
          ParDo.of(new ImportFn(fhirStore, tempGcsPath, deadLetterGcsPath, contentStructure)));
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

    /** The type Import fn. */
    static class ImportFn extends DoFn<HttpBody, HealthcareIOError<HttpBody>> {
      private final String fhirStore;
      private final String tempGcsPath;
      private final String deadLetterGcsPath;
      private final ContentStructure contentStructure;
      private ObjectMapper mapper;
      private ResourceId resourceId;
      private ResourceId deadLetterResourceId;
      private WritableByteChannel ndJsonChannel;

      private transient HealthcareApiClient client;
      private static final Logger LOG = LoggerFactory.getLogger(ImportFn.class);

      /**
       * Instantiates a new Import fn.
       *
       * @param fhirStore the fhir store
       * @param tempGcsPath the temp gcs path
       * @param deadLetterGcsPath the dead letter gcs path
       * @param contentStructure the content structure
       */
      ImportFn(
          String fhirStore,
          String tempGcsPath,
          String deadLetterGcsPath,
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
      public void initBatch() throws IOException {
        // Write each bundle to newline delimited JSON file.
        String filename = String.format("fhirImportBatch-%s.ndjson", UUID.randomUUID().toString());
        this.resourceId = FileSystems.matchNewResource(this.tempGcsPath + filename, false);
        this.deadLetterResourceId =
            FileSystems.matchNewResource(this.deadLetterGcsPath + filename, false);
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
      public void addToBatch(ProcessContext context) throws IOException {
        HttpBody httpBody = context.element();
        try {
          // This will error if not valid JSON an convert Pretty JSON to raw JSON.
          Object data = this.mapper.readValue(httpBody.getData(), Object.class);
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
       * Import batch.
       *
       * @param context the context
       * @throws IOException the io exception
       * @throws InterruptedException the interrupted exception
       */
      @FinishBundle
      public void importBatch(FinishBundleContext context)
          throws IOException, InterruptedException {
        // Write the file with all elements in this bundle to GCS.
        this.ndJsonChannel.close();
        try {
          // Blocking fhirStores.import request.
          assert contentStructure != null;
          Operation operation =
              client.importFhirResource(fhirStore, resourceId.toString(), contentStructure.name());
          client.pollOperation(operation, 500L);
          // Clean up temp file on GCS.
          FileSystems.delete(Collections.singleton(resourceId));
        } catch (IOException | InterruptedException e) {
          LOG.warn(
              String.format(
                  "Failed to import %s with error: %s. Moving to deadletter path %s",
                  resourceId.toString(), e.getMessage(), deadLetterResourceId.toString()));
          FileSystems.copy(
              Collections.singletonList(resourceId),
              Collections.singletonList(deadLetterResourceId));
          FileSystems.delete(Collections.singleton(resourceId));
          throw e;
        }
      }
    }
  }

  /** The type Execute bundles. */
  public static class ExecuteBundles extends PTransform<PCollection<HttpBody>, Write.Result> {
    private final String fhirStore;

    /**
     * Instantiates a new Execute bundles.
     *
     * @param fhirStore the fhir store
     */
    ExecuteBundles(ValueProvider<String> fhirStore) {
      this.fhirStore = fhirStore.get();
    }

    /**
     * Instantiates a new Execute bundles.
     *
     * @param fhirStore the fhir store
     */
    ExecuteBundles(String fhirStore) {
      this.fhirStore = fhirStore;
    }

    @Override
    public Result expand(PCollection<HttpBody> input) {
      return Write.Result.in(
          input.getPipeline(), input.apply(ParDo.of(new ExecuteBundlesFn(fhirStore))));
    }

    /** The type Write Fhir fn. */
    static class ExecuteBundlesFn extends DoFn<HttpBody, HealthcareIOError<HttpBody>> {

      private Counter failedBundles = Metrics.counter(ExecuteBundlesFn.class, "failed-bundles");
      private transient HealthcareApiClient client;
      private final ObjectMapper mapper = new ObjectMapper();
      /** The Fhir store. */
      private final String fhirStore;

      /**
       * Instantiates a new Write Fhir fn.
       *
       * @param fhirStore the Fhir store
       */
      ExecuteBundlesFn(String fhirStore) {
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
        HttpBody body = context.element();
        try {
          // Validate that data was set to valid JSON.
          mapper.readTree(body.getData());
          client.executeFhirBundle(fhirStore, body);
        } catch (IOException e) {
          failedBundles.inc();
          context.output(HealthcareIOError.of(body, e));
        }
      }
    }
  }
}
