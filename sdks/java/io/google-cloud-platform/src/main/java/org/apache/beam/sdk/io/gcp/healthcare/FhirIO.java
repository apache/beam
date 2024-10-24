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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1.model.HttpBody;
import com.google.api.services.healthcare.v1.model.Operation;
import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
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
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.FhirResourcePagesIterator;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * ${@link PCollection} of FHIR resource names in the format of
 * projects/{p}/locations/{l}/datasets/{d}/fhirStores/{f}/fhir/{resourceType}/{id}. This is
 * appropriate for reading the Fhir notifications from a Pub/Sub subscription with {@link
 * PubsubIO#readStrings()} or in cases where you have a manually prepared list of resources that you
 * need to process (e.g. in a text file read with {@link org.apache.beam.sdk.io.TextIO}*) .
 *
 * <p>Get Resource contents from the FHIR Store based on the {@link PCollection} of FHIR resource
 * name strings {@link FhirIO.Read.Result} where one can call {@link Read.Result#getResources()} to
 * retrieve a {@link PCollection} containing the successfully fetched json resources as {@link
 * String}s and/or {@link FhirIO.Read.Result#getFailedReads()} to retrieve a {@link PCollection} of
 * {@link HealthcareIOError} containing the resources that could not be fetched and the exception as
 * a {@link HealthcareIOError}, this can be used to write to the dead letter storage system of your
 * choosing. This error handling is mainly to transparently surface errors where the upstream {@link
 * PCollection} contains FHIR resources that are not valid or are not reachable due to permissions
 * issues.
 *
 * <p>Additionally, you can query an entire FHIR Patient resource's compartment (resources that
 * refer to the patient, and are referred to by the patient) by calling {@link
 * FhirIO#getPatientEverything} to execute a FHIR GetPatientEverythingRequest.
 *
 * <h3>Writing</h3>
 *
 * <p>Write Resources can be written to FHIR with a couple of different methods: including Import or
 * Execute Bundle.
 *
 * <ul>
 *   <li><a
 *       href="https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle">Execute
 *       Bundle</a>
 *       <p>This is best for use cases where you are writing to a non-empty FHIR store with other
 *       clients or otherwise need referential integrity (e.g. A Streaming HL7v2 to FHIR ETL
 *       pipeline).
 *   <li><a
 *       href="https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import">Import</a>
 *       <p>This is best for use cases where you are populating an empty FHIR store with no other
 *       clients. It is faster than the execute bundles method but does not respect referential
 *       integrity and the resources are not written transactionally (e.g. a historical backfill on
 *       a new FHIR store) This requires each resource to contain a client provided ID. It is
 *       important that when using import you give the appropriate <a
 *       href="https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions">permissions</a>
 *       to the Google Cloud Healthcare Service Agent.
 *   <li><a
 *       href="https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/export">Export</a>
 *       <p>This is to export FHIR resources from a FHIR store to Google Cloud Storage or BigQuery.
 *       The output resources are in ndjson (newline delimited json) of FHIR resources. It is
 *       important that when using export you give the appropriate <a
 *       href="https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions">permissions</a>
 *       to the Google Cloud Healthcare Service Agent.
 *   <li><a
 *       href="https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/deidentify">Deidentify</a>
 *       <p>This is to de-identify FHIR resources from a source FHIR store and write the result to a
 *       destination FHIR store. It is important that the destination store must already exist.
 *   <li><a
 *       href="https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/search">Search</a>
 *       <p>This is to search FHIR resources within a given FHIR store. The inputs are individual
 *       FHIR Search queries, represented by the FhirSearchParameter class. The outputs are results
 *       of each Search, represented as a Json array of FHIR resources in string form, with
 *       pagination handled, and an optional input key.
 * </ul>
 *
 * <p>A {@link PCollection} of {@link String} can be ingested into an Fhir store using {@link
 * FhirIO.Write#fhirStoresImport(String, String, String, FhirIO.Import.ContentStructure)} This will
 * return a {@link FhirIO.Write.Result} on which you can call {@link
 * FhirIO.Write.Result#getFailedBodies()} to retrieve a {@link PCollection} of {@link
 * HealthcareIOError} containing the {@link String} that failed to be ingested and the exception.
 *
 * <p>Example
 *
 * <pre>{@code
 * Pipeline pipeline = ...
 *
 * // Tail the FHIR store by retrieving resources based on Pub/Sub notifications.
 * FhirIO.Read.Result readResult = p
 *   .apply("Read FHIR notifications",
 *     PubsubIO.readStrings().fromSubscription(options.getNotificationSubscription()))
 *   .apply(FhirIO.readResources());
 *
 * // happily retrived resources
 * PCollection<String> resources = readResult.getResources();
 * // resource paths that couldn't be retrieved + error context
 * PCollection<HealthcareIOError<String>> failedReads = readResult.getFailedReads();
 *
 * failedReads.apply("Write Resources / Stacktrace for Failed Reads to BigQuery",
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
 *
 * // Export FHIR resources to Google Cloud Storage or BigQuery.
 * String fhirStoreName = ...;
 * String exportUri = ...; // "gs://..." or "bq://..."
 * PCollection<String> resources =
 *     pipeline.apply(FhirIO.exportResources(fhirStoreName, exportUri));
 *
 * // De-identify FHIR resources.
 * String sourceFhirStoreName = ...;
 * String destinationFhirStoreName = ...;
 * DeidentifyConfig deidConfig = new DeidentifyConfig(); // use default DeidentifyConfig
 * pipeline.apply(FhirIO.deidentify(fhirStoreName, destinationFhirStoreName, deidConfig));
 *
 * // Search FHIR resources using an "OR" query.
 * Map<String, String> queries = new HashMap<>();
 * queries.put("name", "Alice,Bob");
 * FhirSearchParameter<String> searchParameter = FhirSearchParameter.of("Patient", queries);
 * PCollection<FhirSearchParameter<String>> searchQueries =
 * pipeline.apply(
 *      Create.of(searchParameter)
 *            .withCoder(FhirSearchParameterCoder.of(StringUtf8Coder.of())));
 * FhirIO.Search.Result searchResult =
 *      searchQueries.apply(FhirIO.searchResources(options.getFhirStore()));
 * PCollection<JsonArray> resources = searchResult.getResources(); // JsonArray of results
 *
 * // Search FHIR resources using an "AND" query with a key.
 * Map<String, List<String>> listQueries = new HashMap<>();
 * listQueries.put("name", Arrays.asList("Alice", "Bob"));
 * FhirSearchParameter<List<String>> listSearchParameter =
 *      FhirSearchParameter.of("Patient", "Alice-Bob-Search", listQueries);
 * PCollection<FhirSearchParameter<List<String>>> listSearchQueries =
 * pipeline.apply(
 *      Create.of(listSearchParameter)
 *            .withCoder(FhirSearchParameterCoder.of(ListCoder.of(StringUtf8Coder.of()))));
 * FhirIO.Search.Result listSearchResult =
 *      searchQueries.apply(FhirIO.searchResources(options.getFhirStore()));
 * PCollection<KV<String, JsonArray>> listResource =
 *      listSearchResult.getKeyedResources(); // KV<"Alice-Bob-Search", JsonArray of results>
 *
 * }</pre>
 *
 * <h3>Updates to the I/O connector code</h3>
 *
 * For any significant updates to this I/O connector, please consider involving corresponding code
 * reviewers mentioned <a
 * href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/OWNERS">
 * here</a>.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FhirIO {

  static final String BASE_METRIC_PREFIX = "fhirio/";
  private static final String LRO_COUNTER_KEY = "counter";
  private static final String LRO_SUCCESS_KEY = "success";
  private static final String LRO_FAILURE_KEY = "failure";
  private static final Logger LOG = LoggerFactory.getLogger(FhirIO.class);

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
   * Search resources from a Fhir store with String parameter values.
   *
   * @return the search
   * @see Search
   */
  public static Search<String> searchResources(String fhirStore) {
    return new Search<>(fhirStore);
  }

  /**
   * Search resources from a Fhir store with any type of parameter values.
   *
   * @return the search
   * @see Search
   */
  public static Search<?> searchResourcesWithGenericParameters(String fhirStore) {
    return new Search<>(fhirStore);
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
   * @param exportUri the destination GCS dir or BigQuery dataset, in the format:
   *     gs://YOUR_BUCKET_NAME/path/to/a/dir | bq://PROJECT_ID.BIGQUERY_DATASET_ID
   * @return the export
   * @see Export
   */
  public static Export exportResources(String fhirStore, String exportUri) {
    return new Export(StaticValueProvider.of(fhirStore), StaticValueProvider.of(exportUri));
  }

  /** @see FhirIO#exportResources(String, String) */
  public static Export exportResources(
      ValueProvider<String> fhirStore, ValueProvider<String> exportUri) {
    return new Export(fhirStore, exportUri);
  }

  /**
   * Deidentify FHIR resources. Intended for use on non-empty FHIR stores
   *
   * @param sourceFhirStore the source fhir store, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param destinationFhirStore the destination fhir store to write de-identified resources, in the
   *     format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param deidConfig the DeidentifyConfig
   * @return the deidentify
   * @see Deidentify
   */
  public static Deidentify deidentify(
      String sourceFhirStore, String destinationFhirStore, DeidentifyConfig deidConfig) {
    return new Deidentify(
        StaticValueProvider.of(sourceFhirStore),
        StaticValueProvider.of(destinationFhirStore),
        StaticValueProvider.of(deidConfig));
  }

  /**
   * Deidentify FHIR resources. Intended for use on non-empty FHIR stores
   *
   * @param sourceFhirStore the source fhir store, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param destinationFhirStore the destination fhir store to write de-identified resources, in the
   *     format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param deidConfig the DeidentifyConfig
   * @return the deidentify
   * @see Deidentify
   */
  public static Deidentify deidentify(
      ValueProvider<String> sourceFhirStore,
      ValueProvider<String> destinationFhirStore,
      ValueProvider<DeidentifyConfig> deidConfig) {
    return new Deidentify(sourceFhirStore, destinationFhirStore, deidConfig);
  }

  /**
   * Get the patient compartment for a FHIR Patient using the GetPatientEverything/$everything API.
   *
   * @see <a
   *     href=https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/Patient-everything></a>
   * @return the patient everything
   * @see FhirIOPatientEverything
   */
  public static FhirIOPatientEverything getPatientEverything() {
    return new FhirIOPatientEverything();
  }

  /**
   * Increments success and failure counters for an LRO. To be used after the LRO has completed.
   * This function leverages the fact that the LRO metadata is always of the format: "counter": {
   * "success": "1", "failure": "1" }
   *
   * @param operation LRO operation object.
   * @param operationSuccessCounter the success counter for the operation.
   * @param operationFailureCounter the failure counter for the operation.
   * @param resourceSuccessCounter the success counter for individual resources in the operation.
   * @param resourceFailureCounter the failure counter for individual resources in the operation.
   */
  private static void incrementLroCounters(
      Operation operation,
      Counter operationSuccessCounter,
      Counter operationFailureCounter,
      Counter resourceSuccessCounter,
      Counter resourceFailureCounter) {
    // Update operation counters.
    com.google.api.services.healthcare.v1.model.Status error = operation.getError();
    if (error == null) {
      operationSuccessCounter.inc();
      LOG.debug(String.format("Operation %s finished successfully.", operation.getName()));
    } else {
      operationFailureCounter.inc();
      LOG.error(
          String.format(
              "Operation %s failed with error code: %d and message: %s.",
              operation.getName(), error.getCode(), error.getMessage()));
    }

    // Update resource counters.
    Map<String, Object> opMetadata = operation.getMetadata();
    if (opMetadata.containsKey(LRO_COUNTER_KEY)) {
      try {
        Map<String, String> counters = (Map<String, String>) opMetadata.get(LRO_COUNTER_KEY);
        if (counters.containsKey(LRO_SUCCESS_KEY)) {
          resourceSuccessCounter.inc(Long.parseLong(counters.get(LRO_SUCCESS_KEY)));
        }
        if (counters.containsKey(LRO_FAILURE_KEY)) {
          Long numFailures = Long.parseLong(counters.get(LRO_FAILURE_KEY));
          resourceFailureCounter.inc(numFailures);
          if (numFailures > 0) {
            LOG.error("Operation " + operation.getName() + " had " + numFailures + " failures.");
          }
        }
      } catch (Exception e) {
        LOG.error("failed to increment LRO counters, error message: " + e.getMessage());
      }
    }
  }

  /** The type Read. */
  public static class Read extends PTransform<PCollection<String>, FhirIO.Read.Result> {

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
        if (pct.has(OUT) && pct.has(DEAD_LETTER)) {
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

    /** The tag for the main output of FHIR resources. */
    public static final TupleTag<String> OUT = new TupleTag<String>() {};
    /** The tag for the deadletter output of FHIR resources. */
    public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
        new TupleTag<HealthcareIOError<String>>() {};

    @Override
    public FhirIO.Read.Result expand(PCollection<String> input) {
      return input.apply("Read FHIR Resources", new GetResourceJsonString());
    }

    /**
     * DoFn to fetch a resource from an Google Cloud Healthcare FHIR store based on resource path.
     *
     * <p>This DoFn consumes a {@link PCollection} of notifications {@link String}s from the FHIR
     * store, and fetches the actual {@link String} object based on the path in the notification and
     * will output a {@link PCollectionTuple} which contains the output and dead-letter {@link
     * PCollection}*.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link FhirIO.Read#OUT} - Contains all {@link PCollection} records successfully read
     *       from the Fhir store.
     *   <li>{@link FhirIO.Read#DEAD_LETTER} - Contains all {@link PCollection} of {@link
     *       HealthcareIOError}* of resources which failed to be fetched from the FHIR store, with
     *       error message and stacktrace.
     * </ul>
     */
    static class GetResourceJsonString extends PTransform<PCollection<String>, FhirIO.Read.Result> {

      /** Instantiates a new Get FHIR resource DoFn. */
      public GetResourceJsonString() {}

      @Override
      public FhirIO.Read.Result expand(PCollection<String> resourceIds) {
        return new FhirIO.Read.Result(
            resourceIds.apply(
                ParDo.of(new ReadResourceFn())
                    .withOutputTags(FhirIO.Read.OUT, TupleTagList.of(FhirIO.Read.DEAD_LETTER))));
      }

      /** DoFn for getting resources from the FHIR store with error handling. */
      static class ReadResourceFn extends DoFn<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(ReadResourceFn.class);
        private static final Counter READ_RESOURCE_ERRORS =
            Metrics.counter(ReadResourceFn.class, BASE_METRIC_PREFIX + "read_resource_error_count");
        private static final Counter READ_RESOURCE_SUCCESS =
            Metrics.counter(
                ReadResourceFn.class, BASE_METRIC_PREFIX + "read_resource_success_count");
        private static final Distribution READ_RESOURCE_LATENCY_MS =
            Metrics.distribution(
                ReadResourceFn.class, BASE_METRIC_PREFIX + "read_resource_latency_ms");

        private HealthcareApiClient client;
        private ObjectMapper mapper;

        /** Instantiates a new get FHIR resource fn. */
        ReadResourceFn() {}

        /**
         * Instantiate healthcare client (version v1).
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
            READ_RESOURCE_ERRORS.inc();
            LOG.warn(
                String.format(
                    "Error fetching Fhir resource with ID %s writing to Dead Letter "
                        + "Queue. Cause: %s Stack Trace: %s",
                    resourceId, e.getMessage(), Throwables.getStackTraceAsString(e)));
            context.output(FhirIO.Read.DEAD_LETTER, HealthcareIOError.of(resourceId, e));
          }
        }

        private String fetchResource(HealthcareApiClient client, String resourceName)
            throws IOException, IllegalArgumentException {
          long startTime = Instant.now().toEpochMilli();

          HttpBody resource = client.readFhirResource(resourceName);
          READ_RESOURCE_LATENCY_MS.update(Instant.now().toEpochMilli() - startTime);

          if (resource == null) {
            throw new IOException(String.format("GET request for %s returned null", resourceName));
          }
          READ_RESOURCE_SUCCESS.inc();
          return mapper.writeValueAsString(resource);
        }
      }
    }
  }

  /** The type Write. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, Write.AbstractResult> {

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
       * Import Method bulk imports resources from GCS. This is ideal for initial loads to empty
       * FHIR stores. <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import></a>.
       */
      IMPORT
    }

    public abstract static class AbstractResult implements POutput {
      private Pipeline pipeline;

      public abstract PCollection<String> getSuccessfulBodies();

      public abstract PCollection<HealthcareIOError<String>> getFailedBodies();

      public abstract PCollection<HealthcareIOError<String>> getFailedFiles();

      @Override
      public Pipeline getPipeline() {
        return this.pipeline;
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /** The type Result. */
    public static class Result extends AbstractResult {

      private final Pipeline pipeline;
      private final PCollection<String> successfulBodies;
      private final PCollection<HealthcareIOError<String>> failedBodies;
      private final PCollection<HealthcareIOError<String>> failedFiles;

      /**
       * Creates a {@link FhirIO.Write.Result} in the given {@link Pipeline}.
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
      @Override
      public PCollection<String> getSuccessfulBodies() {
        return this.successfulBodies;
      }

      /**
       * Gets failed bodies with err.
       *
       * @return the failed inserts with err
       */
      @Override
      public PCollection<HealthcareIOError<String>> getFailedBodies() {
        return this.failedBodies;
      }

      /**
       * Gets failed file imports with err.
       *
       * @return the failed GCS uri with err
       */
      @Override
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
            SUCCESSFUL_BODY,
            successfulBodies,
            FAILED_BODY,
            failedBodies,
            Write.FAILED_FILES,
            failedFiles);
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
          successfulBodies =
              (PCollection<String>) pipeline.apply(Create.empty(StringUtf8Coder.of()));
        }
        this.successfulBodies = successfulBodies;
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

    /**
     * Import method for batch writing resources. @see <a
     * href=https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import></a>
     *
     * @param fhirStore the FHIR store
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
     * Execute Bundle Method executes a batch of requests in batch or as a single transaction @see
     * <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
     *
     * @param fhirStore the fhir store
     * @return the write
     */
    public static Write executeBundles(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(StaticValueProvider.of(fhirStore))
          .setWriteMethod(WriteMethod.EXECUTE_BUNDLE)
          .build();
    }

    /**
     * Execute Bundle Method executes a batch of requests in batch or as a single transaction @see
     * <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
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
    public AbstractResult expand(PCollection<String> input) {
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
          return input
              .apply(
                  MapElements.into(TypeDescriptor.of(FhirBundleParameter.class))
                      .via(FhirBundleParameter::of))
              .setCoder(SerializableCoder.of(FhirBundleParameter.class))
              .apply(new ExecuteBundles(this.getFhirStore()));
      }
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
          "FhirIO.Import should only be used on bounded PCollections as it is "
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
                  .withOutputTags(Write.TEMP_FILES, TupleTagList.of(Write.FAILED_BODY)));

      PCollection<HealthcareIOError<String>> failedBodies =
          writeTmpFileResults
              .get(Write.FAILED_BODY)
              .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));

      PCollection<HealthcareIOError<String>> failedFiles =
          writeTmpFileResults
              .get(Write.TEMP_FILES)
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

      return Write.Result.in(input.getPipeline(), failedBodies, failedFiles);
    }

    /**
     * WriteToFilesFn writes the input JSON to files in the provided temporary GCS location.
     * Multiple inputs are written to the same file, and therefore the input JSON is additionally
     * processed into NDJSON. The size of a single file is determined according to the size of a
     * batch's window (large for bounded PCollections).
     */
    static class WriteBatchToFilesFn extends DoFn<String, ResourceId> {

      private final ValueProvider<String> tempGcsPath;

      private ObjectMapper mapper;

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
      }

      @Setup
      public void init() throws IOException {
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
        // Write the file with all elements in this batch to GCS.
        ndJsonChannel.close();
        context.output(resourceId, window.maxTimestamp(), window);
      }
    }

    /** Import batches of new line delimited json files to FHIR Store. */
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
        files.add(file);
        tempDestinations.add(
            tempDir.resolve(file.getFilename(), StandardResolveOptions.RESOLVE_FILE));
        deadLetterDestinations.add(
            FileSystems.matchNewResource(deadLetterGcsPath.get(), true)
                .resolve(file.getFilename(), StandardResolveOptions.RESOLVE_FILE));
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
          incrementLroCounters(
              operation,
              IMPORT_OPERATION_SUCCESS,
              IMPORT_OPERATION_ERRORS,
              RESOURCES_IMPORTED_SUCCESS,
              RESOURCES_IMPORTED_ERRORS);

          // Clean up temp files on GCS as they were successfully imported to FHIR store and no
          // longer needed.
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
      RESOURCE
    }
  }

  /** The type Execute bundles. */
  public static class ExecuteBundles
      extends PTransform<PCollection<FhirBundleParameter>, ExecuteBundlesResult> {

    /** The TupleTag used for bundles that were executed successfully. */
    public static final TupleTag<FhirBundleResponse> SUCCESSFUL_BUNDLES = new TupleTag<>();

    /** The TupleTag used for bundles that failed to be executed for any reason. */
    public static final TupleTag<HealthcareIOError<FhirBundleParameter>> FAILED_BUNDLES =
        new TupleTag<>();

    private final ValueProvider<String> fhirStore;

    /**
     * Instantiates a new Execute bundles.
     *
     * @param fhirStore the fhir store
     */
    public ExecuteBundles(ValueProvider<String> fhirStore) {
      this.fhirStore = fhirStore;
    }

    public ExecuteBundles(String fhirStore) {
      this.fhirStore = StaticValueProvider.of(fhirStore);
    }

    public ValueProvider<String> getFhirStore() {
      return fhirStore;
    }

    @Override
    public ExecuteBundlesResult expand(PCollection<FhirBundleParameter> input) {
      PCollectionTuple bundles =
          input.apply(
              ParDo.of(new ExecuteBundlesFn(this.fhirStore))
                  .withOutputTags(SUCCESSFUL_BUNDLES, TupleTagList.of(FAILED_BUNDLES)));
      bundles.get(SUCCESSFUL_BUNDLES).setCoder(SerializableCoder.of(FhirBundleResponse.class));
      bundles
          .get(FAILED_BUNDLES)
          .setCoder(HealthcareIOErrorCoder.of(SerializableCoder.of(FhirBundleParameter.class)));

      return ExecuteBundlesResult.in(
          input.getPipeline(), bundles.get(SUCCESSFUL_BUNDLES), bundles.get(FAILED_BUNDLES));
    }

    /** The type Write Fhir fn. */
    static class ExecuteBundlesFn extends DoFn<FhirBundleParameter, FhirBundleResponse> {

      private static final Counter EXECUTE_BUNDLE_ERRORS =
          Metrics.counter(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_error_count");
      private static final Counter EXECUTE_BUNDLE_RESOURCE_ERRORS =
          Metrics.counter(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_resource_error_count");
      private static final Counter EXECUTE_BUNDLE_SUCCESS =
          Metrics.counter(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_success_count");
      private static final Counter EXECUTE_BUNDLE_RESOURCE_SUCCESS =
          Metrics.counter(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_resource_success_count");
      private static final Distribution EXECUTE_BUNDLE_LATENCY_MS =
          Metrics.distribution(
              ExecuteBundlesFn.class, BASE_METRIC_PREFIX + "execute_bundle_latency_ms");

      private transient HealthcareApiClient client;
      private final ObjectMapper mapper = new ObjectMapper();
      /** The Fhir store. */
      private final ValueProvider<String> fhirStore;

      private static final String BUNDLE_TYPE_FIELD = "type";
      private static final String BUNDLE_RESPONSE_TYPE_BATCH = "batch-response";
      private static final String BUNDLE_RESPONSE_TYPE_TRANSACTION = "transaction-response";
      private static final String BUNDLE_ENTRY_FIELD = "entry";
      private static final String BUNDLE_ENTRY_RESPONSE_FIELD = "response";
      private static final String BUNDLE_ENTRY_RESPONSE_STATUS_FIELD = "status";

      /**
       * Instantiates a new Write Fhir fn.
       *
       * @param fhirStore the Fhir store
       */
      ExecuteBundlesFn(ValueProvider<String> fhirStore) {
        this.fhirStore = fhirStore;
      }

      /**
       * Initialize healthcare client (version v1).
       *
       * @throws IOException If the Healthcare client cannot be created.
       */
      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      @ProcessElement
      public void executeBundles(ProcessContext context) {
        String bundle = context.element().getBundle();
        try {
          long startTime = Instant.now().toEpochMilli();
          // Validate that data was set to valid JSON.
          mapper.readTree(bundle);
          HttpBody resp = client.executeFhirBundle(fhirStore.get(), bundle);
          EXECUTE_BUNDLE_LATENCY_MS.update(Instant.now().toEpochMilli() - startTime);

          parseResponse(context, resp);
        } catch (IOException | HealthcareHttpException e) {
          EXECUTE_BUNDLE_ERRORS.inc();
          context.output(FAILED_BUNDLES, HealthcareIOError.of(context.element(), e));
        }
      }

      private void parseResponse(ProcessContext context, HttpBody resp)
          throws JsonProcessingException {
        JsonObject bundle = JsonParser.parseString(resp.toString()).getAsJsonObject();
        String bundleType = bundle.getAsJsonPrimitive(BUNDLE_TYPE_FIELD).getAsString();
        JsonArray entries = bundle.getAsJsonArray(BUNDLE_ENTRY_FIELD).getAsJsonArray();
        if (entries == null) {
          return;
        }
        // A BATCH bundle returns a success response even if entries have failures, as entries are
        // executed separately. However, TRANSACTION bundles fail if any entry fails, and this would
        // have thrown an exception on `client.executeFhirBundle`.
        // Therefore, for BATCH bundles we need to parse the error and success counters.
        if (bundleType.equals(BUNDLE_RESPONSE_TYPE_BATCH)) {
          int success = 0;
          int fail = 0;

          for (JsonElement entry : entries) {
            JsonObject response =
                entry.getAsJsonObject().get(BUNDLE_ENTRY_RESPONSE_FIELD).getAsJsonObject();
            if (response == null) {
              continue;
            }
            String status = response.get(BUNDLE_ENTRY_RESPONSE_STATUS_FIELD).getAsString();
            int statusCode = parseBundleStatus(status);
            // 20X's are successes, otherwise failure.
            if (statusCode / 100 == 2) {
              success++;
              context.output(
                  SUCCESSFUL_BUNDLES, FhirBundleResponse.of(context.element(), entry.toString()));
            } else {
              fail++;
              context.output(
                  FAILED_BUNDLES,
                  HealthcareIOError.of(
                      context.element(), HealthcareHttpException.of(statusCode, entry.toString())));
            }
          }
          EXECUTE_BUNDLE_RESOURCE_SUCCESS.inc(success);
          EXECUTE_BUNDLE_RESOURCE_ERRORS.inc(fail);
        } else if (bundleType.equals(BUNDLE_RESPONSE_TYPE_TRANSACTION)) {
          EXECUTE_BUNDLE_RESOURCE_SUCCESS.inc(entries.size());
          context.output(
              SUCCESSFUL_BUNDLES, FhirBundleResponse.of(context.element(), bundle.toString()));
        }
        EXECUTE_BUNDLE_SUCCESS.inc();
        return;
      }

      // parseBundleStatus parses out the status code from a Bundle.entry.response.status string,
      // which are the first 3 digits of the string.
      private int parseBundleStatus(String status) {
        int statusCode = 404;
        try {
          statusCode = Integer.parseInt(status.substring(0, 3));
        } catch (IndexOutOfBoundsException | NumberFormatException ignored) {
        }
        return statusCode;
      }
    }
  }

  /**
   * ExecuteBundlesResult contains both successfully executed bundles and information help debugging
   * failed executions (eg metadata & error msgs).
   */
  public static class ExecuteBundlesResult extends Write.AbstractResult {

    private final Pipeline pipeline;
    private final PCollection<FhirBundleResponse> successfulBundles;
    private final PCollection<HealthcareIOError<FhirBundleParameter>> failedBundles;

    private ExecuteBundlesResult(
        Pipeline pipeline,
        PCollection<FhirBundleResponse> successfulBundles,
        PCollection<HealthcareIOError<FhirBundleParameter>> failedBundles) {
      this.pipeline = pipeline;
      this.successfulBundles = successfulBundles;
      this.failedBundles = failedBundles;
    }

    /**
     * Entry point for the ExecuteBundlesResult, storing the successful and failed bundles and their
     * metadata.
     */
    public static ExecuteBundlesResult in(
        Pipeline pipeline,
        PCollection<FhirBundleResponse> successfulBundles,
        PCollection<HealthcareIOError<FhirBundleParameter>> failedBundles) {
      return new ExecuteBundlesResult(pipeline, successfulBundles, failedBundles);
    }

    @Override
    public PCollection<String> getSuccessfulBodies() {
      return this.successfulBundles
          .apply(
              MapElements.into(TypeDescriptors.strings())
                  .via(bundleResponse -> bundleResponse.getFhirBundleParameter().getBundle()))
          .setCoder(StringUtf8Coder.of());
    }

    /** Gets successful FhirBundleResponse from execute bundles operation. */
    public PCollection<FhirBundleResponse> getSuccessfulBundles() {
      return this.successfulBundles;
    }

    @Override
    public PCollection<HealthcareIOError<String>> getFailedBodies() {
      return this.failedBundles
          .apply(ParDo.of(new GetStringHealthcareIOErrorFn()))
          .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
    }

    /**
     * Gets failed FhirBundleResponse wrapped inside HealthcareIOError. The bundle field could be
     * null.
     */
    public PCollection<HealthcareIOError<FhirBundleParameter>> getFailedBundles() {
      return this.failedBundles;
    }

    @Override
    public PCollection<HealthcareIOError<String>> getFailedFiles() {
      return super.pipeline.apply(Create.empty(HealthcareIOErrorCoder.of(StringUtf8Coder.of())));
    }

    @Override
    public Pipeline getPipeline() {
      return this.pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          ExecuteBundles.SUCCESSFUL_BUNDLES,
          successfulBundles,
          ExecuteBundles.FAILED_BUNDLES,
          failedBundles);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}

    private static class GetStringHealthcareIOErrorFn
        extends DoFn<HealthcareIOError<FhirBundleParameter>, HealthcareIOError<String>> {

      @ProcessElement
      public void process(ProcessContext context) {
        HealthcareIOError<FhirBundleParameter> input = context.element();
        context.output(
            new HealthcareIOError<String>(
                input.getDataResource().getBundle(),
                input.getErrorMessage(),
                input.getStackTrace(),
                input.getObservedTime(),
                input.getStatusCode()));
      }
    }
  }

  /** Export FHIR resources from a FHIR store to new line delimited json files on GCS. */
  /**
   * Export FHIR resources from a FHIR store to new line delimited json files on GCS or BigQuery.
   * Output PCollection contains the URI where the FHIR store was exported to.
   */
  public static class Export extends PTransform<PBegin, PCollection<String>> {

    private final ValueProvider<String> fhirStore;
    private final ValueProvider<String> exportUri;

    public Export(ValueProvider<String> fhirStore, ValueProvider<String> exportUri) {
      this.fhirStore = fhirStore;
      this.exportUri = exportUri;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply(Create.ofProvider(fhirStore, StringUtf8Coder.of()))
          .apply("PerformExportOperations", ParDo.of(new ExportResourcesFn(this.exportUri)));
    }

    /** A function that schedules an export operation and monitors the status. */
    public static class ExportResourcesFn extends DoFn<String, String> {
      private static final String GCS_PREFIX = "gs://";
      private static final String BQ_PREFIX = "bq://";
      private static final Counter EXPORT_OPERATION_SUCCESS =
          Metrics.counter(
              ExportResourcesFn.class, BASE_METRIC_PREFIX + "export_operation_success_count");
      private static final Counter EXPORT_OPERATION_ERRORS =
          Metrics.counter(
              ExportResourcesFn.class, BASE_METRIC_PREFIX + "export_operation_failure_count");
      private static final Counter RESOURCES_EXPORTED_SUCCESS =
          Metrics.counter(
              ExportResourcesFn.class, BASE_METRIC_PREFIX + "resources_exported_success_count");
      private static final Counter RESOURCES_EXPORTED_ERRORS =
          Metrics.counter(
              ExportResourcesFn.class, BASE_METRIC_PREFIX + "resources_exported_failure_count");

      private HealthcareApiClient client;
      private final ValueProvider<String> exportUri;

      public ExportResourcesFn(ValueProvider<String> exportUri) {
        this.exportUri = exportUri;
      }

      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      @ProcessElement
      public void exportResources(ProcessContext context) throws IOException, InterruptedException {
        final String fhirStore = context.element();
        final String exportUri = this.exportUri.get();

        Operation operation;
        if (exportUri.startsWith(GCS_PREFIX)) {
          operation = client.exportFhirResourceToGcs(fhirStore, exportUri);
        } else if (exportUri.startsWith(BQ_PREFIX)) {
          operation = client.exportFhirResourceToBigQuery(fhirStore, exportUri);
        } else {
          throw new RuntimeException(
              String.format(
                  "Export cannot be executed because export URI (%s) is not from GCS or BigQuery.",
                  exportUri));
        }

        operation = client.pollOperation(operation, 15000L);
        incrementLroCounters(
            operation,
            EXPORT_OPERATION_SUCCESS,
            EXPORT_OPERATION_ERRORS,
            RESOURCES_EXPORTED_SUCCESS,
            RESOURCES_EXPORTED_ERRORS);
        if (operation.getError() != null) {
          throw new RuntimeException(
              String.format(
                  "Export operation (%s) failed. Reason: %s",
                  operation.getName(), operation.getError().getMessage()));
        }
        context.output(exportUri);
      }
    }
  }

  /** Deidentify FHIR resources from a FHIR store to a destination FHIR store. */
  public static class Deidentify extends PTransform<PBegin, PCollection<String>> {

    private final ValueProvider<String> sourceFhirStore;
    private final ValueProvider<String> destinationFhirStore;
    private final ValueProvider<DeidentifyConfig> deidConfig;

    public Deidentify(
        ValueProvider<String> sourceFhirStore,
        ValueProvider<String> destinationFhirStore,
        ValueProvider<DeidentifyConfig> deidConfig) {
      this.sourceFhirStore = sourceFhirStore;
      this.destinationFhirStore = destinationFhirStore;
      this.deidConfig = deidConfig;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .getPipeline()
          .apply(Create.ofProvider(sourceFhirStore, StringUtf8Coder.of()))
          .apply(
              "ScheduleDeidentifyFhirStoreOperations",
              ParDo.of(new DeidentifyFn(destinationFhirStore, deidConfig)));
    }

    /** A function that schedules a deidentify operation and monitors the status. */
    public static class DeidentifyFn extends DoFn<String, String> {

      private static final Counter DEIDENTIFY_OPERATION_SUCCESS =
          Metrics.counter(
              DeidentifyFn.class, BASE_METRIC_PREFIX + "deidentify_operation_success_count");
      private static final Counter DEIDENTIFY_OPERATION_ERRORS =
          Metrics.counter(
              DeidentifyFn.class, BASE_METRIC_PREFIX + "deidentify_operation_failure_count");
      private static final Counter RESOURCES_DEIDENTIFIED_SUCCESS =
          Metrics.counter(
              DeidentifyFn.class, BASE_METRIC_PREFIX + "resources_deidentified_success_count");
      private static final Counter RESOURCES_DEIDENTIFIED_ERRORS =
          Metrics.counter(
              DeidentifyFn.class, BASE_METRIC_PREFIX + "resources_deidentified_failure_count");

      private HealthcareApiClient client;
      private final ValueProvider<String> destinationFhirStore;
      private static final Gson gson = new Gson();
      private final String deidConfigJson;

      public DeidentifyFn(
          ValueProvider<String> destinationFhirStore, ValueProvider<DeidentifyConfig> deidConfig) {
        this.destinationFhirStore = destinationFhirStore;
        this.deidConfigJson = gson.toJson(deidConfig.get());
      }

      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      @ProcessElement
      public void deidentify(ProcessContext context) throws IOException, InterruptedException {
        String sourceFhirStore = context.element();
        String destinationFhirStore = this.destinationFhirStore.get();
        DeidentifyConfig deidConfig = gson.fromJson(this.deidConfigJson, DeidentifyConfig.class);
        Operation operation =
            client.deidentifyFhirStore(sourceFhirStore, destinationFhirStore, deidConfig);
        operation = client.pollOperation(operation, 15000L);
        incrementLroCounters(
            operation,
            DEIDENTIFY_OPERATION_SUCCESS,
            DEIDENTIFY_OPERATION_ERRORS,
            RESOURCES_DEIDENTIFIED_SUCCESS,
            RESOURCES_DEIDENTIFIED_ERRORS);
        if (operation.getError() != null) {
          throw new IOException(
              String.format("DeidentifyFhirStore operation (%s) failed.", operation.getName()));
        }
        context.output(destinationFhirStore);
      }
    }
  }

  /** The type Search. */
  public static class Search<T>
      extends PTransform<PCollection<FhirSearchParameter<T>>, FhirIO.Search.Result> {

    private final ValueProvider<String> fhirStore;

    Search(ValueProvider<String> fhirStore) {
      this.fhirStore = fhirStore;
    }

    Search(String fhirStore) {
      this.fhirStore = StaticValueProvider.of(fhirStore);
    }

    public static class Result implements POutput, PInput {

      private final PCollection<KV<String, JsonArray>> keyedResources;
      private final PCollection<JsonArray> resources;

      private final PCollection<HealthcareIOError<String>> failedSearches;
      PCollectionTuple pct;

      /**
       * Create FhirIO.Search.Result form PCollectionTuple with OUT and DEAD_LETTER tags.
       *
       * @param pct the pct
       * @return the search result
       * @throws IllegalArgumentException the illegal argument exception
       */
      static FhirIO.Search.Result of(PCollectionTuple pct) throws IllegalArgumentException {
        if (pct.has(OUT) && pct.has(DEAD_LETTER)) {
          return new FhirIO.Search.Result(pct);
        } else {
          throw new IllegalArgumentException(
              "The PCollection tuple must have the FhirIO.Search.OUT "
                  + "and FhirIO.Search.DEAD_LETTER tuple tags");
        }
      }

      private Result(PCollectionTuple pct) {
        this.pct = pct;
        this.keyedResources =
            pct.get(OUT).setCoder(KvCoder.of(StringUtf8Coder.of(), JsonArrayCoder.of()));
        this.resources =
            this.keyedResources
                .apply(
                    "Extract Values",
                    MapElements.into(TypeDescriptor.of(JsonArray.class)).via(KV::getValue))
                .setCoder(JsonArrayCoder.of());
        this.failedSearches =
            pct.get(DEAD_LETTER).setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()));
      }

      /**
       * Gets failed searches.
       *
       * @return the failed searches
       */
      public PCollection<HealthcareIOError<String>> getFailedSearches() {
        return failedSearches;
      }

      /**
       * Gets resources.
       *
       * @return the resources
       */
      public PCollection<JsonArray> getResources() {
        return resources;
      }

      /**
       * Gets resources with input SearchParameter key.
       *
       * @return the resources with input SearchParameter key.
       */
      public PCollection<KV<String, JsonArray>> getKeyedResources() {
        return keyedResources;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pct.getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(OUT, keyedResources);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /** The tag for the main output of FHIR Resources from a search. */
    public static final TupleTag<KV<String, JsonArray>> OUT =
        new TupleTag<KV<String, JsonArray>>() {};
    /** The tag for the deadletter output of FHIR Resources. */
    public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
        new TupleTag<HealthcareIOError<String>>() {};

    @Override
    public FhirIO.Search.Result expand(PCollection<FhirSearchParameter<T>> input) {
      PCollectionTuple results =
          input.apply(
              ParDo.of(new SearchResourcesFn(this.fhirStore))
                  .withOutputTags(FhirIO.Search.OUT, TupleTagList.of(FhirIO.Search.DEAD_LETTER)));
      return FhirIO.Search.Result.of(results);
    }

    /**
     * DoFn to fetch resources from an Google Cloud Healthcare FHIR store based on search request
     *
     * <p>This DoFn consumes a {@link PCollection} of search requests consisting of resource type
     * and search parameters, and fetches all matching resources based on the search criteria and
     * will output a {@link PCollectionTuple} which contains the output and dead-letter {@link
     * PCollection}*.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link FhirIO.Search#OUT} - Contains all {@link PCollection} records successfully
     *       search from the Fhir store.
     *   <li>{@link FhirIO.Search#DEAD_LETTER} - Contains all {@link PCollection} of {@link
     *       HealthcareIOError}* of failed searches from the Fhir store, with error message and
     *       stacktrace.
     * </ul>
     */
    class SearchResourcesFn extends DoFn<FhirSearchParameter<T>, KV<String, JsonArray>> {

      private final Counter searchResourcesErrorCount =
          Metrics.counter(
              SearchResourcesFn.class, BASE_METRIC_PREFIX + "search_resource_error_count");
      private final Counter searchResourcesSuccessCount =
          Metrics.counter(
              SearchResourcesFn.class, BASE_METRIC_PREFIX + "search_resource_success_count");
      private final Distribution searchResourcesLatencyMs =
          Metrics.distribution(
              SearchResourcesFn.class, BASE_METRIC_PREFIX + "search_resource_latency_ms");
      private final Logger log = LoggerFactory.getLogger(SearchResourcesFn.class);

      private HealthcareApiClient client;
      private final ValueProvider<String> fhirStore;

      /** Instantiates a new FHIR resources search fn. */
      SearchResourcesFn(ValueProvider<String> fhirStore) {
        this.fhirStore = fhirStore;
      }

      /**
       * Instantiate healthcare client (version v1).
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
        FhirSearchParameter<T> fhirSearchParameters = context.element();
        try {
          context.output(
              KV.of(
                  fhirSearchParameters.getKey(),
                  searchResources(
                      fhirSearchParameters.getResourceType(), fhirSearchParameters.getQueries())));
        } catch (IllegalArgumentException | NoSuchElementException e) {
          searchResourcesErrorCount.inc();
          log.warn(
              String.format(
                  "Error search FHIR resources writing to Dead Letter "
                      + "Queue. Cause: %s Stack Trace: %s",
                  e.getMessage(), Throwables.getStackTraceAsString(e)));
          context.output(
              FhirIO.Search.DEAD_LETTER, HealthcareIOError.of(fhirSearchParameters.toString(), e));
        }
      }

      private JsonArray searchResources(String resourceType, @Nullable Map<String, T> parameters)
          throws NoSuchElementException {
        long start = Instant.now().toEpochMilli();

        HashMap<String, Object> parameterObjects = new HashMap<>();
        if (parameters != null) {
          parameters.forEach(parameterObjects::put);
        }
        FhirResourcePagesIterator iter =
            FhirResourcePagesIterator.ofSearch(
                client, fhirStore.toString(), resourceType, parameterObjects);
        JsonArray result = new JsonArray();
        while (iter.hasNext()) {
          result.addAll(iter.next());
        }
        searchResourcesLatencyMs.update(java.time.Instant.now().toEpochMilli() - start);
        searchResourcesSuccessCount.inc();
        return result;
      }
    }
  }
}
