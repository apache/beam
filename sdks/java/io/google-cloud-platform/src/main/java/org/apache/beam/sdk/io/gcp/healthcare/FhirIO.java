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

import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1.model.Operation;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOWrite.ContentStructure;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOWrite.Import;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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
 * <p>FHIR resources can be read with {@link FhirIORead}, which supports use cases where you have
 * a ${@link PCollection} of resource IDs. This is appropriate for reading the Fhir notifications
 * from a Pub/Sub subscription with {@link PubsubIO#readStrings()} or in cases where you have a
 * manually prepared list of resources that you need to process (e.g. in a text file read with {@link
 * org.apache.beam.sdk.io.TextIO}*) .
 *
 * <p>Fetch Resource contents from Fhir Store based on the {@link PCollection} of resource ID
 * strings {@link FhirIORead.Result} where one can call {@link FhirIORead.Result#getResources()} to
 * retrieve a {@link PCollection} containing the successfully fetched {@link String}s and/or {@link
 * FhirIORead.Result#getFailedReads()}* to retrieve a {@link PCollection} of {@link
 * HealthcareIOError}* containing the resource ID that could not be fetched and the exception as a
 * {@link HealthcareIOError}, this can be used to write to the dead letter storage system of your
 * choosing. This error handling is mainly to transparently surface errors where the upstream {@link
 * PCollection}* contains IDs that are not valid or are not reachable due to permissions issues.
 *
 * <h3>Writing</h3>
 *
 * <p>Write Resources can be written to FHIR with two different methods: Import or Execute Bundle.
 *
 * <p>Execute Bundle This is best for use cases where you are writing to a non-empty FHIR store
 * with other clients or otherwise need referential integrity (e.g. A Streaming HL7v2 to FHIR ETL
 * pipeline).
 *
 * <p>Import This is best for use cases where you are populating an empty FHIR store with no other
 * clients. It is faster than the execute bundles method but does not respect referential integrity
 * and the resources are not written transactionally (e.g. a historical backfill on a new FHIR
 * store) This requires each resource to contain a client provided ID. It is important that when
 * using import you give the appropriate permissions to the Google Cloud Healthcare Service Agent.
 *
 * <p>Export This is to export FHIR resources from a FHIR store to Google Cloud Storage. The output
 * resources are in ndjson (newline delimited json) of FHIR resources. It is important that when
 * using export you give the appropriate permissions to the Google Cloud Healthcare Service Agent.
 *
 * <p>Deidentify This is to de-identify FHIR resources from a source FHIR store and write the
 * result to a destination FHIR store. It is important that the destination store must already
 * exist.
 *
 * <p>Search This is to search FHIR resources within a given FHIR store. The inputs are individual
 * FHIR Search queries, represented by the FhirSearchParameter class. The outputs are results of
 * each Search, represented as a Json array of FHIR resources in string form, with pagination
 * handled, and an optional input key.
 *
 * @see <a href=>https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>
 * @see <a href=>https://cloud.google.com/healthcare/docs/how-tos/permissions-healthcare-api-gcp-products#fhir_store_cloud_storage_permissions></a>
 * @see <a href=>https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/import></a>
 * @see <a href=>https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/export></a>
 * @see <a href=>https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/deidentify></a>
 * @see <a href=>https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.fhirStores/search></a>
 * A {@link PCollection} of {@link String} can be ingested into an Fhir store using {@link
 * FhirIOWrite#fhirStoresImport(String, String, String, ContentStructure)} This will
 * return a {@link FhirIOWrite.Result} on which you can call {@link
 * FhirIOWrite.Result#getFailedBodies()} to retrieve a {@link PCollection} of {@link
 * HealthcareIOError} containing the {@link String} that failed to be ingested and the exception.
 * <p>Example
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
 * // resource IDs that couldn't be retrieved + error context
 * PCollection<HealthcareIOError<String>> failedReads = readResult.getFailedReads();
 *
 * failedReads.apply("Write Resource IDs / Stacktrace for Failed Reads to BigQuery",
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
 * // Export FHIR resources to Google Cloud Storage.
 * String fhirStoreName = ...;
 * String exportGcsUriPrefix = ...;
 * PCollection<String> resources =
 *     pipeline.apply(FhirIO.exportResourcesToGcs(fhirStoreName, exportGcsUriPrefix));
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
 * </pre>
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class FhirIO {

  public static final String BASE_METRIC_PREFIX = "fhirio/";

  private static final String LRO_COUNTER_KEY = "counter";
  private static final String LRO_SUCCESS_KEY = "success";
  private static final String LRO_FAILURE_KEY = "failure";
  private static final Logger LOG = LoggerFactory.getLogger(FhirIO.class);

  /**
   * Read resources from a PCollection of resource IDs (e.g. when subscribing the pubsub
   * notifications)
   *
   * @return the read
   * @see FhirIORead
   */
  public static FhirIORead readResources() {
    return new FhirIORead();
  }

  /**
   * Search resources from a Fhir store with String parameter values.
   *
   * @return the search
   * @see FhirIOSearch
   */
  public static FhirIOSearch<String> searchResources(String fhirStore) {
    return new FhirIOSearch<>(fhirStore);
  }

  /**
   * Search resources from a Fhir store with any type of parameter values.
   *
   * @return the search
   * @see FhirIOSearch
   */
  public static FhirIOSearch<?> searchResourcesWithGenericParameters(String fhirStore) {
    return new FhirIOSearch<>(fhirStore);
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

  /**
   * Export resources to GCS. Intended for use on non-empty FHIR stores
   *
   * @param fhirStore the fhir store, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param exportGcsUriPrefix the destination GCS dir, in the format:
   *     gs://YOUR_BUCKET_NAME/path/to/a/dir
   * @return the export
   * @see Export
   */
  public static Export exportResourcesToGcs(String fhirStore, String exportGcsUriPrefix) {
    return new Export(
        StaticValueProvider.of(fhirStore), StaticValueProvider.of(exportGcsUriPrefix));
  }

  /**
   * Export resources to GCS. Intended for use on non-empty FHIR stores
   *
   * @param fhirStore the fhir store, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id/fhirStores/fhir_store_id
   * @param exportGcsUriPrefix the destination GCS dir, in the format:
   *     gs://YOUR_BUCKET_NAME/path/to/a/dir
   * @return the export
   * @see Export
   */
  public static Export exportResourcesToGcs(
      ValueProvider<String> fhirStore, ValueProvider<String> exportGcsUriPrefix) {
    return new Export(fhirStore, exportGcsUriPrefix);
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
  static void incrementLroCounters(
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

  /** Export FHIR resources from a FHIR store to new line delimited json files on GCS. */
  public static class Export extends PTransform<PBegin, PCollection<String>> {

    private final ValueProvider<String> fhirStore;
    private final ValueProvider<String> exportGcsUriPrefix;

    public Export(ValueProvider<String> fhirStore, ValueProvider<String> exportGcsUriPrefix) {
      this.fhirStore = fhirStore;
      this.exportGcsUriPrefix = exportGcsUriPrefix;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply(Create.ofProvider(fhirStore, StringUtf8Coder.of()))
          .apply(
              "ScheduleExportOperations",
              ParDo.of(new ExportResourcesToGcsFn(this.exportGcsUriPrefix)))
          .apply(FileIO.matchAll())
          .apply(FileIO.readMatches())
          .apply("ReadResourcesFromFiles", TextIO.readFiles());
    }

    /** A function that schedules an export operation and monitors the status. */
    public static class ExportResourcesToGcsFn extends DoFn<String, String> {

      private static final Counter EXPORT_OPERATION_SUCCESS =
          Metrics.counter(
              ExportResourcesToGcsFn.class, BASE_METRIC_PREFIX + "export_operation_success_count");
      private static final Counter EXPORT_OPERATION_ERRORS =
          Metrics.counter(
              ExportResourcesToGcsFn.class, BASE_METRIC_PREFIX + "export_operation_failure_count");
      private static final Counter RESOURCES_EXPORTED_SUCCESS =
          Metrics.counter(
              ExportResourcesToGcsFn.class,
              BASE_METRIC_PREFIX + "resources_exported_success_count");
      private static final Counter RESOURCES_EXPORTED_ERRORS =
          Metrics.counter(
              ExportResourcesToGcsFn.class,
              BASE_METRIC_PREFIX + "resources_exported_failure_count");

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
          throws IOException, InterruptedException {
        String fhirStore = context.element();
        String gcsPrefix = this.exportGcsUriPrefix.get();
        Operation operation = client.exportFhirResourceToGcs(fhirStore, gcsPrefix);
        operation = client.pollOperation(operation, 15000L);
        incrementLroCounters(
            operation,
            EXPORT_OPERATION_SUCCESS,
            EXPORT_OPERATION_ERRORS,
            RESOURCES_EXPORTED_SUCCESS,
            RESOURCES_EXPORTED_ERRORS);
        if (operation.getError() != null) {
          throw new RuntimeException(
              String.format("Export operation (%s) failed.", operation.getName()));
        }
        context.output(String.format("%s/*", gcsPrefix.replaceAll("/+$", "")));
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
}
