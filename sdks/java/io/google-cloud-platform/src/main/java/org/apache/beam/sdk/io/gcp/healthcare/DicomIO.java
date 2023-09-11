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
import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DicomIO connectors allows Beam pipelines to make calls to the Dicom API of the Google Cloud
 * Healthcare API (https://cloud.google.com/healthcare/docs/how-tos#dicom-guide).
 *
 * <h3>Reading Study-Level Metadata</h3>
 *
 * The study-level metadata for a dicom instance can be read with {@link ReadStudyMetadata}.
 * Retrieve the metadata of a dicom instance given its store path as a string. This will return a
 * {@link ReadStudyMetadata.Result}. You can fetch the successful calls using getReadResponse(), and
 * any failed reads using getFailedReads().
 *
 * <h3>Example</h3>
 *
 * {@code Pipeline p = ... String webPath = ... DicomIO.ReadStudyMetadata.Result readMetadataResult
 * = p .apply(Create.of(webPath)) PCollection<String> goodRead =
 * readMetadataResult.getReadResponse() PCollection<String> failRead =
 * readMetadataResult.getFailedReads() }
 */

public class DicomIO {

  static final String BASE_METRIC_PREFIX = "dicomio/";
  private static final String LRO_COUNTER_KEY = "counter";
  private static final String LRO_SUCCESS_KEY = "success";
  private static final String LRO_FAILURE_KEY = "failure";
  private static final Logger LOG = LoggerFactory.getLogger(DicomIO.class);

  public static Deidentify deidentify(
      String sourceDicomStore, String destinationDicomStore, DeidentifyConfig deidConfig) {
    return new Deidentify(
        StaticValueProvider.of(sourceDicomStore),
        StaticValueProvider.of(destinationDicomStore),
        StaticValueProvider.of(deidConfig));
  }

  public static Deidentify deidentify(
      ValueProvider<String> sourceDicomStore,
      ValueProvider<String> destinationDicomStore,
      ValueProvider<DeidentifyConfig> deidConfig) {
    return new Deidentify(sourceDicomStore, destinationDicomStore, deidConfig);
  }

  public static ReadStudyMetadata readStudyMetadata() {
    return new ReadStudyMetadata();
  }

  /**
   * This class makes a call to the retrieve metadata endpoint
   * (https://cloud.google.com/healthcare/docs/how-tos/dicomweb#retrieving_metadata). It defines a
   * function that can be used to process a Pubsub message from a DICOM store, read the DICOM study
   * path and get the metadata of the specified study. You can learn how to configure PubSub
   * messages to be published when an instance is stored by following:
   * https://cloud.google.com/healthcare/docs/how-tos/pubsub. The connector will output a {@link
   * ReadStudyMetadata.Result} which will contain metadata of the study encoded as a json array.
   */
  public static class ReadStudyMetadata
      extends PTransform<PCollection<String>, ReadStudyMetadata.Result> {

    private ReadStudyMetadata() {}

    /** TupleTag for the main output. */
    public static final TupleTag<String> METADATA = new TupleTag<String>() {};
    /** TupleTag for any error response. */
    public static final TupleTag<String> ERROR_MESSAGE = new TupleTag<String>() {};

    public static class Result implements POutput, PInput {
      private PCollection<String> readResponse;

      private PCollection<String> failedReads;

      /** Contains both the response and error outputs from the transformation. */
      PCollectionTuple pct;

      /**
       * Create DicomIO.ReadStudyMetadata.Result from PCollectionTuple which contains the response
       * (with METADATA and ERROR_MESSAGE tags).
       *
       * @param pct the pct
       * @return the read result
       * @throws IllegalArgumentException the illegal argument exception
       */
      static ReadStudyMetadata.Result of(PCollectionTuple pct) throws IllegalArgumentException {
        if (pct.getAll()
            .keySet()
            .containsAll((Collection<?>) TupleTagList.of(METADATA).and(ERROR_MESSAGE))) {
          return new ReadStudyMetadata.Result(pct);
        } else {
          throw new IllegalArgumentException(
              "The PCollection tuple must have the DicomIO.ReadStudyMetadata.METADATA "
                  + "and DicomIO.ReadStudyMetadata.ERROR_MESSAGE tuple tags");
        }
      }

      private Result(PCollectionTuple pct) {
        this.pct = pct;
        this.readResponse = pct.get(METADATA);
        this.failedReads = pct.get(ERROR_MESSAGE);
      }

      /**
       * Gets failed reads.
       *
       * @return the failed reads
       */
      public PCollection<String> getFailedReads() {
        return failedReads;
      }

      /**
       * Gets resources.
       *
       * @return the resources
       */
      public PCollection<String> getReadResponse() {
        return readResponse;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pct.getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(METADATA, readResponse);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /**
     * DoFn to fetch the metadata of a study from a Dicom store based on it's location and study id.
     */
    @SuppressWarnings({"nullness"})
    static class FetchStudyMetadataFn extends DoFn<String, String> {

      private HealthcareApiClient dicomStore;

      FetchStudyMetadataFn() {}

      /**
       * Instantiate the healthcare client (version v1).
       *
       * @throws IOException
       */
      @Setup
      public void instantiateHealthcareClient() throws IOException {
        if (dicomStore == null) {
          this.dicomStore = new HttpHealthcareApiClient();
        }
      }

      /**
       * Process The Pub/Sub message.
       *
       * @param context The input containing the pub/sub message
       */
      @ProcessElement
      public void processElement(ProcessContext context) {
        String dicomWebPath = context.element();
        try {
          // TODO [https://github.com/apache/beam/issues/20582] Change to non-blocking async calls
          String responseData = dicomStore.retrieveDicomStudyMetadata(dicomWebPath);
          context.output(METADATA, responseData);
        } catch (IOException e) {
          String errorMessage = e.getMessage();
          if (errorMessage != null) {
            context.output(ERROR_MESSAGE, errorMessage);
          }
        }
      }
    }

    @Override
    public ReadStudyMetadata.Result expand(PCollection<String> input) {
      return new Result(
          input.apply(
              ParDo.of(new FetchStudyMetadataFn())
                  .withOutputTags(
                      ReadStudyMetadata.METADATA,
                      TupleTagList.of(ReadStudyMetadata.ERROR_MESSAGE))));
    }
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

  /** Deidentify DICOM resources from a DICOM store to a destination DICOM store. */
  public static class Deidentify extends PTransform<PBegin, PCollection<String>> {

    private final ValueProvider<String> sourceDicomStore;
    private final ValueProvider<String> destinationDicomStore;
    private final ValueProvider<DeidentifyConfig> deidConfig;

    public Deidentify(
        ValueProvider<String> sourceDicomStore,
        ValueProvider<String> destinationDicomStore,
        ValueProvider<DeidentifyConfig> deidConfig) {
      this.sourceDicomStore = sourceDicomStore;
      this.destinationDicomStore = destinationDicomStore;
      this.deidConfig = deidConfig;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .getPipeline()
          .apply(Create.ofProvider(sourceDicomStore, StringUtf8Coder.of()))
          .apply(
              "ScheduleDeidentifyDicomStoreOperations",
              ParDo.of(new DeidentifyFn(destinationDicomStore, deidConfig)));
    }

    public static class LroCounters {
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
    }

    /** A function that schedules a deidentify operation and monitors the status. */
    public static class DeidentifyFn extends DoFn<String, String> {

      private HealthcareApiClient dicomStore;
      private final ValueProvider<String> destinationDicomStore;
      private static final Gson gson = new Gson();
      private final String deidConfigJson;

      public DeidentifyFn(
          ValueProvider<String> destinationDicomStore, ValueProvider<DeidentifyConfig> deidConfig) {
        this.destinationDicomStore = destinationDicomStore;
        this.deidConfigJson = gson.toJson(deidConfig.get());
        Preconditions.checkArgumentNotNull(dicomStore);
      }

      @Setup
      public void instantiateHealthcareClient() throws IOException {
        if (dicomStore == null) {
          this.dicomStore = new HttpHealthcareApiClient();
        }
      }

      @ProcessElement
      public void deidentify(ProcessContext context) throws IOException, InterruptedException {
        String sourceDicomStore = context.element();
        String destinationDicomStore = this.destinationDicomStore.get();
        DeidentifyConfig deidConfig = gson.fromJson(this.deidConfigJson, DeidentifyConfig.class);
        Operation operation =
            dicomStore.deidentifyDicomStore(sourceDicomStore, destinationDicomStore, deidConfig);
        operation = dicomStore.pollOperation(operation, 15000L);
        incrementLroCounters(
            operation,
            LroCounters.DEIDENTIFY_OPERATION_SUCCESS,
            LroCounters.DEIDENTIFY_OPERATION_ERRORS,
            LroCounters.RESOURCES_DEIDENTIFIED_SUCCESS,
            LroCounters.RESOURCES_DEIDENTIFIED_ERRORS);
        if (operation.getError() != null) {
          throw new IOException(
              String.format("DeidentifyDicomStore operation (%s) failed.", operation.getName()));
        }
        context.output(destinationDicomStore);
      }
    }
  }
}
