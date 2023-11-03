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

import static org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError.FROM_ROW_FN;
import static org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError.SCHEMA_FOR_STRING_RESOURCE_TYPE;
import static org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError.STRING_RESOURCE_TYPE;
import static org.apache.beam.sdk.io.gcp.healthcare.HealthcareIOError.TO_ROW_FN;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1.model.Operation;
import com.google.auto.value.AutoValue;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.healthcare.DicomIO.Deidentify.Result;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Duration DEFAULT_CALL_API_INTERVAL = Duration.standardSeconds(1L);

  static final String BASE_METRIC_PREFIX = "dicomio/";

  public static Deidentify deidentify(
      String sourceDicomStore, String destinationDicomStore, DeidentifyConfig deidentifyConfig) {
    try {
      return new Deidentify(
          DicomDeIdOperationConfig.builder()
              .setSourceDicomStore(sourceDicomStore)
              .setDestinationDicomStore(destinationDicomStore)
              .setDeidentifyConfigJson(OBJECT_MAPPER.writeValueAsString(deidentifyConfig))
              .build());
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("error converting deidentifyConfig to JSON", e);
    }
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
    @SuppressWarnings("SameNameButDifferent")
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

  /** Deidentify DICOM resources from a DICOM store to a destination DICOM store. */
  public static class Deidentify extends PTransform<@NonNull PBegin, @NonNull Result> {
    private static final TupleTag<Operation> OUTPUT = new TupleTag<Operation>() {};

    private static final TupleTag<HealthcareIOError<String>> FAILURE =
        new TupleTag<HealthcareIOError<String>>() {};
    private final DicomDeIdOperationConfig operationConfig;

    private Deidentify(DicomDeIdOperationConfig config) {
      this.operationConfig = config;
    }

    public Deidentify withAPICallInterval(Duration interval) {
      return new Deidentify(this.operationConfig.toBuilder().setAPICallInterval(interval).build());
    }

    @Override
    public @NonNull Result expand(PBegin input) {
      PCollectionTuple pct =
          input
              .apply("impulse", Impulse.create())
              .apply(
                  DeidentifyFn.class.getSimpleName(),
                  ParDo.of(new DeidentifyFn(this))
                      .withOutputTags(OUTPUT, TupleTagList.of(FAILURE)));
      return new Result(pct);
    }

    private static class LroCounters {
      private static final Counter DEIDENTIFY_OPERATION_SUCCESS =
          Metrics.counter(
              DeidentifyFn.class, BASE_METRIC_PREFIX + "deidentify_operation_success_count");
      private static final Counter DEIDENTIFY_OPERATION_ERRORS =
          Metrics.counter(
              DeidentifyFn.class, BASE_METRIC_PREFIX + "deidentify_operation_failure_count");
    }

    public static class Result implements POutput {
      private final Pipeline pipeline;
      private final PCollection<Operation> output;
      private final PCollection<HealthcareIOError<String>> error;

      Result(PCollectionTuple pct) {
        this.pipeline = pct.getPipeline();
        this.output = pct.get(OUTPUT).setCoder(OperationCoder.of());
        this.error =
            pct.get(FAILURE)
                .setCoder(HealthcareIOErrorCoder.of(StringUtf8Coder.of()))
                .setSchema(
                    SCHEMA_FOR_STRING_RESOURCE_TYPE, STRING_RESOURCE_TYPE, TO_ROW_FN, FROM_ROW_FN);
      }

      public PCollection<Operation> getOperation() {
        return output;
      }

      public PCollection<HealthcareIOError<String>> getError() {
        return error;
      }

      @Override
      public Pipeline getPipeline() {
        return pipeline;
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(
            OUTPUT, output,
            FAILURE, error);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }
    /** A function that schedules a deidentify operation and monitors the status. */
    private static class DeidentifyFn extends DoFn<byte[], Operation> {


      private transient @MonotonicNonNull HttpHealthcareApiClient client;
      private transient @MonotonicNonNull DeidentifyConfig deidentifyConfig;
      private final Deidentify spec;

      DeidentifyFn(Deidentify spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws IOException {
        this.client = new HttpHealthcareApiClient();
        this.deidentifyConfig =
            OBJECT_MAPPER.readValue(spec.operationConfig.getDeidentifyConfigJson(), DeidentifyConfig.class);
      }

      @ProcessElement
      public void process(MultiOutputReceiver receiver) throws JsonProcessingException {
        HttpHealthcareApiClient safeClient = checkStateNotNull(this.client);
        DeidentifyConfig safeConfig = checkStateNotNull(this.deidentifyConfig);
        try {
          Operation operation =
              safeClient.deidentifyDicomStore(
                  spec.operationConfig.getSourceDicomStore(),
                  spec.operationConfig.getDestinationDicomStore(),
                  safeConfig);
          if (operation == null) {
            throw new NullPointerException(
                "Operation is null after De-identify Dicom Store operation.");
          }
          Operation safeOperation = checkStateNotNull(operation);
          LroCounters.DEIDENTIFY_OPERATION_SUCCESS.inc();
          receiver.get(OUTPUT).output(safeOperation);
        } catch (NullPointerException | IOException e) {
          LroCounters.DEIDENTIFY_OPERATION_ERRORS.inc();
          JsonObject sourceObject = new JsonObject();
          sourceObject.addProperty(
              "source_dicom_store", spec.operationConfig.getSourceDicomStore());
          sourceObject.addProperty(
              "destination_dicom_store", spec.operationConfig.getDestinationDicomStore());
          sourceObject.addProperty(
              "deidentify_config", spec.operationConfig.getDeidentifyConfigJson());
          String source = OBJECT_MAPPER.writeValueAsString(sourceObject);
          receiver.get(FAILURE).output(HealthcareIOError.of(source, e));
        }
      }
    }

    static class OperationCoder extends CustomCoder<Operation> {
      static OperationCoder of() {
        return new OperationCoder();
      }

      private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

      @Override
      public void encode(Operation value, OutputStream outStream)
          throws CoderException, IOException {
        String json = OBJECT_MAPPER.writeValueAsString(value);
        STRING_CODER.encode(json, outStream);
      }

      @Override
      public Operation decode(InputStream inStream) throws CoderException, IOException {
        String json = STRING_CODER.decode(inStream);
        return OBJECT_MAPPER.readValue(json, Operation.class);
      }
    }
  }

  @AutoValue
  public abstract static class DicomDeIdOperationConfig implements Serializable {

    static Builder builder() {
      return new AutoValue_DicomIO_DicomDeIdOperationConfig.Builder();
    }

    abstract Builder toBuilder();

    abstract String getSourceDicomStore();

    abstract String getDestinationDicomStore();

    abstract String getDeidentifyConfigJson();


    abstract Duration getAPICallInterval();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSourceDicomStore(String value);

      abstract Builder setDestinationDicomStore(String value);

      abstract Builder setDeidentifyConfigJson(String value);

      abstract Builder setAPICallInterval(Duration value);

      abstract Optional<Duration> getAPICallInterval();

      abstract DicomDeIdOperationConfig autoBuild();

      final DicomDeIdOperationConfig build() {
        if (!getAPICallInterval().isPresent()) {
          setAPICallInterval(DEFAULT_CALL_API_INTERVAL);
        }
        return autoBuild();
      }
    }
  }
}
