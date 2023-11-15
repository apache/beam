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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

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
}
