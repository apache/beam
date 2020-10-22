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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
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
 * The DicomIO connectors allows Beam pipelines to read from the Dicom API from Google Cloud
 * Healthcare.
 *
 * <p>Study-level metadata can be read using {@link ReadDicomStudyMetadata}. It is expecting a
 * PubSub message as input, where the message's body will contain the location of the Study. You can
 * learn how to configure PubSub messages to be published when an instance is stored in a data store
 * by following: https://cloud.google.com/healthcare/docs/how-tos/pubsub. The connector will output
 * a {@link ReadDicomStudyMetadata.Result} which will contain metadata of a study encoded in json
 * string
 */
public class DicomIO {

  /** The type ReadDicomStudyMetadata. */
  public static class ReadDicomStudyMetadata
      extends PTransform<PCollection<PubsubMessage>, DicomIO.ReadDicomStudyMetadata.Result> {

    public ReadDicomStudyMetadata() {}

    public static final TupleTag<String> OUT = new TupleTag<String>() {};
    public static final TupleTag<String> DEAD_LETTER = new TupleTag<String>() {};

    public static class Result implements POutput, PInput {
      private PCollection<String> readResponse;

      private PCollection<String> failedReads;
      /** The Pct. */
      PCollectionTuple pct;

      /**
       * Create DicomIO.ReadDicomStudyMetadata.Result form PCollectionTuple with OUT and DEAD_LETTER
       * tags.
       *
       * @param pct the pct
       * @return the read result
       * @throws IllegalArgumentException the illegal argument exception
       */
      static DicomIO.ReadDicomStudyMetadata.Result of(PCollectionTuple pct)
          throws IllegalArgumentException {
        if (pct.getAll()
            .keySet()
            .containsAll((Collection<?>) TupleTagList.of(OUT).and(DEAD_LETTER))) {
          return new DicomIO.ReadDicomStudyMetadata.Result(pct);
        } else {
          throw new IllegalArgumentException(
              "The PCollection tuple must have the DicomIO.ReadDicomStudyMetadata.OUT "
                  + "and DicomIO.ReadDicomStudyMetadata.DEAD_LETTER tuple tags");
        }
      }

      private Result(PCollectionTuple pct) {
        this.pct = pct;
        this.readResponse = pct.get(OUT);
        this.failedReads = pct.get(DEAD_LETTER);
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
        return ImmutableMap.of(OUT, readResponse);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /**
     * DoFn to fetch the metadata of a study from a Dicom store based on it's location and study id.
     */
    static class FetchStudyMetadataFn extends DoFn<PubsubMessage, String> {

      private HealthcareApiClient dicomStore;

      /**
       * Instantiate the healthcare client.
       *
       * @throws IOException
       */
      @Setup
      public void instantiateHealthcareClient() throws IOException {
        this.dicomStore = new HttpHealthcareApiClient();
      }

      /**
       * Process The Pub/Sub message.
       *
       * @param context The input containing the pub/sub message
       */
      @ProcessElement
      public void processElement(ProcessContext context) {
        PubsubMessage msg = context.element();
        byte[] msgPayload = msg.getPayload();
        try {
          String dicomWebPath = new String(msgPayload, "UTF-8");
          String responseData = dicomStore.retrieveDicomStudyMetadata(dicomWebPath);
          context.output(OUT, responseData);
        } catch (Exception e) {
          context.output(DEAD_LETTER, e.getMessage());
        }
      }
    }

    @Override
    public DicomIO.ReadDicomStudyMetadata.Result expand(PCollection<PubsubMessage> input) {
      return new Result(
          input.apply(
              ParDo.of(new FetchStudyMetadataFn())
                  .withOutputTags(
                      ReadDicomStudyMetadata.OUT,
                      TupleTagList.of(ReadDicomStudyMetadata.DEAD_LETTER))));
    }
  }
}
